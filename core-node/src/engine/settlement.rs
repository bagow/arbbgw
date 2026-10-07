//! Reconcile only owned orders and identified funding ledger entries. No time-window fill sums.
use super::{
    exchanges::get_connector,
    journal::{aggregate, Journal, ManagedTrade},
    real::ExchangeKeys,
};
use reqwest::Client;
use std::sync::Arc;

const FUNDING_EVENT_TIME_TOLERANCE_MS: i64 = 90_000;
const MIN_EXPECTED_FUNDING_LEDGER_USDT: f64 = 0.000_001;

fn require_capture_funding_evidence(
    t: &ManagedTrade,
    events: &[(String, super::orders::FundingEvent)],
) -> anyhow::Result<()> {
    if !t.plan.signal.funding_capture {
        return Ok(());
    }
    let settlement = t
        .plan
        .funding_settlement_ms
        .ok_or_else(|| anyhow::anyhow!("Funding capture has no durable settlement"))?;
    for (leg, exchange, rate, notional) in [
        (
            "A",
            &t.plan.signal.exchange_a,
            t.plan.funding_rate_a,
            t.plan.signal.actual_vol_a_usd,
        ),
        (
            "B",
            &t.plan.signal.exchange_b,
            t.plan.funding_rate_b,
            t.plan.signal.actual_vol_b_usd,
        ),
    ] {
        let rate = rate.ok_or_else(|| {
            anyhow::anyhow!("Funding capture leg {leg} has no durable expected rate")
        })?;
        if !rate.is_finite() || !notional.is_finite() || notional <= 0.0 {
            anyhow::bail!("Funding capture leg {leg} has invalid expected funding");
        }
        // Exchanges need not publish a ledger row if the rounded amount is
        // genuinely zero. Every economically material leg must be proven.
        if rate.abs() * notional < MIN_EXPECTED_FUNDING_LEDGER_USDT {
            continue;
        }
        let found = events.iter().any(|(event_exchange, event)| {
            event_exchange == exchange
                && event.time_ms.abs_diff(settlement)
                    <= FUNDING_EVENT_TIME_TOLERANCE_MS as u64
        });
        if !found {
            anyhow::bail!(
                "Funding capture ledger for leg {leg} ({exchange}) is not visible yet"
            );
        }
    }
    Ok(())
}

pub fn start(j: Journal, http: Client, keys: Arc<ExchangeKeys>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tick.tick().await;
            match j.accounting_due().await {
                Ok(rows) => {
                    for (t, end) in rows {
                        if let Err(e) = j.accounting_attempt(t.id, None).await {
                            tracing::error!("Accounting DB: {}", e);
                            continue;
                        }
                        if let Err(e) = reconcile(&j, &http, &keys, &t, end).await {
                            tracing::warn!("[accounting] trade={} pending: {}", t.id, e);
                            let _ = j.accounting_attempt(t.id, Some(e.to_string())).await;
                        }
                    }
                }
                Err(e) => tracing::error!("Cannot load accounting queue: {}", e),
            }
        }
    });
}
async fn reconcile(
    j: &Journal,
    http: &Client,
    keys: &ExchangeKeys,
    t: &ManagedTrade,
    end: i64,
) -> anyhow::Result<()> {
    let orders = j.orders(t.id).await?;
    for o in &orders {
        if o.observation.fee.is_none() && o.observation.filled_quantity > 0.0 {
            let c =
                get_connector(&o.exchange).ok_or_else(|| anyhow::anyhow!("Missing connector"))?;
            let observed = c
                .reconcile_managed(http, keys, &o.request, o.observation.order_id.as_deref())
                .await
                .map_err(anyhow::Error::msg)?;
            j.observe(o, observed).await?;
        }
    }
    let orders = j.orders(t.id).await?;
    if orders.iter().any(|o| {
        !o.observation.state.terminal()
            || (o.observation.filled_quantity > 0.0 && o.observation.fee.is_none())
    }) {
        anyhow::bail!("Order fees incomplete");
    }
    if aggregate(&orders, "A").remaining() > 1e-9 || aggregate(&orders, "B").remaining() > 1e-9 {
        anyhow::bail!("Trade still has owned exposure");
    }
    let mut events = Vec::new();
    for (leg, ex) in [
        ("A", &t.plan.signal.exchange_a),
        ("B", &t.plan.signal.exchange_b),
    ] {
        if aggregate(&orders, leg).opened == 0.0 {
            continue;
        }
        let c = get_connector(ex).ok_or_else(|| anyhow::anyhow!("Missing connector"))?;
        for e in c
            .funding_events(http, keys, &t.plan.signal.pair, t.created_ms, end)
            .await
            .map_err(anyhow::Error::msg)?
        {
            events.push((ex.clone(), e));
        }
    }
    require_capture_funding_evidence(t, &events)?;
    j.project(t, &orders, true).await?;
    j.settle(t.id, events).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::journal::Plan;
    use crate::engine::orders::FundingEvent;
    use shared::{TradeSignal, TradingSettings};

    fn capture_trade() -> ManagedTrade {
        ManagedTrade {
            id: 7,
            state: "CLOSED".into(),
            reason: Some("FUNDING_CAPTURE_COMPLETE".into()),
            created_ms: 1_000_000,
            plan: Plan {
                funding_deadline_ms: Some(2_030_000),
                funding_settlement_ms: Some(2_000_000),
                funding_rate_a: Some(0.002),
                funding_rate_b: Some(-0.0002),
                signal: TradeSignal {
                    profile_id: "funding".into(),
                    pair: "TESTUSDT".into(),
                    exchange_a: "binance".into(),
                    exchange_b: "kucoin".into(),
                    direction: "SHORT_A_LONG_B".into(),
                    contracts_a: 1.0,
                    contracts_b: 1.0,
                    target_qty_a: 1.0,
                    target_qty_b: 1.0,
                    limit_price_a: 1.0,
                    limit_price_b: 1.0,
                    entry_a: 1.0,
                    entry_b: 1.0,
                    actual_vol_a_usd: 6.0,
                    actual_vol_b_usd: 6.0,
                    break_even_pct: 0.2,
                    spread: 0.0,
                    structural_spread: 0.0,
                    is_a_higher: true,
                    funding_capture: true,
                    expected_funding_pct: 0.176,
                    tick_a: 0.001,
                    tick_b: 0.001,
                    mult_a: 1.0,
                    mult_b: 1.0,
                },
                settings: TradingSettings::default(),
            },
        }
    }

    fn event(id: &str, time_ms: i64, cost: f64) -> FundingEvent {
        FundingEvent {
            id: id.into(),
            time_ms,
            cost,
        }
    }

    #[test]
    fn capture_waits_for_each_material_funding_leg() {
        let trade = capture_trade();
        let only_a = vec![(
            "binance".into(),
            event("a", 2_000_000, -0.012),
        )];
        assert!(require_capture_funding_evidence(&trade, &only_a).is_err());
        let both = vec![
            ("binance".into(), event("a", 2_000_000, -0.012)),
            ("kucoin".into(), event("b", 2_001_000, 0.0012)),
        ];
        assert!(require_capture_funding_evidence(&trade, &both).is_ok());
    }

    #[test]
    fn capture_rejects_unrelated_funding_event() {
        let trade = capture_trade();
        let events = vec![
            ("binance".into(), event("old-a", 1_500_000, -0.012)),
            ("kucoin".into(), event("b", 2_000_000, 0.0012)),
        ];
        assert!(require_capture_funding_evidence(&trade, &events).is_err());
    }
}
