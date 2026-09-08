//! Fresh account snapshots detect external changes; they never manufacture trade fills.
use super::{
    exchanges::get_connector,
    journal::{aggregate, Journal},
    real::ExchangeKeys,
    utils::{now_ms, symbols_match},
};
use crate::state::{EngineContext, PositionSnapshot};
use reqwest::Client;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};
pub fn base_quantity(exchange: &str, raw: f64, multiplier: f64) -> f64 {
    if matches!(exchange, "gate" | "kucoin") {
        raw * multiplier
    } else {
        raw
    }
}
pub fn observed_remaining(
    ctx: &EngineContext,
    exchange: &str,
    pair: &str,
    multiplier: f64,
    entry_buy: bool,
    not_before_ms: i64,
) -> Option<f64> {
    let snapshot = ctx.managed_positions.get(exchange)?;
    if now_ms() - snapshot.received_ms > 15_000 || snapshot.received_ms < not_before_ms {
        return None;
    }
    let signed = snapshot
        .positions
        .iter()
        .filter(|p| symbols_match(&p.symbol, pair))
        .map(|p| base_quantity(exchange, p.size, multiplier))
        .sum::<f64>();
    Some(if entry_buy {
        signed.max(0.0)
    } else {
        (-signed).max(0.0)
    })
}
pub fn start(j: Journal, http: Client, keys: Arc<ExchangeKeys>, ctx: Arc<EngineContext>) {
    tokio::spawn(async move {
        let mut anomalies: HashMap<String, u32> = HashMap::new();
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            timer.tick().await;
            for ex in ["binance", "gate", "kucoin"] {
                if !keys.supports(ex) {
                    continue;
                }
                let conn = get_connector(ex).expect("Known managed connector");
                let positions = match conn.fetch_positions(&http, &keys).await {
                    Ok(p) => p,
                    Err(_) => {
                        ctx.managed_positions.remove(ex);
                        let _ = j
                            .alert(
                                format!("positions:{ex}"),
                                "Fresh position query failed; snapshot invalidated".into(),
                            )
                            .await;
                        continue;
                    }
                };
                let received_ms = now_ms();
                ctx.managed_positions.insert(
                    ex.into(),
                    PositionSnapshot {
                        received_ms,
                        positions: positions.clone(),
                    },
                );
                let trades = match j.active().await {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let mut current_anomalies = std::collections::HashSet::new();
                let mut seen = std::collections::HashSet::new();
                for t in &trades {
                    let s = &t.plan.signal;
                    if s.exchange_a != ex && s.exchange_b != ex {
                        continue;
                    }
                    seen.insert(s.pair.clone());
                    // Exchange position APIs can briefly lag freshly filled orders.
                    if received_ms - t.created_ms < 10_000
                        || ctx.closing_in_flight.contains_key(&t.id)
                        || t.state != "OPEN"
                    {
                        continue;
                    }
                    let orders = match j.orders(t.id).await {
                        Ok(o) => o,
                        Err(_) => continue,
                    };
                    if orders.iter().any(|o| !o.observation.state.terminal()) {
                        continue;
                    }
                    let a = s.exchange_a == ex;
                    let expected = aggregate(&orders, if a { "A" } else { "B" }).remaining()
                        * if (a && !s.is_a_higher) || (!a && s.is_a_higher) {
                            1.0
                        } else {
                            -1.0
                        };
                    let actual = positions
                        .iter()
                        .filter(|p| symbols_match(&p.symbol, &s.pair))
                        .map(|p| base_quantity(ex, p.size, if a { s.mult_a } else { s.mult_b }))
                        .sum::<f64>();
                    let key = format!("{ex}:{}", s.pair);
                    if (actual - expected).abs() > expected.abs().max(1.0) * 1e-8 {
                        current_anomalies.insert(key.clone());
                        let count = anomalies.entry(key.clone()).or_default();
                        *count += 1;
                        if *count >= 2 {
                            let reason=format!("Trade {} {ex} {}: ledger quantity {expected}, exchange quantity {actual}",t.id,s.pair);
                            let _ = j.alert(key, reason.clone()).await;
                            if !ctx.is_killed.load(Ordering::Acquire) {
                                super::managed::kill(&ctx, &j, reason).await;
                            }
                            let _ = j.state(t.id, "CLOSING", "EXPOSURE_MISMATCH").await;
                        }
                    } else {
                        anomalies.remove(&key);
                    }
                }
                for p in positions.iter().filter(|p| p.size != 0.0) {
                    if seen.iter().any(|pair| symbols_match(pair, &p.symbol)) {
                        continue;
                    }
                    let key = format!("unowned:{ex}:{}", p.symbol);
                    current_anomalies.insert(key.clone());
                    let count = anomalies.entry(key.clone()).or_default();
                    *count += 1;
                    if *count >= 2 {
                        let reason = format!(
                            "Unowned position on {ex}: {}; no automatic adoption or close",
                            p.symbol
                        );
                        let _ = j.alert(key, reason.clone()).await;
                        if !ctx.is_killed.load(Ordering::Acquire) {
                            super::managed::kill(&ctx, &j, reason).await;
                        }
                    }
                }
                anomalies.retain(|key, _| {
                    !(key.starts_with(&format!("{ex}:"))
                        || key.starts_with(&format!("unowned:{ex}:")))
                        || current_anomalies.contains(key)
                });
            }
        }
    });
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contract_positions_and_base_positions_have_different_units() {
        assert_eq!(base_quantity("gate", 3.0, 0.01), 0.03);
        assert_eq!(base_quantity("kucoin", -3.0, 0.01), -0.03);
        assert_eq!(base_quantity("binance", 3.0, 0.01), 3.0);
    }
}
