//! Reconcile only owned orders and identified funding ledger entries. No time-window fill sums.
use super::{
    exchanges::get_connector,
    journal::{aggregate, Journal, ManagedTrade},
    real::ExchangeKeys,
};
use reqwest::Client;
use std::sync::Arc;
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
    j.project(t, &orders, true).await?;
    j.settle(t.id, events).await?;
    Ok(())
}
