use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use tokio_rusqlite::Connection;
use crate::engine::real::ExchangeKeys;

/// Asynchronous background worker that queries exchange private trade/income histories
/// and updates trades in real-trader.db with 100% exact exchange verified numbers
/// (real commissions paid, real exit prices, real realized PnL, real funding costs).
///
/// Runs completely detached in the background — never slows down or blocks active fast-path trading.
pub fn start_trade_auditor(
    http: Client,
    keys: Arc<ExchangeKeys>,
    db: Connection,
) {
    tokio::spawn(async move {
        // Wait 8 seconds after startup before initiating background audits
        tokio::time::sleep(Duration::from_secs(8)).await;

        let mut audited_trades = std::collections::HashSet::<i64>::new();

        loop {
            tokio::time::sleep(Duration::from_secs(12)).await;

            let now = crate::engine::utils::now_ms();
            let min_exit_time = now - 48 * 3600 * 1000;

            let trades_to_audit: Vec<(i64, String, String, String, String, i64, i64, f64, f64, Option<f64>, Option<f64>, f64, f64)> = db.call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, pair, exchange_a, exchange_b, direction, entry_time, exit_time, \
                     entry_price_a, entry_price_b, exit_price_a, exit_price_b, \
                     COALESCE(commission_cost, 0.0), COALESCE(funding_cost, 0.0) \
                     FROM trades \
                     WHERE status = 'CLOSED' AND exit_time > ? \
                     ORDER BY id DESC LIMIT 50"
                )?;
                let rows = stmt.query_map([min_exit_time], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                    ))
                })?.filter_map(|r| r.ok()).collect();
                Ok(rows)
            }).await.unwrap_or_default();

            for (id, pair, exa, exb, _dir, entry_time, exit_time, _ent_a, _ent_b, ext_a, ext_b, comm, fund) in trades_to_audit {
                if audited_trades.contains(&id) {
                    continue;
                }

                let exit_ts = if exit_time > 0 { exit_time } else { now };
                let is_complete = comm > 0.0 && ext_a.is_some() && ext_b.is_some();
                if is_complete && (now - exit_ts > 120_000) {
                    audited_trades.insert(id);
                    continue;
                }

                let conn_a = crate::engine::exchanges::get_connector(&exa);
                let conn_b = crate::engine::exchanges::get_connector(&exb);

                if conn_a.is_none() || conn_b.is_none() {
                    audited_trades.insert(id);
                    continue;
                }

                let conn_a = conn_a.unwrap();
                let conn_b = conn_b.unwrap();

                // Window around trade execution: -15s before entry to +15s after exit
                let start_ts = entry_time.saturating_sub(15_000);
                let end_ts = exit_ts.saturating_add(15_000);

                let http_a = http.clone();
                let keys_a = keys.clone();
                let pair_a = pair.clone();

                let http_b = http.clone();
                let keys_b = keys.clone();
                let pair_b = pair.clone();

                let (res_a, res_b) = tokio::join!(
                    conn_a.audit_trade(&http_a, &keys_a, &pair_a, start_ts, end_ts),
                    conn_b.audit_trade(&http_b, &keys_b, &pair_b, start_ts, end_ts)
                );

                let mut real_fee = 0.0;
                let mut audit_ext_a = ext_a;
                let mut audit_ext_b = ext_b;
                let mut audit_pnl_a = None;
                let mut audit_pnl_b = None;
                let mut audit_funding = fund;

                if let Ok(a) = res_a {
                    if a.total_fee > 0.0 { real_fee += a.total_fee; }
                    if a.exit_price.is_some() && audit_ext_a.is_none() { audit_ext_a = a.exit_price; }
                    if let Some(rp) = a.realized_pnl { audit_pnl_a = Some(rp); }
                    if let Some(fc) = a.funding_cost { audit_funding += fc; }
                }

                if let Ok(b) = res_b {
                    if b.total_fee > 0.0 { real_fee += b.total_fee; }
                    if b.exit_price.is_some() && audit_ext_b.is_none() { audit_ext_b = b.exit_price; }
                    if let Some(rp) = b.realized_pnl { audit_pnl_b = Some(rp); }
                    if let Some(fc) = b.funding_cost { audit_funding += fc; }
                }

                if real_fee > 0.0 || audit_ext_a.is_some() || audit_ext_b.is_some() || audit_pnl_a.is_some() || audit_pnl_b.is_some() {
                    let repo = crate::engine::db::TradeRepository::new(db.clone());
                    repo.apply_trade_audit(
                        id,
                        None,
                        None,
                        audit_ext_a,
                        audit_ext_b,
                        if real_fee > 0.0 { real_fee } else { comm },
                        audit_funding,
                        audit_pnl_a,
                        audit_pnl_b,
                        None,
                        None,
                    ).await;
                    tracing::info!(
                        "[auditor] 🧾 Backfilled REAL exchange data for trade #{}: fee=${:.6} (was ${:.6}) ext_a={:?} ext_b={:?}",
                        id, real_fee, comm, audit_ext_a, audit_ext_b
                    );
                }

                audited_trades.insert(id);
                // Pause slightly between audits to respect exchange rate limits
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    });
}
