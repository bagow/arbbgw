//! Passive, bounded book recording. This module has no order transport or exchange keys.
use crate::state::EngineContext;
use shared::SpreadRow;
use tokio::sync::mpsc;
use tokio_rusqlite::{params, Connection};

const MAX_SHADOW_WAL_BYTES: u64 = 512 * 1024 * 1024;

fn shadow_wal_requires_checkpoint(wal_bytes: u64) -> bool {
    wal_bytes > MAX_SHADOW_WAL_BYTES
}

pub async fn start(path: std::path::PathBuf) -> anyhow::Result<mpsc::Sender<Vec<String>>> {
    let mut wal_name = path.as_os_str().to_os_string();
    wal_name.push("-wal");
    let wal_path = std::path::PathBuf::from(wal_name);
    let db = Connection::open(path).await?;
    db.call(|c| {
        c.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;
          PRAGMA journal_size_limit=536870912;
          CREATE TABLE IF NOT EXISTS book_samples(id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);",
        )?;
        Ok(())
    })
    .await?;
    let (tx, mut rx) = mpsc::channel::<Vec<String>>(1);
    tokio::spawn(async move {
        let mut batches = 0u64;
        let mut last_wal_warning: Option<std::time::Instant> = None;
        while let Some(rows) = rx.recv().await {
            let wal_bytes = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
            if shadow_wal_requires_checkpoint(wal_bytes) {
                let checkpoint = db
                    .call(|c| {
                        let status = c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        })?;
                        Ok(status)
                    })
                    .await;
                match checkpoint {
                    Ok((0, _, _)) => {
                        tracing::info!(
                            "Shadow WAL checkpoint recovered recorder after {} bytes",
                            wal_bytes
                        );
                        last_wal_warning = None;
                    }
                    result => {
                        if last_wal_warning
                            .is_none_or(|last| last.elapsed() >= std::time::Duration::from_secs(60))
                        {
                            tracing::warn!(
                                "Shadow recording paused: WAL is {} bytes and checkpoint is busy ({:?})",
                                wal_bytes,
                                result
                            );
                            last_wal_warning = Some(std::time::Instant::now());
                        }
                        continue;
                    }
                }
            }
            batches += 1;
            let prune = batches % 30 == 0;
            if let Err(e) = db.call(move |c| {
                let tx=c.transaction()?;
                for row in rows {tx.execute("INSERT INTO book_samples(snapshot) VALUES(?)",params![row])?;}
                // Up to 22 rows / 100 ms: keep more than the 10-minute canary
                // outcome even with every recording slot occupied. Still a
                // bounded, lossy recorder, never an unbounded trading-path queue.
                if prune {tx.execute("DELETE FROM book_samples WHERE id < (SELECT MAX(id)-250000 FROM book_samples)",[])?;}
                tx.commit()?; Ok(())
            }).await {tracing::warn!("Shadow recording failed: {e}");}
        }
    });
    Ok(tx)
}
pub fn capture(
    spreads: &[SpreadRow],
    ctx: &EngineContext,
    now: i64,
    default_fee_rate: f64,
) -> Vec<String> {
    // A cached book can outlive its subscription. Record those rows as evidence,
    // but never let them consume all slots ahead of currently requested routes.
    let requested = |row: &&SpreadRow| {
        ctx.l2_wanted.contains(&format!("{}:{}", row.exchange_a, row.pair))
            || ctx.l2_wanted.contains(&format!("{}:{}", row.exchange_b, row.pair))
    };
    spreads
        .iter()
        .filter(requested)
        .chain(spreads.iter().filter(|row| !requested(row)))
        .filter_map(|row| capture_row(row, ctx, now, default_fee_rate, "discovery", None))
        .take(20)
        .collect()
}

pub fn capture_diagnostics(
    nominations: &[crate::state::DiagnosticL2Nomination],
    ctx: &EngineContext,
    now: i64,
    default_fee_rate: f64,
) -> Vec<String> {
    nominations
        .iter()
        .filter_map(|nomination| {
            capture_row(
                &nomination.row,
                ctx,
                now,
                default_fee_rate,
                nomination.capture_reason,
                Some(nomination.nominated_ms),
            )
        })
        .take(2)
        .collect()
}

fn capture_row(
    row: &SpreadRow,
    ctx: &EngineContext,
    now: i64,
    default_fee_rate: f64,
    capture_reason: &str,
    nomination_ms: Option<i64>,
) -> Option<String> {
    if ![&row.exchange_a, &row.exchange_b].iter().all(|exchange| {
        super::exchanges::get_connector(exchange)
            .map(|connector| connector.supports_managed_orders())
            .unwrap_or(false)
    }) {
        return None;
    }
    let key_a = format!("{}:{}", row.exchange_a, row.pair);
    let key_b = format!("{}:{}", row.exchange_b, row.pair);
    let a = ctx.l2_books.get(&key_a)?.clone();
    let b = ctx.l2_books.get(&key_b)?.clone();
    let spec_json = |key: &str| {
        ctx.specifications.get(key).map(|value| serde_json::json!({
        "fetched_ms":value.fetched_ms,"quantity_step":value.quantity_step,"price_tick":value.price_tick,
        "min_quantity":value.min_quantity,"max_quantity":value.max_quantity,"min_notional":value.min_notional
    }))
    };
    // Demand is the scanner's requested set, not a subscription acknowledgement.
    // A non-requested book can still be streamed by the bounded warm-retention lane.
    // Include age explicitly: stale observations are evidence, never executable quotes.
    Some(serde_json::json!({
        "recorded_ms":now,
        "build_sha":option_env!("BUILD_SHA"),
        "capture_reason":capture_reason,
        "nomination_ms":nomination_ms,
        "spread":row,
        "book_a":{"updated_ms":a.updated_ms,"age_ms":now-a.updated_ms,"bids":a.bids,"asks":a.asks,
            "requested":ctx.l2_wanted.contains(&key_a)},
        "book_b":{"updated_ms":b.updated_ms,"age_ms":now-b.updated_ms,"bids":b.bids,"asks":b.asks,
            "requested":ctx.l2_wanted.contains(&key_b)},
        "spec_a":spec_json(&key_a),"spec_b":spec_json(&key_b),
        "fee_a":super::utils::get_exchange_fee(&row.exchange_a,&row.pair,default_fee_rate,ctx),
        "fee_b":super::utils::get_exchange_fee(&row.exchange_b,&row.pair,default_fee_rate,ctx),
        "fee_a_updated_ms":ctx.fee_updated_ms.get(&key_a).map(|value|*value),
        "fee_b_updated_ms":ctx.fee_updated_ms.get(&key_b).map(|value|*value)
    }).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DiagnosticL2Nomination, L2Snapshot};

    fn row() -> SpreadRow {
        SpreadRow {
            pair: "ZROUSDT".to_string(),
            exchange_a: "kucoin".to_string(),
            exchange_b: "binance".to_string(),
            price_a: 1.0,
            price_b: 1.01,
            bid_a: 0.999,
            ask_a: 1.0,
            bid_b: 1.01,
            ask_b: 1.011,
            bid_size_a_usd: 50.0,
            ask_size_a_usd: 50.0,
            bid_size_b_usd: 50.0,
            ask_size_b_usd: 50.0,
            source_ms_a: 1_000,
            source_ms_b: 1_010,
            vol_a: 1_000_000.0,
            vol_b: 2_000_000.0,
            funding_a: 0.0,
            funding_b: 0.0,
            structural_spread: 0.05,
            structural_max: 1.0,
            structural_min: -1.0,
            spread: 0.8,
            direction: "binance>kucoin".to_string(),
            has_structural_spread: false,
            structural_verified: true,
        }
    }

    #[test]
    fn oversized_shadow_wal_pauses_before_disk_exhaustion() {
        assert!(!shadow_wal_requires_checkpoint(MAX_SHADOW_WAL_BYTES));
        assert!(shadow_wal_requires_checkpoint(MAX_SHADOW_WAL_BYTES + 1));
    }

    #[test]
    fn retained_diagnostic_row_records_books_after_discovery_disappears() {
        let ctx = EngineContext::default();
        for exchange in ["kucoin", "binance"] {
            ctx.l2_books.insert(
                format!("{exchange}:ZROUSDT"),
                L2Snapshot {
                    bids: vec![(1.0, 10.0)],
                    asks: vec![(1.001, 10.0)],
                    updated_ms: 1_005,
                    received_ms: 1_005,
                },
            );
        }
        let records = capture_diagnostics(
            &[DiagnosticL2Nomination {
                row: row(),
                capture_reason: "unstable_l1_nomination",
                nominated_ms: 1_000,
                expires_ms: 16_000,
            }],
            &ctx,
            1_010,
            0.0005,
        );
        assert_eq!(records.len(), 1);
        let record: serde_json::Value = serde_json::from_str(&records[0]).unwrap();
        assert_eq!(record["capture_reason"], "unstable_l1_nomination");
        assert_eq!(record["nomination_ms"], 1_000);
        assert_eq!(record["book_a"]["age_ms"], 5);
        assert_eq!(record["book_b"]["age_ms"], 5);
        assert_eq!(record["book_a"]["requested"], false);
        assert_eq!(record["book_b"]["requested"], false);
    }

    #[test]
    fn requested_route_is_not_crowded_out_by_twenty_cached_routes() {
        let ctx = EngineContext::default();
        let rows: Vec<_> = (0..21).map(|index| {
            let mut row = row();
            row.pair = format!("SHADOW{index}USDT");
            for exchange in ["kucoin", "binance"] {
                ctx.l2_books.insert(format!("{exchange}:{}", row.pair), L2Snapshot {
                    bids: vec![(1.0, 10.0)],
                    asks: vec![(1.001, 10.0)],
                    updated_ms: 1_005,
                    received_ms: 1_005,
                });
            }
            row
        }).collect();
        // Even a partially requested route is important diagnostic evidence.
        ctx.l2_wanted.insert("kucoin:SHADOW20USDT".to_string());
        let records = capture(&rows, &ctx, 1_010, 0.0005);
        assert_eq!(records.len(), 20);
        let record: serde_json::Value = serde_json::from_str(&records[0]).unwrap();
        assert_eq!(record["spread"]["pair"], "SHADOW20USDT");
        assert_eq!(record["book_a"]["requested"], true);
        assert_eq!(record["book_b"]["requested"], false);

        ctx.l2_wanted.insert("binance:SHADOW20USDT".to_string());
        let records = capture(&rows, &ctx, 1_010, 0.0005);
        let record: serde_json::Value = serde_json::from_str(&records[0]).unwrap();
        assert_eq!(record["book_a"]["requested"], true);
        assert_eq!(record["book_b"]["requested"], true);
    }
}
