//! Passive, bounded book recording. This module has no order transport or exchange keys.
use crate::state::EngineContext;
use shared::SpreadRow;
use tokio::sync::mpsc;
use tokio_rusqlite::{params, Connection};

pub async fn start(path: std::path::PathBuf) -> anyhow::Result<mpsc::Sender<Vec<String>>> {
    let db = Connection::open(path).await?;
    db.call(|c| {
        c.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;
          CREATE TABLE IF NOT EXISTS book_samples(id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);",
        )?;
        Ok(())
    })
    .await?;
    let (tx, mut rx) = mpsc::channel::<Vec<String>>(1);
    tokio::spawn(async move {
        let mut batches = 0u64;
        while let Some(rows) = rx.recv().await {
            batches += 1;
            let prune = batches % 30 == 0;
            if let Err(e) = db.call(move |c| {
                let tx=c.transaction()?;
                for row in rows {tx.execute("INSERT INTO book_samples(snapshot) VALUES(?)",params![row])?;}
                if prune {tx.execute("DELETE FROM book_samples WHERE id < (SELECT MAX(id)-50000 FROM book_samples)",[])?;}
                tx.commit()?; Ok(())
            }).await {tracing::warn!("Shadow recording failed: {e}");}
        }
    });
    Ok(tx)
}
pub fn capture(spreads: &[SpreadRow], ctx: &EngineContext, now: i64) -> Vec<String> {
    spreads.iter().filter(|s| [&s.exchange_a,&s.exchange_b].iter().all(|ex|
        super::exchanges::get_connector(ex).map(|c|c.supports_managed_orders()).unwrap_or(false)))
        .take(20).filter_map(|s| {
            let a=ctx.l2_books.get(&format!("{}:{}",s.exchange_a,s.pair))?.clone();
            let b=ctx.l2_books.get(&format!("{}:{}",s.exchange_b,s.pair))?.clone();
            // Include age explicitly: stale observations are evidence, never executable quotes.
            Some(serde_json::json!({"recorded_ms":now,"build_sha":option_env!("BUILD_SHA"),"spread":s,
                "book_a":{"updated_ms":a.updated_ms,"bids":a.bids,"asks":a.asks},
                "book_b":{"updated_ms":b.updated_ms,"bids":b.bids,"asks":b.asks},
                "fee_a":super::utils::get_exchange_fee(&s.exchange_a,0.001,ctx),
                "fee_b":super::utils::get_exchange_fee(&s.exchange_b,0.001,ctx)
            }).to_string())
        }).collect()
}
