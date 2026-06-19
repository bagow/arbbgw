use crate::engine::db::TradeRepository;
use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use shared::{SpreadRow, TechnicalSettings, TradingSettings};

pub use crate::engine::utils::cooldown_active;

pub async fn run_cycle(
    signals: Vec<shared::TradeSignal>,
    _spreads: Vec<SpreadRow>,
    prices: crate::state::PriceMap,
    settings: TradingSettings,
    http: Client,
    db: Connection,
    keys: Arc<crate::engine::real::ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    let repo = TradeRepository::new(db.clone());
    let trades = repo.load_open_trades(&settings.id).await;
    let num_active = trades.len() as i64;
    let open_pairs: std::collections::HashSet<String> = trades.iter().map(|t| t.pair.clone()).collect();

    // Ensure open trade pairs have L2 subscriptions for exit VWAP checks
    for t in &trades {
        e_ctx.l2_wanted.insert(format!("{}:{}", t.exchange_a, t.pair));
        e_ctx.l2_wanted.insert(format!("{}:{}", t.exchange_b, t.pair));
    }

    if !trades.is_empty() {
        tracing::debug!("[real] Processing {} open trades for profile {}", trades.len(), settings.id);
    }

    // ── Close qualifying trades ────────────────────────────────────────────────
    let live_updates = crate::engine::exit_manager::process_closes(
        &trades, &prices, &settings, &http, &db, &keys, ts, &e_ctx
    ).await;

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = crate::engine::real::now_ms() as u64;
    let elapsed = current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    
    if elapsed > ts.live_update_throttle_ms as u64 && !live_updates.is_empty() {
        LAST_LIVE_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            let rep = TradeRepository::new(db_clone);
            rep.flush_live_updates(live_updates).await;
        });
    }
    // ── Process pending maker orders (check fills, cancel stale) ─────────────
    if ts.entry_order_type == "maker" {
        crate::engine::maker_manager::process_pending_makers(
            &prices, &settings, &http, &db, &keys, ts, &e_ctx
        ).await;
    }

    // ── Open new real trades ──────────────────────────────────────────────────
    // Count pending makers as quasi-active (they represent committed capital)
    let pending_maker_count = e_ctx.pending_makers.len() as i64;
    let effective_active = num_active + pending_maker_count;
    if !settings.is_active || effective_active >= settings.max_active_trades {
        return;
    }

    crate::engine::entry_manager::process_entries(
        signals, &prices, &settings, &http, &db, &keys, ts, &e_ctx, &open_pairs, effective_active
    ).await;
}
