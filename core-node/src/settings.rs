//! core-node settings module — loads profiles from vps-api.db
//!
//! The canonical TradingSettings struct lives in the `shared` crate.
//! This module only handles SQLite ↔ struct mapping.

pub use shared::TechnicalSettings;
pub use shared::TradingSettings;

use tokio_rusqlite::Connection;

// Explicit SELECT column list — NEVER use SELECT * because column indices
// depend on the physical schema order on VPS (may differ from code expectations
// if columns were added via ALTER TABLE historically).
//
// NOTE: disabled_exchanges is intentionally excluded — it may not exist on older DBs.
// It is fetched separately in parse_row with a safe fallback.
const PROFILE_SELECT: &str = "
    SELECT
        id,
        name,
        mode,
        is_active,
        min_entry_spread,
        max_entry_spread,
        trade_volume,
        take_profit_percent,
        trailing_stop_percent,
        hard_stop_percent,
        trailing_activation_percent,
        max_active_trades,
        daily_loss_limit_usdt,
        max_total_exposure_usdt,
        commission_rate,
        slippage_percent,
        funding_rate_8h,
        min_volume_millions,
        collector_interval_secs,
        pending_commands,
        disabled_exchanges,
        leverage,
        margin_mode,
        min_structural_deviation,
        min_depth_percent,
        strategy_type,
        hft_timeout_ms,
        max_holding_time_ms
    FROM trading_profiles
";

/// Load settings for a given mode ('real') from the profiles DB.
#[allow(dead_code)]
pub async fn load_settings(db: &Connection, mode: &str) -> TradingSettings {
    let mode = mode.to_string();
    db.call(move |conn| {
        let sql = format!(
            "{} WHERE mode=? ORDER BY is_active DESC LIMIT 1",
            PROFILE_SELECT
        );
        let result = conn.query_row(&sql, rusqlite::params![mode], |row| Ok(parse_row(row)));
        Ok(result.unwrap_or_default())
    })
    .await
    .unwrap_or_default()
}

/// Load ALL profiles for a given mode — used to run a trade engine cycle per profile.
pub async fn load_all_settings(db: &Connection, mode: &str) -> Vec<TradingSettings> {
    let mode = mode.to_string();
    db.call(move |conn| {
        let sql = format!("{} WHERE mode=?", PROFILE_SELECT);
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params![mode], |row| Ok(parse_row(row)))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    })
    .await
    .unwrap_or_default()
}

fn get_f64(row: &rusqlite::Row, idx: usize) -> Option<f64> {
    match row.get::<_, f64>(idx) {
        Ok(v) => Some(v),
        Err(_) => row.get::<_, i64>(idx).ok().map(|v| v as f64),
    }
}

fn get_i64(row: &rusqlite::Row, idx: usize) -> Option<i64> {
    match row.get::<_, i64>(idx) {
        Ok(v) => Some(v),
        Err(_) => row.get::<_, f64>(idx).ok().map(|v| v as i64),
    }
}

// Column indices match the PROFILE_SELECT list above (0-based).
fn parse_row(row: &rusqlite::Row) -> TradingSettings {
    // Parse by name because upgraded databases can have a different physical column order.
    // Falls back to empty vec on any parse error.
    let disabled_exchanges: Vec<String> = row
        .get::<_, Option<String>>("disabled_exchanges")
        .ok()
        .flatten()
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .unwrap_or_default();

    TradingSettings {
        id: row.get(0).unwrap_or_else(|_| "default".to_string()),
        // col 1 = name (unused in TradingSettings)
        // col 2 = mode (unused in TradingSettings)
        is_active: get_i64(row, 3).unwrap_or(0) != 0,
        min_entry_spread: get_f64(row, 4).unwrap_or(0.5),
        max_entry_spread: get_f64(row, 5).unwrap_or(10.0),
        trade_volume: get_f64(row, 6).unwrap_or(10.0),
        min_trade_volume: 5.0, // No DB column — use fixed default
        take_profit_percent: get_f64(row, 7).unwrap_or(0.2),
        trailing_stop_percent: get_f64(row, 8).unwrap_or(0.3),
        hard_stop_percent: row.get::<_, Option<f64>>("hard_stop_percent").ok().flatten().unwrap_or(1.0),
        trailing_activation_percent: row.get::<_, Option<f64>>("trailing_activation_percent").ok().flatten().unwrap_or(0.1),
        max_active_trades: row.get("max_active_trades").unwrap_or(3),
        daily_loss_limit_usdt: row.get::<_, Option<f64>>("daily_loss_limit_usdt").ok().flatten().unwrap_or(5.0),
        max_total_exposure_usdt: row.get::<_, Option<f64>>("max_total_exposure_usdt").ok().flatten().unwrap_or(300.0),
        commission_rate: row.get("commission_rate").unwrap_or(0.05),
        slippage_percent: row.get("slippage_percent").unwrap_or(0.05),
        funding_rate_8h: row.get("funding_rate_8h").unwrap_or(0.01),
        min_volume_millions: row.get("min_volume_millions").unwrap_or(10.0),
        collector_interval_secs: row.get("collector_interval_secs").unwrap_or(5),
        pending_commands: row.get("pending_commands").ok().flatten(),
        disabled_exchanges,
        leverage: row.get("leverage").unwrap_or(Some(10)).unwrap_or(10),
        margin_mode: row
            .get("margin_mode")
            .unwrap_or(Some("cross".into()))
            .unwrap_or_else(|| "cross".into()),
        min_structural_deviation: row
            .get("min_structural_deviation")
            .unwrap_or(Some(0.0))
            .unwrap_or(0.0),
        min_depth_percent: row
            .get("min_depth_percent")
            .unwrap_or(Some(100.0))
            .unwrap_or(100.0),
        strategy_type: row
            .get::<_, Option<String>>("strategy_type")
            .ok()
            .flatten()
            .unwrap_or_else(|| "ema".to_string()),
        hft_timeout_ms: row
            .get::<_, Option<i64>>("hft_timeout_ms")
            .ok()
            .flatten()
            .unwrap_or(10_000),
        max_holding_time_ms: row
            .get::<_, Option<i64>>("max_holding_time_ms")
            .ok()
            .flatten()
            .unwrap_or(1_800_000),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum KillSyncAction {
    Activate,
    Reset,
    None,
}

fn kill_sync_action(persisted: Option<&str>, active_in_memory: bool) -> KillSyncAction {
    match (persisted, active_in_memory) {
        (Some("1"), false) => KillSyncAction::Activate,
        (Some("0"), true) => KillSyncAction::Reset,
        _ => KillSyncAction::None,
    }
}

/// Spawn background task refreshing ALL profiles from DB every `interval_secs`.
/// Mirrors the persisted kill switch in both directions so an external pause is
/// fail-closed just like a frontend resume.
pub fn spawn_all_settings_refresher(
    db: Connection,
    mode: &'static str,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<TradingSettings>>>,
    interval_secs: u64,
    e_ctx: Option<std::sync::Arc<crate::state::EngineContext>>,
) {
    tokio::spawn(async move {
        let m = mode.to_string();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            let fresh = load_all_settings(&db, &m).await;
            *settings.write().await = fresh;

            if let Some(ref ctx) = e_ctx {
                let persisted = db
                    .call(|conn| {
                        let value: Result<String, _> = conn.query_row(
                            "SELECT value FROM server_cache WHERE key = 'kill_switch_active'",
                            [],
                            |r| r.get(0),
                        );
                        Ok(value.ok())
                    })
                    .await
                    .ok()
                    .flatten();
                let active = ctx
                    .is_killed
                    .load(std::sync::atomic::Ordering::Acquire);
                match kill_sync_action(persisted.as_deref(), active) {
                    KillSyncAction::Activate => {
                        ctx.is_killed
                            .store(true, std::sync::atomic::Ordering::Release);
                        tracing::warn!(
                            "[settings] Persisted kill switch activated; new entries paused."
                        );
                    }
                    KillSyncAction::Reset => {
                        ctx.is_killed
                            .store(false, std::sync::atomic::Ordering::Release);
                        ctx.consecutive_trims
                            .store(0, std::sync::atomic::Ordering::Release);
                        ctx.consecutive_failures
                            .store(0, std::sync::atomic::Ordering::Release);
                        // Clear all exchange cooldowns so trading resumes immediately
                        ctx.exchange_cooldown.clear();
                        tracing::info!("[settings] ✅ KILL-SWITCH RESET by user via frontend Resume button. Engine resumed.");
                    }
                    KillSyncAction::None => {}
                }
            }
        }
    });
}

/// Load global technical settings from DB.
pub async fn load_technical_settings(db: &Connection) -> TechnicalSettings {
    let rows = db
        .call(|conn| {
            let mut stmt = conn.prepare("SELECT key, value FROM technical_settings")?;
            let rows: Vec<(String, String)> = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(rows)
        })
        .await
        .unwrap_or_default();
    TechnicalSettings::from_db_rows(&rows)
}

/// Spawn background task refreshing technical settings from DB.
pub fn spawn_technical_settings_refresher(
    db: Connection,
    ts: std::sync::Arc<tokio::sync::RwLock<TechnicalSettings>>,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            let fresh = load_technical_settings(&db).await;
            *ts.write().await = fresh;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_kill_switch_syncs_in_both_directions() {
        assert_eq!(kill_sync_action(Some("1"), false), KillSyncAction::Activate);
        assert_eq!(kill_sync_action(Some("0"), true), KillSyncAction::Reset);
        assert_eq!(kill_sync_action(Some("1"), true), KillSyncAction::None);
        assert_eq!(kill_sync_action(Some("0"), false), KillSyncAction::None);
        assert_eq!(kill_sync_action(None, false), KillSyncAction::None);
    }
}
