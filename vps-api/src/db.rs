use anyhow::Result;
use rusqlite::params;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;

use crate::models::{Trade, TradingProfile};

pub async fn init_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                exchange_a TEXT NOT NULL,
                exchange_b TEXT NOT NULL,
                direction TEXT NOT NULL,
                volume REAL NOT NULL,
                entry_price_a REAL NOT NULL,
                entry_price_b REAL NOT NULL,
                entry_spread REAL NOT NULL,
                entry_time INTEGER NOT NULL,
                highest_spread_reached REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price_a REAL,
                exit_price_b REAL,
                exit_spread REAL,
                exit_time INTEGER,
                pnl REAL,
                commission_cost REAL,
                funding_cost REAL,
                last_price_a REAL,
                last_price_b REAL,
                last_spread REAL,
                curr_gross_pnl REAL,
                pnl_a REAL,
                pnl_b REAL,
                close_reason TEXT,
                profile_id TEXT DEFAULT 'paper-default'
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

            CREATE TABLE IF NOT EXISTS server_cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                expires_at INTEGER,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trading_profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'paper',
                is_active INTEGER DEFAULT 0,
                min_entry_spread REAL DEFAULT 1.0,
                max_entry_spread REAL DEFAULT 10.0,
                trade_volume REAL DEFAULT 1000.0,
                take_profit_percent REAL DEFAULT 0.1,
                trailing_stop_percent REAL DEFAULT 0.5,
                max_active_trades INTEGER DEFAULT 5,
                commission_rate REAL DEFAULT 0.45,
                slippage_percent REAL DEFAULT 0.1,
                funding_rate_8h REAL DEFAULT 0.01,
                min_volume_millions REAL DEFAULT 10.0,
                collector_interval_secs INTEGER DEFAULT 5,
                pending_commands TEXT,
                owner TEXT,
                created_at INTEGER,
                updated_at INTEGER
            );
            ",
        )?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        conn.execute(
            "INSERT OR IGNORE INTO trading_profiles (id, name, mode, is_active, min_entry_spread, max_entry_spread,
                trade_volume, take_profit_percent, trailing_stop_percent, max_active_trades,
                commission_rate, slippage_percent, funding_rate_8h, min_volume_millions,
                collector_interval_secs, created_at, updated_at, disabled_exchanges)
            VALUES ('paper-default', 'Paper Default', 'paper', 1, 1.0, 10.0, 1000.0, 0.1, 0.5, 5, 0.45, 0.1, 0.01, 10.0, 5, ?, ?, '[]')",
            params![now, now],
        )?;

        conn.execute(
            "INSERT OR IGNORE INTO trading_profiles (id, name, mode, is_active, min_entry_spread, max_entry_spread,
                trade_volume, take_profit_percent, trailing_stop_percent, max_active_trades,
                commission_rate, slippage_percent, funding_rate_8h, min_volume_millions,
                collector_interval_secs, created_at, updated_at, disabled_exchanges)
            VALUES ('real-default', 'Real Trading', 'real', 0, 1.0, 10.0, 100.0, 0.2, 0.3, 3, 0.05, 0.05, 0.01, 10.0, 5, ?, ?, '[]')",
            params![now, now],
        )?;

        Ok(())
    })
    .await?;

    let _ = db.call(|conn| {
        conn.execute(
            "ALTER TABLE trading_profiles ADD COLUMN disabled_exchanges TEXT DEFAULT '[]'",
            [],
        )
        .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
    })
    .await;

    Ok(())
}

pub fn row_to_profile(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradingProfile> {
    Ok(TradingProfile {
        id: row.get(0)?,
        name: row.get(1)?,
        mode: row.get(2)?,
        is_active: row.get::<_, i64>(3)? != 0,
        min_entry_spread: row.get(4)?,
        max_entry_spread: row.get(5)?,
        trade_volume: row.get(6)?,
        take_profit_percent: row.get(7)?,
        trailing_stop_percent: row.get(8)?,
        max_active_trades: row.get(9)?,
        commission_rate: row.get(10)?,
        slippage_percent: row.get(11)?,
        funding_rate_8h: row.get(12)?,
        min_volume_millions: row.get(13)?,
        collector_interval_secs: row.get(14)?,
        pending_commands: row.get(15)?,
        owner: row.get(16)?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
        disabled_exchanges: row.get::<_, Option<String>>(19).unwrap_or(Some("[]".into())),
    })
}

pub fn row_to_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<Trade> {
    Ok(Trade {
        id: row.get(0)?,
        pair: row.get(1)?,
        exchange_a: row.get(2)?,
        exchange_b: row.get(3)?,
        direction: row.get(4)?,
        volume: row.get(5)?,
        entry_price_a: row.get(6)?,
        entry_price_b: row.get(7)?,
        entry_spread: row.get(8)?,
        entry_time: row.get(9)?,
        highest_spread_reached: row.get(10)?,
        status: row.get(11)?,
        exit_price_a: row.get(12)?,
        exit_price_b: row.get(13)?,
        exit_spread: row.get(14)?,
        exit_time: row.get(15)?,
        pnl: row.get(16)?,
        commission_cost: row.get(17)?,
        funding_cost: row.get(18)?,
        last_price_a: row.get(19)?,
        last_price_b: row.get(20)?,
        last_spread: row.get(21)?,
        curr_gross_pnl: row.get(22)?,
        pnl_a: row.get(23)?,
        pnl_b: row.get(24)?,
        close_reason: row.get(25)?,
        profile_id: row.get(26)?,
        current_price_a: None,
        current_price_b: None,
        current_spread: None,
        current_pnl: None,
        gross_pnl: None,
    })
}
