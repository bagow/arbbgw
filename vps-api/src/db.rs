use anyhow::Result;
use rusqlite::params;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;

use crate::models::{Trade, TradingProfile};

/// Init profiles DB (vps-api.db): trading_profiles + server_cache
pub async fn init_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

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
                updated_at INTEGER,
                disabled_exchanges TEXT DEFAULT '[]'
            );
            ",
        )?;

        // Safe, idempotent schema migration — only adds column if it does not exist.
        // Never touches existing row data.
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN disabled_exchanges TEXT DEFAULT '[]'", []);

        // Restore activity for the shared default profiles.
        // After a DB path migration (e.g. env var change), the new DB file may have
        // is_active=0 for paper-default because saveSettings previously sent is_active=0.
        // The shared default profiles should always be active unless explicitly stopped by the user.
        let _ = conn.execute(
            "UPDATE trading_profiles SET is_active=1 WHERE id='paper-default' AND is_active=0",
            [],
        );

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        // INSERT OR IGNORE — only runs once when the row does not yet exist.
        // Existing rows are NEVER modified here — user settings are preserved.
        conn.execute(
            "INSERT OR IGNORE INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,
                 trade_volume,take_profit_percent,trailing_stop_percent,max_active_trades,
                 commission_rate,slippage_percent,funding_rate_8h,min_volume_millions,
                 collector_interval_secs,created_at,updated_at,disabled_exchanges)
            VALUES ('paper-default','Paper Default','paper',1,1.0,10.0,10.0,0.2,0.3,3,0.05,0.05,0.01,10.0,5,?,?,'[]')",
            params![now, now],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,
                 trade_volume,take_profit_percent,trailing_stop_percent,max_active_trades,
                 commission_rate,slippage_percent,funding_rate_8h,min_volume_millions,
                 collector_interval_secs,created_at,updated_at,disabled_exchanges)
            VALUES ('real-default','Real Trading','real',0,1.0,10.0,100.0,0.2,0.3,3,0.05,0.05,0.01,10.0,5,?,?,'[]')",
            params![now, now],
        )?;
        Ok(())
    }).await?;
    Ok(())
}

/// Init trades-only DB (paper-trader.db or real-trader.db)
pub async fn init_trades_db(db: &Connection) -> Result<()> {
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
                gross_pnl REAL,
                commission_cost REAL,
                funding_cost REAL,
                slippage_cost REAL,
                last_price_a REAL,
                last_price_b REAL,
                last_spread REAL,
                curr_gross_pnl REAL,
                pnl_a REAL,
                pnl_b REAL,
                close_reason TEXT,
                profile_id TEXT DEFAULT 'paper-default',
                order_id_a TEXT,
                order_id_b TEXT,
                contracts_a INTEGER,
                contracts_b INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status   ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_profile  ON trades(profile_id);
            CREATE INDEX IF NOT EXISTS idx_trades_exit     ON trades(exit_time);
            ",
        )?;
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN slippage_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_a TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_b TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_a INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_b INTEGER", []);
        Ok(())
    }).await?;
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
        id: row.get("id")?,
        pair: row.get("pair")?,
        exchange_a: row.get("exchange_a")?,
        exchange_b: row.get("exchange_b")?,
        direction: row.get("direction")?,
        volume: row.get("volume")?,
        entry_price_a: row.get("entry_price_a")?,
        entry_price_b: row.get("entry_price_b")?,
        entry_spread: row.get("entry_spread")?,
        entry_time: row.get("entry_time")?,
        highest_spread_reached: row.get("highest_spread_reached")?,
        status: row.get("status")?,
        exit_price_a: row.get("exit_price_a")?,
        exit_price_b: row.get("exit_price_b")?,
        exit_spread: row.get("exit_spread")?,
        exit_time: row.get("exit_time")?,
        pnl: row.get("pnl")?,
        commission_cost: row.get("commission_cost")?,
        funding_cost: row.get("funding_cost")?,
        last_price_a: row.get("last_price_a")?,
        last_price_b: row.get("last_price_b")?,
        last_spread: row.get("last_spread")?,
        curr_gross_pnl: row.get("curr_gross_pnl")?,
        pnl_a: row.get("pnl_a")?,
        pnl_b: row.get("pnl_b")?,
        close_reason: row.get("close_reason")?,
        profile_id: row.get("profile_id")?,
        contracts_a: row.get("contracts_a").unwrap_or(None),
        contracts_b: row.get("contracts_b").unwrap_or(None),
        slippage_cost: row.get("slippage_cost")?,
        current_price_a: None,
        current_price_b: None,
        current_spread: None,
        current_pnl: None,
        gross_pnl: row.get("gross_pnl")?,
    })
}
 
