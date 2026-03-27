-- Migration: Add multi-profile trading support and real-trader endpoints

-- 1. Trading profiles table (one per tab in the UI)
CREATE TABLE IF NOT EXISTS trading_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'paper',   -- 'paper' | 'real'
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
    created_at INTEGER,
    updated_at INTEGER
);

-- 2. Insert default real-trader settings row
INSERT OR IGNORE INTO trading_profiles (id, name, mode, is_active, min_entry_spread, max_entry_spread,
    trade_volume, take_profit_percent, trailing_stop_percent, max_active_trades,
    commission_rate, slippage_percent, funding_rate_8h, min_volume_millions,
    collector_interval_secs, created_at, updated_at)
VALUES ('real-default', 'Real Trading', 'real', 0, 1.0, 10.0, 100.0, 0.2, 0.3, 3, 0.05, 0.05, 0.01, 10.0, 5,
    strftime('%s', 'now') * 1000, strftime('%s', 'now') * 1000);

-- 3. Add profile_id to trades table for isolation
ALTER TABLE trades ADD COLUMN profile_id TEXT DEFAULT 'paper-default';

-- 4. Backfill existing trades as paper-default
UPDATE trades SET profile_id = 'paper-default' WHERE profile_id IS NULL;

-- 5. Real-trader active trades cache key uses profile_id prefix already (via server_cache)
-- No schema change needed for server_cache.
