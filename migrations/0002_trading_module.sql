-- Trading module tables

-- Table for user-configured strategy settings
CREATE TABLE IF NOT EXISTS trading_settings (
    id TEXT PRIMARY KEY,
    is_active INTEGER DEFAULT 0,
    min_entry_spread REAL DEFAULT 1.0,
    max_entry_spread REAL DEFAULT 10.0,
    trade_volume REAL DEFAULT 1000.0,
    trailing_stop_percent REAL DEFAULT 0.5,
    max_active_trades INTEGER DEFAULT 5,
    updated_at INTEGER
);

-- Initialize default row if not exists
INSERT OR IGNORE INTO trading_settings (id, is_active, min_entry_spread, max_entry_spread, trade_volume, trailing_stop_percent, max_active_trades, updated_at)
VALUES ('default', 0, 1.0, 10.0, 1000.0, 0.5, 5, 0);

-- Table for active and closed paper trades
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair TEXT NOT NULL,
    exchange_a TEXT NOT NULL,
    exchange_b TEXT NOT NULL,
    direction TEXT NOT NULL, -- e.g. "binance>bybit"
    volume REAL NOT NULL,
    
    entry_price_a REAL NOT NULL,
    entry_price_b REAL NOT NULL,
    entry_spread REAL NOT NULL,
    entry_time INTEGER NOT NULL,
    
    highest_spread_reached REAL NOT NULL,
    
    status TEXT NOT NULL, -- 'OPEN' or 'CLOSED'
    
    exit_price_a REAL,
    exit_price_b REAL,
    exit_spread REAL,
    exit_time INTEGER,
    pnl REAL
);

CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
