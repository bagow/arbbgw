ALTER TABLE trading_settings ADD COLUMN slippage_percent REAL DEFAULT 1.0;
UPDATE trading_settings SET slippage_percent = 1.0;
