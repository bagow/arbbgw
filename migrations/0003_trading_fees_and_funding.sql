-- Add commissions and funding fields to trading_settings
ALTER TABLE trading_settings ADD COLUMN commission_rate REAL DEFAULT 0.45;
ALTER TABLE trading_settings ADD COLUMN funding_rate_8h REAL DEFAULT 0.01;

-- Add tracking columns to active trades
ALTER TABLE trades ADD COLUMN commission_cost REAL DEFAULT 0;
ALTER TABLE trades ADD COLUMN funding_cost REAL DEFAULT 0;
