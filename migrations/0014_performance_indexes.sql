-- 0012_performance_indexes.sql
-- Optimizing history queries and stats aggregation

-- Speed up history list and PnL aggregation by status
CREATE INDEX IF NOT EXISTS idx_trades_status_exit ON trades (status, exit_time DESC);

-- Speed up spread history chart loading for specific pairs and exchanges
CREATE INDEX IF NOT EXISTS idx_spread_history_lookup ON spread_history (pair, exchange_a, exchange_b, timestamp DESC);
