-- CEX Spread History schema
CREATE TABLE IF NOT EXISTS spread_history (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    pair      TEXT    NOT NULL,
    exchange_a TEXT   NOT NULL,
    exchange_b TEXT   NOT NULL,
    price_a   REAL    NOT NULL,
    price_b   REAL    NOT NULL,
    spread    REAL    NOT NULL,
    timestamp INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_pair_ts ON spread_history (pair, timestamp);
