CREATE TABLE IF NOT EXISTS server_cache (
    key TEXT PRIMARY KEY,
    value TEXT,
    expires_at INTEGER
);
