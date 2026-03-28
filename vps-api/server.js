/**
 * server.js – VPS API Server
 * Replaces Cloudflare Worker + D1 with local Express + Better-SQLite3.
 */

import express from 'express';
import Database from 'better-sqlite3';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();
app.use(express.json({ limit: '50mb' }));

// ─── DB ──────────────────────────────────────────────────────────────────────
const db = new Database('/home/opc/cex-arb/paper-trader/trades.db');
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// Ensure all tables exist (Source of Truth)
db.exec(`
DROP TABLE IF EXISTS trades;
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
    profile_id TEXT NOT NULL DEFAULT 'paper-default'
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

CREATE TABLE IF NOT EXISTS spread_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair TEXT NOT NULL,
    exchange_a TEXT NOT NULL,
    exchange_b TEXT NOT NULL,
    price_a REAL,
    price_b REAL,
    spread REAL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sh_pair ON spread_history(pair, exchange_a, exchange_b);
CREATE INDEX IF NOT EXISTS idx_sh_ts ON spread_history(timestamp);

CREATE TABLE IF NOT EXISTS server_cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS trading_settings (
    id TEXT PRIMARY KEY,
    is_active INTEGER DEFAULT 0,
    min_entry_spread REAL DEFAULT 0.5,
    max_entry_spread REAL DEFAULT 5.0,
    trade_volume REAL DEFAULT 100,
    take_profit_percent REAL DEFAULT 0.1,
    trailing_stop_percent REAL DEFAULT 0.0,
    max_active_trades INTEGER DEFAULT 5,
    commission_rate REAL DEFAULT 0.45,
    funding_rate_8h REAL DEFAULT 0.01,
    slippage_percent REAL DEFAULT 0.1,
    min_volume_millions INTEGER DEFAULT 10,
    collector_interval_secs INTEGER DEFAULT 5,
    pending_commands TEXT,
    updated_at INTEGER
);
INSERT OR IGNORE INTO trading_settings (id) VALUES ('default');
`);

// ─── CORS ─────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ─── Auth ─────────────────────────────────────────────────────────────────────
const COLLECTOR_TOKEN = process.env.COLLECTOR_TOKEN || 'local_dev';

function requireCollectorToken(req, res) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${COLLECTOR_TOKEN}`) {
        res.status(401).json({ error: 'Unauthorized' });
        return false;
    }
    return true;
}

// ─── Cache Helpers ────────────────────────────────────────────────────────────
const getCache = db.prepare(`SELECT value, expires_at FROM server_cache WHERE key = ?`);
const setCache = db.prepare(`INSERT INTO server_cache (key,value,expires_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,expires_at=excluded.expires_at`);

function cacheGet(key) {
    const row = getCache.get(key);
    if (row && row.expires_at > Date.now()) return row;
    return null;
}
function cacheSet(key, value, ttlMs) {
    setCache.run(key, JSON.stringify(value), Date.now() + ttlMs);
}

// ─── Stats Helper ─────────────────────────────────────────────────────────────
function getFullStats() {
    try {
        const basic = db.prepare(`SELECT SUM(pnl) as total_pnl, COUNT(id) as total_trades, AVG(pnl) as avg_pnl, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses FROM trades WHERE status='CLOSED'`).get() || {};
        return { ...basic };
    } catch { return {}; }
}

// ─── Routes ──────────────────────────────────────────────────────────────────

// POST /api/update  – collector pushes data
app.post('/api/update', (req, res) => {
    if (!requireCollectorToken(req, res)) return;
    const { spreads = [], maps = {} } = req.body;
    const now = Date.now();
    cacheSet('snapshot', { ts: now, data: spreads }, 180_000);
    const mapSummaries = {};
    for (const [ex, data] of Object.entries(maps)) mapSummaries[ex] = Object.keys(data);
    cacheSet('exchange_maps', { ts: now, data: mapSummaries }, 180_000);
    return res.json({ ok: true });
});

// GET /api/unified – dashboard main endpoint
app.get('/api/unified', (req, res) => {
    const snapRow = cacheGet('snapshot');
    let snapshot = { ts: Date.now(), data: [] };
    if (snapRow) { try { snapshot = JSON.parse(snapRow.value); } catch {} }

    const active = db.prepare(`SELECT *, last_price_a as current_price_a, last_price_b as current_price_b, last_spread as current_spread, curr_gross_pnl as current_pnl FROM trades WHERE status='OPEN' ORDER BY entry_time DESC`).all();
    const history = db.prepare(`SELECT * FROM trades WHERE status='CLOSED' ORDER BY exit_time DESC LIMIT 100`).all();
    const stats = getFullStats();
    
    const settings = db.prepare(`SELECT * FROM trading_settings WHERE id='default'`).get() || {};
    res.json({ snapshot, trades: { active, history, stats }, settings });
});

// POST /api/trading/active – paper-trader pushes active state
app.post('/api/trading/active', (req, res) => {
    if (!requireCollectorToken(req, res)) return;
    cacheSet('active_trades', { ts: Date.now(), data: req.body }, 60_000);
    res.json({ success: true });
});

// GET /api/trading/settings/all – paper-trader fetches all settings
app.get('/api/trading/settings/all', (req, res) => {
    const s = db.prepare(`SELECT * FROM trading_settings`).all();
    res.json(s);
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = 4000;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`CEX Bridge API running on http://0.0.0.0:${PORT}`);
});
