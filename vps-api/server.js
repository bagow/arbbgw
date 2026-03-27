/**
 * server.js – VPS API Server
 * Replaces Cloudflare Worker + D1 with local Express + SQLite.
 * Port: 8080 (localhost only; Cloudflare Tunnel handles external HTTPS)
 */

import express from 'express';
import Database from 'better-sqlite3';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();
app.use(express.json({ limit: '50mb' }));

// ─── DB ──────────────────────────────────────────────────────────────────────
const db = new Database('/home/opc/trades.db');
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// Create missing tables (trades table already exists)
db.exec(`
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
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// ─── Auth helpers ─────────────────────────────────────────────────────────────
const COLLECTOR_TOKEN = process.env.COLLECTOR_TOKEN || '';
const PAPER_TRADER_URL = process.env.PAPER_TRADER_URL || 'http://localhost:8081';

function requireCollectorToken(req, res) {
    const auth = req.headers['authorization'] || '';
    if (!COLLECTOR_TOKEN || auth !== `Bearer ${COLLECTOR_TOKEN}`) {
        res.status(401).json({ error: 'Unauthorized' });
        return false;
    }
    return true;
}

// ─── Kline helpers (identical to Worker) ──────────────────────────────────────
function klinesUrl(exchange, symbol, interval, limit, endTimeMs) {
    const ivMap = {
        '1m':  { binance:'1m',bybit:'1',okx:'1m',gate:'1m',bitget:'1m',mexc:'Min1',htx:'1min',bingx:'1m',kucoin:'1min',xt:'1m',coinex:'1min' },
        '3m':  { binance:'3m',bybit:'3',okx:'3m',gate:'3m',bitget:'3m',mexc:'Min3',htx:'3min',bingx:'3m',kucoin:'3min',xt:'3m',coinex:'3min' },
        '5m':  { binance:'5m',bybit:'5',okx:'5m',gate:'5m',bitget:'5m',mexc:'Min5',htx:'5min',bingx:'5m',kucoin:'5min',xt:'5m',coinex:'5min' },
        '15m': { binance:'15m',bybit:'15',okx:'15m',gate:'15m',bitget:'15m',mexc:'Min15',htx:'15min',bingx:'15m',kucoin:'15min',xt:'15m',coinex:'15min' },
    };
    const iv = (ivMap[interval] || ivMap['1m'])[exchange] || interval;
    switch (exchange) {
        case 'binance': return `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${iv}&limit=${limit}${endTimeMs?`&endTime=${endTimeMs}`:''}`;
        case 'bybit':   return `https://api.bybit.com/v5/market/kline?category=linear&symbol=${symbol}&interval=${iv}&limit=${limit}${endTimeMs?`&end=${endTimeMs}`:''}`;
        case 'okx': { const inst = symbol.replace(/^([A-Z0-9]+)(USDT)$/,'$1-USDT-SWAP'); return `https://www.okx.com/api/v5/market/candles?instId=${inst}&bar=${iv}&limit=${Math.min(limit,300)}${endTimeMs?`&after=${endTimeMs}`:''}`;  }
        case 'gate': { const c = symbol.replace('USDT','_USDT'); return `https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=${c}&interval=${iv}&limit=${limit}${endTimeMs?`&to=${Math.floor(endTimeMs/1000)}`:''}`;  }
        case 'bitget':  return `https://api.bitget.com/api/v2/mix/market/candles?symbol=${symbol}&productType=USDT-FUTURES&granularity=${iv}&limit=${Math.min(limit,1000)}${endTimeMs?`&endTime=${endTimeMs}`:''}`;
        case 'mexc': { const c = symbol.replace('USDT','_USDT'); return `https://futures.mexc.com/api/v1/contract/kline/${c}?interval=${iv}&limit=${limit}${endTimeMs?`&end=${endTimeMs}`:''}`;  }
        case 'htx':  { const c = symbol.replace('USDT','-USDT'); return `https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=${c}&period=${iv}&size=${limit}`; }
        case 'bingx': { const s = symbol.replace('USDT','-USDT'); return `https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=${s}&interval=${iv}&limit=${limit}${endTimeMs?`&endTime=${endTimeMs}`:''}`;  }
        case 'kucoin': { const m={1:1,3:3,5:5,15:15}; const ivMin=m[parseInt(interval)+'']||1; const ct=symbol.replace('-','')+'M'; return `https://api-futures.kucoin.com/api/v1/kline/query?symbol=${ct}&granularity=${ivMin}${endTimeMs?`&from=${Math.floor((endTimeMs-1000*60*limit)/1000)}&to=${Math.floor(endTimeMs/1000)}`:''}`;  }
        case 'kraken': return `https://api.kraken.com/0/public/OHLC?pair=${symbol}&interval=${iv.replace('m','')}&count=${limit}`;
        case 'coinbase': { const g={'1m':'ONE_MINUTE','5m':'FIVE_MINUTE','15m':'FIFTEEN_MINUTE'}[interval]||'ONE_MINUTE'; return `https://api.coinbase.com/api/v3/brokerage/products/${symbol.replace('USDT','-USDT')}/candles?granularity=${g}&limit=${limit}${endTimeMs?`&end=${Math.floor(endTimeMs/1000)}`:''}`;  }
        default: return null;
    }
}

function parseKlines(exchange, raw) {
    switch (exchange) {
        case 'binance': return (Array.isArray(raw)?raw:[]).map(c=>({ts:c[0],close:parseFloat(c[4])}));
        case 'bybit':   return (raw?.result?.list??[]).map(c=>({ts:parseInt(c[0]),close:parseFloat(c[4])})).reverse();
        case 'okx':     return (raw?.data??[]).map(c=>({ts:parseInt(c[0]),close:parseFloat(c[4])})).reverse();
        case 'gate':    return (Array.isArray(raw)?raw:[]).map(c=>({ts:parseInt(c.t)*1000,close:parseFloat(c.c)}));
        case 'bitget':  { const list=raw?.data||raw||[]; return Array.isArray(list)?list.map(c=>({ts:parseInt(c[0]),close:parseFloat(c[4])})).reverse():[]; }
        case 'mexc':    { const list=raw?.data||raw||[]; return Array.isArray(list)?list.map(c=>({ts:parseInt(c[0])*1000,close:parseFloat(c[2])})):[]; }
        case 'htx':     return (raw?.data??[]).map(c=>({ts:c.id*1000,close:parseFloat(c.close)})).reverse();
        case 'bingx':   return (raw?.data??[]).map(c=>({ts:c.time??parseInt(c[0]),close:parseFloat(c.close??c[4])}));
        case 'kucoin':  return (raw?.data??[]).map(c=>({ts:parseInt(c[0]),close:parseFloat(c[4])}));
        case 'kraken':  { const arr=Object.values(raw?.result??{})[0]||[]; return arr.map(c=>({ts:c[0]*1000,close:parseFloat(c[4])})); }
        case 'coinbase':return (raw?.candles??[]).map(c=>({ts:parseInt(c.start)*1000,close:parseFloat(c.close)})).reverse();
        default: return [];
    }
}

// ─── DB helpers ───────────────────────────────────────────────────────────────
const getCache = db.prepare(`SELECT value, expires_at FROM server_cache WHERE key = ?`);
const setCache = db.prepare(`INSERT INTO server_cache (key,value,expires_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,expires_at=excluded.expires_at`);

function cacheGet(key) {
    return getCache.get(key);
}
function cacheSet(key, value, ttlMs) {
    setCache.run(key, JSON.stringify(value), Date.now() + ttlMs);
}

// ─── Auto-cleanup: keep only last 4h of spread_history ───────────────────────
const cleanupHistory = db.prepare(`DELETE FROM spread_history WHERE timestamp < ?`);
let lastCleanup = 0;
function maybeCleanup() {
    const now = Date.now();
    if (now - lastCleanup < 60_000) return; // at most once per minute
    lastCleanup = now;
    setImmediate(() => {
        try { cleanupHistory.run(now - 4 * 60 * 60 * 1000); } catch {}
    });
}

// ─── Stats helper (mirror of Worker getFullStats) ─────────────────────────────
function getFullStats() {
    const basic = db.prepare(`SELECT SUM(pnl) as total_pnl, COUNT(id) as total_trades, AVG(pnl) as avg_pnl, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) as losses, AVG(exit_time-entry_time) as avg_duration, SUM(CASE WHEN pnl>0 THEN pnl ELSE 0 END) as gross_profit, SUM(CASE WHEN pnl<0 THEN pnl ELSE 0 END) as gross_loss FROM trades WHERE status='CLOSED'`).get() || {};
    const pnlEx = db.prepare(`SELECT ex,SUM(lp) as p,SUM(lc) as c FROM (SELECT exchange_a as ex,SUM(COALESCE(pnl_a,pnl/2)-(COALESCE(commission_cost,0.4*volume/100)+COALESCE(funding_cost,0))/2) as lp,COUNT(*) as lc FROM trades WHERE status='CLOSED' GROUP BY exchange_a UNION ALL SELECT exchange_b as ex,SUM(COALESCE(pnl_b,pnl/2)-(COALESCE(commission_cost,0.4*volume/100)+COALESCE(funding_cost,0))/2) as lp,COUNT(*) as lc FROM trades WHERE status='CLOSED' GROUP BY exchange_b) GROUP BY ex`).all();
    const byPair = db.prepare(`SELECT pair,SUM(pnl) as pnl,COUNT(*) as count FROM trades WHERE status='CLOSED' GROUP BY pair ORDER BY pnl DESC LIMIT 10`).all();
    const mostTraded = db.prepare(`SELECT pair,COUNT(*) as count FROM trades WHERE status='CLOSED' GROUP BY pair ORDER BY count DESC LIMIT 10`).all();
    const hourly = db.prepare(`SELECT CAST(strftime('%H',datetime(exit_time/1000,'unixepoch')) AS INT) as hour,COUNT(*) as count FROM trades WHERE status='CLOSED' GROUP BY hour ORDER BY hour`).all();
    const curve = db.prepare(`SELECT entry_time,exit_time,pnl FROM trades WHERE status='CLOSED' ORDER BY exit_time ASC`).all();
    const rawDirs = db.prepare(`SELECT direction as dir,exchange_a as exA,SUM(pnl) as p FROM trades WHERE status='CLOSED' GROUP BY direction,exchange_a`).all();
    return {
        ...basic,
        pnl_by_exchange: pnlEx.reduce((a,r)=>{a[r.ex]=r.p;return a},{}),
        count_by_exchange: pnlEx.reduce((a,r)=>{a[r.ex]=r.c;return a},{}),
        pnl_by_pair: byPair,
        most_traded_pairs: mostTraded,
        hourly_distribution: hourly.reduce((a,r)=>{a[r.hour]=r.count;return a},{}),
        equity_curve: curve,
        raw_directions: rawDirs,
    };
}

// ─── Routes ──────────────────────────────────────────────────────────────────

// POST /api/update  – rust-collector pushes spreads
app.post('/api/update', (req, res) => {
    if (!requireCollectorToken(req, res)) return;
    const { spreads = [], maps = {} } = req.body;
    if (!Array.isArray(spreads)) return res.status(400).json({ error: 'spreads must be an array' });

    const now = Date.now();

    // Save snapshot
    cacheSet('snapshot', { ts: now, data: spreads }, 180_000);

    // Save exchange maps
    const mapSummaries = {};
    for (const [ex, data] of Object.entries(maps)) mapSummaries[ex] = Object.keys(data);
    cacheSet('exchange_maps', { ts: now, data: mapSummaries }, 180_000);

    // Batch-insert spread_history (threshold >= -1%)
    const toStore = spreads.filter(r => r.spread >= -1.0);
    if (toStore.length > 0) {
        const insert = db.prepare(`INSERT INTO spread_history (pair,exchange_a,exchange_b,price_a,price_b,spread,timestamp) VALUES (?,?,?,?,?,?,?)`);
        const insertMany = db.transaction((rows) => { for (const r of rows) insert.run(r.pair,r.exchangeA,r.exchangeB,r.priceA,r.priceB,r.spread,now); });
        try { insertMany(toStore.slice(0, 200)); } catch {} // cap at 200/update
    }

    // Auto-cleanup spread_history
    maybeCleanup();

    // Get current collector interval from settings
    const settings = db.prepare(`SELECT collector_interval_secs FROM trading_settings WHERE id='default'`).get() || {};

    return res.json({ ok: true, received: spreads.length, stored: toStore.length, config: { push_interval: settings.collector_interval_secs || 5 } });
});

// GET /api/unified – dashboard main endpoint
app.get('/api/unified', (req, res) => {
    const minSp = parseFloat(req.query.min || '0');
    const maxSp = parseFloat(req.query.max || '50');

    const snapRow = cacheGet('snapshot');
    let snapshot = { ts: Date.now(), data: [] };
    if (snapRow) {
        try {
            const parsed = JSON.parse(snapRow.value);
            snapshot.ts = parsed.ts;
            snapshot.data = (parsed.data || []).filter(r => r.spread >= minSp && r.spread <= maxSp).slice(0, 1000);
        } catch {}
    }

    // Active trades: read directly from DB (ignoring ephemeral cache)
    const active = db.prepare(`
        SELECT 
            *,
            last_price_a as current_price_a,
            last_price_b as current_price_b,
            last_spread as current_spread,
            curr_gross_pnl as current_pnl,
            curr_gross_pnl as gross_pnl
        FROM trades 
        WHERE status='OPEN' 
        ORDER BY entry_time DESC
    `).all();

    // History: direct from DB with a much larger limit
    const history = db.prepare(`SELECT * FROM trades WHERE status='CLOSED' ORDER BY exit_time DESC LIMIT 1000`).all();
    const stats = getFullStats();

    const mapsRow = cacheGet('exchange_maps');
    let maps = { data: {} };
    if (mapsRow) { try { maps = JSON.parse(mapsRow.value); } catch {} }

    // Enrich with commission_cost if missing
    const settings = db.prepare(`SELECT * FROM trading_settings WHERE id='default'`).get() || {};
    const cr = settings.commission_rate ?? 0.45;
    for (const t of history) {
        if (t.commission_cost == null) t.commission_cost = t.volume * (cr / 100) * 4;
    }

    res.json({ snapshot, trades: { active, history, stats }, exchanges: maps, settings });
});

// GET /api/snapshot
app.get('/api/snapshot', (req, res) => {
    const minSp = parseFloat(req.query.min || '0');
    const maxSp = parseFloat(req.query.max || '50');
    const row = cacheGet('snapshot');
    if (!row || row.expires_at <= Date.now()) return res.status(503).json({ error: 'No data yet', data: [] });
    const parsed = JSON.parse(row.value);
    if (parsed.data) parsed.data = parsed.data.filter(r => r.spread >= minSp && r.spread <= maxSp).slice(0, 1000);
    res.json(parsed);
});

// GET /api/klines/:exchange/:symbol/:interval/:limit
app.get('/api/klines/:exchange/:symbol/:interval/:limit', async (req, res) => {
    const { exchange, symbol, interval } = req.params;
    const limit = Math.min(parseInt(req.params.limit) || 500, 1000);
    const endTimeMs = req.query.endTime || null;
    const kUrl = klinesUrl(exchange, symbol, interval, limit, endTimeMs);
    if (!kUrl) return res.status(400).json({ error: `Unknown exchange: ${exchange}`, klines: [] });
    try {
        const r = await fetch(kUrl, { headers: { 'User-Agent': 'cex-spread-scanner/1.0' } });
        const raw = await r.json();
        const klines = parseKlines(exchange, raw).filter(c => c.close > 0);
        res.json({ exchange, symbol, klines });
    } catch (e) {
        res.json({ exchange, symbol, klines: [], error: e.message });
    }
});

// GET /api/history/:pair/:exA/:exB
app.get('/api/history/:pair/:exA/:exB', (req, res) => {
    const { pair, exA, exB } = req.params;
    const since = Date.now() - 24 * 60 * 60 * 1000;
    const results = db.prepare(`SELECT pair,exchange_a,exchange_b,price_a,price_b,spread,timestamp FROM spread_history WHERE pair=? AND exchange_a=? AND exchange_b=? AND timestamp>=? ORDER BY timestamp ASC LIMIT 2000`).all(pair.toUpperCase(), exA.toLowerCase(), exB.toLowerCase(), since);
    res.json({ pair, exchangeA: exA, exchangeB: exB, history: results });
});

// GET /api/trading/settings
app.get('/api/trading/settings', (req, res) => {
    const s = db.prepare(`SELECT * FROM trading_settings WHERE id='default'`).get() || {};
    if (s.is_active !== undefined) s.is_active = Boolean(s.is_active);
    res.json(s);
});

// POST /api/trading/settings
app.post('/api/trading/settings', (req, res) => {
    const { is_active, min_entry_spread, max_entry_spread, trade_volume, take_profit_percent=0.1, trailing_stop_percent=0, max_active_trades, commission_rate=0.45, funding_rate_8h=0.01, slippage_percent=0.1, min_volume_millions=10, collector_interval_secs=5 } = req.body;
    db.prepare(`UPDATE trading_settings SET is_active=?,min_entry_spread=?,max_entry_spread=?,trade_volume=?,take_profit_percent=?,trailing_stop_percent=?,max_active_trades=?,commission_rate=?,funding_rate_8h=?,slippage_percent=?,min_volume_millions=?,collector_interval_secs=?,updated_at=? WHERE id='default'`).run(is_active?1:0,parseFloat(min_entry_spread),parseFloat(max_entry_spread),parseFloat(trade_volume),parseFloat(take_profit_percent),parseFloat(trailing_stop_percent),parseInt(max_active_trades),parseFloat(commission_rate),parseFloat(funding_rate_8h),parseFloat(slippage_percent),parseInt(min_volume_millions),parseInt(collector_interval_secs),Date.now());
    res.json({ success: true });
});

// GET /api/trading/trades
app.get('/api/trading/trades', (req, res) => {
    const active = db.prepare(`
        SELECT 
            *,
            last_price_a as current_price_a,
            last_price_b as current_price_b,
            last_spread as current_spread,
            curr_gross_pnl as current_pnl,
            curr_gross_pnl as gross_pnl
        FROM trades 
        WHERE status='OPEN' 
        ORDER BY entry_time DESC
    `).all();
    const history = db.prepare(`SELECT * FROM trades WHERE status='CLOSED' ORDER BY exit_time DESC LIMIT 1000`).all();
    const settings = db.prepare(`SELECT * FROM trading_settings WHERE id='default'`).get() || {};
    const cr = settings.commission_rate ?? 0.45;
    for (const t of history) { if (t.commission_cost==null) t.commission_cost=t.volume*(cr/100)*4; }
    res.json({ active, history, stats: getFullStats() });
});

// DELETE /api/trading/trades
app.delete('/api/trading/trades', (req, res) => {
    db.prepare(`UPDATE trading_settings SET is_active = 0, pending_commands='CLEAR_TRADES' WHERE id='default'`).run();
    db.prepare(`DELETE FROM trades`).run();
    res.json({ success: true, message: 'Trades clear command issued' });
});

// POST /api/trading/commands/ack  – paper-trader acknowledges a command
app.post('/api/trading/commands/ack', (req, res) => {
    const auth = req.headers['authorization'] || '';
    if (!COLLECTOR_TOKEN || auth !== `Bearer ${COLLECTOR_TOKEN}`) return res.status(401).json({ error: 'Unauthorized' });
    const { command } = req.body;
    const s = db.prepare(`SELECT pending_commands FROM trading_settings WHERE id='default'`).get();
    if (s && s.pending_commands === command) db.prepare(`UPDATE trading_settings SET pending_commands=NULL WHERE id='default'`).run();
    res.json({ success: true });
});

// POST /api/archive  – paper-trader archives a closed trade
app.post('/api/archive', (req, res) => {
    if (!requireCollectorToken(req, res)) return;
    const t = req.body;
    try {
        db.prepare(`INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,exit_price_a,exit_price_b,exit_spread,exit_time,pnl,commission_cost,funding_cost,pnl_a,pnl_b,close_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(t.pair,t.exchange_a,t.exchange_b,t.direction,t.volume,t.entry_price_a,t.entry_price_b,t.entry_spread,t.entry_time,t.highest_spread_reached,'CLOSED',t.exit_price_a,t.exit_price_b,t.exit_spread,t.exit_time,t.pnl,t.commission_cost,t.funding_cost,t.pnl_a,t.pnl_b,t.close_reason);
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/trading/active  – paper-trader pushes active trades
app.post('/api/trading/active', (req, res) => {
    if (!requireCollectorToken(req, res)) return;
    const trades = req.body;
    cacheSet('active_trades', { ts: Date.now(), data: trades }, 60_000);
    res.json({ success: true, count: trades.length });
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || '4000');
app.listen(PORT, '127.0.0.1', () => {
    console.log(`CEX API server running on http://127.0.0.1:${PORT}`);
});
