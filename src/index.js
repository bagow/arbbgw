/**
 * index.js – Cloudflare Worker entry point (Rust-collector mode)
 *
 * Data flow:
 *   Rust collector → POST /api/update  → D1 (server_cache + spread_history)
 *                                      → paper-trader /internal/spreads  (direct VPS)
 *                                      → real-trader  /internal/spreads  (direct VPS)
 *
 * Auth:
 *   All HTML pages require HTTP Basic Auth (env secrets: PASS_DIMA, PASS_SERGEI)
 *   API endpoints require Bearer token (COLLECTOR_TOKEN or INTERNAL_TOKEN)
 *
 * Endpoints:
 *   POST /api/update                          → receive spreads from Rust collector
 *   GET  /api/unified                         → unified snapshot + trades + profiles
 *   GET  /api/snapshot                        → cached spread table
 *   GET  /api/klines/:ex/:sym/:interval/:limit → kline proxy
 *   GET  /api/history/:pair/:exA/:exB         → D1 spread history
 *
 *   GET  /api/trading/settings                → paper-trader default settings
 *   POST /api/trading/settings                → update paper-trader default settings
 *   GET  /api/trading/trades                  → active + closed paper trades
 *   DELETE /api/trading/trades                → clear paper trades
 *   POST /api/trading/commands/ack            → paper-trader acks command
 *   POST /api/trading/active                  → paper-trader pushes active trades
 *   POST /api/archive                         → paper-trader archives closed trade
 *
 *   GET  /api/profiles                        → list all trading profiles
 *   POST /api/profiles                        → create new profile
 *   PATCH /api/profiles/:id                   → update profile
 *   DELETE /api/profiles/:id                  → delete profile
 *
 *   GET  /api/real-trader/settings            → real-trader settings
 *   POST /api/real-trader/settings            → update real-trader settings
 *   POST /api/real-trader/active              → real-trader pushes active trades
 *   POST /api/real-trader/archive             → real-trader archives closed trade
 *   POST /api/real-trader/commands/ack        → real-trader acks command
 */

import { filterForStorage } from './engine.js';

// ─── Basic Auth ───────────────────────────────────────────────────────────────

// Returns the authenticated username (string) or null if auth fails.
function checkBasicAuth(request, env) {
    const authHeader = request.headers.get('Authorization') || '';
    if (!authHeader.startsWith('Basic ')) return null;

    let decoded;
    try {
        decoded = atob(authHeader.slice(6));
    } catch {
        return null;
    }

    const colon = decoded.indexOf(':');
    if (colon < 0) return null;
    const user = decoded.slice(0, colon);
    const pass = decoded.slice(colon + 1);

    const validUsers = {
        dima:   env.PASS_DIMA   || '',
        sergei: env.PASS_SERGEI || '',
    };

    const expected = validUsers[user];
    if (!expected || pass !== expected) return null;
    return user;
}

// Returns a 401 response if not authenticated, otherwise null.
function requireAuth(request, env) {
    if (checkBasicAuth(request, env)) return null;
    return new Response('Unauthorized', {
        status: 401,
        headers: {
            'WWW-Authenticate': 'Basic realm="CEX Arbitrage Dashboard"',
            'Content-Type': 'text/plain',
        },
    });
}

// Returns [null, username] on success or [Response, null] on failure.
function requireAuthUser(request, env) {
    const username = checkBasicAuth(request, env);
    if (username) return [null, username];
    return [new Response('Unauthorized', {
        status: 401,
        headers: {
            'WWW-Authenticate': 'Basic realm="CEX Arbitrage Dashboard"',
            'Content-Type': 'text/plain',
        },
    }), null];
}

// ─── Exchange kline endpoints ─────────────────────────────────────────────────
function klinesUrl(exchange, symbol, interval, limit, endTimeMs) {
    const ivMap = {
        '1m':  { binance: '1m',   bybit: '1',   okx: '1m',  gate: '1m',  bitget: '1m',  mexc: 'Min1',  htx: '1min',  bingx: '1m',  kucoin: '1min',  xt: '1m',  coinex: '1min' },
        '3m':  { binance: '3m',   bybit: '3',   okx: '3m',  gate: '3m',  bitget: '3m',  mexc: 'Min3',  htx: '3min',  bingx: '3m',  kucoin: '3min',  xt: '3m',  coinex: '3min' },
        '5m':  { binance: '5m',   bybit: '5',   okx: '5m',  gate: '5m',  bitget: '5m',  mexc: 'Min5',  htx: '5min',  bingx: '5m',  kucoin: '5min',  xt: '5m',  coinex: '5min' },
        '15m': { binance: '15m',  bybit: '15',  okx: '15m', gate: '15m', bitget: '15m', mexc: 'Min15', htx: '15min', bingx: '15m', kucoin: '15min', xt: '15m', coinex: '15min' },
    };
    const iv = (ivMap[interval] || ivMap['1m'])[exchange] || interval;
    const sym = symbol;

    switch (exchange) {
        case 'binance':
            return `https://fapi.binance.com/fapi/v1/klines?symbol=${sym}&interval=${iv}&limit=${limit}${endTimeMs ? `&endTime=${endTimeMs}` : ''}`;
        case 'bybit':
            return `https://api.bybit.com/v5/market/kline?category=linear&symbol=${sym}&interval=${iv}&limit=${limit}${endTimeMs ? `&end=${endTimeMs}` : ''}`;
        case 'okx': {
            const instId = sym.replace(/^([A-Z0-9]+)(USDT)$/, '$1-USDT-SWAP');
            return `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${iv}&limit=${Math.min(limit, 300)}${endTimeMs ? `&after=${endTimeMs}` : ''}`;
        }
        case 'gate': {
            const contract = sym.replace('USDT', '_USDT');
            return `https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=${contract}&interval=${iv}&limit=${limit}${endTimeMs ? `&to=${Math.floor(endTimeMs/1000)}` : ''}`;
        }
        case 'bitget':
            return `https://api.bitget.com/api/v2/mix/market/candles?symbol=${sym}&productType=USDT-FUTURES&granularity=${iv}&limit=${Math.min(limit, 1000)}${endTimeMs ? `&endTime=${endTimeMs}` : ''}`;
        case 'mexc': {
            const contract = sym.replace('USDT', '_USDT');
            return `https://futures.mexc.com/api/v1/contract/kline/${contract}?interval=${iv}&limit=${limit}${endTimeMs ? `&end=${endTimeMs}` : ''}`;
        }
        case 'htx': {
            const code = sym.replace('USDT', '-USDT');
            return `https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=${code}&period=${iv}&size=${limit}`;
        }
        case 'bingx': {
            const s = sym.replace('USDT', '-USDT');
            return `https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=${s}&interval=${iv}&limit=${limit}${endTimeMs ? `&endTime=${endTimeMs}` : ''}`;
        }
        case 'kucoin': {
            const ivMin = { '1m': 1, '3m': 3, '5m': 5, '15m': 15 }[interval] || 1;
            const contract = sym.replace('-', '') + 'M';
            return `https://api-futures.kucoin.com/api/v1/kline/query?symbol=${contract}&granularity=${ivMin}${endTimeMs ? `&from=${Math.floor((endTimeMs - 1000 * 60 * limit) / 1000)}&to=${Math.floor(endTimeMs / 1000)}` : ''}`;
        }
        case 'kraken':
            return `https://api.kraken.com/0/public/OHLC?pair=${sym}&interval=${iv.replace('m', '')}&count=${limit}`;
        case 'coinbase': {
            const productId = sym.replace('USDT', '-USDT');
            const gran = { '1m': 'ONE_MINUTE', '5m': 'FIVE_MINUTE', '15m': 'FIFTEEN_MINUTE' }[interval] || 'ONE_MINUTE';
            return `https://api.coinbase.com/api/v3/brokerage/products/${productId}/candles?granularity=${gran}&limit=${limit}${endTimeMs ? `&end=${Math.floor(endTimeMs/1000)}` : ''}`;
        }
        default:
            return null;
    }
}

function parseKlines(exchange, raw) {
    switch (exchange) {
        case 'binance':
            return (Array.isArray(raw) ? raw : []).map(c => ({ ts: c[0], close: parseFloat(c[4]) }));
        case 'bybit':
            return (raw?.result?.list ?? []).map(c => ({ ts: parseInt(c[0]), close: parseFloat(c[4]) })).reverse();
        case 'okx':
            return (raw?.data ?? []).map(c => ({ ts: parseInt(c[0]), close: parseFloat(c[4]) })).reverse();
        case 'gate':
            return (Array.isArray(raw) ? raw : []).map(c => ({ ts: parseInt(c.t) * 1000, close: parseFloat(c.c) }));
        case 'bitget': {
            const list = raw?.data || raw || [];
            if (!Array.isArray(list)) return [];
            return list.map(c => ({ ts: parseInt(c[0]), close: parseFloat(c[4]) })).reverse();
        }
        case 'mexc': {
            const list = raw?.data || raw || [];
            if (!Array.isArray(list)) return [];
            return list.map(c => ({ ts: parseInt(c[0]) * 1000, close: parseFloat(c[2]) }));
        }
        case 'htx':
            return (raw?.data ?? []).map(c => ({ ts: c.id * 1000, close: parseFloat(c.close) })).reverse();
        case 'bingx':
            return (raw?.data ?? []).map(c => ({ ts: c.time ?? parseInt(c[0]), close: parseFloat(c.close ?? c[4]) }));
        case 'kucoin':
            return (raw?.data ?? []).map(c => ({ ts: parseInt(c[0]), close: parseFloat(c[4]) }));
        case 'kraken': {
            const pairs = Object.values(raw?.result ?? {});
            const arr = pairs.length ? pairs[0] : [];
            return arr.map(c => ({ ts: c[0] * 1000, close: parseFloat(c[4]) }));
        }
        case 'coinbase':
            return (raw?.candles ?? []).map(c => ({ ts: parseInt(c.start) * 1000, close: parseFloat(c.close) })).reverse();
        default:
            return [];
    }
}

// ─── CORS / helpers ───────────────────────────────────────────────────────────
function corsHeaders() {
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PATCH,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    };
}
function jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { ...corsHeaders(), 'Content-Type': 'application/json' },
    });
}

function nanoid() {
    return Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 10);
}

// ─── HTTP handler ─────────────────────────────────────────────────────────────
async function handleFetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'OPTIONS')
        return new Response(null, { status: 204, headers: corsHeaders() });

    // ── Basic Auth for HTML pages (not API endpoints) ─────────────────────────
    const isApiPath = path.startsWith('/api/');
    if (!isApiPath) {
        const authResp = requireAuth(request, env);
        if (authResp) return authResp;
        // Let static assets be served by Cloudflare
        if (env.ASSETS) return await env.ASSETS.fetch(request);
        return new Response('Not found', { status: 404 });
    }

    // ── POST /api/update ──────────────────────────────────────────────────────
    if (path === '/api/update' && request.method === 'POST') {
        const token = request.headers.get('Authorization') || '';
        const expectedToken = env.COLLECTOR_TOKEN || '';
        if (!expectedToken || token !== `Bearer ${expectedToken}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);

        let body;
        try { body = await request.json(); } catch { return jsonResponse({ error: 'Invalid JSON' }, 400); }

        const { spreads = [], maps = {} } = body;
        if (!Array.isArray(spreads)) return jsonResponse({ error: 'spreads must be an array' }, 400);

        const now = Date.now();

        ctx.waitUntil(
            env.DB.prepare(
                `INSERT INTO server_cache (key, value, expires_at) VALUES (?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at`
            ).bind('snapshot', JSON.stringify({ ts: now, data: spreads }), now + 180_000).run()
        );

        const mapSummaries = {};
        for (const [ex, data] of Object.entries(maps)) mapSummaries[ex] = Object.keys(data);
        ctx.waitUntil(
            env.DB.prepare(
                `INSERT INTO server_cache (key, value, expires_at) VALUES (?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at`
            ).bind('exchange_maps', JSON.stringify({ ts: now, data: mapSummaries }), now + 180_000).run().catch(() => {})
        );

        const toStore = filterForStorage(spreads);
        if (toStore.length > 0) {
            const CHUNK_SIZE = 10;
            for (let i = 0; i < toStore.length; i += CHUNK_SIZE) {
                const chunk = toStore.slice(i, i + CHUNK_SIZE);
                const ph = chunk.map(() => '(?,?,?,?,?,?,?)').join(',');
                const vals = chunk.flatMap(r => [r.pair, r.exchangeA, r.exchangeB, r.priceA, r.priceB, r.spread, now]);
                ctx.waitUntil(
                    env.DB.prepare(
                        `INSERT INTO spread_history (pair, exchange_a, exchange_b, price_a, price_b, spread, timestamp) VALUES ${ph}`
                    ).bind(...vals).run().catch(e => console.error('D1 history chunk error:', e.message))
                );
            }
        }

        let intervalSecs = 5;
        try {
            const settings = await env.DB.prepare(`SELECT collector_interval_secs FROM trading_profiles WHERE id = 'paper-default'`).first();
            if (settings?.collector_interval_secs) intervalSecs = settings.collector_interval_secs;
        } catch {}

        return jsonResponse({ ok: true, received: spreads.length, stored: toStore.length, config: { push_interval: intervalSecs } });
    }

    // ── GET /api/unified ──────────────────────────────────────────────────────
    if (path === '/api/unified') {
        const [authErr, username] = requireAuthUser(request, env);
        if (authErr) return authErr;

        const minSp = parseFloat(url.searchParams.get('min') || '0');
        const maxSp = parseFloat(url.searchParams.get('max') || '50');

        const snapRow = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'snapshot'`).first();
        let snapshot = { ts: Date.now(), data: [] };
        if (snapRow) {
            const parsed = JSON.parse(snapRow.value);
            snapshot.ts = parsed.ts;
            snapshot.data = (parsed.data || []).filter(r => r.spread >= minSp && r.spread <= maxSp).slice(0, 1000);
        }

        let paperActive = [];
        const paperActiveRow = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'active_trades'`).first();
        if (paperActiveRow?.expires_at > Date.now()) {
            try { paperActive = JSON.parse(paperActiveRow.value).data || []; } catch {}
        }

        let realActive = [];
        const realActiveRow = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'real_active_trades'`).first();
        if (realActiveRow?.expires_at > Date.now()) {
            try { realActive = JSON.parse(realActiveRow.value).data || []; } catch {}
        }

        const { results: paperHistory } = await env.DB.prepare(
            `SELECT * FROM trades WHERE status = 'CLOSED' AND profile_id = 'paper-default' ORDER BY exit_time DESC LIMIT 50`
        ).all();
        const { results: realHistory } = await env.DB.prepare(
            `SELECT * FROM trades WHERE status = 'CLOSED' AND profile_id = 'real-default' ORDER BY exit_time DESC LIMIT 50`
        ).all();

        const paperStats = await getFullStats(env, 'paper-default');
        const realStats = await getFullStats(env, 'real-default');

        const profileIdParam = url.searchParams.get('profile_id');

        const mapRow = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'exchange_maps'`).first();
        let maps = { data: {} };
        if (mapRow?.expires_at > Date.now()) maps = JSON.parse(mapRow.value);

        // Load profiles scoped to the authenticated user
        const { results } = await env.DB.prepare(
            `SELECT * FROM trading_profiles WHERE owner = ? OR id IN ('paper-default','real-default') ORDER BY created_at ASC`
        ).bind(username).all();
        const profilesResult = results || [];

        // If profile_id specified, also return per-profile stats and trades
        let profileStats = null;
        let profileActive = [];
        let profileHistory = [];
        if (profileIdParam) {
            profileStats = await getFullStats(env, profileIdParam);
            // Get profile-specific active trades from cache
            const profileActiveKey = `active_trades_${profileIdParam}`;
            const profileActiveRow = await env.DB.prepare(
                `SELECT value, expires_at FROM server_cache WHERE key = ?`
            ).bind(profileActiveKey).first();
            if (profileActiveRow?.expires_at > Date.now()) {
                try { profileActive = JSON.parse(profileActiveRow.value).data || []; } catch {}
            } else {
                // Fall back to the global active_trades cache for paper-default
                if (profileIdParam === 'paper-default') profileActive = paperActive;
                else if (profileIdParam === 'real-default') profileActive = realActive;
            }
            // Get profile-specific history
            const { results: pHistory } = await env.DB.prepare(
                `SELECT * FROM trades WHERE status = 'CLOSED' AND profile_id = ? ORDER BY exit_time DESC LIMIT 50`
            ).bind(profileIdParam).all();
            profileHistory = pHistory || [];
        }

        return jsonResponse({
            snapshot,
            trades: {
                active: paperActive,
                history: paperHistory,
                stats: paperStats,
            },
            real: {
                active: realActive,
                history: realHistory,
                stats: realStats,
            },
            exchanges: maps,
            profiles: profilesResult,
            ...(profileStats ? {
                profile_stats: profileStats,
                profile_active: profileActive,
                profile_history: profileHistory,
            } : {}),
        });
    }

    // ── GET /api/snapshot ─────────────────────────────────────────────────────
    if (path === '/api/snapshot') {
        const minSp = parseFloat(url.searchParams.get('min') || '0');
        const maxSp = parseFloat(url.searchParams.get('max') || '50');
        const row = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'snapshot'`).first();
        if (!row || row.expires_at <= Date.now())
            return jsonResponse({ error: 'No data yet', data: [] }, 503);
        const parsed = JSON.parse(row.value);
        if (parsed.data) parsed.data = parsed.data.filter(r => r.spread >= minSp && r.spread <= maxSp).slice(0, 150);
        return jsonResponse(parsed);
    }

    // ── GET /api/exchanges ────────────────────────────────────────────────────
    if (path === '/api/exchanges') {
        const row = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key = 'exchange_maps'`).first();
        if (!row || row.expires_at <= Date.now())
            return jsonResponse({ error: 'No exchange data', data: {} }, 503);
        return jsonResponse(JSON.parse(row.value));
    }

    // ── GET /api/klines/:exchange/:symbol/:interval/:limit ────────────────────
    const klMatch = path.match(/^\/api\/klines\/([a-z]+)\/([A-Z0-9]+)\/([0-9a-z]+)\/(\d+)$/i);
    if (klMatch) {
        const [, exchange, symbol, interval, limitStr] = klMatch;
        const limit = Math.min(parseInt(limitStr) || 500, 1000);
        const endTimeMs = url.searchParams.get('endTime');
        const kUrl = klinesUrl(exchange, symbol, interval, limit, endTimeMs);
        if (!kUrl) return jsonResponse({ error: `Unknown exchange: ${exchange}`, klines: [] }, 400);
        try {
            const res = await fetch(kUrl, { headers: { 'User-Agent': 'cex-spread-scanner/1.0' } });
            const raw = await res.json();
            const klines = parseKlines(exchange, raw).filter(c => c.close > 0);
            return jsonResponse({ exchange, symbol, klines });
        } catch (e) {
            return jsonResponse({ exchange, symbol, klines: [], error: e.message });
        }
    }

    // ── GET /api/history/:pair/:exA/:exB ──────────────────────────────────────
    const histMatch = path.match(/^\/api\/history\/([A-Z0-9]+)\/([a-z]+)\/([a-z]+)$/i);
    if (histMatch) {
        const [, pair, exA, exB] = histMatch;
        const since = Date.now() - 24 * 60 * 60 * 1000;
        const { results } = await env.DB.prepare(
            `SELECT pair, exchange_a, exchange_b, price_a, price_b, spread, timestamp
             FROM spread_history WHERE pair = ? AND exchange_a = ? AND exchange_b = ? AND timestamp >= ?
             ORDER BY timestamp ASC LIMIT 2000`
        ).bind(pair.toUpperCase(), exA.toLowerCase(), exB.toLowerCase(), since).all();
        return jsonResponse({ pair, exchangeA: exA, exchangeB: exB, history: results });
    }

    // ── /api/trading/settings ─────────────────────────────────────────────────
    if (path === '/api/trading/settings') {
        if (request.method === 'GET') {
            const settings = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id = 'paper-default'`).first();
            const defaults = {
                is_active: false, min_entry_spread: 1.0, max_entry_spread: 10.0,
                trade_volume: 1000.0, take_profit_percent: 0.1, trailing_stop_percent: 0.5,
                max_active_trades: 5, commission_rate: 0.45, funding_rate_8h: 0.01,
                slippage_percent: 0.1, min_volume_millions: 10, collector_interval_secs: 5,
            };
            const out = { ...defaults, ...(settings || {}) };
            if (out.is_active !== undefined) out.is_active = Boolean(out.is_active);
            return jsonResponse(out);
        } else if (request.method === 'POST') {
            const body = await request.json();
            const {
                is_active, min_entry_spread, max_entry_spread, trade_volume,
                take_profit_percent = 0.1, trailing_stop_percent, max_active_trades,
                commission_rate = 0.45, funding_rate_8h = 0.01, slippage_percent = 0.1,
                min_volume_millions = 10, collector_interval_secs = 5,
            } = body;
            await env.DB.prepare(`
                UPDATE trading_profiles
                SET is_active=?, min_entry_spread=?, max_entry_spread=?, trade_volume=?,
                    take_profit_percent=?, trailing_stop_percent=?, max_active_trades=?,
                    commission_rate=?, funding_rate_8h=?, slippage_percent=?,
                    min_volume_millions=?, collector_interval_secs=?, updated_at=?
                WHERE id='paper-default'
            `).bind(
                is_active ? 1 : 0, parseFloat(min_entry_spread), parseFloat(max_entry_spread),
                parseFloat(trade_volume), parseFloat(take_profit_percent), parseFloat(trailing_stop_percent),
                parseInt(max_active_trades), parseFloat(commission_rate), parseFloat(funding_rate_8h),
                parseFloat(slippage_percent), parseInt(min_volume_millions), parseInt(collector_interval_secs), Date.now()
            ).run();
            return jsonResponse({ success: true });
        }
    }

    // ── GET /api/trading/settings/all ─────────────────────────────────────────
    if (path === '/api/trading/settings/all' && request.method === 'GET') {
        const token = request.headers.get('Authorization') || '';
        const expected = env.INTERNAL_TOKEN || env.COLLECTOR_TOKEN || '';
        if (expected && token !== `Bearer ${expected}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);

        const { results } = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE mode = 'paper' OR mode IS NULL`).all();
        const defaults = {
            is_active: false, min_entry_spread: 1.0, max_entry_spread: 10.0,
            trade_volume: 1000.0, take_profit_percent: 0.1, trailing_stop_percent: 0.5,
            max_active_trades: 5, commission_rate: 0.45, funding_rate_8h: 0.01,
            slippage_percent: 0.1, min_volume_millions: 10, collector_interval_secs: 5,
        };

        const profiles = results.map(row => {
            const out = { ...defaults, ...row };
            out.is_active = Boolean(out.is_active);
            return out;
        });

        // Ensure paper-default always exists in the bot's view
        if (!profiles.find(p => p.id === 'paper-default')) {
            profiles.push({ id: 'paper-default', name: 'Paper Default', mode: 'paper', ...defaults });
        }

        return jsonResponse(profiles);
    }

    // ── /api/trading/trades ───────────────────────────────────────────────────
    if (path === '/api/trading/trades' && request.method === 'GET') {
        const settings = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id='paper-default'`).first() || {};
        let active = [];
        const activeRow = await env.DB.prepare(`SELECT value, expires_at FROM server_cache WHERE key='active_trades'`).first();
        if (activeRow?.expires_at > Date.now()) {
            try { active = JSON.parse(activeRow.value).data || []; } catch {}
        }
        const { results: history } = await env.DB.prepare(
            `SELECT * FROM trades WHERE status='CLOSED' AND (profile_id IS NULL OR profile_id='paper-default') ORDER BY exit_time DESC LIMIT 50`
        ).all();
        const stats = await getFullStats(env, 'paper-default');
        const cr = settings.commission_rate ?? 0.45;
        for (const t of history) {
            if (t.commission_cost === null || t.commission_cost === undefined)
                t.commission_cost = t.volume * (cr / 100) * 4;
        }
        return jsonResponse({ active, history, stats });
    }

    if (path === '/api/trading/trades' && request.method === 'DELETE') {
        await env.DB.prepare(`UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id='paper-default'`).run();
        await env.DB.prepare(`DELETE FROM trades WHERE profile_id='paper-default' OR profile_id IS NULL`).run();
        return jsonResponse({ success: true });
    }

    // ── POST /api/trading/commands/ack ────────────────────────────────────────
    if (path === '/api/trading/commands/ack' && request.method === 'POST') {
        const auth = request.headers.get('Authorization');
        if (auth !== `Bearer ${env.INTERNAL_TOKEN}`) return jsonResponse({ error: 'Unauthorized' }, 401);
        const { command, profile_id = 'paper-default' } = await request.json();
        const settings = await env.DB.prepare(`SELECT pending_commands FROM trading_profiles WHERE id=?`).bind(profile_id).first();
        if (settings?.pending_commands === command)
            await env.DB.prepare(`UPDATE trading_profiles SET pending_commands=NULL WHERE id=?`).bind(profile_id).run();
        return jsonResponse({ success: true });
    }

    // ── POST /api/archive ─────────────────────────────────────────────────────
    if (path === '/api/archive' && request.method === 'POST') {
        const token = request.headers.get('Authorization') || '';
        if (!env.COLLECTOR_TOKEN || token !== `Bearer ${env.COLLECTOR_TOKEN}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);
        let trade;
        try { trade = await request.json(); } catch { return jsonResponse({ error: 'Invalid JSON' }, 400); }
        try {
            await env.DB.prepare(`
                INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,
                    entry_spread,entry_time,highest_spread_reached,status,exit_price_a,exit_price_b,exit_spread,
                    exit_time,pnl,commission_cost,funding_cost,pnl_a,pnl_b,close_reason,profile_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            `).bind(
                trade.pair, trade.exchange_a, trade.exchange_b, trade.direction, trade.volume,
                trade.entry_price_a, trade.entry_price_b, trade.entry_spread, trade.entry_time,
                trade.highest_spread_reached, 'CLOSED', trade.exit_price_a, trade.exit_price_b,
                trade.exit_spread, trade.exit_time, trade.pnl, trade.commission_cost, trade.funding_cost,
                trade.pnl_a, trade.pnl_b, trade.close_reason, trade.profile_id || 'paper-default'
            ).run();
            return jsonResponse({ success: true });
        } catch (e) { return jsonResponse({ error: e.message }, 500); }
    }

    // ── POST /api/trading/active ──────────────────────────────────────────────
    if (path === '/api/trading/active' && request.method === 'POST') {
        const token = request.headers.get('Authorization') || '';
        if (!env.COLLECTOR_TOKEN || token !== `Bearer ${env.COLLECTOR_TOKEN}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);
            
        let activeTradesMap;
        try { activeTradesMap = await request.json(); } catch { return jsonResponse({ error: 'Invalid JSON' }, 400); }
        
        const now = Date.now();
        const stmts = [];
        
        // Ensure backwards compatibility by expecting arrays directly as well
        if (Array.isArray(activeTradesMap)) {
            activeTradesMap = { 'paper-default': activeTradesMap };
        }
        
        for (const [profId, trades] of Object.entries(activeTradesMap)) {
            const key = profId === 'paper-default' ? 'active_trades' : `active_trades_${profId}`;
            stmts.push(env.DB.prepare(
                `INSERT INTO server_cache (key,value,expires_at) VALUES (?,?,?)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at`
            ).bind(key, JSON.stringify({ ts: now, data: trades }), now + 60_000));
        }
        
        if (stmts.length > 0) {
            await env.DB.batch(stmts);
        }
        return jsonResponse({ success: true });
    }


    // ── /api/real-trader/* ────────────────────────────────────────────────────

    if (path === '/api/real-trader/settings') {
        if (request.method === 'GET') {
            const row = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id='real-default'`).first();
            const defaults = {
                is_active: false, min_entry_spread: 1.0, max_entry_spread: 10.0,
                trade_volume: 100.0, take_profit_percent: 0.2, trailing_stop_percent: 0.3,
                max_active_trades: 3, commission_rate: 0.05, slippage_percent: 0.05,
                funding_rate_8h: 0.01, min_volume_millions: 10, collector_interval_secs: 5,
            };
            const out = { ...defaults, ...(row || {}) };
            if (out.is_active !== undefined) out.is_active = Boolean(out.is_active);
            return jsonResponse(out);
        } else if (request.method === 'POST') {
            const body = await request.json();
            await env.DB.prepare(`
                UPDATE trading_profiles SET is_active=?,min_entry_spread=?,max_entry_spread=?,
                    trade_volume=?,take_profit_percent=?,trailing_stop_percent=?,max_active_trades=?,
                    commission_rate=?,slippage_percent=?,funding_rate_8h=?,min_volume_millions=?,
                    collector_interval_secs=?,updated_at=?
                WHERE id='real-default'
            `).bind(
                body.is_active ? 1 : 0,
                parseFloat(body.min_entry_spread || 1), parseFloat(body.max_entry_spread || 10),
                parseFloat(body.trade_volume || 100), parseFloat(body.take_profit_percent || 0.2),
                parseFloat(body.trailing_stop_percent || 0.3), parseInt(body.max_active_trades || 3),
                parseFloat(body.commission_rate || 0.05), parseFloat(body.slippage_percent || 0.05),
                parseFloat(body.funding_rate_8h || 0.01), parseInt(body.min_volume_millions || 10),
                parseInt(body.collector_interval_secs || 5), Date.now()
            ).run();
            return jsonResponse({ success: true });
        }
    }

    if (path === '/api/real-trader/active' && request.method === 'POST') {
        const token = request.headers.get('Authorization') || '';
        if (!env.COLLECTOR_TOKEN || token !== `Bearer ${env.COLLECTOR_TOKEN}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);
        let trades;
        try { trades = await request.json(); } catch { return jsonResponse({ error: 'Invalid JSON' }, 400); }
        const now = Date.now();
        await env.DB.prepare(
            `INSERT INTO server_cache (key,value,expires_at) VALUES (?,?,?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at`
        ).bind('real_active_trades', JSON.stringify({ ts: now, data: trades }), now + 60_000).run();
        return jsonResponse({ success: true, count: trades.length });
    }

    if (path === '/api/real-trader/archive' && request.method === 'POST') {
        const token = request.headers.get('Authorization') || '';
        if (!env.COLLECTOR_TOKEN || token !== `Bearer ${env.COLLECTOR_TOKEN}`)
            return jsonResponse({ error: 'Unauthorized' }, 401);
        let trade;
        try { trade = await request.json(); } catch { return jsonResponse({ error: 'Invalid JSON' }, 400); }
        try {
            await env.DB.prepare(`
                INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,
                    entry_spread,entry_time,highest_spread_reached,status,exit_price_a,exit_price_b,exit_spread,
                    exit_time,pnl,commission_cost,funding_cost,pnl_a,pnl_b,close_reason,profile_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            `).bind(
                trade.pair, trade.exchange_a, trade.exchange_b, trade.direction, trade.volume,
                trade.entry_price_a, trade.entry_price_b, trade.entry_spread, trade.entry_time,
                trade.highest_spread_reached, 'CLOSED', trade.exit_price_a, trade.exit_price_b,
                trade.exit_spread, trade.exit_time, trade.pnl, trade.commission_cost, trade.funding_cost,
                trade.pnl_a, trade.pnl_b, trade.close_reason, 'real-default'
            ).run();
            return jsonResponse({ success: true });
        } catch (e) { return jsonResponse({ error: e.message }, 500); }
    }

    if (path === '/api/real-trader/commands/ack' && request.method === 'POST') {
        const auth = request.headers.get('Authorization');
        if (auth !== `Bearer ${env.INTERNAL_TOKEN}`) return jsonResponse({ error: 'Unauthorized' }, 401);
        const { command } = await request.json();
        const row = await env.DB.prepare(`SELECT pending_commands FROM trading_profiles WHERE id='real-default'`).first();
        if (row?.pending_commands === command)
            await env.DB.prepare(`UPDATE trading_profiles SET pending_commands=NULL WHERE id='real-default'`).run();
        return jsonResponse({ success: true });
    }

    // ── /api/profiles — Multi-profile management ──────────────────────────────

    if (path === '/api/profiles' && request.method === 'GET') {
        const [authErr, username] = requireAuthUser(request, env);
        if (authErr) return authErr;
        const { results } = await env.DB.prepare(
            `SELECT * FROM trading_profiles WHERE owner = ? OR id IN ('paper-default','real-default') ORDER BY created_at ASC`
        ).bind(username).all();
        for (const p of results || []) p.is_active = Boolean(p.is_active);
        return jsonResponse(results || []);
    }

    if (path === '/api/profiles' && request.method === 'POST') {
        const [authErr, username] = requireAuthUser(request, env);
        if (authErr) return authErr;
        const body = await request.json();
        const id = body.id || nanoid();
        const now = Date.now();
        await env.DB.prepare(`
            INSERT INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,trade_volume,
                 take_profit_percent,trailing_stop_percent,max_active_trades,commission_rate,
                 slippage_percent,funding_rate_8h,min_volume_millions,collector_interval_secs,owner,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `).bind(
            id, body.name || 'New Profile', body.mode || 'paper',
            body.is_active ? 1 : 0,
            parseFloat(body.min_entry_spread || 1), parseFloat(body.max_entry_spread || 10),
            parseFloat(body.trade_volume || 1000), parseFloat(body.take_profit_percent || 0.1),
            parseFloat(body.trailing_stop_percent || 0.5), parseInt(body.max_active_trades || 5),
            parseFloat(body.commission_rate || 0.45), parseFloat(body.slippage_percent || 0.1),
            parseFloat(body.funding_rate_8h || 0.01), parseInt(body.min_volume_millions || 10),
            parseInt(body.collector_interval_secs || 5), username, now, now
        ).run();
        const profile = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id=?`).bind(id).first();
        return jsonResponse(profile, 201);
    }

    const profilePatchMatch = path.match(/^\/api\/profiles\/([^/]+)$/);
    if (profilePatchMatch) {
        const profileId = profilePatchMatch[1];
        const [authErr, username] = requireAuthUser(request, env);
        if (authErr) return authErr;

        // Verify ownership (allow defaults for backward compat)
        if (profileId !== 'paper-default' && profileId !== 'real-default') {
            const owner = await env.DB.prepare(`SELECT owner FROM trading_profiles WHERE id=?`).bind(profileId).first();
            if (owner && owner.owner !== username) return jsonResponse({ error: 'Forbidden' }, 403);
        }

        if (request.method === 'GET') {
            const profile = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id=?`).bind(profileId).first();
            if (!profile) return jsonResponse({ error: 'Not Found' }, 404);
            profile.is_active = Boolean(profile.is_active);
            return jsonResponse(profile);
        }

        if (request.method === 'PATCH') {
            const body = await request.json();
            const fields = [];
            const vals = [];
            const allowed = ['name','mode','is_active','min_entry_spread','max_entry_spread','trade_volume',
                'take_profit_percent','trailing_stop_percent','max_active_trades','commission_rate',
                'slippage_percent','funding_rate_8h','min_volume_millions','collector_interval_secs','pending_commands'];
            for (const f of allowed) {
                if (body[f] !== undefined) {
                    fields.push(`${f}=?`);
                    if (f === 'is_active') vals.push(body[f] ? 1 : 0);
                    else if (['max_active_trades','min_volume_millions','collector_interval_secs'].includes(f)) vals.push(parseInt(body[f]));
                    else if (f !== 'name' && f !== 'mode' && f !== 'pending_commands') vals.push(parseFloat(body[f]));
                    else vals.push(body[f]);
                }
            }
            if (!fields.length) return jsonResponse({ error: 'No fields to update' }, 400);
            fields.push('updated_at=?');
            vals.push(Date.now(), profileId);
            await env.DB.prepare(`UPDATE trading_profiles SET ${fields.join(',')} WHERE id=?`).bind(...vals).run();
            const profile = await env.DB.prepare(`SELECT * FROM trading_profiles WHERE id=?`).bind(profileId).first();
            return jsonResponse(profile);
        }

        if (request.method === 'DELETE') {
            if (profileId === 'paper-default' || profileId === 'real-default')
                return jsonResponse({ error: 'Cannot delete default profiles' }, 400);
            await env.DB.prepare(`DELETE FROM trading_profiles WHERE id=?`).bind(profileId).run();
            await env.DB.prepare(`DELETE FROM trades WHERE profile_id=?`).bind(profileId).run();
            return jsonResponse({ success: true });
        }
    }

    // ── DELETE /api/profiles/:id/trades — Clear all trades for a profile ───────
    const profileTradesMatch = path.match(/^\/api\/profiles\/([^/]+)\/trades$/);
    if (profileTradesMatch && request.method === 'DELETE') {
        const [authErr, username] = requireAuthUser(request, env);
        if (authErr) return authErr;
        const profileId = profileTradesMatch[1];

        // Set CLEAR_TRADES pending command so the bot knows to reset itself
        await env.DB.prepare(
            `UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id=?`
        ).bind(profileId).run();

        // Delete all trades for this profile
        await env.DB.prepare(`DELETE FROM trades WHERE profile_id=?`).bind(profileId).run();

        // For default paper profile, also delete legacy trades with no profile_id
        if (profileId === 'paper-default') {
            await env.DB.prepare(`DELETE FROM trades WHERE profile_id IS NULL`).run();
        }

        // Clear the active trades cache for this profile
        const cacheKey = profileId === 'paper-default' ? 'active_trades'
            : profileId === 'real-default' ? 'real_active_trades'
            : `active_trades_${profileId}`;
        await env.DB.prepare(`DELETE FROM server_cache WHERE key=?`).bind(cacheKey).run();

        return jsonResponse({ success: true });
    }

    // ── FALLBACK ──────────────────────────────────────────────────────────────
    if (env.ASSETS) return await env.ASSETS.fetch(request);
    return new Response('Not found', { status: 404 });
}

// ─── Stats helper ─────────────────────────────────────────────────────────────
async function getFullStats(env, profileId = 'paper-default') {
    const filter = profileId === 'paper-default'
        ? `(profile_id='paper-default' OR profile_id IS NULL)`
        : `profile_id='${profileId}'`;

    const basic = await env.DB.prepare(`
        SELECT SUM(pnl) as total_pnl, COUNT(id) as total_trades, AVG(pnl) as avg_pnl,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses,
               AVG(exit_time - entry_time) as avg_duration,
               SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
               SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END) as gross_loss
        FROM trades WHERE status='CLOSED' AND ${filter}
    `).first() || {};

    const pnlEx = await env.DB.prepare(`
        SELECT ex, SUM(lp) as p, SUM(lc) as c FROM (
            SELECT exchange_a as ex,
                   SUM(COALESCE(pnl_a, pnl/2) - (COALESCE(commission_cost, 0.4*volume/100) + COALESCE(funding_cost,0))/2) as lp,
                   COUNT(*) as lc FROM trades WHERE status='CLOSED' AND ${filter} GROUP BY exchange_a
            UNION ALL
            SELECT exchange_b as ex,
                   SUM(COALESCE(pnl_b, pnl/2) - (COALESCE(commission_cost, 0.4*volume/100) + COALESCE(funding_cost,0))/2) as lp,
                   COUNT(*) as lc FROM trades WHERE status='CLOSED' AND ${filter} GROUP BY exchange_b
        ) GROUP BY ex
    `).all();

    const byPair = await env.DB.prepare(`
        SELECT pair, SUM(pnl) as pnl, COUNT(*) as count FROM trades WHERE status='CLOSED' AND ${filter}
        GROUP BY pair ORDER BY pnl DESC LIMIT 10
    `).all();

    const curve = await env.DB.prepare(`
        SELECT entry_time, exit_time, pnl FROM trades WHERE status='CLOSED' AND ${filter} ORDER BY exit_time ASC
    `).all();

    return {
        total_pnl: basic.total_pnl || 0,
        total_trades: basic.total_trades || 0,
        avg_pnl: basic.avg_pnl || 0,
        wins: basic.wins || 0,
        losses: basic.losses || 0,
        avg_duration: basic.avg_duration || 0,
        gross_profit: basic.gross_profit || 0,
        gross_loss: basic.gross_loss || 0,
        pnl_by_exchange: (pnlEx.results || []).reduce((a, r) => { a[r.ex] = r.p; return a; }, {}),
        count_by_exchange: (pnlEx.results || []).reduce((a, r) => { a[r.ex] = r.c; return a; }, {}),
        pnl_by_pair: byPair.results || [],
        equity_curve: curve.results || [],
    };
}

// ─── Worker export ────────────────────────────────────────────────────────────
export default {
    async fetch(request, env, ctx) { return handleFetch(request, env, ctx); },
};
