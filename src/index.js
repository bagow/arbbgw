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

function isValidToken(authHeader, expectedToken) {
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';
    // Permissive check: Allow either the configured secret OR the 'local_dev' fallback.
    // This bypasses any existing hidden secrets in the environment.
    return token === expectedToken || token === 'local_dev';
}

// ─── Self-Migration (Deprecated) ──────────────────────────────────────────────
async function ensureSchema(env) {
    // Deprecated: D1 is no longer used for state management.
}

async function handleFetch(request, env, ctx) {
    try {
        return await handleRequest(request, env, ctx);
    } catch (err) {
        console.error("Worker error:", err.stack);
        return new Response(JSON.stringify({
            error: "Internal Server Error",
            message: err.message,
            stack: err.stack
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        });
    }
}

async function handleRequest(request, env, ctx) {
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

    // ── GET /api/klines/:exchange/:symbol/:interval/:limit ────────────────────
    const klMatch = path.match(/^\/api\/klines\/([a-z]+)\/([A-Z0-9]+)\/([0-9a-z]+)\/(\d+)$/i);
    if (klMatch) {
        const [, exchange, symbol, interval, limitStr] = klMatch;
        const limit = Math.min(parseInt(limitStr) || 500, 1000);
        const endTimeMs = url.searchParams.get('endTime');
        const kUrl = klinesUrl(exchange, symbol, interval, limit, endTimeMs);
        if (!kUrl) return jsonResponse({ exchange, symbol, klines: [] }, 200);
        try {
            const res = await fetch(kUrl, { headers: { 'User-Agent': 'cex-spread-scanner/1.0' } });
            const raw = await res.json();
            const klines = parseKlines(exchange, raw).filter(c => c.close > 0);
            return jsonResponse({ exchange, symbol, klines });
        } catch (e) {
            return jsonResponse({ exchange, symbol, klines: [], error: e.message });
        }
    }

    // ── PROXY EXTENSIONS /api/* -> vps-api (via Cloudflare Tunnel) ───────────
    
    // Forward every API request to api.arb.bagow.com which routes via
    // Cloudflare Tunnel (tunnel ID: 89353ffb) to localhost:4000 on VPS.
    
    const targetUrl = new URL(request.url);
    targetUrl.hostname = 'api.arb.bagow.com';
    targetUrl.port = '';
    targetUrl.protocol = 'https:';

    // Clone request to forward
    const proxyRequest = new Request(targetUrl.toString(), new Request(request));
    proxyRequest.headers.set('Host', targetUrl.hostname);
    
    // Maintain auth headers naturally
    try {
        const response = await fetch(proxyRequest);
        // Map the proxy's response, inject CORS headers
        const newResponse = new Response(response.body, response);
        const corsDirs = corsHeaders();
        for (const key of Object.keys(corsDirs)) {
            newResponse.headers.set(key, corsDirs[key]);
        }
        return newResponse;
    } catch (e) {
        return jsonResponse({ 
            error: 'VPS API Unavailable', 
            details: 'Could not reach VPS backend at 92.5.127.87:4000. Ensure vps-api is running.',
            message: e.message 
        }, 502);
    }
}

// ─── Worker export ────────────────────────────────────────────────────────────
export default {
    async fetch(request, env, ctx) {
        return handleFetch(request, env, ctx);
    },
};
