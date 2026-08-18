/**
 * app.js – CEX Spread Screener v5 (multi-exchange overlay)
 *
 * Price panel: klines from ALL available exchanges for the pair,
 * each with its own color. Shows which exchange is deviating.
 *
 * Spread histogram: signed % between the two exchanges from the clicked row.
 *
 * Data source: exchange REST kline APIs (instant, no D1 needed).
 */
'use strict';

// ─── Backend URL (same-origin: Worker at arb.bagow.com proxies /api/* to VPS) ─
const API_BASE = window.API_BASE = '';

async function authFetch(url, options = {}) {
    const creds = localStorage.getItem('cex_auth');
    if (creds) {
        options.headers = options.headers || {};
        options.headers['Authorization'] = 'Basic ' + creds;
    }
    return fetch(url, options);
}

// ─── Exchange config ───────────────────────────────────────────────────────────
let KNOWN_EXCHANGES = [];

const EXCHANGE_COLORS = {
    binance:  '#f97316',  // orange
    bybit:    '#60a5fa',  // blue
    gate:     '#a78bfa',  // violet
    kucoin:   '#0ea5e9',  // sky blue
    bitget:   '#00f0ff',  // cyan
    bingx:    '#86efac',  // light green
    okx:      '#34d399',  // emerald
    xt:       '#eab308',  // yellow
    toobit:   '#10b981',  // green
    ascendex: '#ec4899',  // pink
    bitmex:   '#e11d48',  // red
    coinex:   '#d946ef'   // fuchsia
};

// Passed to /api/klines proxy — server maps to exchange-specific codes
const TF_LABELS = { 15: '15s', 60: '1m', 180: '3m', 300: '5m', 900: '15m' };

// ─── Layout constants ─────────────────────────────────────────────────────────
const PAD = { top: 10, right: 60, bottom: 4, left: 64 };
const PAD_H = { top: 8, right: 60, bottom: 28, left: 64 };

// ─── State ────────────────────────────────────────────────────────────────────
const scannerState = {
    snapshot: [],
    sortKey: 'spread',
    sortDir: -1,
    activeRowKey: null,
    activeRow: null,
    // Loaded from /api/trading/settings on init
    threshold: 1,
    maxSpread: 10,
    refreshMs: 10_000,
    refreshTimer: null,
    klines: {},
    liveTicks: {},
    wsTarget: 'a',
    ws: null,
    timeframeSec: 60,
    loadKey: null,
    hiddenExchanges: new Set(), // chart modal: exchanges hidden on price panel
    activeExchanges: new Set(KNOWN_EXCHANGES), // scanner: exchanges shown in table

    // SVG Interaction State
    visibleMinT: null,
    visibleMaxT: null,
    isDragging: false,
    lastDragX: 0,
    isLoadingHistory: false,
};

// ─── DOM refs (lazy-loaded on init) ─────────────────────────────────────────────────────
// $ is an alias for scanner$ (getElementById shorthand)
const $ = id => document.getElementById(id);
let refs = {};

// Proxied DOM ref vars – set by initRefs() so functions can use them directly
let statusDot, statusText, lastUpdate, pairCount, modalTitle, modalSub, modalBackdrop;
let wsLabelA, wsLabelB, wsStatus, legendDyn, svgP, svgH, priceLinesG, chartLegend;

function initRefs() {
    refs = {
        tableBody: $('spread-tbody'),
        pairCount: $('pair-count'),
        statusDot: $('global-status'),
        statusText: $('status-text'),
        lastUpdate: $('last-update'),
        modalBackdrop: $('modal-backdrop'),
        modalTitle: $('modal-title'),
        modalSub: $('modal-sub'),
        modalClose: $('modal-close'),
        legendDyn: $('legend-dynamic'),
        wsLabelA: $('ws-label-a'),
        wsLabelB: $('ws-label-b'),
        wsStatus: $('ws-status'),
        svgP: $('chart-prices'),
        svgH: $('chart-hist'),
        priceLinesG: $('price-lines'),
        exchangeFilters: $('exchange-filters'),
        chartLegend: $('chart-legend'),
    };
    
    // Update proxied variables so that existing code using them directly still works
    statusDot = refs.statusDot;
    statusText = refs.statusText;
    lastUpdate = refs.lastUpdate;
    pairCount = refs.pairCount;
    modalTitle = refs.modalTitle;
    modalSub = refs.modalSub;
    modalBackdrop = refs.modalBackdrop;
    wsLabelA = refs.wsLabelA;
    wsLabelB = refs.wsLabelB;
    wsStatus = refs.wsStatus;
    legendDyn = refs.legendDyn;
    svgP = refs.svgP;
    svgH = refs.svgH;
    priceLinesG = refs.priceLinesG;
    chartLegend = refs.chartLegend;

    // Wire up close button if it exists
    if (refs.modalClose) refs.modalClose.onclick = () => window.closeModal();
}

// ─── SVG helpers ──────────────────────────────────────────────────────────────
function svgEl(tag, attrs = {}) {
    const e = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    return e;
}
function svgDims(el) {
    const r = el.getBoundingClientRect();
    return { W: r.width || 700, H: r.height || 220 };
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function fmtPrice(n) {
    if (n == null || isNaN(n)) return '–';
    if (n >= 1000) return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
    if (n >= 1) return n.toFixed(4);
    return n.toFixed(6);
}
function fmtSpread(n) { return (n ?? 0).toFixed(4) + '%'; }
function fmtTime(ts) { return new Date(ts).toLocaleTimeString('en-GB', { timeZone: 'Europe/Kyiv', hour: '2-digit', minute: '2-digit', second: '2-digit' }); }
function rowKey(r) { return `${r.pair}_${r.exchangeA}_${r.exchangeB}`; }
function signedSpread(pA, pB) {
    if (!pA || !pB) return 0;
    return ((pA - pB) / ((pA + pB) / 2)) * 100;
}

const TV_EXCHANGES = {
    binance:  'BINANCE',
    bybit:    'BYBIT',
    gate:     'GATEIO',
    kucoin:   'KUCOIN',
    bitget:   'BITGET',
    bingx:    'BINGX',
    okx:      'OKX',
    xt:       'XT',
    toobit:   'TOOBIT',
    ascendex: 'ASCENDEX',
    bitmex:   'BITMEX',
    coinex:   'COINEX'
};

// ─── Exchange kline fetch ─────────────────────────────────────────────────────
const BIN_IV = { 1: '1m', 60: '1m', 180: '3m', 300: '5m', 900: '15m' };
const BYB_IV = { 1: '1', 60: '1', 180: '3', 300: '5', 900: '15' };
const OKX_IV = { 1: '1m', 60: '1m', 180: '3m', 300: '5m', 900: '15m' };

async function fetchKlines(exchange, pair, tfSec, endTime = null) {
    // For 15s, we must use internal history from the Worker
    if (tfSec === 15) {
        // We only fetch history for the two exchanges in the active row to save bandwidth
        // But here we might need klines for many exchanges. 
        // Simplest: 15s is ONLY for the active pair comparison.
        if (scannerState.activeRow && (exchange === scannerState.activeRow.exchangeA || exchange === scannerState.activeRow.exchangeB)) {
            try {
                const res = await authFetch(`${API_BASE}/api/history/${pair}/${scannerState.activeRow.exchangeA}/${scannerState.activeRow.exchangeB}`);
                const json = await res.json();
                const history = json.history || [];
                // Map history to kline format {ts, close}
                return history.map(h => ({
                    ts: h.timestamp,
                    close: exchange === scannerState.activeRow.exchangeA ? h.price_a : h.price_b
                }));
            } catch (e) {
                console.error('Failed to fetch 15s internal history:', e);
                return [];
            }
        }
        // Fallback to 1m for other exchanges if 15s selected
        tfSec = 60;
    }

    const proxyInterval = TF_LABELS[tfSec] || '1m';
    try {
        // All exchanges are routed via Worker proxy as per AGENT_README.md
        const endQ = endTime ? `?endTime=${endTime}` : '';
        const res = await authFetch(`${API_BASE}/api/klines/${exchange}/${pair}/${proxyInterval}/500${endQ}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        return (json.klines || []).filter(c => c.close > 0);
    } catch (e) {
        console.error(`Failed to fetch klines for ${exchange}:`, e.message);
        return [];
    }
}

function getEnabledExchanges() {
    let disabled = [];
    if (typeof profiles !== 'undefined' && typeof activeTabId !== 'undefined') {
        const p = profiles.find(pr => pr.id === activeTabId);
        if (p) {
            try { 
                disabled = typeof p.disabled_exchanges === 'string' ? JSON.parse(p.disabled_exchanges) : (p.disabled_exchanges || []); 
            } catch(e){}
        }
    }
    return KNOWN_EXCHANGES.filter(ex => !disabled.includes(ex));
}

/** Fetch klines for the pair's two exchanges (fast — only 2 requests). */
async function loadChartData(row) {
    const key = rowKey(row);
    scannerState.loadKey = key;

    // If viewing a trade from history (exitTime or entryTime is older than 5 min ago), pass target endTime to fetch historical candles
    let targetEndTime = null;
    if (row && (row.entryTime || row.exitTime)) {
        const tradeEnd = row.exitTime || row.entryTime;
        if (Date.now() - tradeEnd > 5 * 60 * 1000) {
            targetEndTime = tradeEnd + 25 * 60 * 1000;
        }
    }

    // Only fetch the 2 exchanges from the clicked row (fast — only 2 requests)
    const pairExchanges = [row.exchangeA, row.exchangeB];
    const results = await Promise.all(
        pairExchanges.map(ex => fetchKlines(ex, row.pair, scannerState.timeframeSec, targetEndTime)
            .then(kl => [ex, kl])
        )
    );

    if (scannerState.loadKey !== key) return; // stale

    scannerState.klines = {};
    for (const [ex, kl] of results) {
        if (kl && kl.length > 0) scannerState.klines[ex] = kl;
    }

    const hasData = Object.keys(scannerState.klines).length > 0;
    if (!hasData && scannerState.activeRow && rowKey(scannerState.activeRow) === key) {
        if (!scannerState.retryCount) scannerState.retryCount = 0;
        if (scannerState.retryCount < 4) {
            scannerState.retryCount++;
            setTimeout(() => {
                if (scannerState.activeRow && rowKey(scannerState.activeRow) === key) {
                    loadChartData(row);
                }
            }, 2500);
        }
    } else {
        scannerState.retryCount = 0;
    }

    // Fallback: If 15s was selected but main exchanges have NO data, try 1m
    if (scannerState.timeframeSec === 15 && row) {
        const hasA = (scannerState.klines[row.exchangeA] || []).length > 0;
        const hasB = (scannerState.klines[row.exchangeB] || []).length > 0;
        if (!hasA && !hasB) {
            console.warn('15s history empty, falling back to 1m');
            scannerState.timeframeSec = 60;
            // Update UI buttons if they exist
            document.querySelectorAll('.tf-btn').forEach(b => {
                b.classList.remove('tf-btn--active');
                if (b.dataset.tf === '60') b.classList.add('tf-btn--active');
            });
            return loadChartData(row);
        }
    }
    scannerState.liveTicks = {};
    
    // Reset pan/zoom on completely new pair load
    scannerState.visibleMinT = null;
    scannerState.visibleMaxT = null;
    
    // Auto-zoom to trade entry/exit if available
    if (row && row.entryTime) {
        const entry = row.entryTime;
        const exit = row.exitTime || Date.now();
        const mid = (entry + exit) / 2;
        const duration = Math.max(60000, exit - entry);
        const halfSpan = Math.max(duration * 4, 10 * 60 * 1000); 
        scannerState.visibleMinT = mid - halfSpan;
        scannerState.visibleMaxT = mid + halfSpan;
    } else if (scannerState.timeframeSec === 15 && scannerState.visibleMinT === null) {
        scannerState.visibleMaxT = Date.now();
        scannerState.visibleMinT = scannerState.visibleMaxT - 15 * 60_000;
    }

    drawCharts();
}

// ─── Status ───────────────────────────────────────────────────────────────────
function setStatus(s, msg) {
    if (statusDot) statusDot.className = `dot dot--${s}`;
    if (statusText) statusText.textContent = msg;
}

// ─── Snapshot ─────────────────────────────────────────────────────────────────
async function fetchSnapshot() {
    setStatus('loading', 'Fetching data…');
    try {
        const url = `${API_BASE}/api/unified?min=${scannerState.threshold}&max=${scannerState.maxSpread}`;
        const res = await authFetch(url);
        if (!res.ok) {
            setStatus('ok', `Waiting for server update…`);
            return;
        }
        const json = await res.json();
        
        // 1. Snapshot
        let rawData = (json.snapshot?.data || []);
        const currentProfile = (typeof profiles !== 'undefined' && typeof activeTabId !== 'undefined') ? profiles.find(p => p.id === activeTabId) : null;
        if (currentProfile) {
            let disabled = [];
            try { disabled = typeof currentProfile.disabled_exchanges === 'string' ? JSON.parse(currentProfile.disabled_exchanges) : (currentProfile.disabled_exchanges || []); } catch(e){}
            
            rawData = rawData.filter(r => {
                if (disabled.includes(r.exchangeA) || disabled.includes(r.exchangeB)) return false;
                return true;
            });
        }
        scannerState.snapshot = rawData;
        
        // 2. Discover exchanges
        const seen = new Set(scannerState.snapshot.flatMap(r => [r.exchangeA, r.exchangeB]));
        seen.forEach(ex => {
            scannerState.activeExchanges.add(ex);
            if (!KNOWN_EXCHANGES.includes(ex)) KNOWN_EXCHANGES.push(ex);
        });
        
        // Also add from global unified state to ensure CEXes with 0 pairs are still recognized
        // IMPORTANT: use .exchanges.data (the actual exchange map), NOT top-level .exchanges keys
        // which include metadata like daily_pnl, daily_pnl_pct, margin_balances, balances, ts etc.
        if (typeof lastUnified !== 'undefined' && lastUnified && lastUnified.exchanges) {
            const exData = lastUnified.exchanges.data || lastUnified.exchanges;
            Object.keys(exData).forEach(ex => {
                // Skip known metadata keys that are NOT exchange names
                if (['data', 'ts', 'balances', 'margin_balances', 'daily_pnl', 'daily_pnl_pct', 'fees', 'positions'].includes(ex)) return;
                if (!KNOWN_EXCHANGES.includes(ex)) KNOWN_EXCHANGES.push(ex);
            });
        }
        
        renderExchangeFilters();
        renderTable();

        // 3. Last update
        lastUpdate && (lastUpdate.textContent = fmtTime(json.snapshot?.ts || Date.now()));
        setStatus('ok', `${scannerState.snapshot.length} pairs above ${scannerState.threshold}%`);

        // 4. (Optional) External state updates if needed (e.g. active trades badge)
    } catch (e) { setStatus('error', `Error: ${e.message}`); }
}

// ─── Exchange filter bar (scanner) ────────────────────────────────────────────

function renderExchangeFilters() {
    const exchangeFiltersEl = refs.exchangeFilters;
    // Collect unique exchanges present in snapshot
    const inSnapshot = [...new Set(scannerState.snapshot.flatMap(r => [r.exchangeA, r.exchangeB]))];
    if (!inSnapshot.length) { 
        if (exchangeFiltersEl) exchangeFiltersEl.innerHTML = ''; 
        if (refs.tableBody) refs.tableBody.innerHTML = '<tr class="placeholder-row"><td colspan="7">No pairs match your active bot volume thresholds</td></tr>';
        if (pairCount) pairCount.textContent = '0 pairs';
        return; 
    }
    if (!exchangeFiltersEl) return;

    exchangeFiltersEl.innerHTML = '';

    // One pill per exchange
    for (const ex of inSnapshot) {
        const color = EXCHANGE_COLORS[ex] || '#94a3b8';
        const active = scannerState.activeExchanges.has(ex);
        const btn = document.createElement('button');
        btn.className = 'ex-filter-btn' + (active ? '' : ' ex-off');
        btn.style.cssText = `color:${color};border-color:${color}`;
        btn.innerHTML = `<span class="dot-icon" style="background:${color}"></span>${ex}`;
        btn.title = active ? `Hide ${ex}` : `Show ${ex}`;
        btn.addEventListener('click', () => {
            if (scannerState.activeExchanges.has(ex)) scannerState.activeExchanges.delete(ex);
            else scannerState.activeExchanges.add(ex);
            renderExchangeFilters();
            renderTable();
        });
        exchangeFiltersEl.appendChild(btn);
    }

    // Shortcut: All / None
    const sep = document.createElement('span'); sep.className = 'ex-filter-sep';
    exchangeFiltersEl.appendChild(sep);

    const makeCtrl = (label, onClick) => {
        const b = document.createElement('button');
        b.className = 'ex-filter-ctrl'; b.textContent = label;
        b.addEventListener('click', onClick); return b;
    };
    exchangeFiltersEl.appendChild(makeCtrl('All', () => {
        inSnapshot.forEach(ex => scannerState.activeExchanges.add(ex));
        renderExchangeFilters(); renderTable();
    }));
    exchangeFiltersEl.appendChild(makeCtrl('None', () => {
        inSnapshot.forEach(ex => scannerState.activeExchanges.delete(ex));
        renderExchangeFilters(); renderTable();
    }));
}

// ─── Table ────────────────────────────────────────────────────────────────────
function renderTable() {
    if (!refs.tableBody) return;

    // Only rows where BOTH exchanges are active in the filter
    let filtered = scannerState.snapshot.filter(r =>
        scannerState.activeExchanges.has(r.exchangeA) && scannerState.activeExchanges.has(r.exchangeB)
    );

    // Filter only by disabled exchanges (min/max spread filtering is for trade signals only, not scanner)
    const currentProfile = (typeof profiles !== 'undefined' && typeof activeTabId !== 'undefined') 
        ? profiles.find(p => p.id === activeTabId) 
        : null;
    
    if (currentProfile) {
        let disabled = [];
        try { disabled = typeof currentProfile.disabled_exchanges === 'string' ? JSON.parse(currentProfile.disabled_exchanges) : (currentProfile.disabled_exchanges || []); } catch(e){}
        const minVol = (parseFloat(currentProfile.min_volume_millions) || 0) * 1_000_000;
        filtered = filtered.filter(r => {
            if (disabled.includes(r.exchangeA) || disabled.includes(r.exchangeB)) return false;
            if (minVol > 0 && ((r.vol_a && r.vol_a < minVol) || (r.vol_b && r.vol_b < minVol))) return false;
            return true;
        });
    }

    const minProfit = (parseFloat(currentProfile?.min_entry_spread) || 0);
    const maxSpread = (parseFloat(currentProfile?.max_entry_spread) || 100);

    const sorted = [...filtered]
        .map(r => { r.profit_pct = r.spread - Math.abs(r.structural_spread || 0); return r; })
        .filter(r => r.profit_pct >= minProfit && r.profit_pct <= maxSpread)
        .sort((a, b) => {
            const av = a[scannerState.sortKey] ?? 0, bv = b[scannerState.sortKey] ?? 0;
            return typeof av === 'string' ? av.localeCompare(bv) * scannerState.sortDir : (av - bv) * scannerState.sortDir;
        });
    const verifiedCount = sorted.filter(r => r.structural_verified !== false).length;
    pairCount && (pairCount.textContent = verifiedCount < sorted.length ? `${verifiedCount} tradeable / ${sorted.length} pairs` : `${sorted.length} pairs`);
    refs.tableBody.innerHTML = '';
    if (!sorted.length) {
    refs.tableBody.innerHTML = `<tr class="placeholder-row"><td colspan="9">No pairs above threshold</td></tr>`;
        return;
    }
    for (const row of sorted) {
        const key = rowKey(row);
        const tr = document.createElement('tr');
        if (key === scannerState.activeRowKey) tr.classList.add('active-row');
        const isVerified = row.structural_verified !== false; // default true if missing
        if (!isVerified) tr.style.opacity = '0.5';
        const sc = row.spread >= 0.5 ? 'spread--high' : row.spread >= 0.2 ? 'spread--medium' : 'spread--low';
        
        let scStruc = '';
        const deviation = row.spread - row.structural_spread;
        if (row.has_structural_spread) {
            scStruc = deviation >= 0.5 ? 'spread--high' : deviation >= 0.2 ? 'spread--medium' : 'spread--low';
        }
        const strucTooltip = `Profit: ${fmtSpread(row.profit_pct)} (spread ${fmtSpread(row.spread)} - structural ${fmtSpread(row.structural_spread)})`;
        
        // Profit already computed in filter stage (structural always ≥ 0 from backend)
        const profitPct = row.profit_pct;
        const profitClass = profitPct >= 0.5 ? 'spread--high' : profitPct >= 0.2 ? 'spread--medium' : 'spread--low';
        const profitStyle = profitPct < 0 ? 'color:var(--red);opacity:0.6' : '';

        const verifiedBadge = isVerified ? '' : '<span title="EMA warming up — not tradeable yet" style="cursor:help;margin-left:4px;">⏳</span>';

        tr.innerHTML = `
          <td>${row.pair}${verifiedBadge}</td><td>${row.exchangeA}</td><td>${row.exchangeB}</td>
          <td>${fmtPrice(row.priceA)}</td><td>${fmtPrice(row.priceB)}</td>
          <td class="spread-cell ${sc}">${fmtSpread(row.spread)}</td>
          <td class="spread-cell ${scStruc}" title="${strucTooltip}">${row.has_structural_spread ? fmtSpread(row.structural_spread) : '<span style="color:var(--text-sub);">~' + fmtSpread(row.structural_spread) + '</span>'}</td>
          <td class="spread-cell ${profitClass}" style="${profitStyle}">${fmtSpread(profitPct)}</td>
          <td class="dir-cell">${row.direction || '\u2013'}</td>`;
        tr.addEventListener('click', () => openModal(row));
        refs.tableBody.appendChild(tr);
    }
}

document.addEventListener('click', e => {
    const th = e.target.closest('thead th[data-sort]');
    if (!th) return;
    const key = th.dataset.sort;
    document.querySelectorAll('thead th').forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
    scannerState.sortDir = (scannerState.sortKey === key) ? scannerState.sortDir * -1 : -1;
    scannerState.sortKey = key;
    th.classList.add(scannerState.sortDir === -1 ? 'sorted-desc' : 'sorted-asc');
    renderTable();
});

// ─── Modal ────────────────────────────────────────────────────────────────────
window.openModal = async function(row) {
    if (!modalTitle) {
        const $ = (id) => document.getElementById(id);
        modalTitle = $('modal-title');
        modalSub = $('modal-sub');
        modalBackdrop = $('modal-backdrop');
        wsLabelA = $('ws-label-a');
        wsLabelB = $('ws-label-b');
        wsStatus = $('ws-status');
        legendDyn = $('legend-dynamic');
        svgP = $('chart-prices');
        svgH = $('chart-hist');
        priceLinesG = $('price-lines');
        chartLegend = $('chart-legend');
        const modalClose = $('modal-close');
        if (modalClose) modalClose.onclick = () => window.closeModal();
    }

    if (scannerState.isModalOpen) return;
    scannerState.isModalOpen = true;
    scannerState.activeRowKey = rowKey(row);
    scannerState.activeRow = row;
    scannerState.klines = {};
    scannerState.liveTicks = {};
    // Ensure the row's exchanges are in KNOWN_EXCHANGES (critical when opening
    // from Active Trades without ever visiting the Scanner tab)
    if (row.exchangeA && !KNOWN_EXCHANGES.includes(row.exchangeA)) KNOWN_EXCHANGES.push(row.exchangeA);
    if (row.exchangeB && !KNOWN_EXCHANGES.includes(row.exchangeB)) KNOWN_EXCHANGES.push(row.exchangeB);
    // By default show only the pair's two exchanges; hide all others among enabled
    const enabledEx = getEnabledExchanges();
    scannerState.hiddenExchanges = new Set(
        enabledEx.filter(ex => ex !== row.exchangeA && ex !== row.exchangeB)
    );

    modalTitle.textContent = row.pair;
    updateModalSub();
    updateTVLink(row);
    updateVolumeInfo(row);
    if (wsLabelA) wsLabelA.textContent = row.exchangeA;
    if (wsLabelB) wsLabelB.textContent = row.exchangeB;

    modalBackdrop.classList.remove('hidden');
    renderTable();

    loadChartData(row).then(() => connectWS(row));
}

function updateModalSub() {
    const row = scannerState.activeRow;
    if (!row) return;
    const exFound = Object.keys(scannerState.klines).join(' / ') || '…';
    modalSub.textContent = `${row.pair} · ${exFound} · ${scannerState.timeframeSec / 60}m · spread: ${row.exchangeA} vs ${row.exchangeB}`;
}

function updateTVLink(row) {
    const tvLink = $('tv-link');
    if (!tvLink || !row) return;

    const exA = TV_EXCHANGES[row.exchangeA] || row.exchangeA.toUpperCase();
    const exB = TV_EXCHANGES[row.exchangeB] || row.exchangeB.toUpperCase();
    const ticker = row.pair.toUpperCase() + '.P';

    const formula = `((${exA}:${ticker} - ${exB}:${ticker}) / ${exB}:${ticker}) * 100`;
    tvLink.href = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(formula)}`;
}

function updateVolumeInfo(row) {
    const volEl = $('chart-volume-info');
    if (!volEl || !row) return;

    const exA = row.exchangeA;
    const exB = row.exchangeB;

    // Try to find volume in snapshot if current row has 0
    let vA = row.volA;
    let vB = row.volB;
    if ((!vA || !vB) && scannerState.snapshot.length > 0) {
        // Find ANY row for this pair to extract volume for exA and exB
        const allForPair = scannerState.snapshot.filter(r => r.pair === row.pair);
        if (!vA) {
            const rowWithA = allForPair.find(r => r.exchangeA === exA || r.exchangeB === exA);
            if (rowWithA) vA = (rowWithA.exchangeA === exA) ? rowWithA.volA : rowWithA.volB;
        }
        if (!vB) {
            const rowWithB = allForPair.find(r => r.exchangeA === exB || r.exchangeB === exB);
            if (rowWithB) vB = (rowWithB.exchangeA === exB) ? rowWithB.volA : rowWithB.volB;
        }
    }

    const fmtVol = v => {
        if (v == null || isNaN(v) || v <= 0) return 'N/A';
        if (v >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
        if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
        if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'k';
        return '$' + Number(v).toFixed(0);
    };

    const colA = EXCHANGE_COLORS[exA] || '#60a5fa';
    const colB = EXCHANGE_COLORS[exB] || '#34d399';

    volEl.innerHTML = `
        <div class="volume-item">
            <span class="dot-icon" style="background:${colA}"></span>
            <span>${exA} 24h Vol:</span>
            <span class="volume-val">${fmtVol(vA)}</span>
        </div>
        <div class="volume-item">
            <span class="dot-icon" style="background:${colB}"></span>
            <span>${exB} 24h Vol:</span>
            <span class="volume-val">${fmtVol(vB)}</span>
        </div>
    `;
}

window.closeModal = function() {
    if (!scannerState.isModalOpen) return;
    scannerState.isModalOpen = false;
    scannerState.loadKey = null;
    scannerState.activeRowKey = null;
    scannerState.activeRow = null;
    scannerState.klines = {};
    scannerState.liveTicks = {};
    scannerState.hiddenExchanges = new Set();
    modalBackdrop.classList.add('hidden');

    if (document.body.classList.contains('iframe-mode')) {
        window.parent.postMessage({ type: 'CLOSE_CHART_MODAL' }, '*');
    }

    disconnectWS(); clearCharts(); renderTable();
}
document.addEventListener('DOMContentLoaded', () => {
    const mc = $('modal-close');
    if (mc) mc.addEventListener('click', closeModal);

    const mb = $('modal-backdrop');
    if (mb) mb.addEventListener('click', e => { if (e.target === mb) closeModal(); });

    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

    // ─── Timeframe buttons ────────────────────────────────────────────────────────
    document.querySelectorAll('.tf-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('tf-btn--active'));
            btn.classList.add('tf-btn--active');
            scannerState.timeframeSec = parseInt(btn.dataset.tf, 10);
            if (scannerState.activeRow) loadChartData(scannerState.activeRow);
        });
    });
});

// ─── Chart rendering ──────────────────────────────────────────────────────────
function clearCharts() {
    priceLinesG.innerHTML = '';
    svgP.querySelectorAll('.inline-label').forEach(e => e.remove());
    ['grid-p', 'ylabel-p', 'bars-hist', 'bars-live', 'grid-h', 'ylabel-h', 'xlabel-h']
        .forEach(id => { const el = $(id); if (el) el.innerHTML = ''; });
    if (legendDyn) legendDyn.innerHTML = '';
}

function drawCharts() {
    const klines = scannerState.klines;
    const live = scannerState.liveTicks;
    const row = scannerState.activeRow;
    if (!row) return;

    // Only consider visible exchanges for X and Y domain
    const visibleEx = Object.keys(klines).filter(ex => !scannerState.hiddenExchanges.has(ex));

    const allTs = [
        ...visibleEx.flatMap(ex => (klines[ex] || []).map(p => p.ts)),
        ...visibleEx.flatMap(ex => (live[ex] || []).map(p => p.ts)),
    ];
    if (!allTs.length) return;

    const dataMinT = Math.min(...allTs);
    const dataMaxT = Math.max(...allTs) || dataMinT + 1;
    
    // Use user-defined zoom bounds if present, otherwise default to full data
    if (scannerState.visibleMinT === null) scannerState.visibleMinT = dataMinT;
    if (scannerState.visibleMaxT === null) scannerState.visibleMaxT = dataMaxT;
    
    // Strict clamp so we can't pan into the future past live data
    if (scannerState.visibleMaxT > dataMaxT + 60000) {
        const span = scannerState.visibleMaxT - scannerState.visibleMinT;
        scannerState.visibleMaxT = dataMaxT + 60000;
        scannerState.visibleMinT = scannerState.visibleMaxT - span;
    }

    const minT = scannerState.visibleMinT;
    const maxT = scannerState.visibleMaxT;

    drawPricePanel(klines, live, minT, maxT, row, dataMinT);
    drawHistPanel(klines, live, minT, maxT, row);
    updateLegend(klines, row);
    updateModalSub();
}

// ─── Price panel ──────────────────────────────────────────────────────────────
function drawPricePanel(klines, live, minT, maxT, row, dataMinT) {
    const { W, H } = svgDims(svgP);
    const iW = W - PAD.left - PAD.right;
    const iH = H - PAD.top - PAD.bottom;

    // Determine visible exchanges first (needed for Y scale AND drawing)
    const highlight = new Set([row.exchangeA, row.exchangeB]);
    const allEx = Object.keys(klines).filter(ex => !scannerState.hiddenExchanges.has(ex));

    // Y range using ONLY visible exchanges and visible TIME WINDOW
    const allVals = [
        ...allEx.flatMap(ex => (klines[ex] || []).filter(p => p.ts >= minT && p.ts <= maxT).map(p => p.close)),
        ...allEx.flatMap(ex => (live[ex] || []).filter(p => p.ts >= minT && p.ts <= maxT).map(p => p.close)),
    ].filter(v => v > 0);
    if (!allVals.length) {
        const gridEl = $('grid-p'), ylEl = $('ylabel-p');
        if (gridEl) gridEl.innerHTML = '';
        if (ylEl) ylEl.innerHTML = '';
        if (priceLinesG) {
            priceLinesG.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle" fill="#64748b" font-size="12" font-family="sans-serif">Fetching klines for ${row.exchangeA} / ${row.exchangeB}…</text>`;
        }
        return;
    }

    let minV = Math.min(...allVals), maxV = Math.max(...allVals);
    const rng = (maxV - minV) * 0.1 || maxV * 0.02 || 0.000001;
    minV -= rng; maxV += rng;

    const xOf = ts => PAD.left + ((ts - minT) / (maxT - minT || 1)) * iW;
    const yOf = v => PAD.top + (1 - (v - minV) / (maxV - minV)) * iH;

    // Grid + Y labels
    const gridEl = $('grid-p'), ylEl = $('ylabel-p');
    gridEl.innerHTML = ''; ylEl.innerHTML = '';
    for (let i = 0; i <= 5; i++) {
        const v = minV + (maxV - minV) * (i / 5);
        const y = yOf(v);
        gridEl.appendChild(svgEl('line', { x1: PAD.left, y1: y, x2: PAD.left + iW, y2: y }));
        const t = svgEl('text', { x: PAD.left - 5, y: y + 3, 'text-anchor': 'end' });
        t.textContent = fmtPrice(v);
        ylEl.appendChild(t);
    }

    // Clear and redraw price lines
    priceLinesG.innerHTML = '';

    // Highlighted exchanges (from the selected row) — already computed above

    // Draw non-highlighted first (background), then highlighted on top.
    // For the pair's two highlighted exchanges: draw exchangeB first, exchangeA last (on top)
    // with slightly different stroke-widths so both lines are visible even when prices overlap.
    const highlightedSorted = allEx.filter(ex => highlight.has(ex)).sort((a, b) => {
        // exchangeA should be drawn LAST (on top)
        if (a === row.exchangeA) return 1;
        if (b === row.exchangeA) return -1;
        return 0;
    });
    const drawOrder = [
        ...allEx.filter(ex => !highlight.has(ex)),
        ...highlightedSorted,
    ];

    for (const ex of drawOrder) {
        const kl = klines[ex] || [];
        const lk = live[ex] || [];
        const color = EXCHANGE_COLORS[ex] || '#94a3b8';
        const isHL = highlight.has(ex);
        // For the pair's two exchanges: use different stroke-widths so both are visible
        // when prices overlap. exchangeB (drawn first) is thicker + semi-transparent,
        // exchangeA (drawn on top) is thinner + fully opaque.
        let sw, opac;
        if (!isHL) {
            sw = 1.2; opac = '0.45';
        } else if (ex === row.exchangeB) {
            sw = 2.8; opac = '0.7';  // thicker, semi-transparent (drawn first / behind)
        } else {
            sw = 1.8; opac = '1';    // thinner, fully opaque (drawn on top)
        }

        // History line
        const ptHist = kl.filter(p => p.close > 0)
            .map(p => `${xOf(p.ts).toFixed(1)},${yOf(p.close).toFixed(1)}`).join(' ');
        if (ptHist) {
            priceLinesG.appendChild(svgEl('polyline', {
                points: ptHist, fill: 'none',
                stroke: color, 'stroke-width': sw, opacity: opac,
                'stroke-linecap': 'round', 'stroke-linejoin': 'round',
            }));
        }

        // Live ticks (dashed)
        const ptLive = lk.filter(p => p.close > 0)
            .map(p => `${xOf(p.ts).toFixed(1)},${yOf(p.close).toFixed(1)}`).join(' ');
        if (ptLive) {
            priceLinesG.appendChild(svgEl('polyline', {
                points: ptLive, fill: 'none',
                stroke: color, 'stroke-width': sw, opacity: opac,
                'stroke-dasharray': '4 2', 'stroke-linecap': 'round',
            }));
        }
    }

    // ─── Trade Event Markers (ENTRY / EXIT) ───
    if (row && row.entryTime && row.entryTime >= minT && row.entryTime <= maxT) {
        const xEntry = xOf(row.entryTime);
        priceLinesG.appendChild(svgEl('line', {
            x1: xEntry, y1: PAD.top, x2: xEntry, y2: H - PAD.bottom,
            stroke: '#22c55e', 'stroke-width': 1.5, 'stroke-dasharray': '4 3', opacity: 0.9
        }));
        const badgeG = svgEl('g', {});
        const bgRect = svgEl('rect', {
            x: xEntry - 28, y: PAD.top + 4, width: 56, height: 18, rx: 4,
            fill: '#22c55e', opacity: 0.95
        });
        const labelTxt = svgEl('text', {
            x: xEntry, y: PAD.top + 16, 'text-anchor': 'middle', fill: '#000',
            'font-size': '10px', 'font-weight': '700', 'font-family': 'monospace'
        });
        labelTxt.textContent = 'ENTRY';
        badgeG.appendChild(bgRect);
        badgeG.appendChild(labelTxt);
        priceLinesG.appendChild(badgeG);
    }
    if (row && row.exitTime && row.exitTime >= minT && row.exitTime <= maxT) {
        const xExit = xOf(row.exitTime);
        priceLinesG.appendChild(svgEl('line', {
            x1: xExit, y1: PAD.top, x2: xExit, y2: H - PAD.bottom,
            stroke: '#ef4444', 'stroke-width': 1.5, 'stroke-dasharray': '4 3', opacity: 0.9
        }));
        const badgeG = svgEl('g', {});
        const bgRect = svgEl('rect', {
            x: xExit - 24, y: PAD.top + 4, width: 48, height: 18, rx: 4,
            fill: '#ef4444', opacity: 0.95
        });
        const labelTxt = svgEl('text', {
            x: xExit, y: PAD.top + 16, 'text-anchor': 'middle', fill: '#fff',
            'font-size': '10px', 'font-weight': '700', 'font-family': 'monospace'
        });
        labelTxt.textContent = 'EXIT';
        badgeG.appendChild(bgRect);
        badgeG.appendChild(labelTxt);
        priceLinesG.appendChild(badgeG);
    }

    // Overlay on top
    const ov = $('overlay-p');
    ov.setAttribute('x', 0); ov.setAttribute('y', 0);
    ov.setAttribute('width', W); ov.setAttribute('height', H);
    svgP.appendChild(ov);
}

// ─── Histogram panel (A vs B from selected row) ───────────────────────────────
function drawHistPanel(klines, live, minT, maxT, row) {
    const { W, H } = svgDims(svgH);
    const iW = W - PAD_H.left - PAD_H.right;
    const iH = H - PAD_H.top - PAD_H.bottom;

    const klA = klines[row.exchangeA] || [];
    const klB = klines[row.exchangeB] || [];

    // Bucket normalization (e.g. 60s for 1m) to match cross-exchange candle timestamps
    const bucketMs = (scannerState.timeframeSec || 60) * 1000;
    const norm = ts => Math.round(ts / bucketMs) * bucketMs;

    const mapB = new Map();
    for (const c of klB) {
        if (c.close > 0) mapB.set(norm(c.ts), c.close);
    }

    const histS = [];
    for (const c of klA) {
        if (c.close <= 0) continue;
        const nTs = norm(c.ts);
        const pB = mapB.get(nTs);
        if (pB != null && pB > 0) {
            histS.push({ ts: nTs, v: signedSpread(c.close, pB) });
        }
    }

    // Live: use liveTicks if available, else skip
    const liveA = live[row.exchangeA] || [];
    const liveB = live[row.exchangeB] || [];
    const mapLB = new Map();
    for (const c of liveB) {
        if (c.close > 0) mapLB.set(norm(c.ts), c.close);
    }
    const liveS = [];
    for (const c of liveA) {
        if (c.close <= 0) continue;
        const nTs = norm(c.ts);
        const pB = mapLB.get(nTs);
        if (pB != null && pB > 0) {
            liveS.push({ ts: nTs, v: signedSpread(c.close, pB) });
        }
    }

    const allS = [...histS, ...liveS];
    if (!allS.length) {
        const gridEl = $('grid-h'), ylEl = $('ylabel-h'), xlEl = $('xlabel-h');
        const bH = $('bars-hist'), bL = $('bars-live');
        if (gridEl) gridEl.innerHTML = '';
        if (ylEl) ylEl.innerHTML = '';
        if (xlEl) xlEl.innerHTML = '';
        if (bH) bH.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle" fill="#64748b" font-size="12" font-family="sans-serif">Spread histogram requires matching candle history from both exchanges</text>`;
        if (bL) bL.innerHTML = '';
        return;
    }

    const visibleVals = allS.filter(p => p.ts >= minT && p.ts <= maxT).map(p => p.v);
    if (!visibleVals.length) return;
    
    const absMax = Math.max(Math.abs(Math.min(...visibleVals)), Math.abs(Math.max(...visibleVals))) * 1.2 || 0.1;
    const minV = -absMax, maxV = absMax;

    const xOf = ts => PAD_H.left + ((ts - minT) / (maxT - minT || 1)) * iW;
    const yOf = v => PAD_H.top + (1 - (v - minV) / (maxV - minV)) * iH;
    const yZero = yOf(0);

    const gridEl = $('grid-h'), ylEl = $('ylabel-h'), xlEl = $('xlabel-h');
    gridEl.innerHTML = ''; ylEl.innerHTML = ''; xlEl.innerHTML = '';

    for (let i = 0; i <= 4; i++) {
        const v = minV + (maxV - minV) * (i / 4);
        const y = yOf(v);
        gridEl.appendChild(svgEl('line', { x1: PAD_H.left, y1: y, x2: PAD_H.left + iW, y2: y }));
        const t = svgEl('text', { x: PAD_H.left - 5, y: y + 3, 'text-anchor': 'end' });
        t.textContent = v.toFixed(3) + '%';
        ylEl.appendChild(t);
    }
    for (let i = 0; i <= 5; i++) {
        const ts = minT + (maxT - minT) * (i / 5);
        const t = svgEl('text', { x: xOf(ts), y: H - PAD_H.bottom + 14, 'text-anchor': 'middle' });
        t.textContent = fmtTime(ts);
        xlEl.appendChild(t);
    }

    const zl = $('zero-line');
    zl.setAttribute('x1', PAD_H.left); zl.setAttribute('y1', yZero);
    zl.setAttribute('x2', PAD_H.left + iW); zl.setAttribute('y2', yZero);

    let barW = 4;
    if (allS.length > 1) {
        barW = Math.max(2, Math.min(18, (iW / (allS.length - 1)) * 0.7));
    }

    const colA = EXCHANGE_COLORS[row.exchangeA] || '#60a5fa';
    const colB = EXCHANGE_COLORS[row.exchangeB] || '#34d399';

    const barsHist = $('bars-hist'); barsHist.innerHTML = '';
    for (const p of histS) {
        const x = xOf(p.ts) - barW / 2;
        const y = p.v >= 0 ? yOf(p.v) : yZero;
        const h = Math.max(1, Math.abs(yOf(p.v) - yZero));
        // Green uses exch A color (A > B), Red uses exch B color (B > A)
        barsHist.appendChild(svgEl('rect', {
            x, y, width: barW, height: h,
            fill: p.v >= 0 ? colA + 'cc' : colB + 'cc',
        }));
    }

    // Trade markers on histogram
    if (row && row.entryTime && row.entryTime >= minT && row.entryTime <= maxT) {
        const xEntry = xOf(row.entryTime);
        barsHist.appendChild(svgEl('line', {
            x1: xEntry, y1: PAD_H.top, x2: xEntry, y2: H - PAD_H.bottom,
            stroke: '#22c55e', 'stroke-width': 1.5, 'stroke-dasharray': '4 3', opacity: 0.8
        }));
    }
    if (row && row.exitTime && row.exitTime >= minT && row.exitTime <= maxT) {
        const xExit = xOf(row.exitTime);
        barsHist.appendChild(svgEl('line', {
            x1: xExit, y1: PAD_H.top, x2: xExit, y2: H - PAD_H.bottom,
            stroke: '#ef4444', 'stroke-width': 1.5, 'stroke-dasharray': '4 3', opacity: 0.8
        }));
    }

    const barsLive = $('bars-live'); barsLive.innerHTML = '';
    for (const p of liveS) {
        const x = xOf(p.ts) - barW / 2;
        const y = p.v >= 0 ? yOf(p.v) : yZero;
        const h = Math.max(1, Math.abs(yOf(p.v) - yZero));
        barsLive.appendChild(svgEl('rect', {
            x, y, width: barW, height: h,
            fill: p.v >= 0 ? colA : colB,
        }));
    }

    const ov = $('overlay-h');
    ov.setAttribute('x', 0); ov.setAttribute('y', 0);
    ov.setAttribute('width', W); ov.setAttribute('height', H);

    // Draw Entry/Exit Markers if present via URL params
    const drawMarker = (ts, label, color, yOffset) => {
        if (!ts || ts < minT || ts > maxT) return;
        const x = xOf(ts);
        barsHist.appendChild(svgEl('line', { x1: x, y1: PAD_H.top, x2: x, y2: H - PAD_H.bottom, stroke: color, 'stroke-dasharray': '4 4', 'stroke-width': 1.5, opacity: 0.8 }));
        const t = svgEl('text', { x: x + 6, y: PAD_H.top + yOffset, fill: color, 'font-size': '11px', 'font-weight': 'bold' });
        t.textContent = label;
        barsHist.appendChild(t);
    };

    if (window.targetEntryTime) drawMarker(window.targetEntryTime, 'ENTRY', '#eab308', 12);
    if (window.targetExitTime) drawMarker(window.targetExitTime, 'EXIT', '#f43f5e', 26);
}

// ─── Chart legend (clickable per-exchange toggles) ────────────────────────────────────
// chartLegend is assigned in initRefs()
function updateLegend(klines, row) {
    if (!chartLegend) return;
    chartLegend.innerHTML = '';

    // Exchange toggle buttons
    const allEx = new Set([...Object.keys(klines), row.exchangeA, row.exchangeB]);
    for (const ex of allEx) {
        const color = EXCHANGE_COLORS[ex] || '#94a3b8';
        const isHL = ex === row.exchangeA || ex === row.exchangeB;
        const hidden = scannerState.hiddenExchanges.has(ex);
        const hasData = (klines[ex] || []).length > 0;

        const btn = document.createElement('button');
        btn.className = 'chart-legend-btn' + (hidden ? ' hidden-ex' : '');
        btn.style.cssText = `color:${color};border-color:${color};font-weight:${isHL ? 700 : 400}`;
        if (!hasData) btn.style.opacity = '0.35';
        
        btn.title = hidden ? `Show ${ex}` : `Hide ${ex}`;
        if (!hasData) btn.title += ' (No data)';
        
        btn.innerHTML = `<span class="dot-icon" style="background:${color}"></span>${ex}`;
        btn.addEventListener('click', () => {
            if (scannerState.hiddenExchanges.has(ex)) scannerState.hiddenExchanges.delete(ex);
            else scannerState.hiddenExchanges.add(ex);
            drawCharts();
        });
        chartLegend.appendChild(btn);
    }

    // ─ Control buttons: Show all / Only pair ─────────────────────────────────
    const pairSet = new Set([row.exchangeA, row.exchangeB]);
    const allKeys = Object.keys(klines);
    const hasHidden = allKeys.some(ex => !pairSet.has(ex) && scannerState.hiddenExchanges.has(ex));

    const makeCtrl = (label, title, onClick) => {
        const b = document.createElement('button');
        b.className = 'chart-legend-btn';
        b.style.cssText = 'color:var(--text-sub);border-color:var(--border);font-weight:400;font-size:.68rem';
        b.title = title;
        b.textContent = label;
        b.addEventListener('click', onClick);
        return b;
    };

    if (hasHidden) {
        chartLegend.appendChild(makeCtrl('+ show all', 'Show all exchanges', () => {
            scannerState.hiddenExchanges.clear();
            drawCharts();
        }));
    } else if (allKeys.length > 2) {
        chartLegend.appendChild(makeCtrl('− only pair', 'Hide non-pair exchanges', () => {
            scannerState.hiddenExchanges = new Set(allKeys.filter(ex => !pairSet.has(ex)));
            drawCharts();
        }));
    }

    // ─ Separator + spread direction labels ─────────────────────────────────
    if (row) {
        const sep = document.createElement('span');
        sep.className = 'chart-legend-sep';
        chartLegend.appendChild(sep);

        const colA = EXCHANGE_COLORS[row.exchangeA] || '#60a5fa';
        const colB = EXCHANGE_COLORS[row.exchangeB] || '#34d399';
        [[colA, `▲ ${row.exchangeA} > ${row.exchangeB}`],
        [colB, `▼ ${row.exchangeB} > ${row.exchangeA}`]].forEach(([col, txt]) => {
            const s = document.createElement('span');
            s.className = 'chart-legend-label';
            s.style.color = col;
            s.textContent = txt;
            chartLegend.appendChild(s);
        });
    }

    if (legendDyn) legendDyn.innerHTML = '';
}

// ResizeObserver
const ro = new ResizeObserver(() => { if (scannerState.activeRow) drawCharts(); });
ro.observe(document.body);

// ─── SVG Interaction (Zoom & Pan) ──────────────────────────────────────────────
async function loadOlderHistory() {
    if (scannerState.isLoadingHistory || !scannerState.activeRow) return;
    scannerState.isLoadingHistory = true;
    
    // Find the absolute minimum loaded timestamp
    const visibleEx = Object.keys(scannerState.klines).filter(ex => !scannerState.hiddenExchanges.has(ex));
    const allTs = visibleEx.flatMap(ex => (scannerState.klines[ex] || []).map(p => p.ts));
    const absoluteMinT = allTs.length ? Math.min(...allTs) : scannerState.visibleMinT;
    
    // Fetch older data for enabled exchanges
    const enabledEx = getEnabledExchanges();
    const results = await Promise.all(
        enabledEx.map(ex => fetchKlines(ex, scannerState.activeRow.pair, scannerState.timeframeSec, absoluteMinT)
            .then(kl => [ex, kl])
        )
    );
    
    let added = false;
    for (const [ex, kl] of results) {
        if (kl.length > 0) {
            // merge and sort
            const existing = scannerState.klines[ex] || [];
            const merged = [...kl, ...existing].sort((a,b) => a.ts - b.ts);
            // remove duplicates just in case
            scannerState.klines[ex] = merged.filter((item, pos, ary) => !pos || item.ts !== ary[pos - 1].ts);
            added = true;
        }
    }
    
    if (added) drawCharts();
    
    // Release lock after a short delay to prevent spamming
    setTimeout(() => { scannerState.isLoadingHistory = false; }, 1000);
}

function handleChartZoom(e) {
    if (!scannerState.visibleMinT || !scannerState.visibleMaxT) return;
    e.preventDefault();
    
    const { W } = svgDims(svgP);
    const iW = W - PAD.left - PAD.right;
    
    // where is the mouse horizontally inside the graph area?
    const rect = svgP.getBoundingClientRect();
    const mouseX = e.clientX - rect.left - PAD.left;
    const ratio = Math.max(0, Math.min(1, mouseX / iW));
    
    const span = scannerState.visibleMaxT - scannerState.visibleMinT;
    const focalTime = scannerState.visibleMinT + span * ratio;
    
    // zoom factor
    const zoom = e.deltaY > 0 ? 1.2 : 0.8;
    const newSpan = span * zoom;
    
    // Min zoom out: max 14 days, Min zoom in: 10 minutes (for 1m timeframe)
    if (newSpan > 14 * 24 * 3600 * 1000 || newSpan < 10 * 60 * 1000) return;
    
    scannerState.visibleMinT = focalTime - newSpan * ratio;
    scannerState.visibleMaxT = focalTime + newSpan * (1 - ratio);
    
    drawCharts();
}

function handleChartPanStart(e) {
    if (e.button !== 0) return; // Only left click
    scannerState.isDragging = true;
    scannerState.lastDragX = e.clientX;
    svgP.style.cursor = 'grabbing';
    svgH.style.cursor = 'grabbing';
}

function handleChartPanMove(e) {
    if (!scannerState.isDragging || !scannerState.visibleMinT) return;
    
    const dx = e.clientX - scannerState.lastDragX;
    scannerState.lastDragX = e.clientX;
    
    const { W } = svgDims(svgP);
    const iW = W - PAD.left - PAD.right;
    const span = scannerState.visibleMaxT - scannerState.visibleMinT;
    
    const dt = -(dx / iW) * span;
    
    scannerState.visibleMinT += dt;
    scannerState.visibleMaxT += dt;
    
    drawCharts();
    
    // If we've panned within 20% of the left edge, load more history
    const visibleEx = Object.keys(scannerState.klines).filter(ex => !scannerState.hiddenExchanges.has(ex));
    const allTs = visibleEx.flatMap(ex => (scannerState.klines[ex] || []).map(p => p.ts));
    const absoluteMinT = allTs.length ? Math.min(...allTs) : scannerState.visibleMinT;
    
    if (scannerState.visibleMinT < absoluteMinT + (span * 0.2)) {
        loadOlderHistory();
    }
}

function handleChartPanEnd() {
    scannerState.isDragging = false;
    if (svgP) svgP.style.cursor = 'crosshair';
    if (svgH) svgH.style.cursor = 'crosshair';
}

let crosshairP = { x: null, y: null };
let crosshairH = { x: null, y: null };

document.addEventListener('DOMContentLoaded', () => {
    const p = $('chart-prices');
    const h = $('chart-hist');
    if (p) {
        p.addEventListener('wheel', handleChartZoom, { passive: false });
        p.addEventListener('mousedown', handleChartPanStart);
    }
    if (h) {
        h.addEventListener('wheel', handleChartZoom, { passive: false });
        h.addEventListener('mousedown', handleChartPanStart);
    }

    ['p', 'h'].forEach(id => {
        const svg = id === 'p' ? p : h;
        if (!svg) return;
        const overlay = $(`overlay-${id}`);
        // Use insertBefore to put crosshairs right above the overlay rect
        const xLine = svgEl('line', { class: 'crosshair', 'stroke-dasharray': '2 2', stroke: 'rgba(255,255,255,0.2)', 'stroke-width': 1, 'pointer-events': 'none', display: 'none' });
        const yLine = svgEl('line', { class: 'crosshair', 'stroke-dasharray': '2 2', stroke: 'rgba(255,255,255,0.2)', 'stroke-width': 1, 'pointer-events': 'none', display: 'none' });
        
        if (overlay) {
            svg.insertBefore(xLine, overlay);
            svg.insertBefore(yLine, overlay);
        } else {
            svg.appendChild(xLine);
            svg.appendChild(yLine);
        }
        
        if (id === 'p') {
            crosshairP.x = xLine; crosshairP.y = yLine;
        } else {
            crosshairH.x = xLine; crosshairH.y = yLine;
        }
        
        svg.addEventListener('mousemove', e => {
            if (scannerState.isDragging) return;
            const ch = id === 'p' ? crosshairP : crosshairH;
            if (!ch.x || !ch.y) return;
            const rect = svg.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            ch.x.setAttribute('x1', x); ch.x.setAttribute('y1', 0);
            ch.x.setAttribute('x2', x); ch.x.setAttribute('y2', rect.height);
            ch.y.setAttribute('x1', 0); ch.y.setAttribute('y1', y);
            ch.y.setAttribute('x2', rect.width); ch.y.setAttribute('y2', y);
            ch.x.style.display = 'block';
            ch.y.style.display = 'block';
        });
        
        svg.addEventListener('mouseleave', () => {
            const ch = id === 'p' ? crosshairP : crosshairH;
            if (!ch.x || !ch.y) return;
            ch.x.style.display = 'none';
            ch.y.style.display = 'none';
        });
    });

    window.addEventListener('mousemove', handleChartPanMove);
    window.addEventListener('mouseup', handleChartPanEnd);
});


// ─── WebSocket ────────────────────────────────────────────────────────────────
function wsConfig(exchange, pair) {
    const sym = pair.toLowerCase();
    if (exchange === 'binance') {
        return {
            url: `wss://fstream.binance.com/ws/${sym}@markPrice`,
            extract: msg => { const d = JSON.parse(msg.data); return d.p ? parseFloat(d.p) : null; },
        };
    } else if (exchange === 'bybit') {
        return {
            url: `wss://stream.bybit.com/v5/public/linear`,
            onOpen: ws => ws.send(JSON.stringify({ op: 'subscribe', args: [`tickers.${pair.toUpperCase()}`] })),
            extract: msg => { const d = JSON.parse(msg.data); const p = d?.data?.lastPrice; return p ? parseFloat(p) : null; },
        };
    } else if (exchange === 'okx') {
        const instId = pair.replace(/^([A-Z0-9]+)(USDT)$/, '$1-USDT-SWAP');
        return {
            url: `wss://ws.okx.com:8443/ws/v5/public`,
            onOpen: ws => ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'mark-price', instId }] })),
            extract: msg => { const d = JSON.parse(msg.data); const p = d?.data?.[0]?.markPx; return p ? parseFloat(p) : null; },
        };
    }
    return null;
}

function connectWS(row) {
    disconnectWS();
    const exTarget = scannerState.wsTarget === 'a' ? row.exchangeA : row.exchangeB;
    const cfg = wsConfig(exTarget, row.pair);
    if (!cfg) {
        if (wsStatus) wsStatus.className = 'ws-indicator ws-indicator--off';
        return;
    }
    let ws;
    try { ws = new WebSocket(cfg.url); } catch { if(wsStatus) wsStatus.className = 'ws-indicator ws-indicator--err'; return; }

    ws.onopen = () => { if(wsStatus) wsStatus.className = 'ws-indicator ws-indicator--on'; if (cfg.onOpen) cfg.onOpen(ws); };
    let wsDrawPending = false;
    ws.onmessage = e => {
        let price;
        try { price = cfg.extract(e); } catch { return; }
        if (!price || isNaN(price) || price <= 0) return;
        if (!scannerState.liveTicks[exTarget]) scannerState.liveTicks[exTarget] = [];
        scannerState.liveTicks[exTarget].push({ ts: Date.now(), close: price });
        const cutoff = Date.now() - 30 * 60_000;
        scannerState.liveTicks[exTarget] = scannerState.liveTicks[exTarget].filter(p => p.ts >= cutoff);
        // Throttle redraws to ~2fps max (WS can fire 30+/sec)
        if (!wsDrawPending) {
            wsDrawPending = true;
            requestAnimationFrame(() => { wsDrawPending = false; drawCharts(); });
        }
    };
    ws.onerror = () => { if(wsStatus) wsStatus.className = 'ws-indicator ws-indicator--err'; };
    ws.onclose = () => {
        if(wsStatus) wsStatus.className = 'ws-indicator ws-indicator--off';
        setTimeout(() => { if (scannerState.activeRow && scannerState.ws === ws) connectWS(scannerState.activeRow); }, 5_000);
    };
    scannerState.ws = ws;
}

function disconnectWS() {
    if(wsStatus) wsStatus.className = 'ws-indicator ws-indicator--off';
    if (scannerState.ws) { scannerState.ws.onclose = null; scannerState.ws.close(); scannerState.ws = null; }
}

document.querySelectorAll('input[name="ws-exchange"]').forEach(r => {
    r.addEventListener('change', () => { scannerState.wsTarget = r.value; if (scannerState.activeRow) connectWS(scannerState.activeRow); });
});

// ─── Controls ─────────────────────────────────────────────────────────────────
function scheduleRefresh() {
    if (scannerState.refreshTimer) clearInterval(scannerState.refreshTimer);
    scannerState.refreshTimer = setInterval(fetchSnapshot, scannerState.refreshMs);
}

/** Load min/max spread and refresh interval from the shared Trading Settings OR a specific profile. */
async function loadScannerSettings(profileId = null) {
    try {
        // 1. Try profile-specific settings first
        if (profileId) {
            const res = await authFetch(`${API_BASE}/api/profiles/${profileId}`);
            if (res.ok) {
                const data = await res.json();
                if (data && data.min_entry_spread != null) {
                    scannerState.threshold = parseFloat(data.min_entry_spread);
                    scannerState.maxSpread = parseFloat(data.max_entry_spread) || 10;
                    scannerState.refreshMs = Math.max(5, parseInt(data.collector_interval_secs) || 5) * 1000;
                    console.log(`Scanner: loaded profile ${profileId} settings (min: ${scannerState.threshold}%, max: ${scannerState.maxSpread}%)`);
                    return;
                }
            }
        }

        // 2. Fallback to global settings
        const gRes = await authFetch(`${API_BASE}/api/trading/settings`);
        if (gRes.ok) {
            const gData = await gRes.json();
            scannerState.threshold = parseFloat(gData.min_entry_spread) || 1;
            scannerState.maxSpread = parseFloat(gData.max_entry_spread) || 10;
            scannerState.refreshMs = Math.max(5, parseInt(gData.collector_interval_secs) || 5) * 1000;
            console.log(`Scanner: loaded global settings (min: ${scannerState.threshold}%)`);
        } else {
            console.warn('Scanner: global settings failed, using defaults');
        }
    } catch (e) {
        console.warn('Scanner: failed to load settings:', e);
    }
}

window.initScanner = async function(profileId = null) {
    console.log('Scanner: Initializing references');
    initRefs();
    
    // Always load latest settings for the active profile when switching to Scanner
    await loadScannerSettings(profileId);
    
    // Force immediate data fetch with new settings
    await fetchSnapshot();
    
    if (!window.scannerAutoRefreshTimer) {
        window.scannerAutoRefreshTimer = true;
        console.log('Scanner: Starting auto-refresh cycle');
    }
    
    // Always reschedule timer in case refresh interval changed
    scheduleRefresh();
};
