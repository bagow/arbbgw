/**
 * trading.js – Multi-tab trading UI
 *
 * Each "tab" is a trading profile (paper or real). Profiles are stored in D1 via
 * API calls. The active tab's data is polled every 10s; others are lazy-loaded.
 * Tab visibility/order is persisted in localStorage.
 */
'use strict';

const API_BASE = '';

// ─── State ────────────────────────────────────────────────────────────────────

let profiles = [];       // all profiles from server
let activeTabId = null;  // currently visible profile id
let pollTimer = null;
let lastUnified = null;  // cached /api/unified response
let activeSubTabId = 'scanner'; // Default to Scanner as requested

// Expanded trade rows per profile
const expandedRows = {};  // profileId -> Set of trade row ids

// Chart.js instances: destroyed and recreated on each render to avoid canvas reuse errors
const chartInstances = {};  // profileId -> { equity, exchange, winloss }

// ─── Boot ─────────────────────────────────────────────────────────────────────

async function boot() {
    await loadProfiles();
    // Restore last active tab from localStorage
    const saved = localStorage.getItem('activeTabId');
    if (saved && profiles.find(p => p.id === saved)) {
        activeTabId = saved;
    } else if (profiles.length) {
        activeTabId = profiles[0].id;
    }
    renderTabBar();
    if (activeTabId) await switchTab(activeTabId);
    startPoll();
}

// ─── API helpers ──────────────────────────────────────────────────────────────

async function apiFetch(path, opts = {}) {
    const res = await fetch(`${API_BASE}${path}`, opts);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}

async function loadProfiles() {
    try {
        profiles = await apiFetch('/api/profiles');
        for (const p of profiles) p.is_active = Boolean(p.is_active);
    } catch (e) {
        console.error('Failed to load profiles:', e);
        profiles = [];
    }
}

async function createProfile(name, mode) {
    const p = await apiFetch('/api/profiles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, mode }),
    });
    profiles.push(p);
    return p;
}

async function deleteProfile(id) {
    await apiFetch(`/api/profiles/${id}`, { method: 'DELETE' });
    profiles = profiles.filter(p => p.id !== id);
}

async function saveProfileSettings(id, settings) {
    await apiFetch(`/api/profiles/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
    });

    // Also update the canonical settings endpoint for the trader service
    const profile = profiles.find(p => p.id === id);
    if (!profile) return;
    const settingsPath = profile.mode === 'real'
        ? '/api/real-trader/settings'
        : '/api/trading/settings';
    await apiFetch(settingsPath, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
    });
}

// ─── Tab bar rendering ────────────────────────────────────────────────────────

function renderTabBar() {
    const bar = document.getElementById('tab-bar');
    const addBtn = document.getElementById('tab-add-btn');
    // Remove all tabs (keep add button)
    bar.querySelectorAll('.tab-item').forEach(el => el.remove());

    for (const p of profiles) {
        const tab = document.createElement('div');
        tab.className = 'tab-item' + (p.id === activeTabId ? ' tab-item--active' : '');
        tab.dataset.id = p.id;

        const badge = p.mode === 'real'
            ? '<span class="tab-badge tab-badge--real">💰</span>'
            : '<span class="tab-badge tab-badge--paper">🧪</span>';

        tab.innerHTML = `
            ${badge}
            <span class="tab-name">${escHtml(p.name)}</span>
            <button class="tab-close" data-id="${p.id}" title="Close tab">×</button>
        `;
        tab.addEventListener('click', (e) => {
            if (e.target.classList.contains('tab-close')) return;
            switchTab(p.id);
        });
        bar.insertBefore(tab, addBtn);
    }
}

// ─── Tab switching ────────────────────────────────────────────────────────────

async function switchTab(id) {
    activeTabId = id;
    localStorage.setItem('activeTabId', id);
    renderTabBar();
    const profile = profiles.find(p => p.id === id);
    if (!profile) return;
    renderPanel(profile, null, null, null, null);
    await refreshActiveTab();
}

// ─── Data polling ─────────────────────────────────────────────────────────────

function startPoll() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(refreshActiveTab, 10000);
}

async function refreshActiveTab() {
    try {
        const profile = profiles.find(p => p.id === activeTabId);
        const profileParam = profile ? `&profile_id=${encodeURIComponent(profile.id)}` : '';
        lastUnified = await apiFetch(`/api/unified${profileParam ? '?' + profileParam.slice(1) : ''}`);
    } catch (e) {
        console.error('Poll failed:', e);
        return;
    }
    const profile = profiles.find(p => p.id === activeTabId);
    if (!profile) return;

    const { trades, real, exchanges, profiles: serverProfiles, profile_stats } = lastUnified;

    // Sync local profile settings from server profiles data
    if (serverProfiles) {
        for (const sp of serverProfiles) {
            const local = profiles.find(p => p.id === sp.id);
            if (local) Object.assign(local, sp);
        }
    }

    const active  = profile.mode === 'real' ? (real?.active  || []) : (trades?.active  || []);
    const history = profile.mode === 'real' ? (real?.history || []) : (trades?.history || []);
    // Use per-profile stats if available, fall back to global
    const stats   = profile_stats || (profile.mode === 'real' ? (real?.stats || {}) : (trades?.stats || {}));

    renderPanel(profile, active, history, stats, exchanges);
}

// ─── Panel rendering ──────────────────────────────────────────────────────────

function renderPanel(profile, active, history, stats, exchanges) {
    const panel = document.getElementById('mt-panel');

    // ─── Polling Update: Only refresh dynamic data ──────────────────────────
    if (panel.dataset.activeProfileId === profile.id) {
        updateDynamicElements(profile, active, stats);
        if (active !== null) {
            renderActiveTable(profile.id, active);
            renderHistoryTable(profile.id, history);
            if (stats && stats.total_trades > 0) renderDashboardCharts(profile.id, stats);
        }
        return;
    }

    // ─── Profile Switch or Initial Load: Render full template ───────────────
    panel.dataset.activeProfileId = profile.id;

    // Destroy any existing Chart.js instances
    if (chartInstances[profile.id]) {
        Object.values(chartInstances[profile.id]).forEach(c => { try { c.destroy(); } catch {} });
        delete chartInstances[profile.id];
    }

    panel.innerHTML = `
        <div class="mt-subnav">
            <button class="mt-nav-btn ${activeSubTabId==='scanner'?'mt-nav-btn--active':''}" data-sub="scanner" onclick="showSubTab(this,'scanner')">Scanner</button>
            <button class="mt-nav-btn ${activeSubTabId==='dashboard'?'mt-nav-btn--active':''}" data-sub="dashboard" onclick="showSubTab(this,'dashboard')">Dashboard</button>
            <button class="mt-nav-btn ${activeSubTabId==='active'?'mt-nav-btn--active':''}" data-sub="active" onclick="showSubTab(this,'active')">Active Trades</button>
            <button class="mt-nav-btn ${activeSubTabId==='history'?'mt-nav-btn--active':''}" data-sub="history" onclick="showSubTab(this,'history')">History</button>
            <button class="mt-nav-btn ${activeSubTabId==='settings'?'mt-nav-btn--active':''}" data-sub="settings" onclick="showSubTab(this,'settings')">Settings</button>
            
            <div id="panel-stats-${profile.id}" style="margin-left:auto;display:flex;align-items:center;gap:10px;">
                ${renderSubNavStats(active, stats, profile.mode)}
            </div>
        </div>

        <!-- Scanner sub-panel -->
        <div class="mt-content" id="mt-sub-scanner" style="display:${activeSubTabId==='scanner'?'flex':'none'}; flex:1; min-height:0;">
            <iframe src="chart.html?profile_id=${profile.id}&iframe=1" style="width:100%; height:100%; border:none; background:var(--bg); display:block; flex:1;"></iframe>
        </div>

        <!-- Dashboard sub-panel -->
        <div class="mt-content" id="mt-sub-dashboard" style="display:${activeSubTabId==='dashboard'?'flex':'none'}; flex:1; flex-direction:column;">
            ${stats && stats.total_trades > 0 ? renderDashboardHTML(profile.id) : '<div class="text-sub" style="padding:48px;text-align:center;flex:1;">📊 No closed trades found for this profile yet.<br/><span style="font-size:0.8rem;">Stats will appear here once the first trade is completed.</span></div>'}
        </div>

        <!-- Active trades sub-panel -->
        <div class="mt-content" id="mt-sub-active" style="display:${activeSubTabId==='active'?'flex':'none'}; flex:1; flex-direction:column;">
            <div class="trading-table-wrap">
                <table>
                    <thead><tr>
                        <th>Pair</th><th>Direction</th><th>Amount</th>
                        <th>Entry Spread</th><th>Cur / Max Spread</th>
                        <th>Net Live PnL</th><th>Time</th>
                    </tr></thead>
                    <tbody id="t-active-body-${profile.id}">
                        ${active === null ? '<tr><td colspan="7" class="text-center text-sub">Loading…</td></tr>' : ''}
                    </tbody>
                    <tfoot style="background:var(--bg-row-alt);font-weight:600;">
                        <tr>
                            <td colspan="5" style="text-align:right;border-top:1px solid var(--border-dim)">
                                Total (<span id="t-count-${profile.id}">${active?.length??0}</span> trades)
                            </td>
                            <td id="t-live-pnl-${profile.id}" style="color:var(--text);border-top:1px solid var(--border-dim)">$0.00</td>
                            <td style="border-top:1px solid var(--border-dim)"></td>
                        </tr>
                    </tfoot>
                </table>
            </div>
        </div>

        <!-- History sub-panel -->
        <div class="mt-content" id="mt-sub-history" style="display:${activeSubTabId==='history'?'flex':'none'}; flex:1; flex-direction:column;">
            <div class="trading-table-wrap">
                <table>
                    <thead><tr>
                        <th>Pair</th><th>Direction</th><th>Amount</th>
                        <th>PnL</th><th>Entry→Exit Spread</th><th>Reason</th><th>Exit Time</th>
                    </tr></thead>
                    <tbody id="t-history-body-${profile.id}">
                        ${history === null ? '<tr><td colspan="7" class="text-center text-sub">Loading…</td></tr>' : ''}
                    </tbody>
                    <tfoot style="background:var(--bg-row-alt);font-weight:600;">
                        <tr>
                            <td colspan="3" style="text-align:right;border-top:1px solid var(--border-dim)">
                                Total (<span>${stats?.total_trades??0}</span> trades)
                            </td>
                            <td style="color:${(stats?.total_pnl||0)>=0?'var(--green)':'var(--red)'};border-top:1px solid var(--border-dim)">
                                ${fmtPnL(stats?.total_pnl||0)}
                            </td>
                            <td colspan="3" style="border-top:1px solid var(--border-dim)"></td>
                        </tr>
                    </tfoot>
                </table>
            </div>
        </div>

        <!-- Settings sub-panel -->
        <div class="mt-content" id="mt-sub-settings" style="display:${activeSubTabId==='settings'?'block':'none'}">
            ${renderSettingsForm(profile)}
        </div>
    `;

    if (active !== null) {
        renderActiveTable(profile.id, active);
        renderHistoryTable(profile.id, history);
        if (stats && stats.total_trades > 0) renderDashboardCharts(profile.id, stats);
    }
}

/** Helper to update stats in sub-nav during polling without full re-render */
function updateDynamicElements(profile, active, stats) {
    const statsEl = document.getElementById(`panel-stats-${profile.id}`);
    if (statsEl) {
        statsEl.innerHTML = renderSubNavStats(active, stats, profile.mode);
    }
}

function renderSubNavStats(active, stats, mode) {
    return `
        ${active !== null ? `
        <span style="font-size:.8rem;color:var(--text-sub);">
            Active: <strong style="color:var(--text)">${active.length}</strong>
            &nbsp;|&nbsp; PnL:
            <strong style="color:${(stats?.total_pnl||0)>=0?'var(--green)':'var(--red)'}">
                ${fmtPnL(stats?.total_pnl||0)}
            </strong>
        </span>` : ''}
        <span class="mode-badge ${mode === 'real' ? 'mode-badge--real' : 'mode-badge--paper'}">
            ${mode === 'real' ? '💰 Real' : '🧪 Paper'}
        </span>
    `;
}

function renderSettingsForm(profile) {
    const p = profile;
    return `
        <section class="trading-settings-section" style="padding:0;background:transparent;border:none;">
            <form id="settings-form-${p.id}" class="settings-grid" onsubmit="return false;">
                <div class="form-group">
                    <label>Min Entry Spread (%)</label>
                    <input type="number" id="sf-min-${p.id}" step="0.1" value="${p.min_entry_spread??1}" />
                </div>
                <div class="form-group">
                    <label>Max Entry Spread (%)</label>
                    <input type="number" id="sf-max-${p.id}" step="0.1" value="${p.max_entry_spread??10}" />
                </div>
                <div class="form-group">
                    <label>Trade Volume (USD)</label>
                    <input type="number" id="sf-vol-${p.id}" step="10" value="${p.trade_volume??1000}" />
                </div>
                <div class="form-group">
                    <label>Max Active Trades</label>
                    <input type="number" id="sf-max-trades-${p.id}" step="1" value="${p.max_active_trades??5}" />
                </div>
                <div class="form-group">
                    <label>Take Profit (%)</label>
                    <input type="number" id="sf-tp-${p.id}" step="any" value="${p.take_profit_percent??0.1}" />
                </div>
                <div class="form-group">
                    <label>Trailing Stop (%)</label>
                    <input type="number" id="sf-trail-${p.id}" step="any" value="${p.trailing_stop_percent??0.5}" />
                </div>
                <div class="form-group">
                    <label>Slippage Rate (%)</label>
                    <input type="number" id="sf-slip-${p.id}" step="any" value="${p.slippage_percent??0.1}" />
                </div>
                <div class="form-group">
                    <label>Commission Rate (%)</label>
                    <input type="number" id="sf-comm-${p.id}" step="any" value="${p.commission_rate??0.45}" />
                </div>
                <div class="form-group">
                    <label>Min Volume (M$)</label>
                    <input type="number" id="sf-minvol-${p.id}" step="1" value="${p.min_volume_millions??10}" />
                </div>
                <div class="form-group">
                    <label>Collector Update (s)</label>
                    <input type="number" id="sf-interval-${p.id}" step="1" min="1" max="60" value="${p.collector_interval_secs??5}" />
                </div>
                <div class="form-actions" style="grid-column:1/-1;display:flex;gap:16px;margin-top:24px;border-top:1px solid var(--border-dim);padding-top:24px;align-items:center;">
                    <button type="button" id="sf-power-${p.id}"
                        onclick="toggleBotPower('${p.id}')"
                        class="trading-save-btn"
                        style="margin:0;width:auto;padding:12px 32px;font-size:.95rem;background:${p.is_active?'var(--red)':'var(--green)'};">
                        ${p.is_active ? 'Stop Bot' : 'Start Bot'}
                    </button>
                    <button type="button"
                        onclick="saveSettings('${p.id}')"
                        class="trading-save-btn"
                        id="sf-save-${p.id}"
                        style="margin:0;width:auto;padding:12px 32px;font-size:.95rem;background:var(--bg-ctrl);color:var(--text);border:1px solid var(--border);">
                        Save Settings
                    </button>
                    <button type="button"
                        onclick="clearTrades('${p.id}', '${p.mode}')"
                        class="trading-save-btn outline-btn"
                        style="margin:0 0 0 auto;width:auto;padding:12px 32px;font-size:.95rem;background:transparent;color:var(--red);border:1px solid var(--red);">
                        Clear All Trades
                    </button>
                </div>
            </form>
        </section>
    `;
}

// ─── Per-profile dashboard HTML scaffold ─────────────────────────────────────

function renderDashboardHTML(profileId) {
    const pid = profileId;
    return `
        <div style="padding:16px 0;display:grid;gap:24px;">
            <!-- KPI row -->
            <div id="dash-kpi-${pid}" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;"></div>
            <!-- Charts row 1: Equity + Win/Loss -->
            <div style="display:grid;grid-template-columns:2fr 1fr;gap:16px;min-height:220px;">
                <div class="chart-card">
                    <div class="chart-card-title">Equity Curve</div>
                    <canvas id="dash-equity-${pid}" height="180"></canvas>
                </div>
                <div class="chart-card">
                    <div class="chart-card-title">Win / Loss</div>
                    <canvas id="dash-winloss-${pid}" height="180"></canvas>
                </div>
            </div>
            <!-- Charts row 2: Exchange PnL -->
            <div class="chart-card" style="min-height:200px;">
                <div class="chart-card-title">PnL by Exchange</div>
                <canvas id="dash-exchange-${pid}" height="180"></canvas>
            </div>
        </div>
    `;
}

// ─── Per-profile dashboard chart rendering ────────────────────────────────────

function renderDashboardCharts(profileId, stats) {
    const pid = profileId;

    // Guard: Chart.js must be loaded
    if (typeof Chart === 'undefined') return;

    // Ensure old instances destroyed (safety net)
    if (chartInstances[pid]) {
        Object.values(chartInstances[pid]).forEach(c => { try { c.destroy(); } catch {} });
    }
    chartInstances[pid] = {};

    // KPI cards
    const kpiEl = document.getElementById(`dash-kpi-${pid}`);
    if (kpiEl) {
        const totalPnl = stats.total_pnl || 0;
        const totalTrades = stats.total_trades || 0;
        const wins = stats.wins || 0;
        const winRate = totalTrades > 0 ? (wins / totalTrades * 100).toFixed(1) : '0.0';
        const avgPnl = stats.avg_pnl || 0;
        kpiEl.innerHTML = [
            ['Total PnL', fmtPnL(totalPnl), totalPnl >= 0 ? 'var(--green)' : 'var(--red)'],
            ['Trades', totalTrades, 'var(--text)'],
            ['Win Rate', `${winRate}%`, wins / (totalTrades || 1) >= 0.5 ? 'var(--green)' : 'var(--red)'],
            ['Avg PnL', fmtPnL(avgPnl), avgPnl >= 0 ? 'var(--green)' : 'var(--red)'],
        ].map(([label, value, color]) => `
            <div style="background:var(--bg-row-alt);border:1px solid var(--border-dim);border-radius:10px;padding:14px 16px;">
                <div style="font-size:.75rem;color:var(--text-sub);margin-bottom:4px;">${label}</div>
                <div style="font-size:1.2rem;font-weight:700;color:${color};">${value}</div>
            </div>
        `).join('');
    }

    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = '#334155';

    // Equity curve
    const equityCtx = document.getElementById(`dash-equity-${pid}`);
    if (equityCtx) {
        const curve = stats.equity_curve || [];
        let sum = 0;
        const labels = [], data = [];
        curve.forEach((p, i) => {
            sum += (p.pnl || 0);
            if (curve.length <= 100 || i % Math.ceil(curve.length / 80) === 0 || i === curve.length - 1) {
                labels.push(`#${i + 1}`);
                data.push(parseFloat(sum.toFixed(2)));
            }
        });
        chartInstances[pid].equity = new Chart(equityCtx, {
            type: 'line',
            data: {
                labels,
                datasets: [{ label: 'Cumulative PnL ($)', data, borderColor: '#10b981', backgroundColor: 'rgba(16,185,129,0.1)', fill: true, tension: 0.1, pointRadius: curve.length > 50 ? 0 : 3 }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } },
        });
    }

    // Win / Loss pie
    const wlCtx = document.getElementById(`dash-winloss-${pid}`);
    if (wlCtx) {
        const wins = stats.wins || 0;
        const losses = stats.losses || 0;
        chartInstances[pid].winloss = new Chart(wlCtx, {
            type: 'pie',
            data: {
                labels: ['Wins', 'Losses'],
                datasets: [{ data: [wins, losses], backgroundColor: ['#10b981', '#ef4444'] }]
            },
            options: { responsive: true, maintainAspectRatio: false },
        });
    }

    // Exchange PnL bar
    const exCtx = document.getElementById(`dash-exchange-${pid}`);
    if (exCtx) {
        const pnlByEx = stats.pnl_by_exchange || {};
        const exLabels = Object.keys(pnlByEx);
        chartInstances[pid].exchange = new Chart(exCtx, {
            type: 'bar',
            data: {
                labels: exLabels,
                datasets: [{ label: 'PnL ($)', data: exLabels.map(ex => parseFloat((pnlByEx[ex] || 0).toFixed(2))), backgroundColor: exLabels.map(ex => (pnlByEx[ex] || 0) >= 0 ? '#3b82f6' : '#ef4444') }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } },
        });
    }
}

// ─── Active trades table ──────────────────────────────────────────────────────

function renderActiveTable(profileId, active) {
    const tbody = document.getElementById(`t-active-body-${profileId}`);
    const livePnlEl = document.getElementById(`t-live-pnl-${profileId}`);
    const countEl = document.getElementById(`t-count-${profileId}`);
    if (!tbody) return;
    if (!expandedRows[profileId]) expandedRows[profileId] = new Set();

    if (countEl) countEl.textContent = active.length;

    let liveSum = 0;
    for (const t of active) liveSum += Number(t.current_pnl ?? 0);
    if (livePnlEl) {
        livePnlEl.textContent = fmtPnL(liveSum);
        livePnlEl.style.color = liveSum > 0 ? 'var(--green)' : liveSum < 0 ? 'var(--red)' : 'var(--text)';
    }

    if (!active.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-sub">No active trades</td></tr>';
        return;
    }

    tbody.innerHTML = active.map(t => {
        const time = new Date(t.entry_time).toLocaleString('en-GB', { timeZone: 'Europe/Kyiv' });
        const pnl = Number(t.current_pnl ?? 0);
        const pnlColor = pnl > 0 ? 'var(--green)' : pnl < 0 ? 'var(--red)' : 'var(--text)';
        const livePnLStr = t.current_pnl != null
            ? `<div style="color:${pnlColor};font-weight:700;">${fmtPnL(pnl)}</div>`
            : `<div style="color:var(--text-sub)">Waiting…</div>`;
        const curSpread = t.current_spread != null ? t.current_spread.toFixed(2) + '%' : (t.entry_spread?.toFixed(2) ?? '0.00') + '%';
        const bestPnl = t.highest_spread_reached != null && t.highest_spread_reached > -5000 ? t.highest_spread_reached : null;
        const bestStr = bestPnl !== null ? bestPnl.toFixed(2) + '%' : '—';
        const isAHigher = t.direction.startsWith(t.exchange_a);
        const rowKey = `${profileId}_${t.id}`;
        const isExpanded = expandedRows[profileId].has(t.id);
        const detailDisplay = isExpanded ? 'table-row' : 'none';

        const detailRow = `
            <tr id="detail-${rowKey}" style="display:${detailDisplay}">
                <td colspan="7" class="p-0">
                    <div class="trade-details-box">
                        <div class="td-col">
                            <div class="td-header">${t.exchange_a} <span class="badge ${isAHigher ? 'badge-short' : 'badge-long'}">${isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${t.entry_price_a}</strong></div>
                            <div class="td-line"><span>Current:</span> <strong>${t.current_price_a ?? '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_a??0)>0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_a??0)}</strong></div>
                        </div>
                        <div class="td-col">
                            <div class="td-header">${t.exchange_b} <span class="badge ${!isAHigher ? 'badge-short' : 'badge-long'}">${!isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${t.entry_price_b}</strong></div>
                            <div class="td-line"><span>Current:</span> <strong>${t.current_price_b ?? '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_b??0)>0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_b??0)}</strong></div>
                        </div>
                        <div class="td-col td-total">
                            <div class="td-line"><span>Gross PnL:</span> <strong style="color:${(t.gross_pnl??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.gross_pnl??0)}</strong></div>
                            <div class="td-line"><span>Comm. Cost:</span> <strong style="color:var(--red)">${fmtCost(t.commission_cost)}</strong></div>
                            <div class="td-total-pnl">Net PnL: ${livePnLStr} <span class="text-sub" style="font-weight:400;font-size:.8rem;">(${fmtDuration(Date.now()-t.entry_time)})</span></div>
                            <div style="margin-top:12px;"><button type="button" class="trading-save-btn outline-btn" style="margin:0;width:100%;padding:8px;font-size:12px;" onclick="openChartIframe('${t.pair}','${t.exchange_a}','${t.exchange_b}',${t.entry_time},null)">📊 View Chart</button></div>
                        </div>
                    </div>
                </td>
            </tr>`;

        return `
            <tr class="clickable-row main-trade-row" onclick="toggleRow('${profileId}',${t.id})">
                <td><strong>${t.pair}</strong></td>
                <td><span style="color:var(--text-sub)">${t.direction}</span></td>
                <td>$${t.volume}</td>
                <td>${(t.entry_spread??0).toFixed(2)}%</td>
                <td>${curSpread} / <span style="color:var(--green)">${bestStr}</span></td>
                <td>${livePnLStr}</td>
                <td style="color:var(--text-sub)">${time}</td>
            </tr>${detailRow}`;
    }).join('');
}

// ─── History table ────────────────────────────────────────────────────────────

function renderHistoryTable(profileId, history) {
    const tbody = document.getElementById(`t-history-body-${profileId}`);
    if (!tbody) return;
    if (!history.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-sub">No closed trades</td></tr>';
        return;
    }
    tbody.innerHTML = history.map(t => {
        const time = new Date(t.exit_time).toLocaleString('en-GB', { timeZone: 'Europe/Kyiv' });
        const pnl = t.pnl ?? 0;
        const pnlColor = pnl > 0 ? 'var(--green)' : 'var(--red)';
        const pnlStr = (pnl > 0 ? '+' : '') + '$' + Math.abs(pnl).toFixed(2);
        const rowKey = `${profileId}_h_${t.id}`;
        const isExpanded = expandedRows[profileId]?.has('h_' + t.id);
        const isAHigher = t.direction.startsWith(t.exchange_a);
        const openTimeStr = new Date(t.entry_time).toLocaleString('en-GB', { timeZone: 'Europe/Kyiv' });

        const detailRow = `
            <tr id="detail-${rowKey}" style="display:${isExpanded ? 'table-row' : 'none'}">
                <td colspan="7" class="p-0">
                    <div class="trade-details-box">
                        <div class="td-col">
                            <div class="td-header">${t.exchange_a} <span class="badge ${isAHigher ? 'badge-short' : 'badge-long'}">${isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${t.entry_price_a}</strong></div>
                            <div class="td-line"><span>Exit:</span> <strong>${t.exit_price_a ?? '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_a??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_a??0)}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Opened:</span> <strong style="font-size:.75rem">${openTimeStr}</strong></div>
                        </div>
                        <div class="td-col">
                            <div class="td-header">${t.exchange_b} <span class="badge ${!isAHigher ? 'badge-short' : 'badge-long'}">${!isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${t.entry_price_b}</strong></div>
                            <div class="td-line"><span>Exit:</span> <strong>${t.exit_price_b ?? '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_b??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_b??0)}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Closed:</span> <strong style="font-size:.75rem">${time}</strong></div>
                        </div>
                        <div class="td-col td-total">
                            <div class="td-line"><span>Comm. Paid:</span> <strong style="color:var(--red)">${fmtCost(t.commission_cost)}</strong></div>
                            <div class="td-line"><span>Funding:</span> <strong style="color:${(t.funding_cost??0)<=0?'var(--green)':'var(--red)'}">${fmtCost(t.funding_cost)}</strong></div>
                            <div class="td-total-pnl">Net PnL: <span style="color:${pnlColor}">${pnlStr}</span> <span class="text-sub" style="font-weight:400;font-size:.8rem;">(${fmtDuration(t.exit_time-t.entry_time)})</span></div>
                            <div style="margin-top:12px;"><button type="button" class="trading-save-btn outline-btn" style="margin:0;width:100%;padding:8px;font-size:12px;" onclick="openChartIframe('${t.pair}','${t.exchange_a}','${t.exchange_b}',${t.entry_time},${t.exit_time})">📊 View Chart</button></div>
                        </div>
                    </div>
                </td>
            </tr>`;

        return `
            <tr class="clickable-row main-trade-row" onclick="toggleRow('${profileId}','h_${t.id}')">
                <td><strong>${t.pair}</strong></td>
                <td><span style="color:var(--text-sub)">${t.direction}</span></td>
                <td>$${t.volume}</td>
                <td style="color:${pnlColor};font-weight:600">${pnlStr}</td>
                <td>${(t.entry_spread??0).toFixed(2)}% &rarr; ${(t.exit_spread??0).toFixed(2)}%</td>
                <td><span class="badge ${t.close_reason==='TP'?'badge-long':t.close_reason==='STOP'?'badge-short':'badge-neutral'}">${t.close_reason??'—'}</span></td>
                <td style="color:var(--text-sub)">${time}</td>
            </tr>${detailRow}`;
    }).join('');
}

// ─── User actions ─────────────────────────────────────────────────────────────

window.toggleRow = function (profileId, id) {
    if (!expandedRows[profileId]) expandedRows[profileId] = new Set();
    const key = `${profileId}_${typeof id === 'string' && id.startsWith('h_') ? id : id}`;
    const el = document.getElementById(`detail-${profileId}_${id}`);
    if (!el) return;
    if (expandedRows[profileId].has(id)) {
        expandedRows[profileId].delete(id);
        el.style.display = 'none';
    } else {
        expandedRows[profileId].add(id);
        el.style.display = 'table-row';
    }
};

window.showSubTab = function (btn, tab) {
    activeSubTabId = tab; // Save user choice globally
    document.querySelectorAll('.mt-nav-btn').forEach(b => b.classList.remove('mt-nav-btn--active'));
    btn.classList.add('mt-nav-btn--active');
    document.querySelectorAll('.mt-content').forEach(c => c.style.display = 'none');
    const el = document.getElementById(`mt-sub-${tab}`);
    if (el) el.style.display = 'block';
};

window.saveSettings = async function (profileId) {
    const g = id => document.getElementById(id)?.value;
    const settings = {
        is_active: (profiles.find(p => p.id === profileId)?.is_active ? 1 : 0),
        min_entry_spread: parseFloat(g(`sf-min-${profileId}`)),
        max_entry_spread: parseFloat(g(`sf-max-${profileId}`)),
        trade_volume: parseFloat(g(`sf-vol-${profileId}`)),
        max_active_trades: parseInt(g(`sf-max-trades-${profileId}`)),
        take_profit_percent: parseFloat(g(`sf-tp-${profileId}`)),
        trailing_stop_percent: parseFloat(g(`sf-trail-${profileId}`)),
        slippage_percent: parseFloat(g(`sf-slip-${profileId}`)),
        commission_rate: parseFloat(g(`sf-comm-${profileId}`)),
        min_volume_millions: parseInt(g(`sf-minvol-${profileId}`)),
        collector_interval_secs: parseInt(g(`sf-interval-${profileId}`)),
    };
    const btn = document.getElementById(`sf-save-${profileId}`);
    try {
        await saveProfileSettings(profileId, settings);
        const p = profiles.find(pr => pr.id === profileId);
        if (p) Object.assign(p, settings);
        if (btn) { btn.textContent = 'Saved!'; btn.style.background = 'var(--green)'; }
    } catch (e) {
        if (btn) { btn.textContent = 'Error!'; btn.style.background = 'var(--red)'; }
        console.error('Save failed:', e);
    }
    setTimeout(() => {
        if (btn) { btn.textContent = 'Save Settings'; btn.style.background = 'var(--bg-ctrl)'; }
    }, 2000);
};

window.toggleBotPower = async function (profileId) {
    const profile = profiles.find(p => p.id === profileId);
    if (!profile) return;
    profile.is_active = !profile.is_active;
    const settings = { ...profile, is_active: profile.is_active ? 1 : 0 };
    await saveProfileSettings(profileId, settings);
    const btn = document.getElementById(`sf-power-${profileId}`);
    if (btn) {
        btn.textContent = profile.is_active ? 'Stop Bot' : 'Start Bot';
        btn.style.background = profile.is_active ? 'var(--red)' : 'var(--green)';
    }
};

window.clearTrades = async function (profileId, mode) {
    if (!confirm('Delete ALL active and historical trades for this profile?')) return;
    const path = mode === 'real' ? '/api/real-trader/trades' : '/api/trading/trades';
    try {
        await fetch(`${API_BASE}${path}`, { method: 'DELETE' });
        await refreshActiveTab();
    } catch (e) { alert('Failed: ' + e.message); }
};

// ─── New tab modal ────────────────────────────────────────────────────────────

document.getElementById('tab-add-btn').addEventListener('click', () => {
    document.getElementById('new-tab-modal').classList.remove('hidden');
    document.getElementById('nt-name').focus();
});
document.getElementById('nt-cancel').addEventListener('click', () => {
    document.getElementById('new-tab-modal').classList.add('hidden');
});
document.getElementById('nt-create').addEventListener('click', async () => {
    const name = document.getElementById('nt-name').value.trim() || 'New Profile';
    const mode = document.querySelector('input[name="nt-mode"]:checked')?.value || 'paper';
    document.getElementById('nt-create').textContent = 'Creating…';
    try {
        const p = await createProfile(name, mode);
        document.getElementById('new-tab-modal').classList.add('hidden');
        document.getElementById('nt-name').value = '';
        renderTabBar();
        await switchTab(p.id);
    } catch (e) {
        alert('Failed to create profile: ' + e.message);
    }
    document.getElementById('nt-create').textContent = 'Create Profile';
});

// Tab close delegation
document.getElementById('tab-bar').addEventListener('click', async (e) => {
    if (!e.target.classList.contains('tab-close')) return;
    const id = e.target.dataset.id;
    if (id === 'paper-default' || id === 'real-default') {
        alert('Cannot close the default profiles.');
        return;
    }
    if (!confirm('Close and delete this trading profile and all its trades?')) return;
    await deleteProfile(id);
    if (activeTabId === id) {
        activeTabId = profiles[0]?.id ?? null;
        localStorage.setItem('activeTabId', activeTabId ?? '');
    }
    renderTabBar();
    if (activeTabId) await switchTab(activeTabId);
});

// ─── Chart iframe ─────────────────────────────────────────────────────────────

window.openChartIframe = function (pair, exA, exB, entryTime, exitTime) {
    const modal = document.getElementById('chart-modal');
    const iframe = document.getElementById('chart-frame');
    if (!modal || !iframe) return;
    let url = `/chart.html?iframe=1&chart=${pair}&exA=${exA}&exB=${exB}&entry=${entryTime}`;
    if (exitTime && exitTime !== 'null') url += `&exit=${exitTime}`;
    iframe.src = url;
    modal.classList.remove('hidden');
};

document.getElementById('chart-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget || e.target.id === 'chart-frame') {
        // only close via postMessage
    }
});
window.addEventListener('message', e => {
    if (e.data?.type === 'CLOSE_CHART_MODAL') {
        document.getElementById('chart-modal').classList.add('hidden');
        document.getElementById('chart-frame').src = '';
    }
});

// ─── Exchange modal ───────────────────────────────────────────────────────────

window.openExchangeModal = function (exName, isOnline, ts, pairsJson) {
    const modal = document.getElementById('exchange-modal');
    if (!modal) return;
    document.getElementById('exchange-modal-title').textContent = exName;
    const timeStr = isOnline ? new Date(ts).toLocaleTimeString() : 'Offline';
    let pairs = [];
    try { pairs = JSON.parse(pairsJson); } catch {}
    document.getElementById('exchange-modal-status').textContent =
        `Status: ${isOnline ? '🟢 Online' : '🔴 Offline'} | Last: ${timeStr} | Pairs: ${pairs.length}`;
    document.getElementById('exchange-modal-content').textContent =
        pairs.sort().map((s, i) => `[${String(i+1).padStart(3,'0')}] ${s}`).join('\n') || 'No data';
    modal.classList.remove('hidden');
};
document.getElementById('close-exchange-modal').addEventListener('click', () => {
    document.getElementById('exchange-modal').classList.add('hidden');
});
document.getElementById('exchange-modal').addEventListener('click', e => {
    if (e.target.id === 'exchange-modal') e.target.classList.add('hidden');
});

// ─── Format helpers ───────────────────────────────────────────────────────────

const fmtPnL = v => { const n = Number(v)||0; if (Math.abs(n)<.005) return '$0.00'; return (n>0?'+':'-')+'$'+Math.abs(n).toFixed(2); };
const fmtCost = v => { const n = Number(v)||0; if (Math.abs(n)<.005) return '$0.00'; return n>0?`-$${Math.abs(n).toFixed(2)}`:`+$${Math.abs(n).toFixed(2)}`; };
const fmtDuration = ms => { const s=Math.floor(ms/1000); if(s<60) return s+'s'; const m=Math.floor(s/60); if(m<60) return m+'m '+(s%60)+'s'; const h=Math.floor(m/60); if(h<24) return h+'h '+(m%60)+'m'; return Math.floor(h/24)+'d '+(h%24)+'h'; };
const escHtml = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

// ─── Start ────────────────────────────────────────────────────────────────────
boot();
