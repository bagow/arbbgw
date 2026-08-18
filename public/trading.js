/**
 * trading.js – Multi-tab trading UI
 * v2.1 – adds BingX, BitMEX, AscendEX, XT.COM, Toobit to CEX tab
 *
 * Each "tab" is a trading profile. Profiles are stored in D1 via
 * API calls. The active tab's data is polled every 10s; others are lazy-loaded.
 * Tab visibility/order is persisted in localStorage.
 */
'use strict';

// ─── Backend URL (VPS via Cloudflare Tunnel) ──────────────────────────────────
const API_BASE = window.API_BASE = window.API_BASE || 'https://api.arb.bagow.com';

// ─── State ────────────────────────────────────────────────────────────────────

let profiles = [];       // all profiles from server
let activeTabId = null;  // currently visible profile id
let pollTimer = null;
let lastUnified = null;  // cached /api/unified response
let activeSubTabId = 'dashboard'; // Dashboard is first by default

// Expanded trade rows per profile
const expandedRows = {};  // profileId -> Set of trade row ids

// Chart.js instances: destroyed and recreated on each render to avoid canvas reuse errors
const chartInstances = {};  // profileId -> { equity, exchange, winloss }

// ─── Boot ─────────────────────────────────────────────────────────────────────

async function boot() {
    try {
        await loadProfiles();
        const loader = document.querySelector('.mt-loading');
        if (loader) loader.style.display = 'none';

        if (!profiles || profiles.length === 0) {
            profiles = [{
                id: 'real-default',
                name: 'Public Real',
                mode: 'real',
                is_active: true
            }];
        }

        // Restore last active tab from localStorage or default
        const saved = localStorage.getItem('activeTabId');
        if (saved && profiles.find(p => p.id === saved)) {
            activeTabId = saved;
        } else {
            activeTabId = profiles[0]?.id || 'real-default';
        }
        
        if (activeTabId) {
            await switchTab(activeTabId);
        }
        startPoll();
    } catch (e) {
        console.warn('Boot fallback initialized:', e.message);
        const loader = document.querySelector('.mt-loading');
        if (loader) loader.style.display = 'none';

        profiles = [{
            id: 'real-default',
            name: 'Public Real',
            mode: 'real',
            is_active: true
        }];
        activeTabId = 'real-default';
        await switchTab(activeTabId);
        startPoll();
    }
}

// ─── API helpers ──────────────────────────────────────────────────────────────

async function apiFetch(path, opts = {}) {
    const creds = localStorage.getItem('cex_auth');
    if (creds) {
        opts.headers = opts.headers || {};
        opts.headers['Authorization'] = 'Basic ' + creds;
    }

    const res = await fetch(`${API_BASE}${path}`, opts);
    if (res.status === 401) {
        const modal = document.getElementById('login-modal');
        if (modal) {
            modal.classList.remove('hidden');
            const unEl = document.getElementById('login-username');
            if (unEl) unEl.focus();
        }
        throw new Error(`HTTP 401 Unauthorized`);
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}

async function loadProfiles() {
    try {
        const data = await apiFetch('/api/profiles');
        if (Array.isArray(data) && data.length > 0) {
            profiles = data;
            for (const p of profiles) p.is_active = Boolean(p.is_active);
            return;
        }
    } catch (e) {
        console.warn('Could not load profiles from API, using default:', e.message);
    }
    profiles = [{
        id: 'real-default',
        name: 'Public Real',
        mode: 'real',
        is_active: true
    }];
}

async function createProfile(name, mode) {
    const p = await apiFetch('/api/profiles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, mode }),
    });
    // Reload all profiles from server to ensure local state is accurate.
    await loadProfiles();
    // Return the profile that matches the newly created one.
    return profiles.find(pr => pr.id === p.id) || p;
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
}

// ─── Tab bar rendering ────────────────────────────────────────────────────────

function renderTabBar() {
    // Legacy bottom tab bar removed
}

// ─── Tab switching ────────────────────────────────────────────────────────────

async function switchTab(id) {
    activeTabId = id;
    localStorage.setItem('activeTabId', id);
    _lastStatsHash = null; // Force chart recreation on profile switch
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
    window.lastPollTime = Date.now();
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

    const { trades, real, exchanges, profiles: serverProfiles, profile_stats, profile_active, profile_history, kill_switch_active } = lastUnified;
    window._killSwitchActive = !!kill_switch_active;
    // Dynamically show/hide Resume button based on kill-switch state
    document.querySelectorAll('[id^="sf-resume-"]').forEach(btn => {
        btn.style.display = window._killSwitchActive ? 'inline-flex' : 'none';
    });
    console.log(`[DEBUG] Received Unified for ${activeTabId}:`, { profile_stats, trades_stats: trades?.stats, kill_switch_active });

    // Sync local profile settings from server profiles data
    if (serverProfiles) {
        for (const sp of serverProfiles) {
            const local = profiles.find(p => p.id === sp.id);
            if (local) Object.assign(local, sp);
        }
    }

    // Unified active trades, history and stats directly
    const active = profile_active !== undefined ? profile_active : (trades?.active || []);
    const history = profile_history !== undefined ? profile_history : (trades?.history || []);
    const stats = profile_stats || trades?.stats || {};

    // Auto-refresh logs if the logs tab is currently active
    if (activeSubTabId === 'logs') {
        window.fetchLogs(true);
    }

    renderPanel(profile, active, history, stats, exchanges);
}

// ─── Panel rendering ──────────────────────────────────────────────────────────

function renderPanel(profile, active, history, stats, exchanges) {
    const panel = document.getElementById('mt-panel');
    if (!panel) {
        console.error('CRITICAL: mt-panel not found in DOM.');
        return;
    }

    // ─── Polling Update: Only refresh dynamic data ──────────────────────────
    if (panel.dataset.activeProfileId === profile.id) {
        updateDynamicElements(profile, active, stats, exchanges);
        if (active !== null) {
            renderActiveTable(profile.id, active);
            renderHistoryTable(profile.id, history);
            const dashWrap = document.getElementById('mt-sub-dashboard');
            const hasStats = stats && stats.total_trades > 0;
            if (dashWrap && hasStats) {
                if (!document.getElementById(`dash-equity-${profile.id}`)) {
                    dashWrap.innerHTML = renderDashboardHTML(profile.id);
                }
                renderDashboardCharts(profile.id, stats, exchanges);
            } else if (dashWrap && !hasStats) {
                if (document.getElementById(`dash-equity-${profile.id}`)) {
                    dashWrap.innerHTML = '<div class="text-sub" style="padding:48px;text-align:center;flex:1;">📊 No closed trades found for this profile yet.<br/><span style="font-size:0.8rem;">Stats will appear here once the first trade is completed.</span></div>';
                }
            }
            
            // Dynamically render CEX UI updates natively during polling
            if (exchanges) {
                const cexWrap = document.getElementById('mt-sub-cex');
                // Only rewrite DOM if it's currently showing the fallback message (or if active tab is cex? Actually, rewriting DOM on every poll is fine since it's just buttons/checkboxes, but better to check if it's needed)
                if (cexWrap && activeSubTabId === 'cex') {
                   // We could do an innerHTML replacement, but we don't want to break checkboxes if user is clicking
                   // Since the user reported it just stays blank, replacing innerHTML entirely is acceptable for the first render
                   if (cexWrap.innerHTML.includes('No exchange data') || activeSubTabId === 'cex') {
                       cexWrap.innerHTML = renderCexTab(profile, exchanges);
                   }
                } else if (cexWrap && cexWrap.innerHTML.includes('No exchange data')) {
                   // If tab is hidden but stuck on loading text, update it in background
                   cexWrap.innerHTML = renderCexTab(profile, exchanges);
                }
            }
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
            <button class="mt-nav-btn ${activeSubTabId==='dashboard'?'mt-nav-btn--active':''}" data-sub="dashboard" onclick="showSubTab(this,'dashboard')">Dashboard</button>
            <button class="mt-nav-btn ${activeSubTabId==='scanner'?'mt-nav-btn--active':''}" data-sub="scanner" onclick="showSubTab(this,'scanner')">Scanner</button>
            <button class="mt-nav-btn ${activeSubTabId==='active'?'mt-nav-btn--active':''}" data-sub="active" onclick="showSubTab(this,'active')">Active Trades</button>
            <button class="mt-nav-btn ${activeSubTabId==='history'?'mt-nav-btn--active':''}" data-sub="history" onclick="showSubTab(this,'history')">History</button>
            <button class="mt-nav-btn ${activeSubTabId==='cex'?'mt-nav-btn--active':''}" data-sub="cex" onclick="showSubTab(this,'cex')">CEX</button>
            <button class="mt-nav-btn ${activeSubTabId==='settings'?'mt-nav-btn--active':''}" data-sub="settings" onclick="showSubTab(this,'settings')">Settings</button>
            <button class="mt-nav-btn ${activeSubTabId==='logs'?'mt-nav-btn--active':''}" data-sub="logs" onclick="showSubTab(this,'logs')">Logs</button>
            
            <div id="panel-stats-${profile.id}" style="margin-left:auto;display:flex;align-items:center;gap:10px;">
                ${renderSubNavStats(active, stats, profile.mode)}
            </div>
        </div>

        <!-- Dashboard sub-panel -->
        <div class="mt-content" id="mt-sub-dashboard" style="display:${activeSubTabId==='dashboard'?'block':'none'}; padding: 0 24px 30px 24px;">
            ${stats && stats.total_trades > 0 ? renderDashboardHTML(profile.id) : '<div class="text-sub" style="padding:48px;text-align:center;">📊 No closed trades found for this profile yet.<br/><span style="font-size:0.8rem;">Stats will appear here once the first trade is completed.</span></div>'}
        </div>

        <!-- Scanner sub-panel (Integrated) -->
        <div class="mt-content" id="mt-sub-scanner" style="display:${activeSubTabId==='scanner'?'block':'none'};">
            <div class="panel-header" style="padding:10px 15px; border-bottom:1px solid var(--border-dim); background:var(--bg-panel);">
                <h1 class="panel-title" style="font-size:1rem;">Live Spreads</h1>
                <span class="panel-sub" id="pair-count" style="margin-left:auto; font-size:0.8rem;">–</span>
            </div>
            <div id="exchange-filters" class="exchange-filters" style="padding:8px 15px; background:var(--bg-panel); border-bottom:1px solid var(--border-dim);"></div>
            <div class="trading-table-wrap">
                <table id="spread-table">
                    <thead>
                        <tr>
                            <th data-sort="pair">Pair</th>
                            <th data-sort="exchangeA">Exch A</th>
                            <th data-sort="exchangeB">Exch B</th>
                            <th data-sort="priceA">Price A</th>
                            <th data-sort="priceB">Price B</th>
                            <th data-sort="spread" class="sorted-desc">Spread %</th>
                            <th data-sort="structural_spread">Struc Spread %</th>
                            <th data-sort="profit_pct">Profit %</th>
                            <th>Dir</th>
                        </tr>
                    </thead>
                    <tbody id="spread-tbody">
                        <tr class="placeholder-row">
                            <td colspan="9" style="text-align:center; padding:40px; color:var(--text-sub);">⟳ Initializing scanner…</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Active trades sub-panel -->
        <div class="mt-content" id="mt-sub-active" style="display:${activeSubTabId==='active'?'block':'none'};">
            <div class="trading-table-wrap">
                <table>
                    <thead><tr>
                        <th>Pair</th><th>Net Live PnL</th><th>Entry Spread</th>
                        <th>Cur / Max Spread</th><th>Direction</th><th>Amount</th>
                        <th>Time</th><th style="width:45px;text-align:center;">Action</th>
                    </tr></thead>
                    <tbody id="t-active-body-${profile.id}">
                        ${active === null ? '<tr><td colspan="8" class="text-center text-sub">Loading…</td></tr>' : ''}
                    </tbody>
                    <tfoot style="background:var(--bg-row-alt);font-weight:600;">
                        <tr>
                            <td style="white-space:nowrap;border-top:1px solid var(--border-dim)">
                                Total (<span id="t-count-${profile.id}">${active?.length??0}</span>)
                            </td>
                            <td id="t-live-pnl-${profile.id}" style="color:var(--text);border-top:1px solid var(--border-dim)">$0.00</td>
                            <td colspan="6" style="border-top:1px solid var(--border-dim)"></td>
                        </tr>
                    </tfoot>
                </table>
            </div>
            ${profile.mode === 'real' ? `
            <div style="display:flex;justify-content:flex-end;padding:16px 24px 8px;">
                <button type="button"
                    onclick="marketCloseAll('${profile.id}')"
                    class="trading-save-btn outline-btn"
                    style="margin:0;width:auto;padding:10px 28px;font-size:.9rem;background:transparent;color:var(--red);border:1px solid var(--red);border-radius:8px;cursor:pointer;">
                    ⚠️ Market Close All Now
                </button>
            </div>
            ` : ''}
        </div>

        <!-- History sub-panel -->
        <div class="mt-content" id="mt-sub-history" style="display:${activeSubTabId==='history'?'block':'none'};">
            ${renderHistoryReconcile(exchanges)}
            <div class="trading-table-wrap">
                <table>
                    <thead><tr>
                        <th>Pair</th><th>PnL</th><th>Entry→Exit Spread</th>
                        <th>Direction</th><th>Amount</th><th>Reason</th><th>Exit Time</th>
                    </tr></thead>
                    <tbody id="t-history-body-${profile.id}">
                        ${history === null ? '<tr><td colspan="7" class="text-center text-sub">Loading…</td></tr>' : ''}
                    </tbody>
                    <tfoot style="background:var(--bg-row-alt);font-weight:600;">
                        <tr>
                            <td style="white-space:nowrap;border-top:1px solid var(--border-dim)">
                                Total (<span id="h-count-${profile.id}">${stats?.total_trades??0}</span>)
                            </td>
                            <td id="h-pnl-${profile.id}" style="color:${(stats?.total_pnl||0)>=0?'var(--green)':'var(--red)'};border-top:1px solid var(--border-dim)">
                                ${fmtPnL(stats?.total_pnl||0)}
                            </td>
                            <td colspan="5" style="border-top:1px solid var(--border-dim)"></td>
                        </tr>
                    </tfoot>
                </table>
            </div>
        </div>

        <!-- CEX sub-panel -->
        <div class="mt-content" id="mt-sub-cex" style="display:${activeSubTabId==='cex'?'block':'none'}; padding: 24px;">
            ${renderCexTab(profile, exchanges)}
        </div>

        <!-- Settings sub-panel -->
        <div class="mt-content" id="mt-sub-settings" style="display:${activeSubTabId==='settings'?'block':'none'}; padding: 24px;">
            ${renderSettingsForm(profile)}
        </div>

        <!-- Logs sub-panel -->
        <div class="mt-content" id="mt-sub-logs" style="display:${activeSubTabId==='logs'?'block':'none'};">
            <div class="logs-wrapper">
                <div class="logs-toolbar">
                    <div class="logs-filters">
                        <div style="background:var(--bg-ctrl); padding:3px; border-radius:6px; display:flex; gap:3px; border:1px solid var(--border-dim);">
                            <button class="mt-nav-btn ${window.logViewMode==='simple'?'mt-nav-btn--active':''}" id="btn-log-simple" onclick="setLogView('simple')" style="padding:6px 14px; margin:0; font-size:12px; border-radius:4px;">✨ Simple</button>
                            <button class="mt-nav-btn ${window.logViewMode==='trade'?'mt-nav-btn--active':''}" id="btn-log-trade" onclick="setLogView('trade')" style="padding:6px 14px; margin:0; font-size:12px; border-radius:4px;">🎯 Trades & Signals</button>
                            <button class="mt-nav-btn ${window.logViewMode==='error'?'mt-nav-btn--active':''}" id="btn-log-error" onclick="setLogView('error')" style="padding:6px 14px; margin:0; font-size:12px; border-radius:4px;">⚠️ Warnings & Errors</button>
                            <button class="mt-nav-btn ${window.logViewMode==='exec'?'mt-nav-btn--active':''}" id="btn-log-exec" onclick="setLogView('exec')" style="padding:6px 14px; margin:0; font-size:12px; border-radius:4px;">⚡ Execution</button>
                            <button class="mt-nav-btn ${window.logViewMode==='all'?'mt-nav-btn--active':''}" id="btn-log-all" onclick="setLogView('all')" style="padding:6px 14px; margin:0; font-size:12px; border-radius:4px;">📜 All Logs</button>
                        </div>
                        <div class="logs-search-wrap">
                            <span class="logs-search-icon">🔍</span>
                            <input type="text" id="logs-search" class="logs-search-input" placeholder="Filter logs (e.g. BTC, RETRY, PnL)..." oninput="filterLogsRealtime()" />
                        </div>
                    </div>
                    
                    <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                        <button id="btn-log-autoscroll" onclick="toggleLogAutoScroll()" class="trading-save-btn outline-btn" style="padding:6px 12px; font-size:12px; border:1px solid var(--border-dim); background:var(--bg-ctrl); color:var(--text);">
                            📌 Auto-scroll: <span id="autoscroll-status" style="color:var(--green);font-weight:700;">ON</span>
                        </button>
                        <button onclick="copyVisibleLogs()" class="trading-save-btn outline-btn" style="padding:6px 12px; font-size:12px; border:1px solid var(--border-dim); background:var(--bg-ctrl); color:var(--text);">
                            📋 Copy
                        </button>
                        <button onclick="window.fetchLogs(false)" class="trading-save-btn" style="padding:6px 16px; font-size:12px; background:var(--accent); color:#fff; border:none;">
                            🔄 Refresh
                        </button>
                        <span style="color:var(--text-sub); font-size:0.75rem; font-family:var(--font-mono);" id="logs-status">Syncing…</span>
                    </div>
                </div>

                <div class="logs-terminal-frame">
                    <div class="logs-terminal-header">
                        <div class="logs-terminal-dots">
                            <div class="logs-terminal-dot red"></div>
                            <div class="logs-terminal-dot yellow"></div>
                            <div class="logs-terminal-dot green"></div>
                        </div>
                        <span class="logs-terminal-title">core-node.service • Live Arb Stream</span>
                        <span style="font-size:0.72rem;color:var(--text-sub);font-family:var(--font-mono);" id="logs-count-badge">0 lines</span>
                    </div>
                    <div id="logs-container" class="logs-terminal-body"></div>
                </div>
            </div>
        </div>
    `;

    if (active !== null) {
        renderActiveTable(profile.id, active);
        renderHistoryTable(profile.id, history);
        if (stats && stats.total_trades > 0) renderDashboardCharts(profile.id, stats, exchanges);
    }

    // After full re-render, re-hook scanner references if on scanner tab
    if (activeSubTabId === 'scanner' && window.initScanner) {
        window.initScanner(profile.id);
    }

    // Load blacklist into textarea from technical settings
    loadBlacklistIntoTextarea();
}

/** Helper to update stats in sub-nav during polling without full re-render */
function updateDynamicElements(profile, active, stats, exchanges) {
    const statsEl = document.getElementById(`panel-stats-${profile.id}`);
    if (statsEl) {
        statsEl.innerHTML = renderSubNavStats(active, stats, profile.mode);
    }
    
    // Sync bot status from server profile on every poll
    updateBotStatusUI(!!profile.is_active, profile.id);

    // Update active PnL list counts
    const tCountEl = document.getElementById(`t-count-${profile.id}`);
    if (tCountEl) tCountEl.textContent = active?.length ?? 0;

    // Update history totals
    const hCountEl = document.getElementById(`h-count-${profile.id}`);
    if (hCountEl && stats) hCountEl.textContent = stats.total_trades ?? 0;
    
    const hPnlEl = document.getElementById(`h-pnl-${profile.id}`);
    if (hPnlEl && stats) {
        hPnlEl.innerHTML = fmtPnL(stats.total_pnl || 0);
        hPnlEl.style.color = (stats.total_pnl || 0) >= 0 ? 'var(--green)' : 'var(--red)';
    }

    const reconcileEl = document.getElementById('history-reconcile-box');
    if (reconcileEl && exchanges) {
        reconcileEl.outerHTML = renderHistoryReconcile(exchanges);
    }
}

function renderHistoryReconcile(exchangesData) {
    if (!exchangesData) return '';
    const cexDelta = Number(exchangesData.cex_balance_delta_total_today ?? 0);
    const dbExpected = Number(exchangesData.db_expected_balance_delta_total ?? 0);
    const residual = Number(exchangesData.cex_vs_db_expected_delta ?? (cexDelta - dbExpected));
    const baseTs = Number(exchangesData.cex_baseline_ts_ms ?? 0);
    const baseTsText = baseTs > 0
        ? new Date(baseTs).toLocaleString('en-GB', { timeZone: 'UTC', hour12: false }) + ' UTC'
        : 'n/a';
    return `
        <div id="history-reconcile-box" class="reconcile-bar" style="margin:0 0 14px 0;">
            <div class="reconcile-item"><span class="reconcile-label">Window</span><span class="reconcile-value" style="color:var(--text-sub)">All closed</span></div>
            <div class="reconcile-item"><span class="reconcile-label">CEX Start</span><span class="reconcile-value">${baseTsText}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">CEX Δ</span><span class="reconcile-value" style="color:${cexDelta>=0?'var(--green)':'var(--red)'}">${fmtPnL(cexDelta)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">DB Expected Δ</span><span class="reconcile-value" style="color:${dbExpected>=0?'var(--green)':'var(--red)'}">${fmtPnL(dbExpected)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">Residual</span><span class="reconcile-value" style="color:${Math.abs(residual)>0.05?'var(--red)':'var(--text-sub)'}">${fmtPnL(residual)}</span></div>
        </div>
    `;
}

function renderSubNavStats(active, stats, mode) {
    const pnl = stats?.total_pnl || 0;
    const pnlClass = pnl >= 0 ? 'pnl-positive' : 'pnl-negative';
    const profile = profiles.find(p => p.id === activeTabId);
    const isRunning = profile ? !!profile.is_active : false;
    return `
        ${active !== null ? `
        <span style="font-size:.78rem;color:var(--text-sub);font-variant-numeric:tabular-nums;">
            Active: <strong style="color:var(--text)">${active.length}</strong>
            ·
            PnL: <strong class="${pnlClass}">${fmtPnL(pnl)}</strong>
        </span>` : ''}
        <span id="subnav-bot-status" style="display:inline-flex;align-items:center;gap:5px;font-size:.78rem;font-weight:600;padding:4px 10px;border-radius:6px;
            background:${isRunning ? 'rgba(34,197,94,0.12)' : 'rgba(239,68,68,0.12)'};
            color:${isRunning ? 'var(--green)' : 'var(--red)'};
            border:1px solid ${isRunning ? 'rgba(34,197,94,0.25)' : 'rgba(239,68,68,0.25)'};
            transition:all .3s ease;">
            <span style="width:7px;height:7px;border-radius:50%;background:${isRunning ? 'var(--green)' : 'var(--red)'};${isRunning ? 'animation:pulse-green 2s infinite;' : ''}"></span>
            ${isRunning ? 'Bot Active' : 'Bot Stopped'}
        </span>
        <span class="mode-badge mode-badge--real">💰 Real</span>
    `;
}

/**
 * Instantly sync all bot status UI elements (Settings badge, Start/Stop button, top-right indicator).
 * Called from toggleBotPower on click AND from updateDynamicElements on every poll.
 */
function updateBotStatusUI(isRunning, profileId) {
    // 1. Settings footer badge
    const badge = document.querySelector('.settings-status-badge');
    if (badge) {
        badge.style.background = isRunning ? 'var(--green-dim)' : 'var(--red-dim)';
        badge.style.color = isRunning ? 'var(--green)' : 'var(--red)';
        badge.style.borderColor = isRunning ? 'rgba(34,197,94,0.3)' : 'rgba(239,68,68,0.3)';
        const dot = badge.querySelector('span');
        if (dot) dot.style.background = isRunning ? 'var(--green)' : 'var(--red)';
        badge.lastChild.textContent = isRunning ? ' BOT ACTIVE' : ' BOT STOPPED';
    }

    // 2. Start/Stop button
    const btn = document.getElementById(`sf-power-${profileId}`);
    if (btn) {
        btn.textContent = isRunning ? '⏹ Stop Bot' : '▶ Start Bot';
        btn.style.background = isRunning ? 'var(--red)' : 'var(--green)';
    }

    // 3. Top-right subnav indicator
    const subnavStatus = document.getElementById('subnav-bot-status');
    if (subnavStatus) {
        subnavStatus.style.background = isRunning ? 'rgba(34,197,94,0.12)' : 'rgba(239,68,68,0.12)';
        subnavStatus.style.color = isRunning ? 'var(--green)' : 'var(--red)';
        subnavStatus.style.borderColor = isRunning ? 'rgba(34,197,94,0.25)' : 'rgba(239,68,68,0.25)';
        const dot = subnavStatus.querySelector('span');
        if (dot) {
            dot.style.background = isRunning ? 'var(--green)' : 'var(--red)';
            dot.style.animation = isRunning ? 'pulse-green 2s infinite' : 'none';
        }
        // Update text node (last child)
        const textNodes = [...subnavStatus.childNodes].filter(n => n.nodeType === 3);
        if (textNodes.length) textNodes[textNodes.length - 1].textContent = isRunning ? ' Bot Active' : ' Bot Stopped';
    }
}

// ─── CEX Tab Logic ────────────────────────────────────────────────────────────

async function toggleExchange(profileId, exchangeName, checkboxEl) {
    const profile = profiles.find(p => p.id === profileId);
    if (!profile) return;
    try {
        checkboxEl.disabled = true;
        let disabled = profile.disabled_exchanges || '[]';
        if (typeof disabled === 'string') disabled = JSON.parse(disabled);

        if (disabled.includes(exchangeName)) {
            disabled = disabled.filter(ex => ex !== exchangeName);
        } else {
            disabled.push(exchangeName);
        }

        const updatedStr = JSON.stringify(disabled);
        await apiFetch(`/api/profiles/${profileId}`, {
            method: 'PATCH',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ disabled_exchanges: disabled })
        });
        profile.disabled_exchanges = updatedStr;
        checkboxEl.checked = !disabled.includes(exchangeName);
    } catch (e) {
        alert('Failed to update: ' + e.message);
        if (typeof profile.disabled_exchanges === 'string') {
            const arr = JSON.parse(profile.disabled_exchanges || '[]');
            checkboxEl.checked = !arr.includes(exchangeName);
        }
    } finally {
        checkboxEl.disabled = false;
    }
}

function renderCexTab(profile, exchangesData) {
    if (!exchangesData || !exchangesData.data) {
        return '<div class="text-sub" style="text-align:center;padding:40px;">No exchange data available yet. Please wait for collector to sync.</div>';
    }

    const REAL_EXCHANGES = new Set(['binance', 'bybit', 'gate', 'kucoin', 'bitget', 'bingx', 'bitmex', 'ascendex', 'xt', 'toobit']);
    const isReal = profile.mode === 'real';
    // Parse disabled_exchanges to a map for quick lookup
    const disabledRaw = profile.disabled_exchanges || '[]';
    let disabledMap = {};
    if (typeof disabledRaw === 'string') {
        try { JSON.parse(disabledRaw).forEach(ex => disabledMap[ex] = true); } catch(e) {}
    } else if (Array.isArray(disabledRaw)) {
        disabledRaw.forEach(ex => disabledMap[ex] = true);
    }

    const KNOWN_EXCHANGES = ['binance', 'bybit', 'gate', 'kucoin', 'okx', 'bitget', 'bingx', 'bitmex', 'ascendex', 'xt', 'toobit'];
    const exNames = KNOWN_EXCHANGES
        .filter(ex => !isReal || REAL_EXCHANGES.has(ex))
        .sort((a, b) => {
            const aActive = !disabledMap[a];
            const bActive = !disabledMap[b];
            const aPos = (exchangesData.positions && exchangesData.positions[a]?.length) || 0;
            const bPos = (exchangesData.positions && exchangesData.positions[b]?.length) || 0;
            const aBal = (exchangesData.balances && exchangesData.balances[a]) || 0;
            const bBal = (exchangesData.balances && exchangesData.balances[b]) || 0;

            // 1. Exchanges with active positions first
            if (aPos > 0 && bPos === 0) return -1;
            if (bPos > 0 && aPos === 0) return 1;

            // 2. Active toggle first (enabled before disabled)
            if (aActive && !bActive) return -1;
            if (!aActive && bActive) return 1;

            // 3. Funded balances (> $0.01) before empty ones
            if (aBal > 0.01 && bBal <= 0.01) return -1;
            if (bBal > 0.01 && aBal <= 0.01) return 1;

            // 4. Higher balance first
            if (Math.abs(aBal - bBal) > 0.01) return bBal - aBal;

            // 5. Alphabetical fallback
            return a.localeCompare(b);
        });

    // Portfolio level aggregates
    let totalWallet = 0;
    let totalAvail = 0;
    let totalPositions = 0;
    let totalDailyPnl = 0;

    for (const ex of exNames) {
        const bal = (exchangesData.balances && exchangesData.balances[ex]) || 0;
        const marginBal = (exchangesData.margin_balances && exchangesData.margin_balances[ex]) || 0;
        const positions = (exchangesData.positions && exchangesData.positions[ex]) || [];
        const dailyPnl = (exchangesData.daily_pnl && exchangesData.daily_pnl[ex]) || 0;
        
        totalWallet += bal;
        totalAvail += marginBal;
        totalPositions += positions.length;
        totalDailyPnl += dailyPnl;
    }

    const totalUsedMargin = Math.max(0, totalWallet - totalAvail);
    const totalDailyPct = totalWallet > 0 ? (totalDailyPnl / totalWallet) * 100 : 0;
    const pnlColor = totalDailyPnl >= 0 ? 'var(--green)' : 'var(--red)';

    let html = `
        <div class="cex-dashboard">
            
            <!-- Executive Portfolio Summary Bar -->
            <div class="cex-summary-bar">
                <div class="cex-summary-card">
                    <div class="cex-summary-label">💳 Total CEX Equity</div>
                    <div class="cex-summary-value">$${totalWallet.toFixed(2)}</div>
                    <div class="cex-summary-sub">Combined wallet balances</div>
                </div>
                <div class="cex-summary-card">
                    <div class="cex-summary-label">🛡️ Available Margin</div>
                    <div class="cex-summary-value" style="color:var(--accent);">$${totalAvail.toFixed(2)}</div>
                    <div class="cex-summary-sub">$${totalUsedMargin.toFixed(2)} currently in trade</div>
                </div>
                <div class="cex-summary-card">
                    <div class="cex-summary-label">⚡ Active Positions</div>
                    <div class="cex-summary-value">${totalPositions}</div>
                    <div class="cex-summary-sub">Across all connected exchanges</div>
                </div>
                <div class="cex-summary-card">
                    <div class="cex-summary-label">📈 Realized PnL (Today)</div>
                    <div class="cex-summary-value" style="color:${pnlColor};">${totalDailyPnl >= 0 ? '+' : ''}$${totalDailyPnl.toFixed(2)}</div>
                    <div class="cex-summary-sub" style="color:${pnlColor};">${totalDailyPct >= 0 ? '+' : ''}${totalDailyPct.toFixed(2)}% ROI</div>
                </div>
            </div>

            <!-- Connected Exchange Cards Grid -->
            <div class="cex-grid">
    `;
    
    for (const exA of exNames) {
        const rawArr = exchangesData.data[exA];
        const bal = (exchangesData.balances && exchangesData.balances[exA]) || 0;
        const marginBal = (exchangesData.margin_balances && exchangesData.margin_balances[exA]) || 0;
        const positions = (exchangesData.positions && exchangesData.positions[exA]) || [];
        const fee = (exchangesData.fees && exchangesData.fees[exA]) || 0;
        const dailyPnl = (exchangesData.daily_pnl && exchangesData.daily_pnl[exA]) || 0;
        const dailyPct = (exchangesData.daily_pnl_pct && exchangesData.daily_pnl_pct[exA]) || 0;

        let maxTickers = 0;
        for (const exB of exNames) {
            if (exA === exB) continue;
            let matchCount = 0;
            if (rawArr && typeof rawArr === 'object' && rawArr[`vs ${exB}`]) {
                matchCount = rawArr[`vs ${exB}`];
                if (matchCount > maxTickers) maxTickers = matchCount;
            }
        }

        const isEnabled = !disabledMap[exA];
        const exPnlColor = dailyPnl >= 0 ? 'var(--green)' : 'var(--red)';
        
        // Margin utilization math
        const marginUsed = Math.max(0, bal - marginBal);
        const usePct = bal > 0 ? Math.min(100, Math.round((marginUsed / bal) * 100)) : 0;
        const barColor = usePct > 80 ? 'var(--red)' : usePct > 40 ? 'var(--amber)' : 'var(--accent)';

        html += `
            <div class="cex-card">
                
                <!-- Exchange Card Header -->
                <div class="cex-card-header">
                    <div class="cex-card-title">
                        <div class="cex-status-dot ${isEnabled ? 'on' : 'off'}"></div>
                        <span class="cex-card-name">${exA.toUpperCase()}</span>
                        <span class="badge ${isEnabled ? 'badge-long' : 'badge-neutral'}">${isEnabled ? 'ACTIVE' : 'OFF'}</span>
                    </div>

                    <label class="switch" title="Toggle ${exA} trading">
                        <input type="checkbox" 
                               ${isEnabled ? 'checked' : ''} 
                               onchange="toggleExchange('${profile.id}', '${exA}', this)">
                        <span class="slider round"></span>
                    </label>
                </div>

                <!-- Balances & Margin Utilization -->
                <div class="cex-balance-box">
                    <div class="cex-balance-row">
                        <div class="cex-balance-col">
                            <span class="cex-balance-title">Wallet Balance</span>
                            <span class="cex-balance-amount" style="color:${bal > 0 ? 'var(--text)' : 'var(--text-sub)'}">$${bal.toFixed(2)}</span>
                        </div>
                        <div class="cex-balance-col" style="text-align:right;">
                            <span class="cex-balance-title">Available Margin</span>
                            <span class="cex-balance-amount" style="color:${marginBal > 0 ? 'var(--green)' : 'var(--text-sub)'}">$${marginBal.toFixed(2)}</span>
                        </div>
                    </div>

                    <div class="cex-progress-wrap">
                        <div class="cex-progress-bar-bg">
                            <div class="cex-progress-bar-fill" style="width:${usePct}%;background:${barColor};"></div>
                        </div>
                        <div class="cex-progress-labels">
                            <span>Margin Used: <strong>${usePct}%</strong> ($${marginUsed.toFixed(2)})</span>
                            <span style="color:${exPnlColor};font-weight:600;">Day: ${dailyPnl >= 0 ? '+' : ''}$${dailyPnl.toFixed(2)} (${dailyPct >= 0 ? '+' : ''}${dailyPct.toFixed(2)}%)</span>
                        </div>
                    </div>
                </div>

                <!-- Badges & Fee Rate -->
                <div class="cex-badges-row">
                    <span>🎯 Active Pairs: <strong style="color:var(--text);">${maxTickers > 0 ? maxTickers + '+' : 'Syncing…'}</strong></span>
                    <span>🏷️ Taker Fee: <strong style="color:var(--text);">${(fee * 100).toFixed(3)}%</strong></span>
                </div>

                <!-- Active Positions List -->
                <div class="cex-pos-section">
                    <div class="cex-pos-title">
                        <span>Active Positions (${positions.length})</span>
                    </div>

                    ${positions.length === 0 ? `
                        <div class="cex-empty-pos">✨ No open positions on ${exA}</div>
                    ` : positions.map(pos => {
                        const isLong = pos.size > 0;
                        const posPnlColor = pos.unrealized_pnl >= 0 ? 'var(--green)' : 'var(--red)';
                        const dirBadge = isLong ? '<span class="badge badge-long">LONG</span>' : '<span class="badge badge-short">SHORT</span>';
                        return `
                            <div class="cex-pos-card">
                                <div class="cex-pos-info">
                                    <div class="cex-pos-sym-row">
                                        ${dirBadge}
                                        <span>${pos.symbol}</span>
                                    </div>
                                    <div class="cex-pos-price-row">
                                        Size: <strong>${Math.abs(pos.size)}</strong> | Entry: <strong>$${pos.entry_price ? Number(pos.entry_price).toFixed(4) : '—'}</strong>
                                    </div>
                                </div>
                                <div class="cex-pos-right">
                                    <div class="cex-pos-pnl" style="color:${posPnlColor}">
                                        ${pos.unrealized_pnl >= 0 ? '+' : ''}$${Number(pos.unrealized_pnl || 0).toFixed(4)}
                                    </div>
                                    <button class="cex-close-btn" onclick="closeActivePosition('${exA}', '${pos.symbol}', ${pos.size})" title="Market Close Position">Close</button>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>

            </div>
        `;
    }
    html += `</div></div>`;
    return html;
}

window.closeActivePosition = async function(exchange, symbol, size) {
    if (!confirm(`Are you sure you want to market close ${symbol} on ${exchange.toUpperCase()}?`)) return;
    try {
        showToast(`Closing ${symbol} on ${exchange.toUpperCase()}…`, 'info');
        const creds = localStorage.getItem('cex_auth') || '';
        const res = await fetch(`${API_BASE}/api/positions/close`, {
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Authorization': 'Basic ' + creds
            },
            body: JSON.stringify({ exchange, symbol, size: parseFloat(size) })
        });
        if (res.ok) {
            showToast(`✅ ${symbol} on ${exchange.toUpperCase()} closed successfully!`, 'success');
            setTimeout(refreshActiveTab, 1000);
        } else {
            let errText = `Failed to close ${symbol} on ${exchange}`;
            try { const body = await res.text(); if (body) errText += ': ' + body; } catch(e) {}
            showToast(errText, 'error');
        }
    } catch (e) {
        showToast(`Error closing ${symbol} on ${exchange}: ${e.message}`, 'error');
    }
};

function renderSettingsForm(profile) {
    const p = profile;
    const isRunning = !!p.is_active;
    return `
        <div style="margin-bottom:20px;display:flex;gap:0;background:var(--bg-row);border-radius:8px;padding:3px;">
            <button class="mt-nav-btn mt-nav-btn--active" id="stab-profile-btn" onclick="switchSettingsTab('profile')"
                style="flex:1;margin:0;border-radius:6px;padding:10px 0;font-size:.9rem;">Profile Strategy Settings</button>
            <button class="mt-nav-btn" id="stab-tech-btn" onclick="switchSettingsTab('technical')"
                style="flex:1;margin:0;border-radius:6px;padding:10px 0;font-size:.9rem;">Technical & Engine Settings</button>
        </div>

        <!-- Profile Settings (per-profile) -->
        <section id="stab-profile" class="trading-settings-section" style="padding:0;background:transparent;border:none;">
            <form id="settings-form-${p.id}" class="settings-container" onsubmit="return false;">
                
                <div class="settings-sections-grid">
                    
                    <!-- Card 1: Capital & Sizing -->
                    <div class="settings-card">
                        <div class="settings-card-header">
                            <div class="settings-card-icon">💰</div>
                            <div>
                                <div class="settings-card-title">Capital & Position Sizing</div>
                                <div class="settings-card-desc">Volume, leverage and concurrent position limits</div>
                            </div>
                        </div>
                        <div class="settings-card-fields">
                            <div class="field-item">
                                <label for="sf-vol-${p.id}">Trade Volume (USD)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-vol-${p.id}" step="1" min="5" value="${p.trade_volume??10}" />
                                </div>
                                <div class="field-hint">Notional volume per leg</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-leverage-${p.id}">Leverage (x)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-leverage-${p.id}" step="1" min="1" max="125" value="${p.leverage??10}" />
                                </div>
                                <div class="field-hint">Isolated cross leverage</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-max-trades-${p.id}">Max Active Trades</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-max-trades-${p.id}" step="1" min="1" max="50" value="${p.max_active_trades??5}" />
                                </div>
                                <div class="field-hint">Max concurrent pairs</div>
                            </div>
                        </div>
                    </div>

                    <!-- Card 2: Targets & Risk Management -->
                    <div class="settings-card">
                        <div class="settings-card-header">
                            <div class="settings-card-icon">🎯</div>
                            <div>
                                <div class="settings-card-title">Targets & Risk Management</div>
                                <div class="settings-card-desc">Take profit, spread expansion stop loss and slippage</div>
                            </div>
                        </div>
                        <div class="settings-card-fields">
                            <div class="field-item">
                                <label for="sf-tp-${p.id}">Take Profit (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-tp-${p.id}" step="0.01" min="0.01" value="${p.take_profit_percent??0.2}" />
                                </div>
                                <div class="field-hint">Target net profit to exit</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-stop-${p.id}">Stop Loss (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-stop-${p.id}" step="0.05" min="0" value="${p.trailing_stop_percent??0.5}" />
                                </div>
                                <div class="field-hint">Spread widening tolerance (0 = off)</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-slip-${p.id}">Slippage IOC (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-slip-${p.id}" step="0.01" min="0.01" max="5" value="${p.slippage_percent??0.5}" />
                                </div>
                                <div class="field-hint">Max price slip per leg</div>
                            </div>
                        </div>
                    </div>

                    <!-- Card 3: Scanner & Liquidity Filters -->
                    <div class="settings-card" style="grid-column: 1 / -1;">
                        <div class="settings-card-header">
                            <div class="settings-card-icon">🔍</div>
                            <div>
                                <div class="settings-card-title">Scanner & Liquidity Filters</div>
                                <div class="settings-card-desc">Spread entry window, structural deviation and orderbook depth</div>
                            </div>
                        </div>
                        <div class="settings-card-fields" style="grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));">
                            <div class="field-item">
                                <label for="sf-min-${p.id}">Min Entry Spread (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-min-${p.id}" step="0.01" value="${p.min_entry_spread??0.5}" />
                                </div>
                                <div class="field-hint">Minimum spread to trigger signal</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-max-${p.id}">Max Entry Spread (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-max-${p.id}" step="0.5" value="${p.max_entry_spread??50}" />
                                </div>
                                <div class="field-hint">Upper bound (filters bad ticks)</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-dev-${p.id}">Min Structural Dev (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-dev-${p.id}" step="0.05" min="0" value="${p.min_structural_deviation??0}" />
                                </div>
                                <div class="field-hint">Spread minus EMA baseline (0 = off)</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-minvol-${p.id}">Min 24h Vol (M$)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-minvol-${p.id}" step="0.5" min="0" value="${p.min_volume_millions??10}" />
                                </div>
                                <div class="field-hint">24h volume floor per exchange</div>
                            </div>
                            <div class="field-item">
                                <label for="sf-depth-${p.id}">L1 Book Depth (%)</label>
                                <div class="field-input-wrap">
                                    <input type="number" id="sf-depth-${p.id}" step="5" min="0" value="${p.min_depth_percent??100}" />
                                </div>
                                <div class="field-hint">% of volume required in top level</div>
                            </div>
                        </div>
                    </div>

                    <!-- Card 4: Blacklist -->
                    <div class="settings-card" style="grid-column: 1 / -1;">
                        <div class="settings-card-header">
                            <div class="settings-card-icon">🚫</div>
                            <div>
                                <div class="settings-card-title">Blacklisted Trading Pairs</div>
                                <div class="settings-card-desc">Excluded from scanner and automated trading (one symbol per line)</div>
                            </div>
                        </div>
                        <div>
                            <textarea id="sf-blacklist"
                                spellcheck="false"
                                placeholder="BTCUSDT&#10;ETHUSDT&#10;..."
                                style="width:100%;min-height:90px;max-height:200px;background:var(--bg-ctrl);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 14px;font-family:var(--font-mono);font-size:.85rem;line-height:1.6;resize:vertical;box-sizing:border-box;"
                            ></textarea>
                        </div>
                    </div>

                </div>

                <!-- Bottom Control Actions Bar -->
                <div class="settings-actions-panel">
                    <div style="display:flex;align-items:center;gap:12px;">
                        <span class="settings-status-badge" style="background:${isRunning ? 'var(--green-dim)' : 'var(--red-dim)'};color:${isRunning ? 'var(--green)' : 'var(--red)'};border:1px solid ${isRunning ? 'rgba(34,197,94,0.3)' : 'rgba(239,68,68,0.3)'};">
                            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${isRunning ? 'var(--green)' : 'var(--red)'};"></span>
                            ${isRunning ? 'BOT ACTIVE' : 'BOT STOPPED'}
                        </span>
                        <span style="color:var(--text-sub);font-size:.82rem;">Profile ID: <strong>${p.id}</strong></span>
                    </div>

                    <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;">
                        <button type="button" id="sf-power-${p.id}"
                            onclick="toggleBotPower('${p.id}')"
                            class="trading-save-btn"
                            style="margin:0;width:auto;padding:10px 24px;font-size:.9rem;background:${isRunning ? 'var(--red)' : 'var(--green)'};">
                            ${isRunning ? '⏹ Stop Bot' : '▶ Start Bot'}
                        </button>
                        <button type="button" id="sf-resume-${p.id}"
                            onclick="resumeKillSwitch()"
                            class="trading-save-btn"
                            style="margin:0;width:auto;padding:10px 24px;font-size:.9rem;background:#ff3b3b;display:${window._killSwitchActive ? 'inline-flex' : 'none'};animation:pulse-red 1.5s infinite;">
                            ⚠️ Resume Suspended Engine
                        </button>
                        <button type="button"
                            onclick="saveSettings('${p.id}')"
                            class="trading-save-btn"
                            id="sf-save-${p.id}"
                            style="margin:0;width:auto;padding:10px 24px;font-size:.9rem;background:var(--accent);color:#fff;border:none;">
                            💾 Save Settings
                        </button>
                        <button type="button"
                            onclick="clearTrades('${p.id}', '${p.mode}')"
                            class="trading-save-btn outline-btn"
                            style="margin:0;width:auto;padding:10px 20px;font-size:.85rem;background:transparent;color:var(--text-sub);border:1px solid var(--border);">
                            🗑️ Wipe Trades
                        </button>
                    </div>
                </div>
            </form>
        </section>

        <!-- Technical Settings (global engine params) -->
        <section id="stab-technical" style="display:none;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;gap:16px;flex-wrap:wrap;">
                <div>
                    <h3 style="margin:0 0 4px 0;font-size:1.15rem;font-weight:700;">Engine Technical Settings</h3>
                    <span style="color:var(--text-sub);font-size:.82rem;">Global low-level parameters — applied to core-node engine runtime</span>
                </div>
                <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;">
                    <div class="logs-search-wrap" style="min-width:220px;">
                        <span class="logs-search-icon">🔍</span>
                        <input type="text" id="tech-search" class="logs-search-input" placeholder="Search parameters..." oninput="filterTechSettingsRealtime()" />
                    </div>
                    <button class="trading-save-btn" id="tech-save-btn" onclick="saveTechnicalSettings()"
                        style="margin:0;width:auto;padding:10px 28px;font-size:.9rem;background:var(--accent);color:#fff;border:none;">💾 Save Technical</button>
                </div>
            </div>
            <div id="tech-settings-table">Loading...</div>
        </section>
    `;
}

// ── Settings sub-tab switching ───────────────────────────────────────────────

window.switchSettingsTab = function(tab) {
    const profEl = document.getElementById('stab-profile');
    const techEl = document.getElementById('stab-technical');
    const profBtn = document.getElementById('stab-profile-btn');
    const techBtn = document.getElementById('stab-tech-btn');
    if (!profEl || !techEl) return;
    if (tab === 'technical') {
        profEl.style.display = 'none';
        techEl.style.display = 'block';
        profBtn.classList.remove('mt-nav-btn--active');
        techBtn.classList.add('mt-nav-btn--active');
        loadTechnicalSettings();
    } else {
        profEl.style.display = 'block';
        techEl.style.display = 'none';
        profBtn.classList.add('mt-nav-btn--active');
        techBtn.classList.remove('mt-nav-btn--active');
    }
};

let _techSettingsData = null;

async function loadTechnicalSettings() {
    const wrap = document.getElementById('tech-settings-table');
    if (!wrap) return;
    try {
        _techSettingsData = await apiFetch('/api/technical-settings');
        renderTechSettingsTable(wrap, _techSettingsData);
    } catch (e) {
        wrap.innerHTML = '<div style="color:var(--red);padding:24px;text-align:center;">Failed to load: ' + e.message + '</div>';
    }
}

// ── Blacklist load/save ──────────────────────────────────────────────────────
let _blacklistOriginal = '';

async function loadBlacklistIntoTextarea() {
    const ta = document.getElementById('sf-blacklist');
    if (!ta) return;
    try {
        const data = await apiFetch('/api/technical-settings');
        if (Array.isArray(data)) {
            const bl = data.find(d => d.key === 'blacklist');
            if (bl && bl.value) {
                const lines = bl.value.split(',').map(s => s.trim()).filter(s => s.length > 0);
                ta.value = lines.join('\n');
                _blacklistOriginal = bl.value;
            }
        }
    } catch (e) {
        console.warn('Failed to load blacklist:', e);
    }
}

window.filterTechSettingsRealtime = function() {
    const wrap = document.getElementById('tech-settings-table');
    if (wrap && _techSettingsData) {
        renderTechSettingsTable(wrap, _techSettingsData);
    }
};

function renderTechSettingsTable(wrap, data) {
    const CATEGORY_META = {
        execution: { title: 'Order Execution & Latency', icon: '⚡', desc: 'Order placement type, verification polling and network timeouts' },
        cooldowns: { title: 'Cooldowns & Balance Guards', icon: '⏱️', desc: 'Temporary pauses after failed fills or margin lock errors' },
        spreads:   { title: 'Spread Math & Stale Data', icon: '📊', desc: 'Ticker freshness thresholds and long-term EMA time constants' },
        entry:     { title: 'Volatility & Depth Guards', icon: '🛡️', desc: 'Bid-ask width limits, volume discrepancy and listing age checks' },
        intervals: { title: 'Engine Cycles & Intervals', icon: '🔄', desc: 'Background polling frequencies and database write throttle' },
        general:   { title: 'General Parameters', icon: '⚙️', desc: 'Miscellaneous engine runtime parameters' },
    };

    const FRIENDLY_NAMES = {
        entry_order_type: 'Order Placement Mode',
        api_timeout_ms: 'API Network Timeout (ms)',
        fill_verify_retries: 'Fill Verification Retries',
        fill_verify_delay_ms: 'Fill Verify Interval (ms)',
        close_verify_retries: 'Close Verification Retries',
        close_verify_delay_ms: 'Close Verify Interval (ms)',
        emergency_close_retries: 'Emergency Close Retries',
        exit_sanity_pct: 'Max Price Sanity Deviation',
        pair_cooldown_ms: 'Unfilled Pair Cooldown (ms)',
        exchange_cooldown_ms: 'Exchange Margin Cooldown (ms)',
        asymmetric_cooldown_ms: 'Asymmetric Leg Cooldown (ms)',
        margin_check_multiplier: 'Margin Buffer Multiplier',
        stale_secs: 'Max Ticker Stale Time (s)',
        intra_spread_max_pct: 'Max Internal Bid-Ask Spread (%)',
        intra_spread_limit: 'Max Ask/Bid Ratio Limit',
        ema_tau_secs: 'Structural Spread EMA Tau (s)',
        vol_diff_max_pct: 'Max Volume Leg Mismatch (%)',
        vol_tolerance_pct: 'Trade Volume Tolerance (±%)',
        min_listing_age_days: 'Min Coin Listing Age (days)',
        binance_min_notional: 'Binance Min Notional ($)',
        default_fee_rate: 'Fallback Taker Fee Fraction',
        max_spreads_per_cycle: 'Max Spreads Evaluated / Tick',
        balance_refresh_secs: 'Balance Refresh Interval (s)',
        live_update_throttle_ms: 'Trade PnL DB Update Throttle (ms)',
        diag_log_interval: 'Diagnostic Log Frequency (cycles)',
        multiplier_refresh_secs: 'Contract Specs Refresh (s)',
    };

    // Keys that exist in DB but have zero effect in engine code or are duplicate of Profile settings
    const HIDDEN_KEYS = new Set([
        'safety_margin', 'deviation_safety', 'max_price_ratio',
        'max_funding_diff', 'min_ioc_tolerance_pct', 'funding_cache_secs',
        'depth_multiplier', 'blacklist', 'intra_spread_limit',
        'vol_tolerance_pct', 'multiplier_refresh_secs', 'default_fee_rate',
    ]);

    const searchQuery = (document.getElementById('tech-search')?.value || '').trim().toLowerCase();

    // Group by category
    const groups = {};
    for (const item of data) {
        if (HIDDEN_KEYS.has(item.key)) continue;
        if (searchQuery) {
            const matchKey = item.key.toLowerCase().includes(searchQuery);
            const matchName = (FRIENDLY_NAMES[item.key] || '').toLowerCase().includes(searchQuery);
            const matchDesc = (item.description || '').toLowerCase().includes(searchQuery);
            if (!matchKey && !matchName && !matchDesc) continue;
        }
        const cat = item.category || 'general';
        if (!groups[cat]) groups[cat] = [];
        groups[cat].push(item);
    }

    const catOrder = ['execution', 'cooldowns', 'spreads', 'entry', 'intervals', 'general'];
    let html = '<div class="settings-sections-grid">';
    let renderedCards = 0;

    for (const cat of catOrder) {
        const items = groups[cat];
        if (!items || items.length === 0) continue;
        renderedCards++;
        const meta = CATEGORY_META[cat] || { title: cat, icon: '⚙️', desc: '' };

        html += `
            <div class="settings-card" style="grid-column: 1 / -1;">
                <div class="settings-card-header">
                    <div class="settings-card-icon">${meta.icon}</div>
                    <div>
                        <div class="settings-card-title">${meta.title}</div>
                        <div class="settings-card-desc">${meta.desc}</div>
                    </div>
                </div>
                <div class="settings-card-fields" style="grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));">
        `;

        for (const item of items) {
            const ENUMS = {
                entry_order_type: [
                    { value: 'market', label: 'Market — guaranteed fill, no asymmetric risk' },
                    { value: 'ioc', label: 'IOC — limit with slippage protection, risk of partial fill' },
                ],
            };

            const inputStyle = 'width:100%;height:38px;background:var(--bg-ctrl);color:var(--text);border:1px solid var(--border);border-radius:var(--radius-sm);padding:6px 12px;font-family:var(--font-mono);font-size:.88rem;box-sizing:border-box;outline:none;';
            let valueInput;
            if (ENUMS[item.key]) {
                const opts = ENUMS[item.key].map(o =>
                    `<option value="${o.value}" ${item.value === o.value ? 'selected' : ''}>${o.label}</option>`
                ).join('');
                valueInput = `<select id="ts-${item.key}" style="${inputStyle}">${opts}</select>`;
            } else {
                valueInput = `<input type="text" id="ts-${item.key}" value="${escHtml(item.value)}" style="${inputStyle}" />`;
            }

            const friendlyName = FRIENDLY_NAMES[item.key] || item.key;

            html += `
                <div class="field-item" style="background:var(--bg-ctrl);padding:14px;border-radius:8px;border:1px solid var(--border-dim);gap:8px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-weight:600;font-size:0.84rem;color:var(--text);">${friendlyName}</span>
                        <code style="font-size:0.7rem;color:var(--text-sub);background:rgba(255,255,255,0.05);padding:2px 6px;border-radius:4px;">${item.key}</code>
                    </div>
                    <div class="field-input-wrap">
                        ${valueInput}
                    </div>
                    <div class="field-hint" style="font-size:0.73rem;color:var(--text-sub);line-height:1.4;">${item.description || ''}</div>
                </div>
            `;
        }

        html += `</div></div>`;
    }

    html += '</div>';

    if (renderedCards === 0) {
        html = `<div style="text-align:center;padding:48px;color:var(--text-sub);">✨ No technical settings match "${escHtml(searchQuery)}"</div>`;
    }

    wrap.innerHTML = html;
}

window.saveTechnicalSettings = async function() {
    if (!_techSettingsData) return;
    const btn = document.getElementById('tech-save-btn');
    const changes = {};
    let count = 0;
    for (const item of _techSettingsData) {
        const el = document.getElementById('ts-' + item.key);
        if (el && el.value !== item.value) {
            changes[item.key] = el.value;
            count++;
        }
    }
    if (count === 0) {
        showToast('No changes detected', 'info');
        return;
    }
    try {
        if (btn) { btn.textContent = 'Saving...'; btn.disabled = true; }
        await apiFetch('/api/technical-settings', {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(changes),
        });
        // Update local cache
        for (const item of _techSettingsData) {
            if (changes[item.key] !== undefined) item.value = changes[item.key];
        }
        if (btn) { btn.textContent = 'Saved!'; btn.style.background = 'var(--green)'; }
        showToast(`${count} setting(s) saved — engine will pick up in ~10s`, 'success');
    } catch (e) {
        if (btn) { btn.textContent = 'Error!'; btn.style.background = 'var(--red)'; }
        showToast('Save failed: ' + e.message, 'error');
    }
    setTimeout(() => {
        if (btn) { btn.textContent = 'Save Technical'; btn.style.background = ''; btn.disabled = false; }
    }, 2000);
};


// ─── Per-profile dashboard HTML scaffold ─────────────────────────────────────

function renderDashboardHTML(profileId) {
    const pid = profileId;
    return `
        <div style="padding:16px 0;display:grid;gap:24px;">
            <!-- KPI row -->
            <div id="dash-kpi-${pid}" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;"></div>
            
            <!-- Balance Analysis (Stacked) -->
            <div class="chart-card">
                <div class="chart-card-title">Balance Analysis</div>
                <div style="position:relative;height:320px;width:100%;">
                    <canvas id="dash-balance-${pid}"></canvas>
                </div>
            </div>

            <!-- Top Tokens and Pairs -->
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
                <div class="chart-card">
                    <div class="chart-card-title">Top Tokens by PnL</div>
                    <div class="table-wrap" style="max-height: 240px; overflow-y: auto;">
                        <table style="width:100%;text-align:left;">
                            <thead><tr><th>Token</th><th style="text-align:right">PnL</th></tr></thead>
                            <tbody id="dash-toptokens-${pid}"></tbody>
                        </table>
                    </div>
                </div>
                <div class="chart-card">
                    <div class="chart-card-title">Top Exchange Pairs by PnL</div>
                    <div class="table-wrap" style="max-height: 240px; overflow-y: auto;">
                        <table style="width:100%;text-align:left;">
                            <thead><tr><th>Exchange Pair</th><th style="text-align:right">PnL</th></tr></thead>
                            <tbody id="dash-toppairs-${pid}"></tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- Charts row 1: Equity + Win/Loss -->
            <div style="display:grid;grid-template-columns:2fr 1fr;gap:16px;">
                <div class="chart-card">
                    <div class="chart-card-title">Equity Curve</div>
                    <div style="position:relative;height:240px;width:100%;">
                        <canvas id="dash-equity-${pid}"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <div class="chart-card-title">Win / Loss</div>
                    <div style="position:relative;height:240px;width:100%;">
                        <canvas id="dash-winloss-${pid}"></canvas>
                    </div>
                </div>
            </div>
            <!-- Charts row 2: Exchange PnL -->
            <div class="chart-card">
                <div class="chart-card-title">PnL by Exchange</div>
                <div style="position:relative;height:260px;width:100%;">
                    <canvas id="dash-exchange-${pid}"></canvas>
                </div>
            </div>
        </div>
    `;
}

// ─── KPI Cards helper (extracted for reuse) ───────────────────────────────────
function renderKPICards(pid, stats, exchanges) {
    const kpiEl = document.getElementById(`dash-kpi-${pid}`);
    if (!kpiEl) return;
    const totalPnl = stats.total_pnl || 0;
    const totalTrades = stats.total_trades || 0;
    const wins = stats.wins || 0;
    const winRate = totalTrades > 0 ? (wins / totalTrades * 100).toFixed(1) : '0.0';
    const avgPnl = stats.avg_pnl || 0;
    const avgDuration = stats.avg_duration || 0;
    const realBalances = (exchanges && exchanges.balances) || {};
    const totalBalance = Object.values(realBalances).reduce((s, v) => s + v, 0);

    const kpis = [
        ['💰', 'Total Balance', `$${totalBalance.toFixed(2)}`, 'var(--text)'],
        ['📈', 'Total PnL', fmtPnL(totalPnl), totalPnl >= 0 ? 'var(--green)' : 'var(--red)'],
        ['📊', 'Trades', totalTrades, 'var(--text)'],
        ['🎯', 'Win Rate', `${winRate}%`, wins / (totalTrades || 1) >= 0.5 ? 'var(--green)' : 'var(--red)'],
        ['⚡', 'Avg PnL', fmtPnL(avgPnl), avgPnl >= 0 ? 'var(--green)' : 'var(--red)'],
        ['⏱', 'Avg Duration', typeof fmtDuration === 'function' ? fmtDuration(avgDuration) : avgDuration + 'ms', 'var(--text)'],
    ];
    kpiEl.innerHTML = kpis.map(([icon, label, value, color]) => `
        <div class="kpi-card">
            <div class="kpi-label">${icon} ${label}</div>
            <div class="kpi-value" style="color:${color}">${value}</div>
        </div>
    `).join('');
}

// ─── Top tables helper (extracted for reuse) ──────────────────────────────────
function renderTopTables(pid, stats) {
    const ttEl = document.getElementById(`dash-toptokens-${pid}`);
    if (ttEl && stats.top_tokens && stats.top_tokens.length > 0) {
        ttEl.innerHTML = stats.top_tokens.map(t => `<tr><td><strong>${t.token}</strong></td><td style="text-align:right;color:${(t.pnl||0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl||0)}</td></tr>`).join('');
    } else if (ttEl) {
        ttEl.innerHTML = '<tr><td colspan="2" style="text-align:center;color:var(--text-sub)">No data</td></tr>';
    }
    const tpEl = document.getElementById(`dash-toppairs-${pid}`);
    if (tpEl && stats.top_exchange_pairs && stats.top_exchange_pairs.length > 0) {
        tpEl.innerHTML = stats.top_exchange_pairs.map(p => `<tr><td><strong>${p.pair}</strong></td><td style="text-align:right;color:${(p.pnl||0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(p.pnl||0)}</td></tr>`).join('');
    } else if (tpEl) {
        tpEl.innerHTML = '<tr><td colspan="2" style="text-align:center;color:var(--text-sub)">No data</td></tr>';
    }
}

// ─── Per-profile dashboard chart rendering ────────────────────────────────────

// Hash stats to detect actual data changes and skip unnecessary chart updates
let _lastStatsHash = null;
function statsHash(stats) {
    if (!stats) return '';
    return `${stats.total_trades}_${stats.total_pnl}_${stats.wins}_${stats.losses}_${(stats.equity_curve||[]).length}`;
}

function renderDashboardCharts(profileId, stats, exchanges) {
    const pid = profileId;

    // Guard: Chart.js must be loaded
    if (typeof Chart === 'undefined') return;

    // Skip update if stats haven't changed (prevents chart jank on poll)
    const hash = statsHash(stats);
    if (_lastStatsHash === hash && chartInstances[pid] && Object.keys(chartInstances[pid]).length > 0) {
        // Stats unchanged — just update KPI text, skip expensive chart work
        renderKPICards(pid, stats, exchanges);
        renderTopTables(pid, stats);
        return;
    }
    _lastStatsHash = hash;

    // Only destroy if instances exist — avoids unnecessary GC pressure
    if (chartInstances[pid]) {
        Object.values(chartInstances[pid]).forEach(c => { try { c.destroy(); } catch {} });
    }
    chartInstances[pid] = {};

    // KPI cards
    renderKPICards(pid, stats, exchanges);

    // Top Tokens & Exchange Pairs
    renderTopTables(pid, stats);

    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = '#1c2235';

    // Balance Analysis Chart
    const balCtx = document.getElementById(`dash-balance-${pid}`);
    if (balCtx) {
        const curve = stats.equity_curve || [];
        const labels = [];
        
        const exBalances = {};
        const exKeys = new Set();
        curve.forEach(p => { if(p.ex_a) exKeys.add(p.ex_a); if(p.ex_b) exKeys.add(p.ex_b); });
        if (exKeys.size === 0) exKeys.add('Capital');

        // Use real exchange balances as starting capital (fallback to $0)
        const realBal = (exchanges && exchanges.balances) || {};
        const colors = ['#60a5fa', '#f43f5e', '#34d399', '#c084fc', '#fbbf24', '#fb923c'];
        let cIdx = 0;
        const colorMap = {};
        exKeys.forEach(ex => {
            exBalances[ex] = realBal[ex] || 0;
            colorMap[ex] = colors[cIdx % colors.length];
            cIdx++;
        });

        const historyByEx = {};
        exKeys.forEach(ex => historyByEx[ex] = []);
        const historyTotal = [];

        curve.forEach((p, i) => {
            if (p.ex_a) exBalances[p.ex_a] += (p.pnl || 0) / 2;
            else if (exKeys.has('Capital')) exBalances['Capital'] += (p.pnl || 0) / 2;
            
            if (p.ex_b) exBalances[p.ex_b] += (p.pnl || 0) / 2;
            else if (exKeys.has('Capital')) exBalances['Capital'] += (p.pnl || 0) / 2;
            
            if (curve.length <= 150 || i % Math.ceil(curve.length / 100) === 0 || i === curve.length - 1) {
                labels.push(`#${i + 1}`);
                let total = 0;
                exKeys.forEach(ex => {
                    historyByEx[ex].push(parseFloat(exBalances[ex].toFixed(2)));
                    total += exBalances[ex];
                });
                historyTotal.push(parseFloat(total.toFixed(2)));
            }
        });

        const datasets = [];
        exKeys.forEach(ex => {
            datasets.push({
                label: ex,
                data: historyByEx[ex],
                borderColor: colorMap[ex],
                backgroundColor: colorMap[ex] + '33', // 20% opacity
                fill: true,
                tension: 0.1,
                stack: 'exchanges',
                pointRadius: curve.length > 50 ? 0 : 2
            });
        });

        datasets.push({
            label: 'Total Capitalization',
            data: historyTotal,
            borderColor: '#fbbf24',
            borderDash: [5, 5],
            borderWidth: 2,
            backgroundColor: 'transparent',
            fill: false,
            tension: 0.1,
            stack: 'total', // separate stack so it overlays and doesn't compound
            pointRadius: curve.length > 50 ? 0 : 3
        });

        chartInstances[pid].balance = new Chart(balCtx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', align: 'end' },
                    tooltip: { callbacks: { label: function(c) { return c.dataset.label + ': $' + c.raw.toLocaleString(); } } }
                },
                scales: {
                    x: { grid: { color: '#1f2937' } },
                    y: { stacked: true, grid: { color: '#1f2937' } }
                }
            }
        });
    }

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
        const isExpanded = expandedRows[profileId]?.has(t.id);
        const detailDisplay = isExpanded ? 'table-row' : 'none';

        // Funding rate display
        const frA = t.funding_rate_a ?? 0;
        const frB = t.funding_rate_b ?? 0;
        const frA_pct = (frA * 100).toFixed(4);
        const frB_pct = (frB * 100).toFixed(4);
        const frA_cost = (frA * (t.volume ?? 0)).toFixed(4);
        const frB_cost = (frB * (t.volume ?? 0)).toFixed(4);
        const frA_color = frA >= 0 ? 'var(--red)' : 'var(--green)';
        const frB_color = frB >= 0 ? 'var(--red)' : 'var(--green)';
        const fundNextTs = t.funding_next_ts ?? 0;

        const detailRow = `
            <tr id="detail-${rowKey}" style="display:${detailDisplay}">
                <td colspan="8" class="p-0">
                    <div class="trade-details-box">
                        <div class="td-col">
                            <div class="td-header">${t.exchange_a} <span class="badge ${isAHigher ? 'badge-short' : 'badge-long'}">${isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${fmtPx(t.entry_price_a)}</strong></div>
                            <div class="td-line"><span>Current:</span> <strong>${t.current_price_a ? fmtPx(t.current_price_a) : '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_a??0)>0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_a??0)}</strong></div>
                            <div class="td-line"><span>Funding Rate:</span> <strong style="color:${frA_color}">${frA_pct}%</strong> <span class="text-sub" style="font-size:.75rem">($${frA_cost}/8h)</span></div>
                        </div>
                        <div class="td-col">
                            <div class="td-header">${t.exchange_b} <span class="badge ${!isAHigher ? 'badge-short' : 'badge-long'}">${!isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${fmtPx(t.entry_price_b)}</strong></div>
                            <div class="td-line"><span>Current:</span> <strong>${t.current_price_b ? fmtPx(t.current_price_b) : '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_b??0)>0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_b??0)}</strong></div>
                            <div class="td-line"><span>Funding Rate:</span> <strong style="color:${frB_color}">${frB_pct}%</strong> <span class="text-sub" style="font-size:.75rem">($${frB_cost}/8h)</span></div>
                        </div>
                        <div class="td-col td-total">
                            <div class="td-line"><span>Gross PnL:</span> <strong style="color:${(t.gross_pnl??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.gross_pnl??0)}</strong></div>
                            <div class="td-line"><span>Comm. Cost:</span> <strong style="color:var(--red)">${fmtCost(t.commission_cost)}</strong></div>
                            <div class="td-line"><span>Entry Slip:</span> <strong style="color:var(--red)">${fmtCost(t.slippage_cost)}</strong></div>
                            <div class="td-line"><span>Funding In:</span> <strong class="funding-countdown" data-next="${fundNextTs}" style="font-family:'Roboto Mono',monospace; font-variant-numeric:tabular-nums; color:var(--text-sub)">--:--:--</strong></div>
                            <div class="td-total-pnl">Net PnL: ${livePnLStr} <span class="text-sub" style="font-weight:400;font-size:.8rem;">(${fmtDuration(Date.now()-t.entry_time)})</span></div>
                            <div style="margin-top:12px; display:flex; gap:8px;">
                                <button type="button" class="trading-save-btn outline-btn" style="margin:0;flex:1;padding:8px;font-size:12px;" onclick='openModal({pair:"${t.pair}",exchangeA:"${t.exchange_a}",exchangeB:"${t.exchange_b}",priceA:${t.entry_price_a},priceB:${t.entry_price_b},spread:${t.entry_spread},volA:0,volB:0,direction:"${t.direction}",entryTime:${t.entry_time},exitTime:null})'>📊 View Chart</button>
                                <button type="button" class="trading-save-btn outline-btn" style="margin:0;flex:1;padding:8px;font-size:12px;color:var(--red);border-color:var(--red);" onclick="marketCloseTrade('${profileId}', ${t.id}, event)">❌ Market Close</button>
                            </div>
                        </div>
                    </div>
                </td>
            </tr>`;

        return `
            <tr class="clickable-row main-trade-row" onclick="toggleRow('${profileId}',${t.id})">
                <td><strong>${t.pair}</strong></td>
                <td>${livePnLStr}</td>
                <td>${(t.entry_spread??0).toFixed(2)}%</td>
                <td>${curSpread} / <span style="color:var(--green)">${bestStr}</span></td>
                <td><span style="color:var(--text-sub)">${t.direction}</span></td>
                <td>$${fmtPx(t.volume)}</td>
                <td style="color:var(--text-sub)">${time}</td>
                <td style="text-align:center;" onclick="event.stopPropagation()">
                    <button type="button" class="tab-close" style="color:var(--red);font-size:1.15rem;padding:2px 8px;cursor:pointer;background:transparent;border:1px solid rgba(239,68,68,0.25);border-radius:4px;" onclick="marketCloseTrade('${profileId}', ${t.id}, event)" title="Market Close Trade">✕</button>
                </td>
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
        const pnlStr = fmtPnL(pnl);
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
                            <div class="td-line"><span>Entry:</span> <strong>${fmtPx(t.entry_price_a)}</strong></div>
                            <div class="td-line"><span>Exit:</span> <strong>${t.exit_price_a ? fmtPx(t.exit_price_a) : '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_a??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_a??0)}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Opened:</span> <strong style="font-size:.75rem">${openTimeStr}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Closed:</span> <strong style="font-size:.75rem">${time}</strong></div>
                        </div>
                        <div class="td-col">
                            <div class="td-header">${t.exchange_b} <span class="badge ${!isAHigher ? 'badge-short' : 'badge-long'}">${!isAHigher ? 'SHORT' : 'LONG'}</span></div>
                            <div class="td-line"><span>Entry:</span> <strong>${fmtPx(t.entry_price_b)}</strong></div>
                            <div class="td-line"><span>Exit:</span> <strong>${t.exit_price_b ? fmtPx(t.exit_price_b) : '—'}</strong></div>
                            <div class="td-line"><span>Leg PnL:</span> <strong style="color:${(t.pnl_b??0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl_b??0)}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Opened:</span> <strong style="font-size:.75rem">${openTimeStr}</strong></div>
                            <div class="td-line"><span style="color:var(--text-sub)">Closed:</span> <strong style="font-size:.75rem">${time}</strong></div>
                        </div>
                        <div class="td-col td-total">
                            <div class="td-line"><span>Gross PnL:</span> <strong style="color:${((t.pnl_a??0)+(t.pnl_b??0))>=0?'var(--green)':'var(--red)'}">${fmtPnL((t.pnl_a??0)+(t.pnl_b??0))}</strong></div>
                            <div class="td-line"><span>Comm. Paid:</span> <strong style="color:var(--red)">${fmtCost(t.commission_cost)}</strong></div>
                            <div class="td-line"><span>Entry Slip:</span> <strong style="color:var(--red)">${fmtCost(t.slippage_cost)}</strong></div>
                            <div class="td-line"><span>Exit Slip:</span> <strong style="color:var(--red)">${fmtCost(t.exit_slippage_cost??0)}</strong></div>
                            <div class="td-line"><span>Total Slip:</span> <strong style="color:var(--red)">${fmtCost((t.slippage_cost??0)+(t.exit_slippage_cost??0))}</strong></div>
                            <div class="td-line"><span>Funding:</span> <strong style="color:${(t.funding_cost??0)<=0?'var(--green)':'var(--red)'}">${fmtCost(t.funding_cost)}</strong></div>
                            <div class="td-total-pnl">Net PnL: <span style="color:${pnlColor}">${pnlStr}</span> <span class="text-sub" style="font-weight:400;font-size:.8rem;">(${fmtDuration(t.exit_time-t.entry_time)})</span></div>
                            <div style="margin-top:12px;">
                                <button type="button" class="trading-save-btn outline-btn" style="margin:0;width:100%;padding:8px;font-size:12px;" 
                                    onclick='openModal({pair:"${t.pair}",exchangeA:"${t.exchange_a}",exchangeB:"${t.exchange_b}",priceA:${t.price_a},priceB:${t.price_b},spread:${t.entry_spread},volA:0,volB:0,direction:"${t.direction}",entryTime:${t.entry_time},exitTime:${t.exit_time}})'>📊 View Chart</button>
                            </div>
                        </div>
                    </div>
                </td>
            </tr>`;

        return `
            <tr class="clickable-row main-trade-row" onclick="toggleRow('${profileId}','h_${t.id}')">
                <td><strong>${t.pair}</strong></td>
                <td style="color:${pnlColor};font-weight:600">${pnlStr}</td>
                <td>${(t.entry_spread??0).toFixed(2)}% &rarr; ${(t.exit_spread??0).toFixed(2)}%</td>
                <td><span style="color:var(--text-sub)">${t.direction}</span></td>
                <td>$${fmtPx(t.volume)}</td>
                <td><span class="badge ${t.close_reason==='TP'||t.close_reason==='ZERO_SPREAD'?'badge-long':t.close_reason==='STOP'?'badge-short':'badge-neutral'}">${t.close_reason??'—'}</span></td>
                <td style="color:var(--text-sub)">${time}</td>
            </tr>${detailRow}`;
    }).join('');
}

// ─── UI Helpers ───────────────────────────────────────────────────────────────
function showToast(message, type = 'info') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    const toast = document.createElement('div');
    toast.className = `toast toast--${type}`;
    const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️';
    toast.innerHTML = `<span class="toast-icon">${icon}</span><span class="toast-msg">${message}</span>`;
    container.appendChild(toast);
    
    // Trigger animation
    setTimeout(() => toast.classList.add('toast--visible'), 10);
    
    // Auto-remove
    setTimeout(() => {
        toast.classList.remove('toast--visible');
        setTimeout(() => toast.remove(), 400);
    }, 4000);
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

    // (Re)init scanner when tab is shown
    if (tab === 'scanner' && window.initScanner) {
        window.initScanner(activeTabId);
    }
    if (tab === 'logs') {
        window.fetchLogs();
    }
};
window.logViewMode = window.logViewMode || 'simple';
window.lastFetchedLogs = window.lastFetchedLogs || '';
window._logAutoScroll = true;

window.setLogView = function(mode) {
    window.logViewMode = mode;
    ['simple', 'trade', 'error', 'exec', 'all'].forEach(m => {
        const btn = document.getElementById(`btn-log-${m}`);
        if (btn) btn.classList.toggle('mt-nav-btn--active', mode === m);
    });
    window.renderLogsHTML(window.lastFetchedLogs);
};

window.toggleLogAutoScroll = function() {
    window._logAutoScroll = !window._logAutoScroll;
    const statusEl = document.getElementById('autoscroll-status');
    if (statusEl) {
        statusEl.textContent = window._logAutoScroll ? 'ON' : 'PAUSED';
        statusEl.style.color = window._logAutoScroll ? 'var(--green)' : 'var(--amber)';
    }
};

window.copyVisibleLogs = function() {
    const el = document.getElementById('logs-container');
    if (!el) return;
    const text = el.innerText || el.textContent;
    if (!text || text.trim() === '') {
        showToast('No logs to copy', 'info');
        return;
    }
    navigator.clipboard.writeText(text).then(() => {
        showToast('📋 Logs copied to clipboard!', 'success');
    }).catch(() => {
        showToast('Failed to copy logs', 'error');
    });
};

window.filterLogsRealtime = function() {
    window.renderLogsHTML(window.lastFetchedLogs);
};

window.renderLogsHTML = function(text) {
    const el = document.getElementById('logs-container');
    const countBadge = document.getElementById('logs-count-badge');
    if (!el) return;

    const rawLines = text.split('\n').filter(l => l.trim().length > 0);
    const searchQuery = (document.getElementById('logs-search')?.value || '').trim().toLowerCase();
    const mode = window.logViewMode || 'simple';

    // ── Pure Trade Translator for Simple Mode ──
    const translateSimpleLog = (line) => {
        let m;
        // Entry Trigger
        if ((m = line.match(/FIRE (\S+) \[(\w+)\] A=(\S+) c=(\d+) lim=([\d.]+) \| B=(\S+) c=(\d+) lim=([\d.]+) spread=([\d.]+)% be=([\d.]+)%/))) {
            return {
                level: 'ENTRY',
                badgeClass: 'log-badge-info',
                lineClass: 'log-line--trade',
                text: `🚀 ВХОД ${m[1]}: ${m[3]} ↔ ${m[6]} | Объём: ${m[4]}×${m[7]} шт | Спред: ${m[9]}% > BE: ${m[10]}% (${m[2]})`
            };
        }
        // Fill Result
        if ((m = line.match(/RESULT (\S+) A=(\S+) B=(\S+) phys_a=([\d.]+) phys_b=([\d.]+) fill_a=([\d.]+) fill_b=([\d.]+) latency=(\d+)ms/))) {
            const aOk = m[2] === 'true', bOk = m[3] === 'true';
            if (aOk && bOk) {
                return {
                    level: 'FILLED',
                    badgeClass: 'log-badge-trade',
                    lineClass: 'log-line--trade',
                    text: `✅ ${m[1]}: ОБЕ НОГИ ЗАЛИТЫ — Leg A: $${m[6]} ($${m[4]}) | Leg B: $${m[7]} ($${m[5]}) [${m[8]}мс]`
                };
            }
            if (!aOk && !bOk) {
                return {
                    level: 'REJECT',
                    badgeClass: 'log-badge-warn',
                    lineClass: 'log-line--warn',
                    text: `⏹ ${m[1]}: ОРДЕРА НЕ ИСПОЛНИЛИСЬ — позиция не открыта [${m[8]}мс]`
                };
            }
            return {
                level: 'PARTIAL',
                badgeClass: 'log-badge-error',
                lineClass: 'log-line--error',
                text: `⚠️ ${m[1]}: ЧАСТИЧНЫЙ ФИЛЛ — Leg A=${aOk ? '✅' : '❌'} | Leg B=${bOk ? '✅' : '❌'} [${m[8]}мс]`
            };
        }
        // Open Verified
        if ((m = line.match(/\[EXCHANGE_VERIFIED\] ✅ OPEN (\S+) phys=([\d.]+) .* A_price=([\d.]+) B_price=([\d.]+) fees=([\d.]+) slip=([\d.]+) actual_sp=([\d.]+)%/))) {
            return {
                level: 'OPEN',
                badgeClass: 'log-badge-trade',
                lineClass: 'log-line--trade',
                text: `✨ ПОЗИЦИЯ ОТКРЫТА ${m[1]}: Объём $${m[2]} | A: $${m[3]}, B: $${m[4]} | Спред: ${m[7]}% | Комиссия: -$${m[5]}`
            };
        }
        // Retries
        if ((m = line.match(/RETRY second leg (\S+) for (\S+) with max slippage ([\d.]+)%/))) {
            return {
                level: 'RETRY',
                badgeClass: 'log-badge-warn',
                lineClass: 'log-line--warn',
                text: `🔄 ${m[2]}: Повтор второй ноги на ${m[1]} со slippage ${m[3]}%`
            };
        }
        if ((m = line.match(/RETRY SUCCESS: second leg (\S+) filled on retry/))) {
            return {
                level: 'FILLED',
                badgeClass: 'log-badge-trade',
                lineClass: 'log-line--trade',
                text: `✅ ${m[1]}: ВТОРАЯ НОГА УСПЕШНО ЗАЛИТА на повторе`
            };
        }
        // Close Trigger
        if ((m = line.match(/Closing (\d+) (\S+) reason=(\S+) pct=([\d.-]+)% exit_sp=([\d.-]+)/))) {
            const reasons = {
                TP: '🎯 Тейк-профит',
                STOP: '🛑 Стоп-лосс',
                MANUAL: '👤 Ручной выход',
                ZERO_SPREAD: '📉 Схождение спреда',
                RETRY_CLOSE: '🔄 Повтор закрытия'
            };
            const rName = reasons[m[3]] || m[3];
            const pct = parseFloat(m[4]);
            return {
                level: 'CLOSE',
                badgeClass: 'log-badge-info',
                lineClass: 'log-line--trade',
                text: `📤 ЗАКРЫТИЕ #${m[1]} ${m[2]}: ${rName} | PnL: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}% | Спред: ${m[5]}%`
            };
        }
        // Completed PnL
        if ((m = line.match(/EXCHANGE_VERIFIED.*PnL id=(\d+) pna=([\d.-]+) pnb=([\d.-]+) gross=([\d.-]+)/))) {
            const gross = parseFloat(m[4]);
            const isProfit = gross >= 0;
            return {
                level: isProfit ? 'PROFIT' : 'LOSS',
                badgeClass: isProfit ? 'log-badge-trade' : 'log-badge-error',
                lineClass: isProfit ? 'log-line--trade' : 'log-line--error',
                text: `${isProfit ? '💰' : '💸'} СДЕЛКА #${m[1]} ЗАВЕРШЕНА: Leg A=$${m[2]} | Leg B=$${m[3]} | ИТОГ = ${isProfit ? '+' : ''}$${gross.toFixed(2)}`
            };
        }
        // Asymmetry / Abort
        if ((m = line.match(/FULL ASYMMETRY DUMPED (\S+) on (\S+).*Trimming Loss: ([\d.-]+), Trimming Fee: ([\d.-]+)/))) {
            return {
                level: 'DUMP',
                badgeClass: 'log-badge-error',
                lineClass: 'log-line--error',
                text: `❌ СБРОС АСИММЕТРИИ ${m[1]} на ${m[2]}: сделка отменена (убыток -$${m[3]}, ком. -$${m[4]})`
            };
        }
        if (line.includes('Both IOC unfilled') && (m = line.match(/Both IOC unfilled (\S+) — no exposure \((\d+)ms\)/))) {
            return {
                level: 'REJECT',
                badgeClass: 'log-badge-warn',
                lineClass: 'log-line--warn',
                text: `⏹ ${m[1]}: Оба ордера не залиты — риска нет (${m[2]}мс)`
            };
        }
        if (line.includes('KILL-SWITCH IS ACTIVE') || line.includes('KILL-SWITCH TRIGGERED')) {
            return {
                level: 'ALERT',
                badgeClass: 'log-badge-error',
                lineClass: 'log-line--error',
                text: `🚨 АВАРИЙНЫЙ СТОП: движок приостановил торговлю`
            };
        }
        return null;
    };

    const translateLog = (line) => {
        let m;
        // ── ENTRY FLOW ──
        if ((m = line.match(/FIRE (\S+) \[(\w+)\] A=(\S+) c=(\d+) lim=([\d.]+) \| B=(\S+) c=(\d+) lim=([\d.]+) spread=([\d.]+)% be=([\d.]+)%/))) {
            return `🚀 ВХОД ${m[1]}: ${m[3]} ↔ ${m[6]} | объём ${m[4]}×${m[7]} шт | спред ${m[9]}% > BE ${m[10]}% (${m[2]})`;
        }
        if ((m = line.match(/VWAP entry OK (\S+): VWAP spread=([\d.]+)% > BE=([\d.]+)%/))) {
            return `✅ ${m[1]}: VWAP проверка пройдена — спред стакана ${m[2]}% > BE ${m[3]}%`;
        }
        if ((m = line.match(/Executing Leg A \((\S+)\) then Leg B \((\S+)\) SEQUENTIALLY for (\S+)/))) {
            return `⚡ ${m[3]}: исполнение ног — сначала ${m[1]}, затем ${m[2]}`;
        }
        if ((m = line.match(/RESULT (\S+) A=(\S+) B=(\S+) phys_a=([\d.]+) phys_b=([\d.]+) fill_a=([\d.]+) fill_b=([\d.]+) latency=(\d+)ms/))) {
            const aOk = m[2] === 'true', bOk = m[3] === 'true';
            if (aOk && bOk) return `✅ ${m[1]}: ОБЕ НОГИ ЗАЛИТЫ — A: ${m[6]} ($${m[4]}) | B: ${m[7]} ($${m[5]}) [${m[8]}мс]`;
            if (!aOk && !bOk) return `⛔ ${m[1]}: ОБЕ НОГИ ОТКЛОНЕНЫ — ордера не исполнились [${m[8]}мс]`;
            return `⚠️ ${m[1]}: ЧАСТИЧНЫЙ ФИЛЛ — A=${m[2]} B=${m[3]} [${m[8]}мс]`;
        }
        if ((m = line.match(/RETRY second leg (\S+) for (\S+) with max slippage ([\d.]+)%/))) {
            return `🔄 ${m[2]}: ПОВТОР второй ноги на ${m[1]} со slippage ${m[3]}%`;
        }
        if ((m = line.match(/Both IOC unfilled (\S+) — no exposure \((\d+)ms\)/))) {
            return `⏹ ${m[1]}: оба IOC ордера не залиты — риска нет (${m[2]}мс)`;
        }
        if (line.includes('Inserting confirmed trade')) {
            return `✅ Сделка подтверждена и записана в БД`;
        }

        // ── SKIP & REJECT ──
        if ((m = line.match(/SKIP (\S+) — no fresh L2 WS data for (\S+) \(age: (-?\d+)ms\) \/ (\S+) \(age: (-?\d+)ms\)/))) {
            return `⏭ ${m[1]}: нет свежего L2 стакана (${m[2]} age=${m[3]}ms, ${m[4]} age=${m[5]}ms)`;
        }
        if ((m = line.match(/SKIP (\S+) L1 depth too shallow: A=([\d.]+)\$ B=([\d.]+)\$ < ([\d.]+)\$/))) {
            return `⏭ ${m[1]}: тонкий L1 стакан (A=$${m[2]}, B=$${m[3]} < порога $${m[4]})`;
        }
        if ((m = line.match(/SKIP (\S+): insufficient margin (\S+) \(avail=\$([\d.]+), required=\$([\d.]+).*lev=(\d+)x\)/))) {
            return `⏭ ${m[1]}: нехватка маржи на ${m[2]} (доступно $${m[3]}, нужно $${m[4]})`;
        }

        // ── ABORT ──
        if ((m = line.match(/ABORT (\S+): VWAP spread ([\d.-]+)% < break-even ([\d.]+)%/))) {
            return `🛑 ${m[1]}: ОТМЕНА — VWAP спред ${m[2]}% ниже безубытка ${m[3]}%`;
        }

        // ── CLOSE FLOW ──
        if ((m = line.match(/Closing (\d+) (\S+) reason=(\S+) pct=([\d.-]+)% exit_sp=([\d.-]+)/))) {
            const reasons = {TP:'тейк-профит',STOP:'стоп-лосс / расширение',MANUAL:'ручной выход',ZERO_SPREAD:'схлопывание спреда',RETRY_CLOSE:'повтор закрытия'};
            return `📤 ЗАКРЫТИЕ #${m[1]} ${m[2]}: ${reasons[m[3]]||m[3]} | PnL ${m[4]}%`;
        }
        if ((m = line.match(/EXCHANGE_VERIFIED.*PnL id=(\d+) pna=([\d.-]+) pnb=([\d.-]+) gross=([\d.-]+)/))) {
            const gross = parseFloat(m[4]);
            return `${gross >= 0 ? '💰' : '💸'} СДЕЛКА #${m[1]} ЗАКРЫТА: Leg A=$${m[2]} | Leg B=$${m[3]} | ИТОГО=${gross >= 0 ? '+' : ''}$${gross.toFixed(2)}`;
        }
        if (line.includes('CLOSE FAILED OR UNVERIFIED')) {
            return `❌ Ошибка закрытия ордера — повтор через 5с`;
        }

        // ── RISK ──
        if ((m = line.match(/MARKET REGIME ALERT: (\S+) velocity ([\d.]+)%/))) {
            return `🌪 ${m[1]}: резкий импульс цены ${m[2]}% — пауза волатильности`;
        }
        if (line.includes('KILL-SWITCH IS ACTIVE') || line.includes('KILL-SWITCH TRIGGERED')) {
            return `🚨 АВАРИЙНЫЙ СТОП: движок временно приостановил новые входы.`;
        }

        return null;
    };

    let renderedCount = 0;
    let htmlOutput = '';

    rawLines.forEach((line, idx) => {
        let clean = line;
        let timeStr = '';
        let level = 'INFO';

        // Extract timestamp
        const tsMatch = line.match(/(\d{4}-\d{2}-\d{2}T)?(\d{2}:\d{2}:\d{2})(\.\d+)?Z?/);
        if (tsMatch) {
            timeStr = tsMatch[2];
        }

        // Simple mode: pure trade events only
        if (mode === 'simple') {
            const simpleItem = translateSimpleLog(line);
            if (!simpleItem) return;

            if (searchQuery && !simpleItem.text.toLowerCase().includes(searchQuery) && !line.toLowerCase().includes(searchQuery)) {
                return;
            }

            renderedCount++;
            const lineNum = String(renderedCount).padStart(3, '0');

            let safeContent = escHtml(simpleItem.text);
            safeContent = safeContent.replace(/\b([A-Z0-9]{2,}USDT)\b/g, '<span style="color:#60a5fa;font-weight:700;">$1</span>');
            safeContent = safeContent.replace(/(\+?\$[\d.-]+|\+?[\d.-]+%)/g, '<span style="color:#fbbf24;font-weight:600;">$1</span>');

            htmlOutput += `
                <div class="log-line ${simpleItem.lineClass}">
                    <span class="log-line-num">${lineNum}</span>
                    <span style="color:var(--text-sub);font-size:0.75rem;min-width:60px;flex-shrink:0;">${timeStr}</span>
                    <span class="log-badge-level ${simpleItem.badgeClass}">${simpleItem.level}</span>
                    <div class="log-line-content">${safeContent}</div>
                </div>
            `;
            return;
        }

        if (line.includes(' ERROR ') || line.includes('[ERROR]') || line.includes('ERR') || line.includes('reject') || line.includes('KILL-SWITCH')) {
            level = 'ERROR';
        } else if (line.includes(' WARN ') || line.includes('[WARN]') || line.includes('ABORT') || line.includes('SKIP') || line.includes('ALERT')) {
            level = 'WARN';
        } else if (line.includes('FIRE') || line.includes('RESULT') || line.includes('Closing') || line.includes('EXCHANGE_VERIFIED') || line.includes('SIGNAL')) {
            level = 'TRADE';
        }

        // Mode filtering
        if (mode === 'trade' && level !== 'TRADE') return;
        if (mode === 'error' && level !== 'ERROR' && level !== 'WARN') return;
        if (mode === 'exec' && !line.includes('Executing') && !line.includes('RESULT') && !line.includes('RETRY') && !line.includes('IOC') && !line.includes('fill') && !line.includes('orderId')) return;

        // Search query filtering
        if (searchQuery) {
            if (!line.toLowerCase().includes(searchQuery)) return;
        }

        renderedCount++;
        const lineNum = String(renderedCount).padStart(3, '0');

        // Check translation
        const translated = translateLog(line);
        let displayContent = translated || line;

        // Format symbol highlights
        let safeContent = escHtml(displayContent);
        safeContent = safeContent.replace(/\b([A-Z0-9]{2,}USDT)\b/g, '<span style="color:#60a5fa;font-weight:700;">$1</span>');
        safeContent = safeContent.replace(/(\+?\$[\d.-]+|\+?[\d.-]+%)/g, '<span style="color:#fbbf24;font-weight:600;">$1</span>');

        const levelClass = level === 'ERROR' ? 'log-line--error' : level === 'WARN' ? 'log-line--warn' : level === 'TRADE' ? 'log-line--trade' : '';
        const badgeClass = level === 'ERROR' ? 'log-badge-error' : level === 'WARN' ? 'log-badge-warn' : level === 'TRADE' ? 'log-badge-trade' : 'log-badge-info';

        htmlOutput += `
            <div class="log-line ${levelClass}">
                <span class="log-line-num">${lineNum}</span>
                <span style="color:var(--text-sub);font-size:0.75rem;min-width:60px;flex-shrink:0;">${timeStr}</span>
                <span class="log-badge-level ${badgeClass}">${level}</span>
                <div class="log-line-content">${safeContent}</div>
            </div>
        `;
    });

    if (renderedCount === 0) {
        htmlOutput = `<div style="text-align:center;padding:48px 0;color:var(--text-sub);">✨ No log entries match the current filter (${mode}${searchQuery ? ' + "' + escHtml(searchQuery) + '"' : ''})</div>`;
    }

    el.innerHTML = htmlOutput;
    if (countBadge) countBadge.textContent = `${renderedCount} lines`;

    if (window._logAutoScroll) {
        el.scrollTop = 0;
    }
};

window.fetchLogs = async function(isBackground = false) {
    const el = document.getElementById('logs-container');
    const statusEl = document.getElementById('logs-status');
    if (!el) return;
    
    if (!isBackground && (!window.lastFetchedLogs || window.lastFetchedLogs.trim() === '')) {
        el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-sub);">⏳ Loading logs stream from core-node…</div>';
    }
    
    try {
        const creds = localStorage.getItem('cex_auth') || '';
        const res = await fetch(`${API_BASE}/api/logs/core-node`, {
            headers: { 'Authorization': 'Basic ' + creds }
        });
        if (res.status === 401) throw new Error("Unauthorized");
        if (!res.ok) throw new Error("Status " + res.status);
        const text = await res.text();
        
        window.lastFetchedLogs = text;
        window.renderLogsHTML(text);
        
        if (statusEl) statusEl.textContent = `Synced: ${new Date().toLocaleTimeString()}`;
    } catch(e) {
        if (!isBackground) el.innerHTML = `<div style="color:var(--red);padding:24px;text-align:center;">Failed to load logs: ${e.message}</div>`;
        if (statusEl) statusEl.textContent = `Sync failed`;
    }
};



window.saveSettings = async function (profileId) {
    const g = id => document.getElementById(id)?.value;
    // NOTE: is_active is intentionally excluded here — use Start/Stop Bot for that.
    const settings = {
        trade_volume:              parseFloat(g(`sf-vol-${profileId}`)),
        take_profit_percent:       parseFloat(g(`sf-tp-${profileId}`)),
        trailing_stop_percent:     parseFloat(g(`sf-stop-${profileId}`)) || 0.0,
        min_entry_spread:          parseFloat(g(`sf-min-${profileId}`)),
        max_entry_spread:          parseFloat(g(`sf-max-${profileId}`)),
        min_structural_deviation:  parseFloat(g(`sf-dev-${profileId}`)) || 0.0,
        max_active_trades:         parseInt(g(`sf-max-trades-${profileId}`)),
        leverage:                  parseInt(g(`sf-leverage-${profileId}`)) || 10,
        min_volume_millions:       parseFloat(g(`sf-minvol-${profileId}`)),
        min_depth_percent:         parseFloat(g(`sf-depth-${profileId}`)),
        slippage_percent:          parseFloat(g(`sf-slip-${profileId}`)) || 0.5,
    };
    const btn = document.getElementById(`sf-save-${profileId}`);
    try {
        await saveProfileSettings(profileId, settings);
        const p = profiles.find(pr => pr.id === profileId);
        if (p) Object.assign(p, settings);

        // Save blacklist to technical settings (newline → comma-separated)
        const ta = document.getElementById('sf-blacklist');
        if (ta) {
            const newBlacklist = ta.value.split('\n')
                .map(s => s.trim().toUpperCase())
                .filter(s => s.length > 0)
                .join(',');
            if (newBlacklist !== _blacklistOriginal) {
                await apiFetch('/api/technical-settings', {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ blacklist: newBlacklist }),
                });
                _blacklistOriginal = newBlacklist;
            }
        }

        if (btn) { btn.textContent = 'Saved!'; btn.style.background = 'var(--green)'; }
        showToast('Settings saved successfully', 'success');
    } catch (e) {
        if (btn) { btn.textContent = 'Error!'; btn.style.background = 'var(--red)'; }
        console.error('Save failed:', e);
        showToast(`Save failed: ${e.message || 'Unknown error'}`, 'error');
    }
    setTimeout(() => {
        if (btn) { btn.textContent = 'Save Settings'; btn.style.background = 'var(--bg-ctrl)'; }
    }, 2000);
};

window.toggleBotPower = async function (profileId) {
    const profile = profiles.find(p => p.id === profileId);
    if (!profile) return;
    const previousState = profile.is_active;
    const newActive = !previousState;
    profile.is_active = newActive;

    // Instantly update all UI elements
    updateBotStatusUI(!!newActive, profileId);

    // Send ONLY is_active — do not spread the whole profile object
    // to avoid sending stale or unintended field values.
    try {
        await saveProfileSettings(profileId, { is_active: newActive ? 1 : 0 });
        showToast(`Bot ${newActive ? 'Started' : 'Stopped'}`, 'success');
    } catch (e) {
        profile.is_active = previousState; // Revert locally
        updateBotStatusUI(!!previousState, profileId); // Revert UI
        console.error('Bot toggle failed:', e);
        showToast(`Action failed: ${e.message || 'Check ownership'}`, 'error');
    }
};

window.resumeKillSwitch = async function () {
    if (!confirm('Resume engine after kill-switch suspension?')) return;
    try {
        await apiFetch('/api/kill-switch/reset', { method: 'POST' });
        window._killSwitchActive = false;
        // Hide all resume buttons
        document.querySelectorAll('[id^="sf-resume-"]').forEach(btn => {
            btn.style.display = 'none';
        });
        showToast('Engine resumed! Kill-switch reset.', 'success');
    } catch (e) {
        console.error('Kill-switch reset failed:', e);
        showToast(`Resume failed: ${e.message || 'Unknown error'}`, 'error');
    }
};

window.clearTrades = async function (profileId, mode) {
    if (!confirm('Delete ALL active and historical trades for this profile?')) return;
    try {
        await apiFetch(`/api/profiles/${profileId}/trades`, { method: 'DELETE' });
        await refreshActiveTab();
    } catch (e) { alert('Failed: ' + e.message); }
};

window.marketCloseAll = async function (profileId) {
    if (!confirm('Close ALL active positions on ALL exchanges at market price now?')) return;
    try {
        await apiFetch(`/api/profiles/${profileId}/market-close-all`, { method: 'POST' });
        showToast('Market close requested for all active trades', 'success');
        await refreshActiveTab();
    } catch (e) { alert('Failed: ' + e.message); }
};

window.marketCloseTrade = async function (profileId, tradeId, event) {
    if (event) event.stopPropagation();
    if (!confirm('Close this position at market price?')) return;
    try {
        showToast('Requesting market close…', 'info');
        await apiFetch(`/api/trades/${tradeId}/close`, { method: 'POST' });
        showToast('✅ Market close requested', 'success');
        setTimeout(refreshActiveTab, 1000);
    } catch (e) {
        showToast('Failed to close trade: ' + e.message, 'error');
    }
};

// ─── Login Form ───────────────────────────────────────────────────────────────
const loginForm = document.getElementById('login-form');
if (loginForm) {
    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const btn = e.target.querySelector('button');
        const errEl = document.getElementById('login-error');
        const u = document.getElementById('login-username').value.trim();
        const p = document.getElementById('login-password').value.trim();
        
        btn.textContent = 'Connecting...';
        errEl.textContent = '';
        
        const creds = btoa(u + ':' + p);
        const originalCreds = localStorage.getItem('cex_auth');
        localStorage.setItem('cex_auth', creds);
        
        try {
            // Test auth before proceeding
            const opts = { headers: { 'Authorization': 'Basic ' + creds } };
            const res = await fetch(`${API_BASE}/api/profiles`, opts);
            if (res.status === 401) throw new Error('Unauthorized');
            
            document.getElementById('login-modal').classList.add('hidden');
            
            // Re-boot application layout fully
            const loader = document.querySelector('.mt-loading');
            if (loader) loader.style.display = 'flex';
            const panel = document.getElementById('mt-panel');
            if (panel) panel.innerHTML = '';
            
            await boot();
        } catch (err) {
            errEl.textContent = 'Invalid username or password.';
            if (originalCreds) localStorage.setItem('cex_auth', originalCreds);
            else localStorage.removeItem('cex_auth');
        } finally {
            btn.textContent = 'Connect';
        }
    });
}

// ─── Chart Modal (Integrated) ────────────────────────────────────────────────
// Handled by chart.js globals: window.openModal, window.closeModal

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

const fmtPnL = v => { 
    const n = Number(v)||0; 
    if (n === 0) return '$0.00000';
    let abs = Math.abs(n);
    return (n>0?'+':'-') + '$' + abs.toFixed(5); 
};
const fmtCost = v => { 
    const n = Number(v)||0; 
    if (n === 0) return '$0.00000';
    let abs = Math.abs(n);
    return n>0 ? `-$${abs.toFixed(5)}` : `+$${abs.toFixed(5)}`; 
};
// Clean floating-point price display: removes artifacts like 0.063461999999999996
const fmtPx = v => {
    const n = Number(v);
    if (!n || !isFinite(n)) return '0';
    // Use parseFloat(toPrecision) to strip trailing noise
    return parseFloat(n.toPrecision(10)).toString();
};
const fmtDuration = ms => { const s=Math.floor(ms/1000); if(s<60) return s+'s'; const m=Math.floor(s/60); if(m<60) return m+'m '+(s%60)+'s'; const h=Math.floor(m/60); if(h<24) return h+'h '+(m%60)+'m'; return Math.floor(h/24)+'d '+(h%24)+'h'; };
const escHtml = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

// ─── Start ────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', boot);

// Funding countdown updater — uses real exchange timestamp when available
setInterval(() => {
    const now = Date.now();
    
    const pollEl = document.getElementById('poll-countdown');
    if (pollEl) {
        if (!window.lastPollTime) window.lastPollTime = now;
        let diff = 10 - Math.floor((now - window.lastPollTime) / 1000);
        if (diff < 0) diff = 0;
        pollEl.textContent = diff + 's';
    }

    // Fallback: next 8h UTC mark
    let fallbackTarget = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate(), 0, 0, 0));
    while (fallbackTarget.getTime() <= now) {
        fallbackTarget.setUTCHours(fallbackTarget.getUTCHours() + 8);
    }

    document.querySelectorAll('.funding-countdown').forEach(el => {
        const nextTs = parseInt(el.dataset.next || '0');
        const target = (nextTs > now) ? nextTs : fallbackTarget.getTime();
        const diff = target - now;
        if (diff <= 0) { el.textContent = '00h 00m 00s'; return; }
        const h = String(Math.floor(diff / 3600000)).padStart(2, '0');
        const m = String(Math.floor((diff % 3600000) / 60000)).padStart(2, '0');
        const s = String(Math.floor((diff % 60000) / 1000)).padStart(2, '0');
        el.textContent = `${h}h ${m}m ${s}s`;
    });
}, 1000);

// redeploy
// deploy trigger

