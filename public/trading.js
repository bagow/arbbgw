/**
 * trading.js – Multi-tab trading UI
 * v2.1 – adds BingX, BitMEX, AscendEX, XT.COM, Toobit to CEX tab
 *
 * Each "tab" is a trading profile. Profiles are stored in D1 via
 * API calls. The active tab's data is polled every 10s; others are lazy-loaded.
 * Tab visibility/order is persisted in localStorage.
 */
'use strict';

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

        // Restore last active tab from localStorage
        const saved = localStorage.getItem('activeTabId');
        if (saved && profiles.find(p => p.id === saved)) {
            activeTabId = saved;
        } else if (profiles.length) {
            activeTabId = profiles[0].id;
        }
        
        if (profiles.length === 0) {
            const panel = document.getElementById('mt-panel');
            if (panel) panel.innerHTML = '<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;color:var(--text-sub);"><h3>No Profiles Found</h3><p>Click the ＋ button below to create one.</p></div>';
        } else {
            renderTabBar();
            if (activeTabId) {
                await switchTab(activeTabId);
            }
            startPoll();
        }
    } catch (e) {
        console.warn('Boot halted due to error:', e.message);
        const loader = document.querySelector('.mt-loading');
        if (loader) loader.style.display = 'none';
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
    const bar = document.getElementById('tab-bar');
    const addBtn = document.getElementById('tab-add-btn');
    // Remove all tabs (keep add button)
    bar.querySelectorAll('.tab-item').forEach(el => el.remove());

    for (const p of profiles) {
        const tab = document.createElement('div');
        tab.className = 'tab-item' + (p.id === activeTabId ? ' tab-item--active' : '');
        tab.dataset.id = p.id;

        const badge = '<span class="tab-badge tab-badge--real">💰</span>';

        const isDefault = p.id === 'real-default';
        let dispName = escHtml(p.name);
        if (p.id === 'real-default') dispName = 'Public Real';

        tab.innerHTML = `
            ${badge}
            <span class="tab-name">${dispName}</span>
            ${isDefault ? '' : `<button class="tab-close" data-id="${p.id}" title="Close tab">&times;</button>`}
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

    // Use profile-scoped data if available (from profile_id param in request)
    // Use profile-scoped data
    let active, history;
    if (profile_active !== undefined) {
        active  = profile_active;
        history = profile_history || [];
    } else {
        active  = profile.mode === 'real' ? (real?.active  || []) : (trades?.active  || []);
        history = profile.mode === 'real' ? (real?.history || []) : (trades?.history || []);
    }

    // Auto-refresh logs if the logs tab is currently active
    if (activeSubTabId === 'logs') {
        window.fetchLogs(true);
    }

    // Use per-profile stats if available, fall back to global
    const stats = profile_stats || (profile.mode === 'real' ? (real?.stats || {}) : (trades?.stats || {}));

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
                        <th>Time</th>
                    </tr></thead>
                    <tbody id="t-active-body-${profile.id}">
                        ${active === null ? '<tr><td colspan="7" class="text-center text-sub">Loading…</td></tr>' : ''}
                    </tbody>
                    <tfoot style="background:var(--bg-row-alt);font-weight:600;">
                        <tr>
                            <td style="white-space:nowrap;border-top:1px solid var(--border-dim)">
                                Total (<span id="t-count-${profile.id}">${active?.length??0}</span>)
                            </td>
                            <td id="t-live-pnl-${profile.id}" style="color:var(--text);border-top:1px solid var(--border-dim)">$0.00</td>
                            <td colspan="5" style="border-top:1px solid var(--border-dim)"></td>
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
        <div class="mt-content" id="mt-sub-logs" style="display:${activeSubTabId==='logs'?'block':'none'}; padding: 24px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
                <h2 style="margin:0;">System Logs (core-node)</h2>
                <div style="display:flex; align-items:center; gap:16px;">
                    <span style="color:var(--text-sub); font-size:0.85rem;" id="logs-status">Auto-updating...</span>
                    <div style="background:var(--bg-row); padding:4px; border-radius:6px; display:flex; gap:4px;">
                        <button class="mt-nav-btn ${window.logViewMode==='trade'?'mt-nav-btn--active':''}" id="btn-log-trade" onclick="setLogView('trade')" style="padding:6px 16px; margin:0; font-size:13px;">Trades</button>
                        <button class="mt-nav-btn ${window.logViewMode==='tech'?'mt-nav-btn--active':''}" id="btn-log-tech" onclick="setLogView('tech')" style="padding:6px 16px; margin:0; font-size:13px;">Technical</button>
                    </div>
                    <button class="trading-save-btn outline-btn" onclick="window.fetchLogs(false)" style="padding:8px 16px; width:auto; margin:0;">Force Refresh</button>
                </div>
            </div>
            <div id="logs-container" style="background:var(--bg-row-alt); color:var(--text); padding:16px; border:1px solid var(--border-dim); border-radius:8px; height:65vh; overflow-y:auto; font-family:var(--font-mono); font-size: 1.1rem; line-height: 1.6; white-space:pre-wrap;"></div>
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
    return `
        ${active !== null ? `
        <span style="font-size:.78rem;color:var(--text-sub);font-variant-numeric:tabular-nums;">
            Active: <strong style="color:var(--text)">${active.length}</strong>
            ·
            PnL: <strong class="${pnlClass}">${fmtPnL(pnl)}</strong>
        </span>` : ''}
        <span class="mode-badge mode-badge--real">💰 Real</span>
    `;
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
        return '<div class="text-sub" style="text-align:center;">No exchange data available yet. Please wait for collector to sync.</div>';
    }

    // Real trading: exchanges that have API keys configured
    const REAL_EXCHANGES = new Set(['binance', 'bybit', 'gate', 'kucoin', 'bitget', 'bingx', 'bitmex', 'ascendex', 'xt', 'toobit']);
    const isReal = profile.mode === 'real';
    const KNOWN_EXCHANGES = ['binance', 'bybit', 'gate', 'kucoin', 'okx', 'bitget', 'bingx', 'bitmex', 'ascendex', 'xt', 'toobit'];
    const exNames = KNOWN_EXCHANGES
        .filter(ex => !isReal || REAL_EXCHANGES.has(ex))
        .sort();
    
    // Parse disabled_exchanges to a map for quick lookup
    const disabledRaw = profile.disabled_exchanges || '[]';
    let disabledMap = {};
    if (typeof disabledRaw === 'string') {
        try { JSON.parse(disabledRaw).forEach(ex => disabledMap[ex] = true); } catch(e) {}
    } else if (Array.isArray(disabledRaw)) {
        disabledRaw.forEach(ex => disabledMap[ex] = true);
    }

    const snapshotAgeMs = Number(exchangesData.snapshot_age_ms ?? -1);
    const snapshotAgeText = snapshotAgeMs >= 0
        ? `${(snapshotAgeMs / 1000).toFixed(1)}s`
        : 'n/a';
    const balancesAgeMs = Number(exchangesData.balances_age_ms ?? -1);
    const positionsAgeMs = Number(exchangesData.positions_age_ms ?? -1);
    const feesAgeMs = Number(exchangesData.fees_age_ms ?? -1);
    const cexBaselineTs = Number(exchangesData.cex_baseline_ts_ms ?? 0);
    const cexBaselineText = cexBaselineTs > 0
        ? new Date(cexBaselineTs).toLocaleString('en-GB', { timeZone: 'UTC', hour12: false }) + ' UTC'
        : 'n/a';
    const balancesAgeText = balancesAgeMs >= 0 ? `${(balancesAgeMs / 1000).toFixed(1)}s` : 'n/a';
    const positionsAgeText = positionsAgeMs >= 0 ? `${(positionsAgeMs / 1000).toFixed(1)}s` : 'n/a';
    const feesAgeText = feesAgeMs >= 0 ? `${(feesAgeMs / 1000).toFixed(1)}s` : 'n/a';
    const cexDeltaTotal = Number(exchangesData.cex_balance_delta_total_today ?? 0);
    const dbClosedTodayTotal = Number(exchangesData.db_closed_pnl_today_total ?? 0);
    const cexVsDbDelta = Number(exchangesData.cex_vs_db_delta_today ?? (cexDeltaTotal - dbClosedTodayTotal));
    const dbOpenLivePnlTotal = Number(exchangesData.db_open_live_pnl_total ?? 0);
    const dbExpectedDeltaTotal = Number(exchangesData.db_expected_balance_delta_total ?? (dbClosedTodayTotal + dbOpenLivePnlTotal));
    const unexplainedResidual = Number(exchangesData.cex_vs_db_expected_delta ?? (cexDeltaTotal - dbExpectedDeltaTotal));
    const cexDeltaByExchange = exchangesData.cex_balance_delta_today || {};
    const dbOpenLiveByExchange = exchangesData.db_open_live_pnl_by_exchange || {};
    const dbExpectedByExchange = exchangesData.db_expected_delta_by_exchange || {};
    const residualByExchange = exchangesData.cex_vs_db_expected_by_exchange || {};
    const STALE_WARN_MS = 30_000;
    const STALE_CRIT_MS = 90_000;
    const ageTuples = [
        ['snapshot', snapshotAgeMs],
        ['balances', balancesAgeMs],
        ['positions', positionsAgeMs],
        ['fees', feesAgeMs]
    ];
    const knownAges = ageTuples.map(([, ms]) => ms).filter(ms => ms >= 0);
    const maxKnownAgeMs = knownAges.length ? Math.max(...knownAges) : -1;
    const staleParts = ageTuples
        .filter(([, ms]) => ms >= STALE_WARN_MS)
        .map(([name]) => name);
    let staleBanner = '';
    if (maxKnownAgeMs >= STALE_WARN_MS) {
        const isCritical = maxKnownAgeMs >= STALE_CRIT_MS;
        const cls = isCritical ? 'stale-banner--crit' : 'stale-banner--warn';
        staleBanner = `
            <div class="stale-banner ${cls}" style="margin-bottom:14px;">
                ⚠ Data is stale (${(maxKnownAgeMs / 1000).toFixed(1)}s). Affected: ${staleParts.join(', ')}.
            </div>
        `;
    }
    const driftColor = Math.abs(cexVsDbDelta) > 0.05 ? 'var(--red)' : 'var(--text-sub)';
    const reconcileBar = `
        <div class="reconcile-bar" style="margin-bottom:14px;">
            <div class="reconcile-item"><span class="reconcile-label">CEX \u0394 (UTC)</span><span class="reconcile-value" style="color:${cexDeltaTotal>=0?'var(--green)':'var(--red)'}">${fmtPnL(cexDeltaTotal)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">DB Closed</span><span class="reconcile-value" style="color:${dbClosedTodayTotal>=0?'var(--green)':'var(--red)'}">${fmtPnL(dbClosedTodayTotal)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">Drift</span><span class="reconcile-value" style="color:${driftColor}">${fmtPnL(cexVsDbDelta)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">Open Live</span><span class="reconcile-value" style="color:${dbOpenLivePnlTotal>=0?'var(--green)':'var(--red)'}">${fmtPnL(dbOpenLivePnlTotal)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">Expected \u0394</span><span class="reconcile-value" style="color:${dbExpectedDeltaTotal>=0?'var(--green)':'var(--red)'}">${fmtPnL(dbExpectedDeltaTotal)}</span></div>
            <div class="reconcile-item"><span class="reconcile-label">Residual</span><span class="reconcile-value" style="color:${Math.abs(unexplainedResidual)>0.05?'var(--red)':'var(--text-sub)'}">${fmtPnL(unexplainedResidual)}</span></div>
        </div>
    `;

    let html = `
        <div class="cex-meta" style="margin-bottom:16px;padding:10px 0;border-bottom:1px solid var(--border-dim);">
            <span>Snapshot: <strong>${snapshotAgeText}</strong></span>
            <span>Balances: <strong>${balancesAgeText}</strong></span>
            <span>Positions: <strong>${positionsAgeText}</strong></span>
            <span>Fees: <strong>${feesAgeText}</strong></span>
            <span>Update: <strong id="poll-countdown">10s</strong></span>
        </div>
        ${reconcileBar}
        ${staleBanner}
        <div class="cex-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:20px;">
    `;
    
    for (const exA of exNames) {
        let pairsList = '';
        let maxTickers = 0;
        const rawArr = exchangesData.data[exA];
        const bal = (exchangesData.balances && exchangesData.balances[exA]) || 0;
        const marginBal = (exchangesData.margin_balances && exchangesData.margin_balances[exA]) || 0;
        const positions = (exchangesData.positions && exchangesData.positions[exA]) || [];
        const fee = (exchangesData.fees && exchangesData.fees[exA]) || 0;
        const dailyPnl = (exchangesData.daily_pnl && exchangesData.daily_pnl[exA]) || 0;
        const dailyPct = (exchangesData.daily_pnl_pct && exchangesData.daily_pnl_pct[exA]) || 0;
        const cexBalanceDelta = Number(cexDeltaByExchange[exA] ?? 0);
        const dbOpenLiveEx = Number(dbOpenLiveByExchange[exA] ?? 0);
        const dbExpectedEx = Number(dbExpectedByExchange[exA] ?? (dailyPnl + dbOpenLiveEx));
        const residualEx = Number(residualByExchange[exA] ?? (cexBalanceDelta - dbExpectedEx));
        for (const exB of exNames) {
            if (exA === exB) continue;
            let matchCount = 0;
            // The Rust backend sends pre-computed matches like: { "vs bybit": 150 }
            if (rawArr && typeof rawArr === 'object' && rawArr[`vs ${exB}`]) {
                matchCount = rawArr[`vs ${exB}`];
                if (matchCount > maxTickers) maxTickers = matchCount;
            }
            if (matchCount > 0) {
                pairsList += `
                    <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-dim);">
                        <span style="color:var(--text-sub);">vs <strong style="color:var(--text);">${exB}</strong></span>
                        <span style="font-family:var(--font-mono);font-size:0.9rem;">${matchCount} pairs</span>
                    </div>
                `;
            }
        }
        
        const isEnabled = !disabledMap[exA];
        const dailyPnlColor = dailyPnl > 0 ? 'var(--green)' : dailyPnl < 0 ? 'var(--red)' : 'var(--text-sub)';
        
        html += `
            <div class="cex-card">
                <div class="cex-card-header">
                    <div>
                        <div class="cex-card-title">
                            <div class="cex-status-dot ${isEnabled ? 'on' : 'off'}"></div>
                            <h3 style="margin:0;font-size:1.1rem;font-weight:700;text-transform:uppercase;">${exA}</h3>
                        </div>
                        <div style="font-size:.82rem;font-family:var(--font-mono);margin-top:4px;margin-left:18px;">
                            <span style="color:${dailyPnlColor}">${dailyPnl > 0 ? '+' : ''}$${dailyPnl.toFixed(2)} <span style="font-size:.72rem;opacity:.7">(${dailyPct > 0 ? '+' : ''}${dailyPct.toFixed(2)}%)</span></span>
                        </div>
                    </div>
                    <div class="cex-wallet">
                        <div class="cex-wallet-label">Wallet</div>
                        <div class="cex-wallet-value" style="color:${bal > 0 ? 'var(--green)' : 'var(--text-sub)'}">$${bal.toFixed(2)}</div>
                        <div class="cex-wallet-label" style="margin-top:4px;">Available</div>
                        <div class="cex-wallet-value" style="color:${marginBal > 0 ? 'var(--green)' : 'var(--text-sub)'}">$${marginBal.toFixed(2)}</div>
                    </div>
                </div>
                <div class="cex-meta">
                    <span>Tickers: <strong>${maxTickers}+</strong></span>
                    <span>Fee: <strong>${(fee * 100).toFixed(3)}%</strong></span>
                    <span>CEX \u0394: <strong style="color:${cexBalanceDelta > 0 ? 'var(--green)' : cexBalanceDelta < 0 ? 'var(--red)' : 'var(--text-sub)'}">${fmtPnL(cexBalanceDelta)}</strong></span>
                    <span>Res: <strong style="color:${Math.abs(residualEx) > 0.05 ? 'var(--red)' : 'var(--text-sub)'}">${fmtPnL(residualEx)}</strong></span>
                </div>
                
                <div style="margin-bottom:12px;">
                    ${pairsList || '<div class="text-sub" style="font-size:.85rem;padding:8px 0;">No matching pairs.</div>'}
                </div>

                <div style="flex:1;margin-bottom:16px;">
                    <div style="font-size:.72rem;color:var(--text-sub);margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;font-weight:600;">Active Positions</div>
                    ${positions.length === 0 ? '<div class="text-sub" style="font-size:.82rem;">No active positions</div>' : positions.map(pos => {
                        const isLong = pos.size > 0;
                        const pnlColor = pos.unrealized_pnl >= 0 ? 'var(--green)' : 'var(--red)';
                        const dirBadge = isLong ? '<span class="badge badge-long">L</span>' : '<span class="badge badge-short">S</span>';
                        return `<div style="display:flex;justify-content:space-between;align-items:center;font-size:.82rem;margin-bottom:4px;font-family:var(--font-mono);">
                            <span>${dirBadge} ${pos.symbol} <span style="color:var(--text-sub)">${Math.abs(pos.size)}</span></span>
                            <span style="display:flex;align-items:center;gap:6px;">
                                <span style="color:${pnlColor}">${pos.unrealized_pnl >= 0 ? '+' : ''}$${pos.unrealized_pnl.toFixed(4)}</span>
                                <button onclick="closeActivePosition('${exA}', '${pos.symbol}', ${pos.size})" style="background:none;border:none;color:var(--text-sub);cursor:pointer;font-size:1rem;line-height:1;padding:0;transition:color .15s;" onmouseover="this.style.color='var(--red)'" onmouseout="this.style.color='var(--text-sub)'" title="Market Close">\u00d7</button>
                            </span>
                        </div>`;
                    }).join('')}
                </div>
                
                <label style="display:flex;align-items:center;gap:10px;cursor:pointer;padding-top:14px;border-top:1px solid var(--border-dim);user-select:none;">
                    <input type="checkbox" 
                           ${isEnabled ? 'checked' : ''} 
                           onchange="toggleExchange('${profile.id}', '${exA}', this)"
                           style="width:16px;height:16px;cursor:pointer;accent-color:var(--accent);">
                    <span style="font-weight:500;font-size:.88rem;">${isEnabled ? 'Trading Enabled' : 'Trading Disabled'}</span>
                </label>
            </div>
        `;
    }
    html += `</div>`;
    return html;
}


window.closeActivePosition = async function(exchange, symbol, size) {
    if (!confirm(`Are you sure you want to completely close ${symbol} on ${exchange} at market price?`)) return;
    try {
        showToast(`Closing ${symbol} on ${exchange}…`, 'info');
        const res = await authFetch(`${API_BASE}/api/positions/close`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ exchange, symbol, size })
        });
        if (res.ok) {
            showToast(`✅ ${symbol} on ${exchange} — position closed!`, 'success');
        } else {
            let errText = `Failed to close ${symbol} on ${exchange}`;
            try { const body = await res.text(); if (body) errText += ': ' + body; } catch(e) {}
            showToast(errText, 'error');
        }
        // Refresh data regardless of success/failure to update positions display
        setTimeout(window.fetchUnifiedData, 500);
    } catch (e) {
        showToast(`Error closing ${symbol} on ${exchange}: ${e.message}`, 'error');
    }
};

function renderSettingsForm(profile) {
    const p = profile;
    return `
        <div style="margin-bottom:20px;display:flex;gap:0;background:var(--bg-row);border-radius:8px;padding:3px;">
            <button class="mt-nav-btn mt-nav-btn--active" id="stab-profile-btn" onclick="switchSettingsTab('profile')"
                style="flex:1;margin:0;border-radius:6px;padding:10px 0;font-size:.9rem;">Profile Settings</button>
            <button class="mt-nav-btn" id="stab-tech-btn" onclick="switchSettingsTab('technical')"
                style="flex:1;margin:0;border-radius:6px;padding:10px 0;font-size:.9rem;">Technical Settings</button>
        </div>

        <!-- Profile Settings (per-profile) -->
        <section id="stab-profile" class="trading-settings-section" style="padding:0;background:transparent;border:none;">
            <form id="settings-form-${p.id}" class="settings-grid" onsubmit="return false;">
                <div class="form-group">
                    <label>Trade Volume (USD)</label>
                    <input type="number" id="sf-vol-${p.id}" step="0.01" value="${p.trade_volume??10}" />
                </div>
                <div class="form-group">
                    <label>Take Profit (%)</label>
                    <input type="number" id="sf-tp-${p.id}" step="0.01" value="${p.take_profit_percent??0.2}" />
                </div>
                <div class="form-group">
                    <label>Min Entry Spread (%)</label>
                    <input type="number" id="sf-min-${p.id}" step="0.01" value="${p.min_entry_spread??0.5}" />
                </div>
                <div class="form-group">
                    <label>Max Entry Spread (%)</label>
                    <input type="number" id="sf-max-${p.id}" step="0.1" value="${p.max_entry_spread??50}" />
                </div>
                <div class="form-group">
                    <label>Max Active Trades</label>
                    <input type="number" id="sf-max-trades-${p.id}" step="1" value="${p.max_active_trades??5}" />
                </div>
                <div class="form-group">
                    <label>Leverage (x)</label>
                    <input type="number" id="sf-leverage-${p.id}" step="1" min="1" max="125" value="${p.leverage??10}" />
                </div>
                <div class="form-group">
                    <label>Min Volume (M$)</label>
                    <input type="number" id="sf-minvol-${p.id}" step="0.1" value="${p.min_volume_millions??10}" />
                </div>
                <div class="form-group">
                    <label>L1 Depth (% of vol)</label>
                    <input type="number" id="sf-depth-${p.id}" step="1" title="Minimum L1 volume required as % of Trade Volume" value="${p.min_depth_percent??100}" />
                </div>
                <div class="form-group">
                    <label>Slippage IOC (%)</label>
                    <input type="number" id="sf-slip-${p.id}" step="0.01" min="0.01" max="5" title="Max price tolerance for IOC limit orders (entry & close). Also used as fallback close tolerance. Set = (spread/4) to avoid killing your own PnL." value="${p.slippage_percent??0.5}" />
                </div>

                <!-- Blacklist textarea -->
                <div style="grid-column:1/-1;margin-top:16px;border-top:1px solid var(--border-dim);padding-top:16px;">
                    <label style="display:block;text-transform:uppercase;font-size:.7rem;letter-spacing:.5px;color:var(--text-sub);margin-bottom:8px;">
                        🚫 Blacklisted Pairs <span style="font-weight:400;text-transform:none;letter-spacing:0;">(one per line — excluded from scanner & trading)</span>
                    </label>
                    <textarea id="sf-blacklist"
                        spellcheck="false"
                        placeholder="BTCUSDT&#10;ETHUSDT&#10;..."
                        style="width:100%;min-height:100px;max-height:220px;background:var(--bg-ctrl);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 14px;font-family:var(--font-mono);font-size:.85rem;line-height:1.6;resize:vertical;box-sizing:border-box;"
                    ></textarea>
                </div>

                <div class="form-actions" style="grid-column:1/-1;display:flex;gap:16px;margin-top:24px;border-top:1px solid var(--border-dim);padding-top:24px;align-items:center;flex-wrap:wrap;">
                    <button type="button" id="sf-power-${p.id}"
                        onclick="toggleBotPower('${p.id}')"
                        class="trading-save-btn"
                        style="margin:0;width:auto;padding:12px 32px;font-size:.95rem;background:${p.is_active?'var(--red)':'var(--green)'};">
                        ${p.is_active ? 'Stop Bot' : 'Start Bot'}
                    </button>
                    <button type="button" id="sf-resume-${p.id}"
                        onclick="resumeKillSwitch()"
                        class="trading-save-btn"
                        style="margin:0;width:auto;padding:12px 32px;font-size:.95rem;background:#ff3b3b;display:${window._killSwitchActive?'inline-flex':'none'};animation:pulse-red 1.5s infinite;">
                        ⚠️ Engine Suspended — Resume
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
                        style="margin:0;width:auto;padding:12px 32px;font-size:.95rem;background:transparent;color:var(--text-sub);border:1px solid var(--border);">
                        Clear DB (Wipe Trades)
                    </button>
                </div>
            </form>
        </section>

        <!-- Technical Settings (global engine params) -->
        <section id="stab-technical" style="display:none;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                <div>
                    <h3 style="margin:0 0 4px 0;font-size:1.1rem;">Engine Technical Settings</h3>
                    <span style="color:var(--text-sub);font-size:.8rem;">Global parameters — affect all profiles</span>
                </div>
                <button class="trading-save-btn" id="tech-save-btn" onclick="saveTechnicalSettings()"
                    style="margin:0;width:auto;padding:10px 28px;font-size:.9rem;">Save Technical</button>
            </div>
            <div id="tech-settings-table" style="color:var(--text-sub);text-align:center;padding:32px;">Loading...</div>
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
        wrap.innerHTML = '<div style="color:var(--red);padding:24px;">Failed to load: ' + e.message + '</div>';
    }
}

// ── Blacklist load/save ──────────────────────────────────────────────────────

let _blacklistOriginal = '';

async function loadBlacklistIntoTextarea() {
    const ta = document.getElementById('sf-blacklist');
    if (!ta) return;
    try {
        const data = await apiFetch('/api/technical-settings');
        const bl = data.find(d => d.key === 'blacklist');
        if (bl && bl.value) {
            // Convert comma-separated to newline-separated for textarea
            const lines = bl.value.split(',').map(s => s.trim()).filter(s => s.length > 0);
            ta.value = lines.join('\n');
            _blacklistOriginal = bl.value;
        }
    } catch (e) {
        console.warn('Failed to load blacklist:', e);
    }
}

function renderTechSettingsTable(wrap, data) {
    const CATEGORY_LABELS = {
        spreads: '📊 Spread Computation',
        entry: '🎯 Entry Filters',
        cooldowns: '⏱️ Cooldowns',
        execution: '⚡ Order Execution',
        intervals: '🔄 Refresh Intervals',
        general: '⚙️ General',
    };
    // Keys that exist in DB but have zero effect in engine code or are managed elsewhere — hide from UI
    const HIDDEN_KEYS = new Set([
        'safety_margin', 'deviation_safety', 'max_price_ratio',
        'max_funding_diff', 'min_ioc_tolerance_pct', 'funding_cache_secs',
        'depth_multiplier', 'blacklist',
    ]);
    // Group by category
    const groups = {};
    for (const item of data) {
        if (HIDDEN_KEYS.has(item.key)) continue;
        const cat = item.category || 'general';
        if (!groups[cat]) groups[cat] = [];
        groups[cat].push(item);
    }
    const catOrder = ['spreads', 'entry', 'cooldowns', 'execution', 'intervals', 'general'];
    let html = '';
    for (const cat of catOrder) {
        const items = groups[cat];
        if (!items || items.length === 0) continue;
        html += `<div style="margin-bottom:24px;">
            <div style="font-weight:600;font-size:1rem;color:var(--text);padding:10px 0 8px;border-bottom:2px solid var(--border-dim);margin-bottom:2px;">
                ${CATEGORY_LABELS[cat] || cat}
            </div>
            <table style="width:100%;border-collapse:collapse;table-layout:fixed;">
                <colgroup>
                    <col style="width:200px;" />
                    <col style="width:180px;" />
                    <col />
                </colgroup>
                <thead><tr>
                    <th style="text-align:left;padding:8px 12px;font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;color:var(--text-sub);border-bottom:1px solid var(--border-dim);">Parameter</th>
                    <th style="text-align:left;padding:8px 12px;font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;color:var(--text-sub);border-bottom:1px solid var(--border-dim);">Value</th>
                    <th style="text-align:left;padding:8px 12px;font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;color:var(--text-sub);border-bottom:1px solid var(--border-dim);">Description</th>
                </tr></thead>
                <tbody>`;
        for (const item of items) {
            // Enum-type settings get a dropdown instead of text input
            const ENUMS = {
                entry_order_type: [
                    { value: 'market', label: 'Market — guaranteed fill, no asymmetric risk' },
                    { value: 'ioc', label: 'IOC — limit with slippage protection, risk of partial fill' },
                ],
            };
            const inputStyle = 'width:100%;background:var(--bg-ctrl);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-family:var(--font-mono);font-size:.85rem;box-sizing:border-box;';
            let valueCell;
            if (ENUMS[item.key]) {
                const opts = ENUMS[item.key].map(o =>
                    `<option value="${o.value}" ${item.value === o.value ? 'selected' : ''}>${o.label}</option>`
                ).join('');
                valueCell = `<select id="ts-${item.key}" style="${inputStyle}">${opts}</select>`;
            } else {
                valueCell = `<input type="text" id="ts-${item.key}" value="${escHtml(item.value)}" style="${inputStyle}" />`;
            }
            html += `<tr style="border-bottom:1px solid var(--border-dim);">
                <td style="padding:8px 12px;font-family:var(--font-mono);font-size:.85rem;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${item.key}</td>
                <td style="padding:8px 12px;">${valueCell}</td>
                <td style="padding:8px 12px;font-size:.78rem;color:var(--text-sub);line-height:1.5;word-wrap:break-word;overflow-wrap:break-word;white-space:normal;">${item.description || ''}</td>
            </tr>`;
        }
        html += '</tbody></table></div>';
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
                <td colspan="7" class="p-0">
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
                            <div class="td-line"><span>Exit Slip:</span> <strong style="color:var(--red)">${fmtCost(t.exit_slippage_cost??0)}</strong></div>
                            <div class="td-line"><span>Total Slip:</span> <strong style="color:var(--red)">${fmtCost((t.slippage_cost??0)+(t.exit_slippage_cost??0))}</strong></div>
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
window.logViewMode = window.logViewMode || 'trade';
window.lastFetchedLogs = window.lastFetchedLogs || '';

window.setLogView = function(mode) {
    window.logViewMode = mode;
    document.getElementById('btn-log-trade').classList.toggle('mt-nav-btn--active', mode === 'trade');
    document.getElementById('btn-log-tech').classList.toggle('mt-nav-btn--active', mode === 'tech');
    // Re-fetch from server since mode affects server-side filtering
    window.fetchLogs(false);
};

window.renderLogsHTML = function(text) {
    const el = document.getElementById('logs-container');
    if (!el) return;
    const isScrolled = el.scrollTop > 10;
        
    const translateLog = (line) => {
        // ── ENTRY FLOW ──
        let m;
        if ((m = line.match(/FIRE (\S+) \[(\w+)\] A=(\S+) c=(\d+) lim=([\d.]+) \| B=(\S+) c=(\d+) lim=([\d.]+) spread=([\d.]+)% be=([\d.]+)%/))) {
            return `🚀 ОТКРЫТИЕ ${m[1]}: ${m[3]}↔${m[6]} | кол-во: ${m[4]}×${m[7]} шт | спред ${m[9]}% > безубыток ${m[10]}% | тип: ${m[2]}`;
        }
        if ((m = line.match(/VWAP entry OK (\S+): VWAP spread=([\d.]+)% > BE=([\d.]+)%/))) {
            return `✅ ${m[1]}: проверка ордербука пройдена — реальный спред ${m[2]}% > безубыток ${m[3]}%`;
        }
        if ((m = line.match(/Executing Leg A \((\S+)\) then Leg B \((\S+)\) SEQUENTIALLY for (\S+)/))) {
            return `⚡ ${m[3]}: отправка ордеров последовательно — сначала ${m[1]}, потом ${m[2]}`;
        }
        if ((m = line.match(/RESULT (\S+) A=(\S+) B=(\S+) phys_a=([\d.]+) phys_b=([\d.]+) fill_a=([\d.]+) fill_b=([\d.]+) latency=(\d+)ms/))) {
            const aOk = m[2] === 'true', bOk = m[3] === 'true';
            if (aOk && bOk) return `✅ ${m[1]}: ОБЕ НОГИ ИСПОЛНЕНЫ — A: ${m[6]} по ${m[4]} | B: ${m[7]} по ${m[5]} | ${m[8]}мс`;
            if (!aOk && !bOk) return `⛔ ${m[1]}: ОБЕ НОГИ НЕ ИСПОЛНЕНЫ — ордера не прошли (${m[8]}мс)`;
            return `⚠️ ${m[1]}: ЧАСТИЧНОЕ ИСПОЛНЕНИЕ — A=${m[2]} B=${m[3]} (${m[8]}мс)`;
        }
        if ((m = line.match(/Both IOC unfilled (\S+) — no exposure \((\d+)ms\)/))) {
            return `⏹ ${m[1]}: оба IOC ордера не исполнились — позиции нет, риска нет (${m[2]}мс)`;
        }
        if (line.includes('Inserting confirmed trade')) {
            return `✅ СДЕЛКА ОТКРЫТА И ЗАПИСАНА В БАЗУ`;
        }

        // ── SKIP / REJECT ──
        if ((m = line.match(/SKIP (\S+) — no fresh L2 WS data for (\S+) \(age: (-?\d+)ms\) \/ (\S+) \(age: (-?\d+)ms\)/))) {
            const ageA = parseInt(m[3]), ageB = parseInt(m[5]);
            const probA = ageA < 0 ? `${m[2]} нет данных` : ageA > 5000 ? `${m[2]} устарел ${(ageA/1000).toFixed(1)}с` : `${m[2]} ок`;
            const probB = ageB < 0 ? `${m[4]} нет данных` : ageB > 5000 ? `${m[4]} устарел ${(ageB/1000).toFixed(1)}с` : `${m[4]} ок`;
            return `⏭ ${m[1]}: нет свежего ордербука — ${probA}, ${probB}`;
        }
        if ((m = line.match(/SKIP (\S+) L1 depth too shallow: A=([\d.]+)\$ B=([\d.]+)\$ < ([\d.]+)\$/))) {
            return `⏭ ${m[1]}: стакан слишком тонкий — A=$${m[2]} B=$${m[3]}, нужно минимум $${m[4]}`;
        }
        if ((m = line.match(/SKIP (\S+): insufficient margin (\S+) \(avail=\$([\d.]+), required=\$([\d.]+).*lev=(\d+)x\)/))) {
            return `⏭ ${m[1]}: не хватает маржи на ${m[2]} — доступно $${m[3]}, нужно $${m[4]} (плечо ${m[5]}x)`;
        }

        // ── ABORT ──
        if ((m = line.match(/ABORT (\S+): VWAP spread ([\d.-]+)% < break-even ([\d.]+)% \(L1 spread=([\d.]+)%\)/))) {
            return `🛑 ${m[1]}: ОТМЕНА — реальный спред в стакане ${m[2]}% ниже безубытка ${m[3]}% (L1 показывал ${m[4]}%). Сделка была бы убыточной.`;
        }
        if ((m = line.match(/ABORT (\S+): Insufficient L2 depth! req A=[\d.]+ \(has ([\d.]+)\), req B=[\d.]+ \(has ([\d.]+)\)/))) {
            return `🛑 ${m[1]}: ОТМЕНА — в стакане не хватает ликвидности (A=${m[2]} B=${m[3]} монет)`;
        }

        // ── LEG ERRORS ──
        if ((m = line.match(/LEG_A reject (\S+)\/(\S+): (.*)/))) {
            return `❌ ${m[2]}: ошибка ноги A на ${m[1]} — ${m[3]}`;
        }
        if ((m = line.match(/LEG_B reject (\S+)\/(\S+): (.*)/))) {
            return `❌ ${m[2]}: ошибка ноги B на ${m[1]} — ${m[3]}`;
        }
        if ((m = line.match(/Leg A failed for (\S+)/))) {
            return `⛔ ${m[1]}: нога A провалилась — нога B отменена чтобы не было однобокой позиции`;
        }

        // ── CLOSE FLOW ──
        if ((m = line.match(/Closing (\d+) (\S+) reason=(\S+) pct=([\d.-]+)% exit_sp=([\d.-]+)/))) {
            const reasons = {TP:'тейк-профит',STOP:'трейлинг-стоп',MANUAL:'ручное закрытие',ZERO_SPREAD:'спред схлопнулся',RETRY_CLOSE:'повтор закрытия'};
            return `📤 ЗАКРЫТИЕ #${m[1]} ${m[2]}: причина — ${reasons[m[3]]||m[3]} | PnL ${m[4]}%`;
        }
        if ((m = line.match(/EXCHANGE_VERIFIED.*PnL id=(\d+) pna=([\d.-]+) pnb=([\d.-]+) gross=([\d.-]+)/))) {
            const gross = parseFloat(m[4]);
            const icon = gross >= 0 ? '💰' : '💸';
            return `${icon} СДЕЛКА #${m[1]} ЗАКРЫТА: PnL ноги A=$${m[2]} B=$${m[3]} | итого=$${m[4]}`;
        }
        if (line.includes('CLOSE FAILED OR UNVERIFIED')) {
            return `❌ ОШИБКА ЗАКРЫТИЯ — ордер не подтверждён биржей, повтор через 5с`;
        }
        if (line.includes('POSITION STILL EXISTS after close')) {
            return `⚠️ ПОЗИЦИЯ ВСЁ ЕЩЁ ОТКРЫТА после попытки закрытия — повторная попытка`;
        }

        // ── MIRAGE ──
        if ((m = line.match(/VWAP MIRAGE \((\w+)\): L1 spread=([\d.-]+)%, VWAP spread=([\d.-]+)%/))) {
            return `👻 МИРАЖ (${m[1]}): L1 показывал ${m[2]}%, реальный VWAP ${m[3]}% — закрываем по факту`;
        }
        if ((m = line.match(/FORCE CLOSE (\S+) after (\d+)s/))) {
            return `⏰ ${m[1]}: ПРИНУДИТЕЛЬНОЕ ЗАКРЫТИЕ через ${m[2]}с зависания — лучше зафиксировать`;
        }

        // ── RISK ──
        if ((m = line.match(/MARKET REGIME ALERT: (\S+) velocity ([\d.]+)% in (\d+)ms/))) {
            return `🌪 ${m[1]}: резкое движение цены ${m[2]}% за ${m[3]}мс — пауза 15с от волатильности`;
        }
        if (line.includes('KILL-SWITCH IS ACTIVE')) {
            return `🚨 АВАРИЙНАЯ ОСТАНОВКА: движок заблокирован после 10 провалов подряд. Нужен перезапуск.`;
        }
        if (line.includes('KILL-SWITCH TRIGGERED')) {
            return `💀 АВАРИЙНАЯ ОСТАНОВКА АКТИВИРОВАНА — 10 IOC ордеров подряд не исполнились`;
        }
        if ((m = line.match(/ASYMMETRIC FILL (\S+): A \((\S+)\) filled ([\d.]+), B \((\S+)\) filled ([\d.]+)/))) {
            return `⚠️ ${m[1]}: НЕРАВНОМЕРНОЕ ИСПОЛНЕНИЕ — ${m[2]}=${m[3]} vs ${m[4]}=${m[5]}, подрезаем лишнее`;
        }
        if ((m = line.match(/LATENCY ALERT: (\S+) RTT >800ms/))) {
            return `🐢 ${m[1]}: задержка API >800мс — биржа на паузе 1 мин`;
        }

        // ── VERIFY ──
        if ((m = line.match(/Leg ([AB]) fill confirmed on slow attempt (\d+)/))) {
            return `✅ Нога ${m[1]}: исполнение подтверждено с попытки #${m[2]}`;
        }
        if ((m = line.match(/Leg ([AB]) fill unverified/))) {
            return `🔍 Нога ${m[1]}: исполнение не подтверждено — проверяем позицию на бирже`;
        }
        if ((m = line.match(/Leg ([AB]) orderId=\S+ accepted but fill not yet confirmed/))) {
            return `⏳ Нога ${m[1]}: ордер принят биржей, ждём подтверждения исполнения...`;
        }

        // ── SCANNER ──
        if ((m = line.match(/\[scanner\] SIGNAL (\S+): (\S+) vs (\S+) spread=([\d.]+)% BE=([\d.]+)%/))) {
            return `📡 СИГНАЛ ${m[1]}: ${m[2]}↔${m[3]} спред=${m[4]}% безубыток=${m[5]}%`;
        }
        if ((m = line.match(/\[scanner\] REJECT (\S+).*?->\s*(.*)/))) {
            return `🛡 ФИЛЬТР ${m[1]}: ${m[2]}`;
        }

        // ── SETTINGS (only shown on change now) ──
        if (line.includes('[settings]')) {
            return line; // already formatted in backend with ОБНОВЛЕНО
        }

        // ── MANUAL CLOSE ──
        if ((m = line.match(/MANUAL_CLOSE detected for trade (\d+) (\S+) on (\S+)\/(\S+)/))) {
            return `🖐 РУЧНОЕ ЗАКРЫТИЕ сделки #${m[1]} ${m[2]} (${m[3]}/${m[4]})`;
        }

        return null; // no translation — show original
    };

    const formatLogLine = (line) => {
        if (!line.trim()) return '';
        let cleanLine = line;
        const match = line.match(/Z\s+(INFO|ERROR|WARN|DEBUG)\s+core_node.*?:\s+(.*)/);
        if (match) {
            const timeMatch = line.match(/(\d{2}:\d{2}:\d{2})\.\d+Z/);
            const timeStr = timeMatch ? `[${timeMatch[1]}] ` : '';
            cleanLine = `${timeStr}[${match[1]}] ${match[2]}`;
        } else if (line.includes('core-node')) {
            cleanLine = line.split('core-node')[1] || line;
        }

        // Context filtering
        if (cleanLine.includes('[settings]')) {
            if (!cleanLine.includes(`id=${activeTabId}`)) return '';
        }

        // Sub-Type filtering (Trades vs Technical)
        const isTradeLog = cleanLine.includes('[real]') || cleanLine.includes('[settings]') || (cleanLine.includes('[scanner]') && cleanLine.includes('REJECT'));
        const isTechLog = !isTradeLog;
        if (window.logViewMode === 'trade' && isTechLog) return '';
        if (window.logViewMode === 'tech' && isTradeLog) return '';

        // Extract time prefix for translated lines
        const timePfx = cleanLine.match(/^\[[\d:]+\]/)?.[0] || '';

        // Try translating to Russian
        const translated = translateLog(cleanLine);
        let displayLine = translated || cleanLine;
        if (translated && timePfx && !translated.startsWith('[')) {
            displayLine = `${timePfx} ${translated}`;
        }

        let html = escHtml(displayLine);
        let style = 'color: var(--text);';

        if (html.includes('❌') || html.includes('💀') || html.includes('🚨')) {
            style = 'color: #ff4b4b; font-weight: bold;';
        } else if (html.includes('⏭') || html.includes('⏹')) {
            style = 'color: var(--text-sub);';
        } else if (html.includes('🚀') || html.includes('⚡')) {
            style = 'color: #00f2fe;';
        } else if (html.includes('✅')) {
            style = 'color: var(--green); font-weight: 600;';
        } else if (html.includes('💰') || html.includes('💸')) {
            style = 'color: #ffcc00; font-weight: bold;';
        } else if (html.includes('⚙')) {
            style = 'color: #a8b2d1;';
        } else if (html.includes('🛑') || html.includes('⛔')) {
            style = 'color: #ff8c42;';
        } else if (html.includes('⚠')) {
            style = 'color: #ff4b4b; background: rgba(255,0,0,0.08); padding: 2px 4px; border-radius: 4px;';
        } else if (html.includes('🛡')) {
            style = 'color: #e8a87c;';
        } else if (html.includes('📡')) {
            style = 'color: #64dfdf;';
        } else if (html.includes('🌪') || html.includes('👻')) {
            style = 'color: #c89bff;';
        }

        // Bold pair names
        html = html.replace(/\b([A-Z0-9]{2,}USDT)\b/g, '<span style="color:#fff;font-weight:700;">$1</span>');
        return `<div style="padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.02); ${style}">${html}</div>`;
        };

        const formattedHTML = text.split('\n').filter(l => l.trim()).map(formatLogLine).join('');
        
        if (el.innerHTML !== formattedHTML) {
            el.innerHTML = formattedHTML;
        }
        
        if (!isScrolled) {
            el.scrollTop = 0;
        }
};

window.fetchLogs = async function(isBackground = false) {
    const el = document.getElementById('logs-container');
    if (!el) return;
    
    if (!isBackground) el.textContent = "Loading logs...";
    
    try {
        const creds = localStorage.getItem('cex_auth') || '';
        const logMode = window.logViewMode || 'trade';
        const res = await fetch(`${API_BASE}/api/logs/core-node?mode=${logMode}`, {
            headers: { 'Authorization': 'Basic ' + creds }
        });
        if (res.status === 401) throw new Error("Unauthorized");
        if (!res.ok) throw new Error("Status " + res.status);
        const text = await res.text();
        
        window.lastFetchedLogs = text;
        window.renderLogsHTML(text);
        
        const statusEl = document.getElementById('logs-status');
        if (statusEl) statusEl.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
    } catch(e) {
        if (!isBackground) el.textContent = "Failed to load logs: " + e.message;
        const statusEl = document.getElementById('logs-status');
        if (statusEl) statusEl.textContent = `Error: ${e.message}`;
    }
};

window.saveSettings = async function (profileId) {
    const g = id => document.getElementById(id)?.value;
    // NOTE: is_active is intentionally excluded here — use Start/Stop Bot for that.
    const settings = {
        trade_volume:          parseFloat(g(`sf-vol-${profileId}`)),
        take_profit_percent:   parseFloat(g(`sf-tp-${profileId}`)),
        min_entry_spread:      parseFloat(g(`sf-min-${profileId}`)),
        max_entry_spread:      parseFloat(g(`sf-max-${profileId}`)),
        max_active_trades:     parseInt(g(`sf-max-trades-${profileId}`)),
        leverage:              parseInt(g(`sf-leverage-${profileId}`)) || 10,
        min_volume_millions:   parseFloat(g(`sf-minvol-${profileId}`)),
        min_depth_percent:     parseFloat(g(`sf-depth-${profileId}`)),
        slippage_percent:      parseFloat(g(`sf-slip-${profileId}`)) || 0.5,
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

    // Send ONLY is_active — do not spread the whole profile object
    // to avoid sending stale or unintended field values.
    try {
        await saveProfileSettings(profileId, { is_active: newActive ? 1 : 0 });
        const btn = document.getElementById(`sf-power-${profileId}`);
        if (btn) {
            btn.textContent = newActive ? 'Stop Bot' : 'Start Bot';
            btn.style.background = newActive ? 'var(--red)' : 'var(--green)';
        }
        showToast(`Bot ${newActive ? 'Started' : 'Stopped'}`, 'success');
    } catch (e) {
        profile.is_active = previousState; // Revert locally
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
        await apiFetch(`/api/trades/${tradeId}/close`, { method: 'POST' });
        showToast('Market close requested', 'success');
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
    const mode = document.querySelector('input[name="nt-mode"]:checked')?.value || 'real';
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

// Tab close delegation
document.getElementById('tab-bar').addEventListener('click', async (e) => {
    if (!e.target.classList.contains('tab-close')) return;
    const id = e.target.dataset.id;
    if (id === 'real-default') {
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

