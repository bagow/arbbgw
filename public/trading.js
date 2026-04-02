/**
 * trading.js – Multi-tab trading UI
 *
 * Each "tab" is a trading profile (paper or real). Profiles are stored in D1 via
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

        const badge = p.mode === 'real'
            ? '<span class="tab-badge tab-badge--real">💰</span>'
            : '<span class="tab-badge tab-badge--paper">🧪</span>';

        const isDefault = p.id === 'paper-default' || p.id === 'real-default';
        let dispName = escHtml(p.name);
        if (p.id === 'paper-default') dispName = 'Public Paper';
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

    const { trades, real, exchanges, profiles: serverProfiles, profile_stats, profile_active, profile_history } = lastUnified;
    console.log(`[DEBUG] Received Unified for ${activeTabId}:`, { profile_stats, trades_stats: trades?.stats });

    // Sync local profile settings from server profiles data
    if (serverProfiles) {
        for (const sp of serverProfiles) {
            const local = profiles.find(p => p.id === sp.id);
            if (local) Object.assign(local, sp);
        }
    }

    // Use profile-scoped data if available (from profile_id param in request)
    // Otherwise fall back to the global paper/real data
    let active, history;
    if (profile_active !== undefined) {
        active  = profile_active;
        history = profile_history || [];
    } else {
        active  = profile.mode === 'real' ? (real?.active  || []) : (trades?.active  || []);
        history = profile.mode === 'real' ? (real?.history || []) : (trades?.history || []);
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
        updateDynamicElements(profile, active, stats);
        if (active !== null) {
            renderActiveTable(profile.id, active);
            renderHistoryTable(profile.id, history);
            const dashWrap = document.getElementById('mt-sub-dashboard');
            const hasStats = stats && stats.total_trades > 0;
            if (dashWrap && hasStats) {
                if (!document.getElementById(`dash-equity-${profile.id}`)) {
                    dashWrap.innerHTML = renderDashboardHTML(profile.id);
                }
                renderDashboardCharts(profile.id, stats);
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
                            <th>Dir</th>
                        </tr>
                    </thead>
                    <tbody id="spread-tbody">
                        <tr class="placeholder-row">
                            <td colspan="7" style="text-align:center; padding:40px; color:var(--text-sub);">⟳ Initializing scanner…</td>
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
        <div class="mt-content" id="mt-sub-history" style="display:${activeSubTabId==='history'?'block':'none'};">
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
                                Total (<span id="h-count-${profile.id}">${stats?.total_trades??0}</span> trades)
                            </td>
                            <td id="h-pnl-${profile.id}" style="color:${(stats?.total_pnl||0)>=0?'var(--green)':'var(--red)'};border-top:1px solid var(--border-dim)">
                                ${fmtPnL(stats?.total_pnl||0)}
                            </td>
                            <td colspan="3" style="border-top:1px solid var(--border-dim)"></td>
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
    `;

    if (active !== null) {
        renderActiveTable(profile.id, active);
        renderHistoryTable(profile.id, history);
        if (stats && stats.total_trades > 0) renderDashboardCharts(profile.id, stats);
    }

    // After full re-render, re-hook scanner references if on scanner tab
    if (activeSubTabId === 'scanner' && window.initScanner) {
        window.initScanner(profile.id);
    }
}

/** Helper to update stats in sub-nav during polling without full re-render */
function updateDynamicElements(profile, active, stats) {
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

    // Real trading: only Gate/HTX/KuCoin have API keys
    const REAL_EXCHANGES = new Set(['gate', 'htx', 'kucoin']);
    const isReal = profile.mode === 'real';
    const exNames = Object.keys(exchangesData.data)
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

    let html = `<div class="cex-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:24px;">`;
    
    for (const exA of exNames) {
        let pairsList = '';
        let maxTickers = 0;
        const rawArr = exchangesData.data[exA];
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
        
        html += `
            <div class="stat-card" style="display:flex;flex-direction:column; background:var(--bg-panel); border:1px solid var(--border); border-radius:12px; padding:20px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                    <div style="display:flex;align-items:center;gap:10px;">
                        <div style="width:10px;height:10px;border-radius:50%;background:${isEnabled?'var(--green)':'var(--text-sub)'}"></div>
                        <h3 style="margin:0;font-size:1.2rem;font-weight:600;text-transform:uppercase;">${exA}</h3>
                    </div>
                    <div style="text-align:right;">
                        <span style="font-size:0.75rem;color:var(--text-sub);display:block;text-transform:uppercase;letter-spacing:0.5px;">Tickers</span>
                        <span style="font-family:var(--font-mono);font-weight:600;">${maxTickers}+</span>
                    </div>
                </div>
                
                <div style="flex:1;max-height:160px;overflow-y:auto;margin-bottom:20px;padding-right:8px;">
                    ${pairsList || '<div class="text-sub" style="font-size:0.9rem;padding:12px 0;">No matching pairs found.</div>'}
                </div>
                
                <label style="display:flex;align-items:center;gap:12px;cursor:pointer;padding-top:16px;border-top:1px solid var(--border-dim); user-select:none;">
                    <input type="checkbox" 
                           ${isEnabled ? 'checked' : ''} 
                           onchange="toggleExchange('${profile.id}', '${exA}', this)"
                           style="width: 18px; height: 18px; cursor:pointer;">
                    <span style="font-weight:500; font-size:0.95rem;">${isEnabled ? 'Trading Enabled' : 'Trading Disabled'}</span>
                </label>
            </div>
        `;
    }
    html += `</div>`;
    return html;
}

function renderSettingsForm(profile) {
    const p = profile;
    return `
        <section class="trading-settings-section" style="padding:0;background:transparent;border:none;">
            <form id="settings-form-${p.id}" class="settings-grid" onsubmit="return false;">
                <div class="form-group">
                    <label>Min Entry Spread (%)</label>
                    <input type="number" id="sf-min-${p.id}" step="0.0001" value="${p.min_entry_spread??1}" />
                </div>
                <div class="form-group">
                    <label>Max Entry Spread (%)</label>
                    <input type="number" id="sf-max-${p.id}" step="0.0001" value="${p.max_entry_spread??10}" />
                </div>
                <div class="form-group">
                    <label>Trade Volume (USD)</label>
                    <input type="number" id="sf-vol-${p.id}" step="0.0001" value="${p.trade_volume??1000}" />
                </div>
                <div class="form-group">
                    <label>Max Active Trades</label>
                    <input type="number" id="sf-max-trades-${p.id}" step="1" value="${p.max_active_trades??5}" />
                </div>
                <div class="form-group">
                    <label>Take Profit (%)</label>
                    <input type="number" id="sf-tp-${p.id}" step="0.0001" value="${p.take_profit_percent??0.1}" />
                </div>
                <div class="form-group">
                    <label>Trailing Stop (%)</label>
                    <input type="number" id="sf-trail-${p.id}" step="0.0001" value="${p.trailing_stop_percent??0.5}" />
                </div>
                <div class="form-group">
                    <label>Slippage Rate (%)</label>
                    <input type="number" id="sf-slip-${p.id}" step="0.0001" value="${p.slippage_percent??0.1}" />
                </div>
                <div class="form-group">
                    <label>Commission Rate (%)</label>
                    <input type="number" id="sf-comm-${p.id}" step="any" value="${p.commission_rate??0.45}" />
                </div>
                <div class="form-group">
                    <label>Min Volume (M$)</label>
                    <input type="number" id="sf-minvol-${p.id}" step="0.1" value="${p.min_volume_millions??10}" />
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
        const avgDuration = stats.avg_duration || 0;
        kpiEl.innerHTML = [
            ['Total PnL', fmtPnL(totalPnl), totalPnl >= 0 ? 'var(--green)' : 'var(--red)'],
            ['Trades', totalTrades, 'var(--text)'],
            ['Win Rate', `${winRate}%`, wins / (totalTrades || 1) >= 0.5 ? 'var(--green)' : 'var(--red)'],
            ['Avg PnL', fmtPnL(avgPnl), avgPnl >= 0 ? 'var(--green)' : 'var(--red)'],
            ['Avg Duration', typeof fmtDuration === 'function' ? fmtDuration(avgDuration) : avgDuration + 'ms', 'var(--text)'],
        ].map(([label, value, color]) => `
            <div style="background:var(--bg-row-alt);border:1px solid var(--border-dim);border-radius:10px;padding:14px 16px;">
                <div style="font-size:.75rem;color:var(--text-sub);margin-bottom:4px;">${label}</div>
                <div style="font-size:1.2rem;font-weight:700;color:${color};">${value}</div>
            </div>
        `).join('');
    }

    // Top Tokens array
    const ttEl = document.getElementById(`dash-toptokens-${pid}`);
    if (ttEl && stats.top_tokens && stats.top_tokens.length > 0) {
        ttEl.innerHTML = stats.top_tokens.map(t => `<tr><td><strong>${t.token}</strong></td><td style="text-align:right;color:${(t.pnl||0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(t.pnl||0)}</td></tr>`).join('');
    } else if (ttEl) {
        ttEl.innerHTML = '<tr><td colspan="2" style="text-align:center;color:var(--text-sub)">No data</td></tr>';
    }

    // Top Exchange Pairs array
    const tpEl = document.getElementById(`dash-toppairs-${pid}`);
    if (tpEl && stats.top_exchange_pairs && stats.top_exchange_pairs.length > 0) {
        tpEl.innerHTML = stats.top_exchange_pairs.map(p => `<tr><td><strong>${p.pair}</strong></td><td style="text-align:right;color:${(p.pnl||0)>=0?'var(--green)':'var(--red)'}">${fmtPnL(p.pnl||0)}</td></tr>`).join('');
    } else if (tpEl) {
        tpEl.innerHTML = '<tr><td colspan="2" style="text-align:center;color:var(--text-sub)">No data</td></tr>';
    }

    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = '#334155';

    // Balance Analysis Chart
    const balCtx = document.getElementById(`dash-balance-${pid}`);
    if (balCtx) {
        const curve = stats.equity_curve || [];
        const labels = [];
        
        const exBalances = {};
        const exKeys = new Set();
        curve.forEach(p => { if(p.ex_a) exKeys.add(p.ex_a); if(p.ex_b) exKeys.add(p.ex_b); });
        if (exKeys.size === 0) exKeys.add('Capital');

        const colors = ['#60a5fa', '#f43f5e', '#34d399', '#c084fc', '#fbbf24'];
        let cIdx = 0;
        const colorMap = {};
        exKeys.forEach(ex => {
            exBalances[ex] = 100; // $100 base for each exchange
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
                <td><span style="color:var(--text-sub)">${t.direction}</span></td>
                <td>$${t.volume}</td>
                <td style="color:${pnlColor};font-weight:600">${pnlStr}</td>
                <td>${(t.entry_spread??0).toFixed(2)}% &rarr; ${(t.exit_spread??0).toFixed(2)}%</td>
                <td><span class="badge ${t.close_reason==='TP'?'badge-long':t.close_reason==='STOP'?'badge-short':'badge-neutral'}">${t.close_reason??'—'}</span></td>
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
    toast.className = `toast toast--\${type}`;
    const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️';
    toast.innerHTML = `<span class="toast-icon">\${icon}</span><span class="toast-msg">\${message}</span>`;
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
};

window.saveSettings = async function (profileId) {
    const g = id => document.getElementById(id)?.value;
    // NOTE: is_active is intentionally excluded here — use Start/Stop Bot for that.
    const settings = {
        min_entry_spread:      parseFloat(g(`sf-min-${profileId}`)),
        max_entry_spread:      parseFloat(g(`sf-max-${profileId}`)),
        trade_volume:          parseFloat(g(`sf-vol-${profileId}`)),
        max_active_trades:     parseInt(g(`sf-max-trades-${profileId}`)),
        take_profit_percent:   parseFloat(g(`sf-tp-${profileId}`)),
        trailing_stop_percent: parseFloat(g(`sf-trail-${profileId}`)),
        slippage_percent:      parseFloat(g(`sf-slip-${profileId}`)),
        commission_rate:       parseFloat(g(`sf-comm-${profileId}`)),
        min_volume_millions:   parseInt(g(`sf-minvol-${profileId}`)),
        collector_interval_secs: parseInt(g(`sf-interval-${profileId}`)),
    };
    const btn = document.getElementById(`sf-save-${profileId}`);
    try {
        await saveProfileSettings(profileId, settings);
        const p = profiles.find(pr => pr.id === profileId);
        if (p) Object.assign(p, settings);
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

window.clearTrades = async function (profileId, mode) {
    if (!confirm('Delete ALL active and historical trades for this profile?')) return;
    try {
        await apiFetch(`/api/profiles/${profileId}/trades`, { method: 'DELETE' });
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

const fmtPnL = v => { const n = Number(v)||0; if (Math.abs(n)<.005) return '$0.00'; return (n>0?'+':'-')+'$'+Math.abs(n).toFixed(2); };
const fmtCost = v => { const n = Number(v)||0; if (Math.abs(n)<.005) return '$0.00'; return n>0?`-$${Math.abs(n).toFixed(2)}`:`+$${Math.abs(n).toFixed(2)}`; };
const fmtDuration = ms => { const s=Math.floor(ms/1000); if(s<60) return s+'s'; const m=Math.floor(s/60); if(m<60) return m+'m '+(s%60)+'s'; const h=Math.floor(m/60); if(h<24) return h+'h '+(m%60)+'m'; return Math.floor(h/24)+'d '+(h%24)+'h'; };
const escHtml = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

// ─── Start ────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', boot);
