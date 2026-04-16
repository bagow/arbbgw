use anyhow::Result;
use rusqlite::params;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;

use crate::models::{Trade, TradingProfile};

/// Init profiles DB (vps-api.db): trading_profiles + server_cache
pub async fn init_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=5000;

            CREATE TABLE IF NOT EXISTS server_cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                expires_at INTEGER,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trading_profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'real',
                is_active INTEGER DEFAULT 0,
                min_entry_spread REAL DEFAULT 1.0,
                max_entry_spread REAL DEFAULT 10.0,
                trade_volume REAL DEFAULT 1000.0,
                take_profit_percent REAL DEFAULT 0.1,
                trailing_stop_percent REAL DEFAULT 0.5,
                max_active_trades INTEGER DEFAULT 5,
                commission_rate REAL DEFAULT 0.45,
                slippage_percent REAL DEFAULT 0.1,
                funding_rate_8h REAL DEFAULT 0.01,
                min_volume_millions REAL DEFAULT 10.0,
                collector_interval_secs INTEGER DEFAULT 5,
                pending_commands TEXT,
                owner TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                disabled_exchanges TEXT DEFAULT '[]',
                min_depth_percent REAL DEFAULT 100.0
            );
            ",
        )?;

        // Safe, idempotent schema migration — only adds column if it does not exist.
        // Never touches existing row data.
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN disabled_exchanges TEXT DEFAULT '[]'", []);
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN leverage INTEGER DEFAULT 10", []);
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN margin_mode TEXT DEFAULT 'cross'", []);
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN min_structural_deviation REAL DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN min_depth_percent REAL DEFAULT 100.0", []);

        // ── Technical settings table (global engine params) ──
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS technical_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                description TEXT,
                category TEXT DEFAULT 'general'
            );
        ")?;

        // Seed defaults — INSERT OR IGNORE preserves user changes
        let tech_defaults: &[(&str, &str, &str, &str)] = &[
            // ── Spread computation ──
            ("spread_threshold",       "-1.0",
             "Minimum spread % to include in the screener table. Spreads below this are filtered out before display. Set negative (e.g. -1.0) to show losing spreads for diagnostics. Formula: row is shown if spread >= spread_threshold.",
             "spreads"),
            ("spread_max",             "50.0",
             "Maximum spread % — any spread above this value is treated as a data anomaly (stale price, glitch) and excluded. Prevents false signals from exchange API lag. Formula: row is dropped if spread > spread_max.",
             "spreads"),
            ("stale_secs",             "30",
             "Seconds before ticker data is considered stale and excluded from spread computation. Each exchange price has a timestamp; if now - timestamp > stale_secs, the price is skipped. Lower = stricter freshness, fewer pairs. Higher = more pairs but risk of trading on outdated prices.",
             "spreads"),
            ("intra_spread_limit",     "1.02",
             "Maximum ask/bid ratio acceptable per exchange during spread computation. Filters out illiquid pairs where the exchange's own spread is too wide. Formula: if ask/bid > intra_spread_limit, the pair is excluded from that exchange. 1.02 = max 2% internal spread.",
             "spreads"),
            ("ema_tau_secs",           "86400",
             "EMA (Exponential Moving Average) time constant in seconds for computing 'structural spread' — the long-term average spread between two exchanges for a pair. Used to detect temporary deviations vs permanent price differences. 86400 = 24h. Smaller values (e.g. 3600) make EMA more reactive but may catch noise. Larger values smooth out noise but react slower to regime changes. Formula: alpha = 1 - exp(-dt / ema_tau_secs), ema = ema_prev * (1 - alpha) + current * alpha.",
             "spreads"),
            ("blacklist",              "USOILUSDT,XMRUSDT,TRUMPUSDT,LUNAUSDT,BTTUSDT,BTCSTUSDT",
             "Comma-separated list of trading pairs to permanently exclude from all trading and screener. Use for pairs with known issues: delistings, extreme volatility, regulatory problems. Case-sensitive, must match exact pair name (e.g. BTCUSDT).",
             "spreads"),

            // ── Entry filters ──
            ("intra_spread_max_pct",   "1.0",
             "Maximum bid-ask spread % on each individual exchange to allow entry. Protects against opening trades in illiquid markets where you'd lose on the intra-exchange spread itself. Formula: intra_pct = (ask - bid) / bid × 100. Entry blocked if intra_pct > intra_spread_max_pct on either exchange. 1.0 = max 1% bid-ask width.",
             "entry"),
            ("depth_multiplier",       "2.0",
             "Required order book depth as a multiplier of your trade_volume (from profile settings). Formula: min_depth_usd = trade_volume × depth_multiplier. If the bid/ask size on either exchange is < min_depth_usd, entry is skipped. Ensures enough liquidity to fill without excessive slippage. 2.0 = need 2× your order size in the book.",
             "entry"),
            ("vol_diff_max_pct",       "5.0",
             "Maximum allowed difference between the actual USD volumes of the two legs, as a percentage of leg A. Catches rounding errors where contract sizes differ significantly. Formula: vol_diff = |vol_a - vol_b| / vol_a × 100. Entry blocked if vol_diff > vol_diff_max_pct. 5.0 = max 5% volume mismatch between legs.",
             "entry"),
            ("vol_tolerance_pct",      "15.0",
             "Maximum deviation of actual trade volume from the target trade_volume (profile setting), as ±%. Prevents over/under-sizing due to contract rounding. Formula: max_vol = trade_volume × (1 + vol_tolerance_pct/100), min_vol = trade_volume × (1 - vol_tolerance_pct/100). Entry blocked if actual_vol outside [min_vol, max_vol]. 15.0 = ±15% tolerance.",
             "entry"),
            ("min_listing_age_days",   "30",
             "Minimum number of days since a contract was listed on an exchange to allow trading. New listings often have extreme volatility, poor liquidity, and unreliable funding. Formula: age = now - listing_date. Entry blocked if age < min_listing_age_days × 86400 seconds. 30 = skip contracts less than 30 days old.",
             "entry"),
            ("binance_min_notional",   "5.05",
             "Binance futures minimum order value in USD. Orders below this are rejected by Binance API with 'MIN_NOTIONAL' error. Formula: entry blocked if exchange=binance and actual_vol < binance_min_notional. Set slightly above $5 to account for rounding. 5.05 is the safe minimum.",
             "entry"),
            ("margin_check_multiplier","1.5",
             "Required available balance as a multiplier of order notional. Prevents opening trades when balance is too low to cover margin + fees. Formula: entry allowed if available_balance >= notional × margin_check_multiplier. 1.5 = need 50% more balance than the order size. Accounts for existing positions, unrealized PnL, and fees.",
             "entry"),
            ("max_spreads_per_cycle",  "200",
             "Maximum number of spread opportunities to evaluate per engine cycle (10ms tick). Spreads are sorted by profitability, so top N are checked first. Higher = more opportunities found but more CPU per tick. 200 is optimal for 5-8 exchanges. Formula: for row in spreads.take(max_spreads_per_cycle).",
             "entry"),
            ("default_fee_rate",       "0.0005",
             "Default taker fee rate used when the real fee rate hasn't been fetched from the exchange API yet. Expressed as a fraction: 0.0005 = 0.05%. Used in break-even calculation: break_even = 2 × (fee_a + fee_b) + 4 × slippage. Once real fees are cached (refreshed every balance_refresh_secs), this fallback is not used.",
             "entry"),

            // ── Cooldowns ──
            ("exchange_cooldown_ms",   "1800000",
             "Cooldown period in milliseconds applied to an exchange after a margin/balance error. During this period, no new trades will be opened on that exchange. Prevents rapid-fire failures when balance is insufficient. 1800000 = 30 minutes. Applied per-exchange (e.g. if binance fails, bybit is unaffected).",
             "cooldowns"),
            ("pair_cooldown_ms",       "300000",
             "Cooldown period in milliseconds applied to a specific pair after an unfilled IOC order (Both IOC unfilled). Prevents repeatedly trying the same pair that isn't filling. 300000 = 5 minutes. Applied per-pair across all exchanges.",
             "cooldowns"),
            ("asymmetric_cooldown_ms", "1500000",
             "Cooldown period in milliseconds applied to a pair after an asymmetric fill (one leg filled, other didn't → emergency close). This is a more serious event so the cooldown is longer. 1500000 = 25 minutes. Prevents the same pair from causing repeated asymmetric losses.",
             "cooldowns"),

            // ── Order execution ──
            ("entry_order_type",       "market",
             "How entry orders are placed. 'market' = Market orders with guaranteed fill on both legs (no asymmetric risk, but possible slippage). 'ioc' = IOC Limit orders with price tolerance from slippage_percent settings (slippage-protected, but risk of one leg not filling → emergency close at loss).",
             "execution"),
            ("api_timeout_ms",         "2000",
             "Network timeout in milliseconds for placing entry and exit orders. Lower = faster cancellation of hanging legs on failed exchanges (avoids asymmetric risks), but increases risk of dropping a slow-but-successful exchange fill. Recommended: 1500-2500ms.",
             "execution"),
            ("fill_verify_retries",    "10",
             "Number of API polling attempts to verify an entry order was filled and get the actual fill price. After placing both legs simultaneously, the engine polls each exchange for fill confirmation. Formula: for attempt in 0..fill_verify_retries { poll; sleep(fill_verify_delay_ms) }. 10 retries × 15ms = 150ms max verification window.",
             "execution"),
            ("fill_verify_delay_ms",   "15",
             "Delay in milliseconds between fill verification retries for entry orders. Keep low for fast confirmation. Formula: total_verify_time ≤ fill_verify_retries × fill_verify_delay_ms. 15ms is optimal for exchange API response times.",
             "execution"),
            ("close_verify_retries",   "20",
             "Number of API polling attempts to verify a close/exit order and get the real exit price for accurate PnL. More retries than entry because accurate exit price is critical for PnL reporting. Formula: for _ in 0..close_verify_retries { poll; sleep(close_verify_delay_ms) }. 20 × 25ms = 500ms max.",
             "execution"),
            ("close_verify_delay_ms",  "25",
             "Delay in milliseconds between close verification retries. Slightly longer than entry—exit prices are less time-sensitive since the position is already closed. Formula: total_close_verify ≤ close_verify_retries × close_verify_delay_ms.",
             "execution"),
            ("emergency_close_retries","3",
             "Number of retry attempts for emergency market close when an asymmetric fill occurs (one leg filled, other didn't). Each retry uses market order (price=0) to guarantee closure. Delay doubles each attempt: 50ms, 100ms, 150ms. Critical for preventing orphaned positions.",
             "execution"),
            ("exit_sanity_pct",        "0.20",
             "Maximum allowed price deviation between entry and exit price, as a fraction. Protects against API returning garbage exit prices that would corrupt PnL. Formula: if |exit - entry| / entry > exit_sanity_pct, the reported exit price is discarded and the entry price is used instead. 0.20 = max 20% price change between entry and exit is considered valid.",
             "execution"),

            // ── Intervals ──
            ("balance_refresh_secs",   "60",
             "Interval in seconds between background refreshes of exchange balances and taker fee rates. Lower = more up-to-date balances but more API calls. Higher = fewer API calls but risk of stale balance data. 60 = refresh every minute. Affects margin_check_multiplier accuracy.",
             "intervals"),
            ("multiplier_refresh_secs","3600",
             "Interval in seconds between refreshes of contract multipliers and tick sizes from exchange APIs. These change rarely (only when exchanges update contract specs). 3600 = refresh hourly. Affects contract size calculations and price rounding.",
             "intervals"),
            ("funding_cache_secs",     "300",
             "Duration in seconds to cache funding rates before re-fetching. Funding rates update every 8 hours on most exchanges, so 300s (5 min) cache is sufficient. Lower = more API calls with no benefit.",
             "intervals"),
            ("diag_log_interval",      "100",
             "Frequency of diagnostic log output, expressed as 'every N engine cycles'. One cycle ≈ 10ms, so 100 = log every ~1 second. Logs include: total spreads, in-range count, supported pairs, active trades, cooled exchanges, and cached fee rates. Useful for monitoring engine health.",
             "intervals"),
            ("live_update_throttle_ms","1000",
             "Minimum interval in milliseconds between live PnL database updates for open trades. Prevents excessive DB writes that could slow down the engine. 1000 = update open trade PnL at most once per second. Lower = more responsive UI but more DB load.",
             "intervals"),
        ];
        for (key, value, desc, cat) in tech_defaults {
            conn.execute(
                "INSERT OR IGNORE INTO technical_settings (key, value, description, category) VALUES (?, ?, ?, ?)",
                params![key, value, desc, cat],
            )?;
        }

        // NOTE: is_active is NOT restored here — profile starts only via user's Start button.
        // Previously this auto-set is_active=1 on every deploy, overriding user's Stop.

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        conn.execute(
            "INSERT OR IGNORE INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,
                 trade_volume,take_profit_percent,trailing_stop_percent,max_active_trades,
                 commission_rate,slippage_percent,funding_rate_8h,min_volume_millions,
                 collector_interval_secs,created_at,updated_at,disabled_exchanges,min_depth_percent)
            VALUES ('real-default','Real Trading','real',0,1.0,10.0,100.0,0.2,0.3,3,0.05,0.05,0.01,10.0,5,?,?,'[]',100.0)",
            params![now, now],
        )?;
        Ok(())
    }).await?;
    Ok(())
}

/// Init trades-only DB (real-trader.db)
pub async fn init_trades_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=5000;

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
                gross_pnl REAL,
                commission_cost REAL,
                funding_cost REAL,
                slippage_cost REAL,
                last_price_a REAL,
                last_price_b REAL,
                last_spread REAL,
                curr_gross_pnl REAL,
                pnl_a REAL,
                pnl_b REAL,
                close_reason TEXT,
                profile_id TEXT DEFAULT 'real-default',
                order_id_a TEXT,
                order_id_b TEXT,
                contracts_a INTEGER,
                contracts_b INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status   ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_profile  ON trades(profile_id);
            CREATE INDEX IF NOT EXISTS idx_trades_exit     ON trades(exit_time);
            CREATE INDEX IF NOT EXISTS idx_trades_profile_status ON trades(profile_id, status);
            CREATE INDEX IF NOT EXISTS idx_trades_status_profile_exit ON trades(status, profile_id, exit_time);
            ",
        )?;
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN slippage_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN commission_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_a TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_b TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_a INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_b INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_next_ts INTEGER", []);
        Ok(())
    }).await?;
    Ok(())
}

pub fn row_to_profile(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradingProfile> {
    Ok(TradingProfile {
        id: row.get(0)?,
        name: row.get(1)?,
        mode: row.get(2)?,
        is_active: row.get::<_, i64>(3)? != 0,
        min_entry_spread: row.get(4)?,
        max_entry_spread: row.get(5)?,
        trade_volume: row.get(6)?,
        take_profit_percent: row.get(7)?,
        trailing_stop_percent: row.get(8)?,
        max_active_trades: row.get(9)?,
        commission_rate: row.get(10)?,
        slippage_percent: row.get(11)?,
        funding_rate_8h: row.get(12)?,
        min_volume_millions: row.get(13)?,
        collector_interval_secs: row.get(14)?,
        pending_commands: row.get(15)?,
        owner: row.get(16)?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
        disabled_exchanges: row.get::<_, Option<String>>(19).unwrap_or(Some("[]".into())),
        leverage: row.get::<_, Option<i64>>(20).unwrap_or(Some(10)).unwrap_or(10),
        margin_mode: row.get::<_, Option<String>>(21).unwrap_or(Some("cross".into())).unwrap_or_else(|| "cross".into()),
        min_structural_deviation: row.get::<_, Option<f64>>(22).unwrap_or(Some(0.0)).unwrap_or(0.0),
        min_depth_percent: row.get::<_, Option<f64>>(23).unwrap_or(Some(100.0)).unwrap_or(100.0),
    })
}

pub fn row_to_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<Trade> {
    Ok(Trade {
        id: row.get("id")?,
        pair: row.get("pair")?,
        exchange_a: row.get("exchange_a")?,
        exchange_b: row.get("exchange_b")?,
        direction: row.get("direction")?,
        volume: row.get("volume")?,
        entry_price_a: row.get("entry_price_a")?,
        entry_price_b: row.get("entry_price_b")?,
        entry_spread: row.get("entry_spread")?,
        entry_time: row.get("entry_time")?,
        highest_spread_reached: row.get("highest_spread_reached")?,
        status: row.get("status")?,
        exit_price_a: row.get("exit_price_a")?,
        exit_price_b: row.get("exit_price_b")?,
        exit_spread: row.get("exit_spread")?,
        exit_time: row.get("exit_time")?,
        pnl: row.get("pnl")?,
        commission_cost: row.get("commission_cost").unwrap_or(Some(0.0)),
        funding_cost: row.get("funding_cost").unwrap_or(Some(0.0)),
        last_price_a: row.get("last_price_a").unwrap_or(None),
        last_price_b: row.get("last_price_b").unwrap_or(None),
        last_spread: row.get("last_spread").unwrap_or(None),
        curr_gross_pnl: row.get("curr_gross_pnl").unwrap_or(None),
        pnl_a: row.get("pnl_a").unwrap_or(None),
        pnl_b: row.get("pnl_b").unwrap_or(None),
        close_reason: row.get("close_reason").unwrap_or(None),
        profile_id: row.get("profile_id").unwrap_or(Some("real-default".into())),
        contracts_a: row.get("contracts_a").unwrap_or(None),
        contracts_b: row.get("contracts_b").unwrap_or(None),
        slippage_cost: row.get("slippage_cost").unwrap_or(Some(0.0)),
        current_price_a: None,
        current_price_b: None,
        current_spread: None,
        current_pnl: None,
        gross_pnl: row.get("gross_pnl").unwrap_or(None),
        funding_rate_a: row.get("funding_rate_a").unwrap_or(None),
        funding_rate_b: row.get("funding_rate_b").unwrap_or(None),
        funding_next_ts: row.get("funding_next_ts").unwrap_or(None),
    })
}
 
