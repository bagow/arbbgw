#!/usr/bin/env python3
"""Guarded bounded production canary. Never prints or reads exchange secrets."""
import json
import secrets
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path("/home/opc/core-node")
PROFILE_DB = ROOT / "vps-api.db"
TRADE_DB = ROOT / "real-trader.db"
PROFILE_ID = "real-default"
MANAGED_VENUES = ("binance", "kucoin")
DISABLED = ["htx", "toobit", "ascendex", "bingx", "bitget", "bitmex", "gate", "xt", "bybit"]
WATCHDOG_CACHE_KEY = "canary_watchdog_token"
DEPENDENCY_READINESS_LOSS_LIMIT = 5
MIN_MONITOR_SECONDS = 60
MAX_MONITOR_SECONDS = 21_600
CANARY_SHUTDOWN_GRACE_SECONDS = 450


def abort_on_signal(signum, _frame):
    raise TimeoutError(f"canary interrupted by signal {signum}")


def health():
    request = urllib.request.Request("http://127.0.0.1:3003/health")
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return json.load(response)


def counter_delta(before, after):
    """Return non-zero changes in process-local monotonic health counters."""
    delta = {}
    for key in sorted(set(before) | set(after)):
        change = int(after.get(key, 0)) - int(before.get(key, 0))
        if change:
            delta[key] = change
    return delta


def configured_monitor_seconds(config):
    """Validate the bounded observation window before arming any watchdog."""
    monitor_seconds = int(config.get("monitor_seconds", 1800))
    if not MIN_MONITOR_SECONDS <= monitor_seconds <= MAX_MONITOR_SECONDS:
        raise RuntimeError(
            f"monitor_seconds outside {MIN_MONITOR_SECONDS}..{MAX_MONITOR_SECONDS}"
        )
    return monitor_seconds


def configured_filled_trade_limit(config):
    """Bound the total number of filled trades created by this run."""
    limit = int(config.get("max_filled_trades", 1))
    if not 1 <= limit <= 10:
        raise RuntimeError("max_filled_trades outside 1..10")
    return limit


def configured_active_trade_limit(config):
    """Bound simultaneous canary exposure independently of the batch target."""
    limit = int(config.get("max_active_trades", 1))
    if not 1 <= limit <= 5:
        raise RuntimeError("max_active_trades outside 1..5")
    return limit


def required_canary_margin(config, margin_multiplier):
    """Reserve enough venue margin for every permitted simultaneous leg."""
    total_notional = (
        float(config["notional_usdt"])
        * configured_active_trade_limit(config)
    )
    return (
        total_notional / float(config["leverage"]) * margin_multiplier
        + total_notional * 0.002
    )


def observe_health(report, current):
    """Keep compact maxima so the final post-disable snapshot is not misleading."""
    observed = report.setdefault(
        "monitor_observation",
        {"samples": 0, "scanner_max": {}, "venues_max": {}},
    )
    observed["samples"] += 1
    for key, value in (current.get("scanner_status") or {}).items():
        if isinstance(value, (int, float)):
            observed["scanner_max"][key] = max(
                int(value), int(observed["scanner_max"].get(key, 0))
            )
    for venue in current.get("venues") or []:
        exchange = venue.get("exchange")
        if not exchange:
            continue
        maxima = observed["venues_max"].setdefault(exchange, {})
        for key in ("books", "fresh_books", "wanted_books", "current_specs"):
            value = venue.get(key)
            if isinstance(value, (int, float)):
                maxima[key] = max(int(value), int(maxima.get(key, 0)))


def observe_market_readiness(report, current_gap_samples, market_ready):
    """Record recoverable market-data gaps without ending the bounded run."""
    if market_ready:
        if current_gap_samples:
            report["market_readiness_recoveries"] = (
                int(report.get("market_readiness_recoveries", 0)) + 1
            )
            report["last_market_readiness_gap_samples"] = current_gap_samples
        report["market_readiness_current_gap_samples"] = 0
        return 0
    current_gap_samples += 1
    report["market_readiness_outages"] = int(
        report.get("market_readiness_outages", 0)
    ) + (1 if current_gap_samples == 1 else 0)
    report["market_readiness_current_gap_samples"] = current_gap_samples
    report["market_readiness_max_gap_samples"] = max(
        current_gap_samples,
        int(report.get("market_readiness_max_gap_samples", 0)),
    )
    return current_gap_samples


def profile_connection():
    db = sqlite3.connect(PROFILE_DB, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


def trade_connection():
    db = sqlite3.connect(TRADE_DB, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


def backup_databases(label):
    destination = ROOT / "canary-backups" / label
    destination.mkdir(parents=True, exist_ok=False)
    for source in (PROFILE_DB, TRADE_DB):
        with sqlite3.connect(source, timeout=10) as src, sqlite3.connect(
            destination / source.name
        ) as dst:
            src.backup(dst)
    return str(destination)


def snapshot_profile():
    with profile_connection() as db:
        row = db.execute(
            "SELECT * FROM trading_profiles WHERE id=?", (PROFILE_ID,)
        ).fetchone()
        if row is None:
            raise RuntimeError("canary profile is missing")
        return dict(row)


def restore_profile_halted(snapshot, watchdog_token=None):
    snapshot = dict(snapshot)
    snapshot["is_active"] = 0
    columns = [name for name in snapshot if name != "id"]
    assignments = ",".join(f'"{name}"=?' for name in columns)
    values = [snapshot[name] for name in columns]
    with profile_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        if watchdog_token is not None:
            current = db.execute(
                "SELECT value FROM server_cache WHERE key=?", (WATCHDOG_CACHE_KEY,)
            ).fetchone()
            if current is not None and current[0] != watchdog_token:
                raise RuntimeError("canary watchdog lease belongs to a newer run")
        updated = db.execute(
            f"UPDATE trading_profiles SET {assignments} WHERE id=?",
            (*values, snapshot["id"]),
        ).rowcount
        if updated != 1:
            raise RuntimeError("canary profile restore did not update exactly one row")
        db.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) "
            "ON CONFLICT(key) DO UPDATE SET value='1'"
        )
        if watchdog_token is not None:
            db.execute(
                "DELETE FROM server_cache WHERE key=? AND value=?",
                (WATCHDOG_CACHE_KEY, watchdog_token),
            )
        db.commit()


def watchdog_program(token, start_id, delay_seconds):
    return f'''import sqlite3,time
time.sleep({int(delay_seconds)})
for _ in range(60):
    profile = None
    try:
        profile = sqlite3.connect({str(PROFILE_DB)!r}, timeout=10)
        profile.execute("PRAGMA busy_timeout=10000")
        profile.execute("BEGIN IMMEDIATE")
        row = profile.execute("SELECT value FROM server_cache WHERE key=?", ({WATCHDOG_CACHE_KEY!r},)).fetchone()
        if row is None or row[0] != {token!r}:
            profile.rollback()
            break
        with sqlite3.connect({str(TRADE_DB)!r}, timeout=10) as trades:
            trades.execute("PRAGMA busy_timeout=10000")
            trades.execute("UPDATE trades SET manual_close_requested=1 WHERE id>? AND status NOT IN ('CLOSED','CANCELLED')", ({int(start_id)},))
        profile.execute("UPDATE trading_profiles SET is_active=0,updated_at=? WHERE id=?", (int(time.time()*1000), {PROFILE_ID!r}))
        profile.execute("INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) ON CONFLICT(key) DO UPDATE SET value='1'")
        profile.execute("DELETE FROM server_cache WHERE key=? AND value=?", ({WATCHDOG_CACHE_KEY!r}, {token!r}))
        profile.commit()
        break
    except Exception:
        if profile is not None:
            try:
                profile.rollback()
            except Exception:
                pass
        time.sleep(5)
    finally:
        if profile is not None:
            profile.close()
'''


def arm_fail_closed_watchdog(start_id, delay_seconds):
    token = secrets.token_hex(16)
    subprocess.Popen(
        [sys.executable, "-c", watchdog_program(token, start_id, delay_seconds)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    return token


def disable_profile(request_close_after=None):
    with profile_connection() as db:
        db.execute(
            "UPDATE trading_profiles SET is_active=0,updated_at=? WHERE id=?",
            (int(time.time() * 1000), PROFILE_ID),
        )
    if request_close_after is not None:
        with trade_connection() as db:
            db.execute(
                "UPDATE trades SET manual_close_requested=1 WHERE id>? AND status NOT IN ('CLOSED','CANCELLED')",
                (request_close_after,),
            )


def stop_and_drain(start_id, wait_seconds=420):
    disable_profile(start_id)
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        trades = active_canary_trades(start_id)
        if all(t["status"] in ("CLOSED", "CANCELLED") for t in trades):
            return True
        time.sleep(2)
    with profile_connection() as db:
        db.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) "
            "ON CONFLICT(key) DO UPDATE SET value='1'"
        )
    return False


def active_canary_trades(start_id):
    with trade_connection() as db:
        return [
            dict(row)
            for row in db.execute(
                "SELECT t.id,t.pair,t.exchange_a,t.exchange_b,t.status,t.accounting_status,"
                "t.entry_time,t.exit_time,t.pnl,t.gross_pnl,t.commission_cost,t.funding_cost,"
                "t.close_reason,t.current_net_estimate,t.valuation_ms,t.opened_qty_a,t.opened_qty_b,"
                "EXISTS(SELECT 1 FROM execution_orders o WHERE o.trade_id=t.id "
                "AND COALESCE(CAST(json_extract(o.observation,'$.filled_quantity') AS REAL),0)>0) has_fill "
                "FROM trades t WHERE t.id>? ORDER BY t.id",
                (start_id,),
            )
        ]


def filled_canary_trades(trades):
    """Return attempts with exchange-confirmed exposure, excluding zero-fill IOC rows."""
    return [trade for trade in trades if bool(trade.get("has_fill"))]


def wait_for_readiness(config, report):
    expected = config["expected_build_sha"]
    deadline = time.monotonic() + 300
    consecutive = 0
    while time.monotonic() < deadline:
        current = health()
        report["preflight_health"] = current
        if current.get("build_sha") != expected:
            raise RuntimeError(f"build mismatch: {current.get('build_sha')} != {expected}")
        consecutive = consecutive + 1 if current.get("trading_ready") else 0
        if consecutive >= 5:
            report["readiness_consecutive_samples"] = consecutive
            return
        time.sleep(1)
    raise RuntimeError(
        "core was not trading_ready for five consecutive samples within 300 seconds"
    )


def preflight(config):
    current_health = health()
    expected = config["expected_build_sha"]
    if current_health.get("build_sha") != expected:
        raise RuntimeError(f"build mismatch: {current_health.get('build_sha')} != {expected}")
    # With the profile deliberately inactive, dynamic public subscriptions have
    # no wanted symbols after a process restart.  Requiring market_ready here
    # would make a safely halted canary impossible to start.  Full market and
    # trading readiness is still required for five consecutive samples by
    # wait_for_readiness() after the one-pair profile is enabled.
    if not all(current_health.get(key) for key in (
        "process_healthy", "private_ready"
    )):
        raise RuntimeError("core process or private dependencies are not healthy")
    if current_health.get("entries_halted") is not True:
        raise RuntimeError("canary must start from a persisted halted state")
    with trade_connection() as db:
        active = db.execute(
            "SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED')"
        ).fetchone()[0]
        if active:
            raise RuntimeError(f"existing active trades: {active}")
        start_id = db.execute("SELECT COALESCE(MAX(id),0) FROM trades").fetchone()[0]
        if db.execute(
            "SELECT COUNT(*) FROM execution_orders WHERE json_extract(observation,'$.state') NOT IN ('Filled','Cancelled','Rejected')"
        ).fetchone()[0]:
            raise RuntimeError("non-terminal execution orders exist")
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("trade database quick_check failed")
    with profile_connection() as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(trading_profiles)")}
        required = {
            "hard_stop_percent",
            "trailing_activation_percent",
            "daily_loss_limit_usdt",
            "max_total_exposure_usdt",
        }
        if not required.issubset(columns):
            raise RuntimeError("risk-control schema is incomplete")
        row = db.execute(
            "SELECT is_active,pending_commands FROM trading_profiles WHERE id=?",
            (PROFILE_ID,),
        ).fetchone()
        if row is None or row[1] is not None:
            raise RuntimeError("profile must exist without pending commands")
        kill = db.execute("SELECT value FROM server_cache WHERE key='kill_switch_active'").fetchone()
        if not kill or kill[0] != "1":
            raise RuntimeError("persisted kill switch must be active before canary")
        cached = db.execute("SELECT value FROM server_cache WHERE key='exchange_maps'").fetchone()
        if not cached:
            raise RuntimeError("missing private account cache")
        accounts = json.loads(cached[0])
        margin_row = db.execute(
            "SELECT value FROM technical_settings WHERE key='margin_check_multiplier'"
        ).fetchone()
        margin_multiplier = max(1.0, float(margin_row[0]) if margin_row else 1.5)
        now = int(time.time() * 1000)
        balance_stamps = accounts.get("balances_ts_by_exchange") or {}
        position_stamps = accounts.get("positions_ts_by_exchange") or {}
        for venue in MANAGED_VENUES:
            if now - int(balance_stamps.get(venue, 0)) > 60_000 or now - int(
                position_stamps.get(venue, 0)
            ) > 60_000:
                raise RuntimeError(f"stale private snapshot for {venue}")
            if accounts.get("positions", {}).get(venue):
                raise RuntimeError(f"existing exchange position on {venue}")
            required_margin = required_canary_margin(config, margin_multiplier)
            available = float(accounts.get("margin_balances", {}).get(venue, 0))
            if available < required_margin:
                raise RuntimeError(f"insufficient {venue} margin: {available:.4f} < {required_margin:.4f}")
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("profile database quick_check failed")
    return start_id, accounts


def configure_and_enable(config, watchdog_token):
    now = int(time.time() * 1000)
    with profile_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        updated = db.execute(
            "UPDATE trading_profiles SET is_active=1,trade_volume=?,leverage=?,max_active_trades=?,"
            "min_volume_millions=?,"
            "min_entry_spread=?,max_entry_spread=?,take_profit_percent=?,trailing_stop_percent=?,"
            "hard_stop_percent=?,trailing_activation_percent=?,daily_loss_limit_usdt=?,"
            "max_total_exposure_usdt=?,slippage_percent=?,min_structural_deviation=?,"
            "min_depth_percent=?,max_holding_time_ms=?,disabled_exchanges=?,updated_at=? WHERE id=?",
            (
                config["notional_usdt"],
                config["leverage"],
                configured_active_trade_limit(config),
                config["min_volume_millions"],
                config["min_entry_spread"],
                config["max_entry_spread"],
                config["take_profit_percent"],
                config["trailing_stop_percent"],
                config["hard_stop_percent"],
                config["trailing_activation_percent"], config["daily_loss_limit_usdt"],
                config["max_total_exposure_usdt"], config["slippage_percent"],
                config["min_structural_deviation"], config["min_depth_percent"],
                config["max_holding_time_ms"], json.dumps(DISABLED), now, PROFILE_ID,
            ),
        ).rowcount
        if updated != 1:
            raise RuntimeError("canary configuration did not update exactly one profile")
        db.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES(?,?,0) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (WATCHDOG_CACHE_KEY, watchdog_token),
        )
        db.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','0',0) "
            "ON CONFLICT(key) DO UPDATE SET value='0'"
        )
        db.commit()


def collect_result(start_id):
    trades = active_canary_trades(start_id)
    with trade_connection() as db:
        events = [
            dict(row)
            for row in db.execute(
                "SELECT trade_id,event,at_ms,detail FROM execution_events WHERE trade_id>? ORDER BY at_ms,id",
                (start_id,),
            )
        ]
        order_states = [
            dict(row)
            for row in db.execute(
                "SELECT trade_id,leg,exchange,json_extract(observation,'$.state') state,"
                "json_extract(observation,'$.filled_quantity') filled_quantity,"
                "json_extract(observation,'$.average_price') average_price,json_extract(observation,'$.fee') fee "
                "FROM execution_orders WHERE trade_id>? ORDER BY created_ms,client_id",
                (start_id,),
            )
        ]
    with profile_connection() as db:
        cache = db.execute("SELECT value FROM server_cache WHERE key='exchange_maps'").fetchone()
        accounts = json.loads(cache[0]) if cache else {}
        profile = dict(db.execute(
            "SELECT is_active,trade_volume,leverage,max_active_trades,min_entry_spread,take_profit_percent,"
            "hard_stop_percent,daily_loss_limit_usdt,max_total_exposure_usdt,max_holding_time_ms,min_volume_millions "
            "FROM trading_profiles WHERE id=?", (PROFILE_ID,)
        ).fetchone())
    return {
        "trades": trades,
        "execution_events": events,
        "orders": order_states,
        "balances": accounts.get("balances"),
        "margin_balances": accounts.get("margin_balances"),
        "profile": profile,
        "health": health(),
    }


def main():
    signal.signal(signal.SIGALRM, abort_on_signal)
    signal.signal(signal.SIGTERM, abort_on_signal)
    signal.signal(signal.SIGHUP, abort_on_signal)
    config = json.load(sys.stdin)
    if config.get("enabled") is not True or config.get("notional_usdt") != 6.0:
        raise RuntimeError("explicit enabled=true and notional_usdt=6.0 are required")
    leverage = config.get("leverage")
    if isinstance(leverage, bool) or not isinstance(leverage, int) or not 1 <= leverage <= 10:
        raise RuntimeError("canary leverage must be an integer in 1..10")
    min_volume_millions = float(config.get("min_volume_millions", 0))
    if not 0.5 <= min_volume_millions <= 3.0:
        raise RuntimeError("min_volume_millions outside 0.5..3.0")
    monitor_seconds = configured_monitor_seconds(config)
    max_filled_trades = configured_filled_trade_limit(config)
    max_active_trades = configured_active_trade_limit(config)
    if max_filled_trades < max_active_trades:
        raise RuntimeError("max_filled_trades must be at least max_active_trades")
    required_exposure = float(config["notional_usdt"]) * max_active_trades
    if float(config["max_total_exposure_usdt"]) < required_exposure:
        raise RuntimeError(
            "max_total_exposure_usdt is below simultaneous canary notional"
        )
    # Leave enough time for a trade first seen at the end of the observation
    # window to drain through the existing 420-second fail-closed close path.
    signal.alarm(monitor_seconds + CANARY_SHUTDOWN_GRACE_SECONDS)
    start_id = None
    profile_before = None
    watchdog_token = None
    report = {"ok": False, "started_ms": int(time.time() * 1000), "config": config}
    try:
        profile_before = snapshot_profile()
        start_id, before_accounts = preflight(config)
        report["balances_before"] = before_accounts.get("balances")
        report["backup"] = backup_databases(str(report["started_ms"]))
        watchdog_token = arm_fail_closed_watchdog(start_id, monitor_seconds + 60)
        configure_and_enable(config, watchdog_token)
        report["watchdog_armed"] = True
        wait_for_readiness(config, report)
        entry_decisions_before = dict(
            report["preflight_health"].get("entry_decisions") or {}
        )
        report["entry_decisions_before"] = entry_decisions_before
        deadline = time.monotonic() + monitor_seconds
        first_seen = False
        dependency_readiness_lost_samples = 0
        market_readiness_lost_samples = 0
        while time.monotonic() < deadline:
            trades = active_canary_trades(start_id)
            filled_trades = filled_canary_trades(trades)
            report["filled_trades_seen"] = len(filled_trades)
            unfilled_attempts = len(trades) - len(filled_trades)
            if unfilled_attempts:
                report["unfilled_attempts_seen"] = unfilled_attempts
            if len(filled_trades) > max_filled_trades:
                disable_profile(start_id)
                raise RuntimeError("canary exceeded its filled-trade limit")
            if filled_trades and not first_seen:
                first_seen = True
                report["first_trade_seen_ms"] = int(time.time() * 1000)
            target_reached = len(filled_trades) >= max_filled_trades
            if target_reached:
                # No additional entries after the requested batch size. Existing
                # exposure remains owned by the engine and drains below.
                disable_profile()
            if (
                target_reached
                and all(
                    trade["status"] in ("CLOSED", "CANCELLED")
                    and trade["accounting_status"] == "VERIFIED"
                    for trade in filled_trades
                )
            ):
                report["outcome"] = (
                    "verified_trade"
                    if max_filled_trades == 1
                    else "verified_trade_batch"
                )
                break
            current_health = health()
            observe_health(report, current_health)
            if current_health.get("entries_halted"):
                disable_profile(start_id)
                report["outcome"] = "kill_switch"
                break
            # Process and private account dependencies are always critical.
            dependencies_ready = (
                current_health.get("process_healthy") is True
                and current_health.get("private_ready") is True
            )
            if not dependencies_ready:
                dependency_readiness_lost_samples += 1
                if (
                    dependency_readiness_lost_samples
                    >= DEPENDENCY_READINESS_LOSS_LIMIT
                ):
                    disable_profile(start_id)
                    report["outcome"] = "readiness_lost"
                    report["readiness_component"] = "dependencies"
                    report["readiness_lost_samples"] = (
                        dependency_readiness_lost_samples
                    )
                    break
            else:
                dependency_readiness_lost_samples = 0

            # Market-data loss is recoverable and cannot create an entry: the
            # engine requires trading_ready and independently rejects books
            # older than 750 ms. Keep the bounded observer alive so a Binance
            # reconnect does not permanently end the canary. Process/private
            # dependency loss above remains fail-closed.
            market_readiness_lost_samples = observe_market_readiness(
                report,
                market_readiness_lost_samples,
                target_reached or current_health.get("market_ready") is True,
            )
            time.sleep(1)
        else:
            report["outcome"] = "monitor_timeout" if first_seen else "no_trade"

        if first_seen:
            if not stop_and_drain(start_id):
                raise RuntimeError(
                    "canary exposure did not close within 420 seconds; kill switch persisted"
                )
        else:
            disable_profile()
        report.update(collect_result(start_id))
        report["entry_decision_delta"] = counter_delta(
            entry_decisions_before,
            report["health"].get("entry_decisions") or {},
        )
        report["ok"] = True
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        if start_id is not None:
            try:
                report["cleanup_drained"] = stop_and_drain(start_id)
                report.update(collect_result(start_id))
            except Exception as cleanup_exc:
                report["cleanup_error"] = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
    finally:
        if profile_before is not None:
            try:
                restore_profile_halted(profile_before, watchdog_token)
                report["profile_restored_inactive"] = True
                report["kill_switch_restored"] = True
            except Exception as restore_exc:
                report["profile_restored_inactive"] = False
                report["kill_switch_restored"] = False
                report["profile_restore_error"] = (
                    f"{type(restore_exc).__name__}: {restore_exc}"
                )
                report["ok"] = False
        signal.alarm(0)
        report["finished_ms"] = int(time.time() * 1000)
        print(json.dumps(report, indent=2, allow_nan=False))
    if not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
