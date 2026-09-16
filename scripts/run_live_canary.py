#!/usr/bin/env python3
"""Guarded one-trade production canary. Never prints or reads exchange secrets."""
import json
import signal
import sqlite3
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


def restore_profile(snapshot):
    columns = [name for name in snapshot if name != "id"]
    assignments = ",".join(f'"{name}"=?' for name in columns)
    values = [snapshot[name] for name in columns]
    with profile_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        updated = db.execute(
            f"UPDATE trading_profiles SET {assignments} WHERE id=?",
            (*values, snapshot["id"]),
        ).rowcount
        if updated != 1:
            raise RuntimeError("canary profile restore did not update exactly one row")
        db.commit()


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
                "SELECT id,pair,exchange_a,exchange_b,status,accounting_status,entry_time,exit_time,pnl,gross_pnl,commission_cost,funding_cost,close_reason,current_net_estimate,valuation_ms "
                "FROM trades WHERE id>? ORDER BY id",
                (start_id,),
            )
        ]


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
        if row is None or row[0] or row[1] is not None:
            raise RuntimeError("profile must be inactive without pending commands")
        kill = db.execute("SELECT value FROM server_cache WHERE key='kill_switch_active'").fetchone()
        if kill and kill[0] == "1":
            raise RuntimeError("kill switch is active")
        cached = db.execute("SELECT value FROM server_cache WHERE key='exchange_maps'").fetchone()
        if not cached:
            raise RuntimeError("missing private account cache")
        accounts = json.loads(cached[0])
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
            required_margin = (
                config["notional_usdt"] * 1.2
                + config["notional_usdt"] * 0.002
            )
            available = float(accounts.get("margin_balances", {}).get(venue, 0))
            if available < required_margin:
                raise RuntimeError(f"insufficient {venue} margin: {available:.4f} < {required_margin:.4f}")
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("profile database quick_check failed")
    return start_id, accounts


def configure_and_enable(config):
    now = int(time.time() * 1000)
    with profile_connection() as db:
        db.execute(
            "UPDATE trading_profiles SET is_active=1,trade_volume=?,leverage=1,max_active_trades=1,"
            "min_entry_spread=?,max_entry_spread=?,take_profit_percent=?,trailing_stop_percent=?,"
            "hard_stop_percent=?,trailing_activation_percent=?,daily_loss_limit_usdt=?,"
            "max_total_exposure_usdt=?,slippage_percent=?,min_structural_deviation=?,"
            "min_depth_percent=?,max_holding_time_ms=?,disabled_exchanges=?,updated_at=? WHERE id=?",
            (
                config["notional_usdt"],
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
        )


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
            "hard_stop_percent,daily_loss_limit_usdt,max_total_exposure_usdt,max_holding_time_ms "
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
    signal.alarm(3150)
    config = json.load(sys.stdin)
    if config.get("enabled") is not True or config.get("notional_usdt") != 6.0:
        raise RuntimeError("explicit enabled=true and notional_usdt=6.0 are required")
    monitor_seconds = int(config.get("monitor_seconds", 1800))
    if not 60 <= monitor_seconds <= 2700:
        raise RuntimeError("monitor_seconds outside 60..2700")
    start_id = None
    profile_before = None
    report = {"ok": False, "started_ms": int(time.time() * 1000), "config": config}
    try:
        wait_for_readiness(config, report)
        start_id, before_accounts = preflight(config)
        report["balances_before"] = before_accounts.get("balances")
        profile_before = snapshot_profile()
        report["backup"] = backup_databases(str(report["started_ms"]))
        configure_and_enable(config)
        deadline = time.monotonic() + monitor_seconds
        first_seen = False
        while time.monotonic() < deadline:
            trades = active_canary_trades(start_id)
            if len(trades) > 1:
                disable_profile(start_id)
                raise RuntimeError("more than one canary trade was created")
            if trades and not first_seen:
                first_seen = True
                disable_profile()
                report["first_trade_seen_ms"] = int(time.time() * 1000)
            if (
                trades
                and trades[0]["status"] in ("CLOSED", "CANCELLED")
                and trades[0]["accounting_status"] == "VERIFIED"
            ):
                report["outcome"] = "verified_trade"
                break
            if health().get("entries_halted"):
                disable_profile(start_id)
                report["outcome"] = "kill_switch"
                break
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
                restore_profile(profile_before)
                report["profile_restored"] = True
            except Exception as restore_exc:
                report["profile_restored"] = False
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
