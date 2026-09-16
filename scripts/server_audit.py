#!/usr/bin/env python3
"""Read-only production diagnostics. Never reads secrets or sends exchange orders."""
import hashlib
import json
from pathlib import Path
import shutil
import signal
import sys
import time
import sqlite3
import subprocess
import urllib.error
import urllib.request

from audit_performance import summarize
from audit_shadow import summarize_shadow


def inspect_database(path, kind):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        deadline = time.monotonic() + 15
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        result = {}
        try:
            result["quick_check"] = [r[0] for r in db.execute("PRAGMA quick_check")]
        except sqlite3.OperationalError as exc:
            result["quick_check_error"] = str(exc)
        deadline = time.monotonic() + 15
        if kind == "trades":
            rows = db.execute("SELECT id,status,accounting_status,pnl,exit_time FROM trades").fetchall()
            result["performance"] = summarize(rows)
            result["recent_trades"] = [dict(r) for r in db.execute(
                "SELECT id,pair,exchange_a,exchange_b,status,accounting_status,entry_time,exit_time,"
                "pnl,gross_pnl,commission_cost,funding_cost,close_reason FROM trades ORDER BY id DESC LIMIT 30"
            )]
            result["active_trades"] = [dict(r) for r in db.execute(
                "SELECT id,pair,status,entry_time,opened_qty_a,opened_qty_b,closed_qty_a,closed_qty_b "
                "FROM trades WHERE status NOT IN ('CLOSED','CANCELLED')"
            )]
            result["order_states"] = [dict(r) for r in db.execute(
                "SELECT json_extract(observation,'$.state') AS state,COUNT(*) AS count "
                "FROM execution_orders GROUP BY state"
            )]
        elif kind == "profiles":
            result["profiles"] = [dict(r) for r in db.execute(
                "SELECT id,is_active,trade_volume,leverage,min_entry_spread,take_profit_percent,"
                "trailing_stop_percent,max_active_trades,max_holding_time_ms,disabled_exchanges "
                "FROM trading_profiles"
            )]
            kill = db.execute("SELECT value FROM server_cache WHERE key='kill_switch_active'").fetchone()
            result["kill_switch"] = kill[0] if kill else None
            cached = db.execute("SELECT value FROM server_cache WHERE key='exchange_maps'").fetchone()
            if cached:
                value = json.loads(cached[0])
                result["cached_accounts"] = {k: value.get(k) for k in (
                    "ts", "balances_ts", "positions_ts", "balances_ts_by_exchange",
                    "positions_ts_by_exchange", "fees_ts_by_symbol", "balances",
                    "margin_balances", "positions"
                )}
        elif kind == "shadow":
            result["scope"] = "Latest 10000 recorded snapshots, not complete history"
            result["quality"] = summarize_shadow(
                json.loads(r[0]) for r in db.execute(
                    "SELECT snapshot FROM book_samples ORDER BY id DESC LIMIT 10000")
            )
        return result


def main():
    # The remote process must stop even if its SSH client disappears.
    signal.signal(signal.SIGALRM, lambda *_: sys.exit("AUDIT: overall deadline exceeded"))
    signal.alarm(100)
    result = {}
    print("AUDIT: service logs", file=sys.stderr, flush=True)
    logs = subprocess.run(["journalctl", "-u", "core-node", "--since=-30min", "-n", "5000", "--no-pager"],
                          capture_output=True, text=True, timeout=15)
    lines = logs.stdout.splitlines()
    result["recent_log_counts"] = {label: sum(pattern in line for line in lines) for label, pattern in (
        ("binance_subscription_rejected", "[binance_l2_chunk] Subscription rejected"),
        ("binance_receive_failed", "[binance_l2_chunk] Receive failed"),
        ("binance_connection_failed", "[binance_l2_chunk] Connect failed"),
        ("binance_banned", "[binance] IP still banned"),
        ("engine_panics", "ENGINE LOOP PANIC"),
        ("database_locked", "database is locked"),
    )}
    result["recent_log_count"] = len(lines)
    # Only transport errors from the unauthenticated public market-data stream;
    # never export private order/account logs or service environments.
    prefix = "[binance_l2_chunk] Receive failed: "
    result["binance_public_transport_errors"] = list(dict.fromkeys(
        line.split(prefix, 1)[1][:400] for line in lines if prefix in line
    ))[:10]
    for service in ("core-node", "vps-api", "cloudflared"):
        proc = subprocess.run([
            "systemctl", "show", service, "--property=ActiveState,SubState,MainPID,ActiveEnterTimestamp,NRestarts"
        ], capture_output=True, text=True, timeout=10)
        result[service] = proc.stdout.strip()
    for binary in ("core-node", "vps-api"):
        path = Path("/usr/local/bin") / binary
        if path.is_file():
            result[binary + "_binary"] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "mtime": path.stat().st_mtime,
            }
    try:
        try:
            response = urllib.request.urlopen("http://127.0.0.1:3003/health", timeout=5)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            result["health_http_status"] = response.status
            result["health"] = json.load(response)
    except Exception as exc:
        result["health_error"] = type(exc).__name__
    root = Path("/home/opc/core-node")
    for filename, kind in (("real-trader.db", "trades"), ("vps-api.db", "profiles"), ("shadow-books.db", "shadow")):
        print("AUDIT: database " + kind, file=sys.stderr, flush=True)
        try:
            result[filename] = inspect_database(root / filename, kind)
        except Exception as exc:
            result[filename] = {"error": str(exc)}
    disk = shutil.disk_usage(root if root.exists() else "/")
    result["disk_free_bytes"] = disk.free
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
