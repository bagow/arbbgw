#!/usr/bin/env python3
"""Read-only production diagnostics. Never reads secrets or sends exchange orders."""
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import urllib.request

from audit_performance import summarize


def inspect_database(path, kind):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        result = {"quick_check": [r[0] for r in db.execute("PRAGMA quick_check")]}
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
                    "ts", "balances_ts", "positions_ts", "balances", "margin_balances", "positions"
                )}
        elif kind == "shadow":
            result["samples"] = dict(db.execute(
                "SELECT COUNT(*) AS count,MIN(json_extract(snapshot,'$.recorded_ms')) AS first_ms,"
                "MAX(json_extract(snapshot,'$.recorded_ms')) AS last_ms FROM book_samples"
            ).fetchone())
        return result


def main():
    result = {}
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
        with urllib.request.urlopen("http://127.0.0.1:3003/health", timeout=5) as response:
            result["health"] = json.load(response)
    except Exception as exc:
        result["health_error"] = type(exc).__name__
    root = Path("/home/opc/core-node")
    for filename, kind in (("real-trader.db", "trades"), ("vps-api.db", "profiles"), ("shadow-books.db", "shadow")):
        try:
            result[filename] = inspect_database(root / filename, kind)
        except Exception as exc:
            result[filename] = {"error": str(exc)}
    disk = shutil.disk_usage(root if root.exists() else "/")
    result["disk_free_bytes"] = disk.free
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
