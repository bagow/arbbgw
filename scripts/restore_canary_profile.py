#!/usr/bin/env python3
"""Restore one production profile from a pre-canary SQLite backup safely."""
import json
import sqlite3
import sys
import time
from pathlib import Path


ROOT = Path("/home/opc/core-node")
PROFILE_ID = "real-default"
SAFE_LABEL_CHARS = set("0123456789")
REPORT_FIELDS = (
    "id",
    "is_active",
    "trade_volume",
    "leverage",
    "min_entry_spread",
    "max_entry_spread",
    "take_profit_percent",
    "trailing_stop_percent",
    "max_active_trades",
    "min_volume_millions",
    "min_structural_deviation",
    "min_depth_percent",
    "slippage_percent",
    "daily_loss_limit_usdt",
    "max_total_exposure_usdt",
    "max_holding_time_ms",
    "disabled_exchanges",
)


def connect(path, read_only=False):
    if read_only:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    else:
        db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


def report_view(row):
    return {name: row[name] for name in REPORT_FIELDS if name in row.keys()}


def main():
    request = json.load(sys.stdin)
    label = str(request.get("backup_label", ""))
    if request.get("enabled") is not True or not label or not set(label) <= SAFE_LABEL_CHARS:
        raise RuntimeError("explicit enabled=true and a numeric backup_label are required")

    backup = ROOT / "canary-backups" / label / "vps-api.db"
    current = ROOT / "vps-api.db"
    trades = ROOT / "real-trader.db"
    if not backup.is_file() or not current.is_file() or not trades.is_file():
        raise RuntimeError("current or backup database is missing")

    with connect(trades, read_only=True) as trade_db:
        active = trade_db.execute(
            "SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED')"
        ).fetchone()[0]
        if active:
            raise RuntimeError(f"refusing profile restore with {active} active trades")

    with connect(backup, read_only=True) as source:
        if source.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("backup database quick_check failed")
        source_row = source.execute(
            "SELECT * FROM trading_profiles WHERE id=?", (PROFILE_ID,)
        ).fetchone()
        if source_row is None:
            raise RuntimeError("profile is missing from canary backup")
        source_columns = set(source_row.keys())

    with connect(current) as target:
        target.execute("BEGIN IMMEDIATE")
        if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("current database quick_check failed")
        current_row = target.execute(
            "SELECT * FROM trading_profiles WHERE id=?", (PROFILE_ID,)
        ).fetchone()
        if current_row is None:
            raise RuntimeError("current profile is missing")
        current_columns = set(current_row.keys())
        columns = sorted(
            (source_columns & current_columns) - {"id", "is_active", "updated_at"}
        )
        assignments = ",".join(f'"{name}"=?' for name in columns)
        values = [source_row[name] for name in columns]
        updated = target.execute(
            f"UPDATE trading_profiles SET {assignments},is_active=?,updated_at=? WHERE id=?",
            (
                *values,
                current_row["is_active"],
                int(time.time() * 1000),
                PROFILE_ID,
            ),
        ).rowcount
        if updated != 1:
            raise RuntimeError("profile restore did not update exactly one row")
        target.commit()
        restored = target.execute(
            "SELECT * FROM trading_profiles WHERE id=?", (PROFILE_ID,)
        ).fetchone()

    print(
        json.dumps(
            {
                "ok": True,
                "backup_label": label,
                "before": report_view(current_row),
                "after": report_view(restored),
                "preserved_active_state": bool(restored["is_active"]),
            },
            indent=2,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()
