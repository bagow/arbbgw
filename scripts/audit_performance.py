#!/usr/bin/env python3
"""Read a consistent SQLite ledger snapshot without credentials or order transport."""
import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sqlite3


def summarize(rows):
    states = Counter()
    excluded = Counter()
    outcomes = []
    for row in rows:
        states[row["status"]] += 1
        if row["status"] != "CLOSED":
            continue
        if row["accounting_status"] != "VERIFIED":
            excluded[row["accounting_status"] or "UNKNOWN"] += 1
            continue
        value = row["pnl"]
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            excluded["INVALID_VERIFIED_PNL"] += 1
            continue
        if row["exit_time"] is None:
            excluded["MISSING_EXIT_TIME"] += 1
            continue
        outcomes.append((row["exit_time"], row["id"], value))
    outcomes.sort()
    values = [v for _, _, v in outcomes]
    wins = sum(v > 0 for v in values)
    losses = sum(v < 0 for v in values)
    gain = math.fsum(v for v in values if v > 0)
    loss = -math.fsum(v for v in values if v < 0)
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "scope": "Closed VERIFIED trades only; no forecast of future returns",
        "status_counts": dict(states),
        "excluded_closed": dict(excluded),
        "active_count": sum(n for s, n in states.items() if s not in ("CLOSED", "CANCELLED")),
        "verified_closed_count": len(values),
        "wins": wins,
        "losses": losses,
        "breakeven": len(values) - wins - losses,
        "win_rate_percent": wins / len(values) * 100 if values else None,
        "net_pnl_usdt": math.fsum(values) if values else None,
        "average_net_pnl_usdt": math.fsum(values) / len(values) if values else None,
        "profit_factor": gain / loss if loss else None,
        "realized_pnl_max_drawdown_usdt": drawdown if values else None,
        "limitations": [
            "Drawdown excludes unrealized exposure, deposits and withdrawals.",
            "Null profit factor means no observed losses, not proven profitability.",
            "VERIFIED reflects the ledger label; this tool does not recheck exchange evidence.",
        ],
    }


def audit(path, profile=None):
    # mode=ro refuses nonexistent files and forbids writes; do not use immutable
    # because a live database may contain committed data in its WAL.
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        sql = "SELECT id,status,accounting_status,pnl,exit_time FROM trades"
        if profile is not None:
            rows = db.execute(sql + " WHERE profile_id=?", (profile,)).fetchall()
        else:
            rows = db.execute(sql).fetchall()
        return summarize(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--profile")
    args = parser.parse_args()
    try:
        print(json.dumps(audit(args.database, args.profile), indent=2, allow_nan=False))
    except (sqlite3.Error, ValueError) as exc:
        parser.exit(1, f"Ledger audit failed: {exc}\n")
