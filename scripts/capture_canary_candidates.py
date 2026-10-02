#!/usr/bin/env python3
"""Read-only, one-second candidate telemetry for a running live canary."""

import argparse
import json
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request


EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT = 0.02


def read_cache(db_path: Path, key: str):
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=2) as db:
        row = db.execute("SELECT value FROM server_cache WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def telemetry_config(profile, settings, canary=None):
    """Build the observer thresholds from the immutable canary contract.

    The live runner applies its profile changes after it starts.  Reading only
    the database therefore races with activation and can classify an entire
    run using the profile which existed before the canary.  The run config is
    the authoritative contract when it is available; the database remains a
    backwards-compatible fallback for standalone observers.
    """
    canary = canary or {}

    def configured(run_key, profile_key):
        return canary.get(run_key, profile[profile_key])

    return {
        "trade_volume": float(configured("notional_usdt", "trade_volume")),
        "min_entry_spread": float(
            configured("min_entry_spread", "min_entry_spread")
        ),
        "max_entry_spread": float(
            configured("max_entry_spread", "max_entry_spread")
        ),
        "min_volume": float(
            configured("min_volume_millions", "min_volume_millions")
        )
        * 1_000_000.0,
        "min_structural_deviation": float(
            configured("min_structural_deviation", "min_structural_deviation")
        ),
        "min_depth_percent": float(
            configured("min_depth_percent", "min_depth_percent")
        ),
        "intra_spread_max_pct": float(
            settings.get("intra_spread_max_pct", "0.2")
        ),
        "default_fee_rate": float(settings.get("default_fee_rate", "0.0005")),
        "config_source": "run_config" if canary else "database_profile",
    }


def load_profile(root: Path, run_dir=None):
    uri = (root / "vps-api.db").resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=2) as db:
        db.row_factory = sqlite3.Row
        profile = db.execute(
            "SELECT trade_volume,min_entry_spread,max_entry_spread,min_volume_millions,"
            "min_structural_deviation,min_depth_percent "
            "FROM trading_profiles WHERE id='real-default'"
        ).fetchone()
        settings = dict(
            db.execute(
                "SELECT key,value FROM technical_settings WHERE key IN "
                "('intra_spread_max_pct','default_fee_rate')"
            )
        )
    if profile is None:
        raise RuntimeError("real-default profile is missing")
    canary = None
    if run_dir is not None:
        config_path = run_dir / "config.json"
        if config_path.is_file():
            canary = json.loads(config_path.read_text())
    return telemetry_config(dict(profile), settings, canary)


def classify(row, fees, config):
    bid_a = float(row.get("bidA") or 0.0)
    ask_a = float(row.get("askA") or 0.0)
    bid_b = float(row.get("bidB") or 0.0)
    ask_b = float(row.get("askB") or 0.0)
    valid_quotes = min(bid_a, ask_a, bid_b, ask_b) > 0.0
    intra_a = (ask_a - bid_a) / bid_a * 100.0 if valid_quotes else None
    intra_b = (ask_b - bid_b) / bid_b * 100.0 if valid_quotes else None
    spread = float(row.get("spread") or 0.0)
    pair = str(row.get("pair") or "")
    exchange_a = str(row.get("exchangeA") or "")
    exchange_b = str(row.get("exchangeB") or "")
    fee_a = float(fees.get(f"{exchange_a}:{pair}", config["default_fee_rate"]))
    fee_b = float(fees.get(f"{exchange_b}:{pair}", config["default_fee_rate"]))
    break_even = (
        2.0 * (fee_a + fee_b) * 100.0
        + 4.0 * EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT
    )
    structural = max(0.0, float(row.get("structural_spread") or 0.0))
    net_excursion = spread - structural - break_even
    stable = (
        bool(row.get("structural_verified"))
        and bool(row.get("has_structural_spread"))
        and float(row.get("structural_max") or 0.0)
        >= float(row.get("structural_min") or 0.0)
    )
    spread_gate = config["min_entry_spread"] <= spread <= config["max_entry_spread"]
    volume_gate = (
        float(row.get("volA") or 0.0) >= config["min_volume"]
        and float(row.get("volB") or 0.0) >= config["min_volume"]
    )
    intra_gate = (
        valid_quotes
        and intra_a <= config["intra_spread_max_pct"]
        and intra_b <= config["intra_spread_max_pct"]
    )
    deviation_gate = net_excursion >= config["min_structural_deviation"]
    source_ms_a = int(row.get("sourceMsA") or 0)
    source_ms_b = int(row.get("sourceMsB") or 0)
    source_skew_ms = (
        abs(source_ms_a - source_ms_b)
        if source_ms_a > 0 and source_ms_b > 0
        else None
    )
    is_a_higher = str(row.get("direction") or "").startswith(exchange_a)
    sell_top_usd = float(
        row.get("bidSizeAUsd" if is_a_higher else "bidSizeBUsd") or 0.0
    )
    buy_top_usd = float(
        row.get("askSizeBUsd" if is_a_higher else "askSizeAUsd") or 0.0
    )
    required_top_depth_usd = (
        float(config.get("trade_volume", 6.0))
        * float(config.get("min_depth_percent", 500.0))
        / 100.0
    )
    top_depth_usd = min(sell_top_usd, buy_top_usd)
    result = dict(row)
    result.update(
        {
            "fee_a": fee_a,
            "fee_b": fee_b,
            "break_even_pct": break_even,
            "intra_a_pct": intra_a,
            "intra_b_pct": intra_b,
            "net_excursion_pct": net_excursion,
            "gap_to_entry_pct": net_excursion - config["min_structural_deviation"],
            "spread_gate": spread_gate,
            "volume_gate": volume_gate,
            "intra_gate": intra_gate,
            "stable_regime": stable,
            "deviation_gate": deviation_gate,
            "source_skew_ms": source_skew_ms,
            "sell_top_usd": sell_top_usd,
            "buy_top_usd": buy_top_usd,
            "top_depth_usd": top_depth_usd,
            "required_top_depth_usd": required_top_depth_usd,
            "top_depth_gate": top_depth_usd >= required_top_depth_usd,
            "l1_candidate": spread_gate
            and volume_gate
            and intra_gate
            and stable
            and deviation_gate,
        }
    )
    return result


def health():
    try:
        with urllib.request.urlopen(
            "http://127.0.0.1:3003/health", timeout=3
        ) as response:
            status = getattr(response, "status", 200)
            value = json.load(response)
    except urllib.error.HTTPError as error:
        # The core returns its most useful readiness diagnosis in the JSON body
        # of a fail-closed 503. Preserve that body instead of reducing a market
        # or private-feed outage to the uninformative string "HTTPError".
        status = error.code
        try:
            value = json.load(error)
        except Exception as body_error:
            return {
                "error": type(error).__name__,
                "detail": str(error)[:300],
                "http_status": status,
                "body_error": type(body_error).__name__,
            }
    except Exception as error:
        return {"error": type(error).__name__, "detail": str(error)[:300]}
    if not isinstance(value, dict):
        return {
            "error": "InvalidHealthBody",
            "detail": f"expected object, received {type(value).__name__}",
            "http_status": status,
        }
    result = {
        key: value.get(key)
        for key in (
            "healthy",
            "process_healthy",
            "market_ready",
            "private_ready",
            "order_ws_ready",
            "trading_ready",
            "entries_halted",
            "supervisor_age_ms",
            "active_trades",
            "entry_decisions",
            "scanner_status",
            "venues",
        )
    }
    result["http_status"] = status
    return result


def emit(value):
    print(json.dumps(value, separators=(",", ":"), allow_nan=False), flush=True)


def supervisor_running(run_dir: Path, proc_root: Path = Path("/proc")):
    """Return None before the supervisor publishes its PID, else liveness."""
    pid_path = run_dir / "runner.pid"
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text().strip())
    except (OSError, ValueError):
        return False
    return pid > 0 and (proc_root / str(pid)).exists()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--max-seconds", type=int, default=21_600)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    root = args.root.resolve()
    run_dir = args.run_dir.resolve()
    config = load_profile(root, run_dir)
    started = time.time()
    last_snapshot = None
    samples = 0
    rows_seen = 0
    top_by_pair = {}
    counts = {
        "spread_gate": 0,
        "volume_gate": 0,
        "intra_gate": 0,
        "stable_regime": 0,
        "deviation_gate": 0,
        "l1_candidate": 0,
    }
    emit({"kind": "start", "started": int(started), "config": config})
    while time.time() - started < args.max_seconds:
        try:
            snapshot = read_cache(root / "vps-api.db", "snapshot") or {}
            accounts = read_cache(root / "vps-api.db", "exchange_maps") or {}
        except Exception as error:
            emit(
                {
                    "kind": "read_error",
                    "ts": int(time.time() * 1000),
                    "error": type(error).__name__,
                    "detail": str(error)[:300],
                }
            )
            time.sleep(max(0.1, args.interval))
            continue
        snapshot_ts = snapshot.get("ts")
        if snapshot_ts != last_snapshot:
            last_snapshot = snapshot_ts
            samples += 1
            classified = [
                classify(row, accounts.get("fees") or {}, config)
                for row in snapshot.get("data") or []
            ]
            rows_seen += len(classified)
            for row in classified:
                for key in counts:
                    counts[key] += int(bool(row[key]))
                pair = row.get("pair") or "?"
                if pair not in top_by_pair or row["gap_to_entry_pct"] > top_by_pair[pair][
                    "gap_to_entry_pct"
                ]:
                    top_by_pair[pair] = row
            emit(
                {
                    "kind": "snapshot",
                    "ts": snapshot_ts,
                    "rows": classified,
                    "health": health(),
                }
            )
        if (run_dir / "result.json").exists():
            stop_reason = "canary_result"
            break
        if supervisor_running(run_dir) is False:
            stop_reason = "canary_supervisor_missing"
            break
        time.sleep(max(0.1, args.interval))
    else:
        stop_reason = "max_seconds"
    top = sorted(
        top_by_pair.values(), key=lambda row: row["gap_to_entry_pct"], reverse=True
    )[:30]
    emit(
        {
            "kind": "summary",
            "stop_reason": stop_reason,
            "elapsed_seconds": round(time.time() - started),
            "samples": samples,
            "rows_seen": rows_seen,
            "gate_counts": counts,
            "top_by_pair": top,
        }
    )


if __name__ == "__main__":
    main()
