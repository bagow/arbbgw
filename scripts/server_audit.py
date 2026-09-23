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
from audit_shadow import summarize_shadow, valid_book


def _float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def sample_scanner_funnel(path, samples=60, delay=0.25):
    """Sample public spread snapshots and replay the visible scanner stages."""
    observations = []
    profile = None
    technical = {}
    last_ts = None
    for index in range(samples):
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            if profile is None:
                row = db.execute(
                    "SELECT id,min_entry_spread,max_entry_spread,min_volume_millions,"
                    "min_structural_deviation,disabled_exchanges FROM trading_profiles "
                    "WHERE is_active=1 ORDER BY id LIMIT 1"
                ).fetchone()
                profile = dict(row) if row else None
                technical = {
                    row["key"]: row["value"]
                    for row in db.execute("SELECT key,value FROM technical_settings")
                }
            cached = db.execute(
                "SELECT value FROM server_cache WHERE key='snapshot'"
            ).fetchone()
        if cached:
            snapshot = json.loads(cached[0])
            stamp = snapshot.get("ts")
            if stamp != last_ts:
                observations.append(snapshot)
                last_ts = stamp
        if index + 1 < samples:
            time.sleep(delay)

    selected_settings = {
        key: technical.get(key)
        for key in (
            "spread_threshold", "spread_max", "stale_secs", "intra_spread_max_pct",
            "vol_tolerance_pct", "min_listing_age_days", "binance_min_notional",
            "margin_check_multiplier", "max_spreads_per_cycle", "default_fee_rate",
            "entry_order_type", "diag_log_interval",
        )
    }
    result = {
        "scope": "Distinct server_cache snapshots observed over 15 seconds; public market data only",
        "profile_id": profile.get("id") if profile else None,
        "technical_settings": selected_settings,
        "distinct_snapshots": len(observations),
        "nonempty_snapshots": sum(bool(item.get("data")) for item in observations),
    }
    if not profile:
        result["error"] = "no active profile"
        return result

    try:
        disabled = set(json.loads(profile.get("disabled_exchanges") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        disabled = set()
    min_entry = _float(profile.get("min_entry_spread"))
    max_entry = _float(profile.get("max_entry_spread"))
    min_volume = _float(profile.get("min_volume_millions")) * 1_000_000.0
    min_structural_deviation = _float(profile.get("min_structural_deviation"))
    default_fee = _float(technical.get("default_fee_rate"), 0.0005)
    # Matches the production helper: four fee legs plus four expected IOC legs at 0.02%.
    estimated_break_even = 4.0 * default_fee * 100.0 + 0.08
    stages = {
        "snapshot_rows": 0,
        "enabled_venues": 0,
        "raw_spread_in_entry_window": 0,
        "both_24h_volumes_above_profile_minimum": 0,
        "configured_structural_filter_passed": 0,
        "positive_after_estimated_break_even": 0,
        "configured_entry_preflight_estimate": 0,
    }
    top = {}
    per_snapshot = []
    for snapshot in observations:
        local_max = None
        local_count = 0
        for row in snapshot.get("data") or []:
            spread = _float(row.get("spread"))
            structural = abs(_float(row.get("structural_spread")))
            adjusted = spread - structural
            local_count += 1
            stages["snapshot_rows"] += 1
            local_max = spread if local_max is None else max(local_max, spread)
            key = "|".join(str(row.get(name, "")) for name in (
                "pair", "exchangeA", "exchangeB", "direction"
            ))
            public = {
                "pair": row.get("pair"),
                "exchange_a": row.get("exchangeA"),
                "exchange_b": row.get("exchangeB"),
                "direction": row.get("direction"),
                "spread_pct": round(spread, 4),
                "structural_spread_pct": round(structural, 4),
                "adjusted_spread_pct": round(adjusted, 4),
                "vol_a_usd": round(_float(row.get("volA")), 2),
                "vol_b_usd": round(_float(row.get("volB")), 2),
                "structural_verified": row.get("structural_verified") is True,
            }
            if key not in top or spread > top[key]["spread_pct"]:
                top[key] = public
            if row.get("exchangeA") in disabled or row.get("exchangeB") in disabled:
                continue
            stages["enabled_venues"] += 1
            if not min_entry <= spread <= max_entry:
                continue
            stages["raw_spread_in_entry_window"] += 1
            if _float(row.get("volA")) < min_volume or _float(row.get("volB")) < min_volume:
                continue
            stages["both_24h_volumes_above_profile_minimum"] += 1
            structural_reserve = 0.0
            if min_structural_deviation > 0.0:
                if row.get("structural_verified") is not True or adjusted < min_structural_deviation:
                    continue
                structural_reserve = structural
            stages["configured_structural_filter_passed"] += 1
            if spread - structural_reserve <= estimated_break_even:
                continue
            stages["positive_after_estimated_break_even"] += 1
            required_spread = max(
                min_entry,
                structural_reserve + max(min_structural_deviation, estimated_break_even),
            )
            if spread <= required_spread:
                continue
            stages["configured_entry_preflight_estimate"] += 1
        per_snapshot.append({
            "ts": snapshot.get("ts"),
            "rows": local_count,
            "max_spread_pct": round(local_max, 4) if local_max is not None else None,
        })
    result["estimated_break_even_pct_using_default_fee"] = round(estimated_break_even, 4)
    result["observation_stage_counts"] = stages
    result["samples"] = per_snapshot
    result["top_unique_rows"] = sorted(
        top.values(), key=lambda row: row["spread_pct"], reverse=True
    )[:20]
    return result


def summarize_recorded_funnel(shadow_path, profiles_path, build_sha):
    """Replay non-private scanner stages against the bounded shadow history."""
    with sqlite3.connect(profiles_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        db.row_factory = sqlite3.Row
        profile_row = db.execute(
            "SELECT id,min_entry_spread,max_entry_spread,min_volume_millions,trade_volume,"
            "min_depth_percent,min_structural_deviation,disabled_exchanges FROM trading_profiles "
            "WHERE is_active=1 ORDER BY id LIMIT 1"
        ).fetchone()
    if not profile_row:
        return {"error": "no active profile"}
    profile = dict(profile_row)
    try:
        disabled = set(json.loads(profile.get("disabled_exchanges") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        disabled = set()
    min_entry = _float(profile["min_entry_spread"])
    max_entry = _float(profile["max_entry_spread"])
    min_volume = _float(profile["min_volume_millions"]) * 1_000_000.0
    required_depth = _float(profile["trade_volume"]) * _float(profile["min_depth_percent"]) / 100.0
    min_structural_deviation = _float(profile["min_structural_deviation"])
    stage_names = (
        "current_build_samples", "both_l2_books_fresh_and_valid", "enabled_venues",
        "raw_spread_in_entry_window", "both_24h_volumes_above_profile_minimum",
        "configured_structural_filter_passed", "positive_after_actual_break_even",
        "l2_top_levels_above_configured_depth", "configured_entry_preflight",
    )
    counts = {name: 0 for name in stage_names}
    timestamps = {name: set() for name in stage_names}
    top = {}
    with sqlite3.connect(shadow_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        rows = db.execute(
            "SELECT snapshot FROM book_samples ORDER BY id DESC LIMIT 10000"
        )
        for raw, in rows:
            sample = json.loads(raw)
            if str(sample.get("build_sha")) != str(build_sha):
                continue
            stamp = sample.get("recorded_ms")
            spread_row = sample.get("spread") or {}

            def passed(stage):
                counts[stage] += 1
                timestamps[stage].add(stamp)

            passed("current_build_samples")
            if not valid_book(sample.get("book_a") or {}, stamp) or not valid_book(sample.get("book_b") or {}, stamp):
                continue
            passed("both_l2_books_fresh_and_valid")
            if spread_row.get("exchangeA") in disabled or spread_row.get("exchangeB") in disabled:
                continue
            passed("enabled_venues")
            spread = _float(spread_row.get("spread"))
            structural = abs(_float(spread_row.get("structural_spread")))
            adjusted = spread - structural
            key = "|".join(str(spread_row.get(name, "")) for name in (
                "pair", "exchangeA", "exchangeB", "direction"
            ))
            candidate = {
                "pair": spread_row.get("pair"),
                "spread_pct": round(spread, 4),
                "structural_spread_pct": round(structural, 4),
                "adjusted_spread_pct": round(adjusted, 4),
                "vol_a_usd": round(_float(spread_row.get("volA")), 2),
                "vol_b_usd": round(_float(spread_row.get("volB")), 2),
                "structural_verified": spread_row.get("structural_verified") is True,
            }
            if key not in top or adjusted > top[key]["adjusted_spread_pct"]:
                top[key] = candidate
            if not min_entry <= spread <= max_entry:
                continue
            passed("raw_spread_in_entry_window")
            if _float(spread_row.get("volA")) < min_volume or _float(spread_row.get("volB")) < min_volume:
                continue
            passed("both_24h_volumes_above_profile_minimum")
            structural_reserve = 0.0
            if min_structural_deviation > 0.0:
                if spread_row.get("structural_verified") is not True or adjusted < min_structural_deviation:
                    continue
                structural_reserve = structural
            passed("configured_structural_filter_passed")
            fee_a = _float(sample.get("fee_a"), 0.001)
            fee_b = _float(sample.get("fee_b"), 0.001)
            break_even = 2.0 * (fee_a + fee_b) * 100.0 + 0.08
            if spread - structural_reserve <= break_even:
                continue
            passed("positive_after_actual_break_even")
            is_a_higher = str(spread_row.get("direction", "")).startswith(
                str(spread_row.get("exchangeA", ""))
            )
            side_a = "bids" if is_a_higher else "asks"
            side_b = "asks" if is_a_higher else "bids"
            level_a = (sample["book_a"].get(side_a) or [[0.0, 0.0]])[0]
            level_b = (sample["book_b"].get(side_b) or [[0.0, 0.0]])[0]
            if _float(level_a[0]) * _float(level_a[1]) < required_depth \
                    or _float(level_b[0]) * _float(level_b[1]) < required_depth:
                continue
            passed("l2_top_levels_above_configured_depth")
            required_spread = max(
                min_entry,
                structural_reserve + max(min_structural_deviation, break_even),
            )
            if spread > required_spread:
                passed("configured_entry_preflight")
    return {
        "scope": "Latest 10000 shadow rows, restricted to the currently deployed build",
        "build_sha": build_sha,
        "profile_id": profile["id"],
        "configured_depth_usd": round(required_depth, 4),
        "observation_stage_counts": counts,
        "distinct_timestamp_stage_counts": {
            name: len(values) for name, values in timestamps.items()
        },
        "top_adjusted_unique_rows": sorted(
            top.values(), key=lambda row: row["adjusted_spread_pct"], reverse=True
        )[:20],
    }


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
                "entry_price_a,entry_price_b,exit_price_a,exit_price_b,opened_qty_a,opened_qty_b,"
                "closed_qty_a,closed_qty_b,last_price_a,last_price_b,curr_gross_pnl,"
                "current_net_estimate,valuation_ms,peak_net_percent,pnl,gross_pnl,commission_cost,"
                "funding_cost,close_reason "
                "FROM trades ORDER BY id DESC LIMIT 30"
            )]
            result["active_trades"] = [dict(r) for r in db.execute(
                "SELECT id,pair,status,entry_time,opened_qty_a,opened_qty_b,closed_qty_a,closed_qty_b "
                "FROM trades WHERE status NOT IN ('CLOSED','CANCELLED')"
            )]
            result["order_states"] = [dict(r) for r in db.execute(
                "SELECT json_extract(observation,'$.state') AS state,COUNT(*) AS count "
                "FROM execution_orders GROUP BY state"
            )]
            result["pending_accounting"] = [dict(r) for r in db.execute(
                "SELECT id,pair,exchange_a,exchange_b,accounting_status,last_accounting_check,"
                "accounting_error FROM trades WHERE status='CLOSED' "
                "AND accounting_status!='VERIFIED' AND id IN (SELECT trade_id FROM execution_plans) "
                "ORDER BY id DESC LIMIT 20"
            )]
            # Operational state only: deliberately omit exchange/client order IDs.
            result["pending_accounting_orders"] = [dict(r) for r in db.execute(
                "SELECT o.trade_id,o.leg,o.exchange,"
                "json_extract(o.request,'$.reduce_only') AS reduce_only,"
                "json_extract(o.request,'$.quantity') AS requested_quantity,"
                "json_extract(o.observation,'$.state') AS state,"
                "json_extract(o.observation,'$.filled_quantity') AS filled_quantity,"
                "json_extract(o.observation,'$.average_price') AS average_price,"
                "json_extract(o.observation,'$.fee') AS fee,o.updated_ms "
                "FROM execution_orders o JOIN trades t ON t.id=o.trade_id "
                "WHERE t.status='CLOSED' AND t.accounting_status!='VERIFIED' "
                "ORDER BY o.trade_id DESC,o.created_ms LIMIT 80"
            )]
            result["recent_managed_orders"] = [dict(r) for r in db.execute(
                "SELECT o.trade_id,o.leg,o.exchange,"
                "json_extract(o.request,'$.reduce_only') AS reduce_only,"
                "json_extract(o.request,'$.is_buy') AS is_buy,"
                "json_extract(o.request,'$.quantity') AS requested_quantity,"
                "json_extract(o.request,'$.limit_price') AS limit_price,"
                "json_extract(o.request,'$.multiplier') AS multiplier,"
                "json_extract(o.observation,'$.state') AS state,"
                "json_extract(o.observation,'$.filled_quantity') AS filled_quantity,"
                "json_extract(o.observation,'$.average_price') AS average_price,"
                "json_extract(o.observation,'$.fee') AS fee,o.created_ms,o.updated_ms "
                "FROM execution_orders o WHERE o.trade_id IN "
                "(SELECT trade_id FROM execution_plans ORDER BY trade_id DESC LIMIT 10) "
                "ORDER BY o.trade_id DESC,o.created_ms LIMIT 80"
            )]
            result["recent_execution_events"] = [dict(r) for r in db.execute(
                "SELECT trade_id,event,at_ms FROM execution_events WHERE trade_id IN "
                "(SELECT trade_id FROM execution_plans ORDER BY trade_id DESC LIMIT 10) "
                "ORDER BY trade_id DESC,at_ms LIMIT 160"
            )]
        elif kind == "profiles":
            result["profiles"] = [dict(r) for r in db.execute(
                "SELECT id,is_active,trade_volume,leverage,min_entry_spread,max_entry_spread,"
                "take_profit_percent,trailing_stop_percent,max_active_trades,max_holding_time_ms,"
                "min_volume_millions,min_structural_deviation,min_depth_percent,slippage_percent,"
                "daily_loss_limit_usdt,max_total_exposure_usdt,disabled_exchanges "
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
    logs = subprocess.run(["journalctl", "-u", "core-node", "--since=-15min", "-n", "2500", "--no-pager"],
                          capture_output=True, text=True, timeout=30)
    lines = logs.stdout.splitlines()
    print("AUDIT: service logs loaded", file=sys.stderr, flush=True)
    result["recent_log_counts"] = {label: sum(pattern in line for line in lines) for label, pattern in (
        ("binance_subscription_rejected", "[binance_l2_chunk] Subscription rejected"),
        ("binance_receive_failed", "[binance_l2_chunk] Receive failed"),
        ("binance_connection_failed", "[binance_l2_chunk] Connect failed"),
        ("binance_depth_backlog", "Backlog detected:"),
        ("binance_banned", "[binance] IP still banned"),
        ("engine_panics", "ENGINE LOOP PANIC"),
        ("database_locked", "database is locked"),
        ("scanner_signals", "[scanner] SIGNAL"),
        ("scanner_rejects", "[scanner] REJECT"),
        ("scanner_depth_shallow", "L1 depth shallow"),
        ("scanner_lot_impossible", "physically impossible to balance lot sizes"),
        ("scanner_volume_too_high", "actual_vol exceeds max_v"),
        ("scanner_volume_outside_range", "actual volume outside configured range"),
        ("scanner_below_notional", "below min_notional"),
        ("scanner_structural_deviation", "ema deviation"),
        ("scanner_health_rejected", "health check failed"),
        ("managed_fee_preflight_failed", "[managed] fee preflight failed"),
        ("managed_binance_l2_refresh_failed", "[managed] Binance L2 refresh failed"),
        ("kucoin_managed_fee_lookup_failed", "[kucoin] managed fee lookup failed"),
        ("kucoin_managed_fee_evidence_rejected", "[kucoin] managed fee evidence rejected"),
        ("accounting_pending", "[accounting] trade="),
        ("managed_entries_halted", "[managed] NEW ENTRIES HALTED"),
    )}
    result["recent_log_count"] = len(lines)
    result["binance_l2_quality_tail"] = [
        line.split("[binance_l2_quality] ", 1)[1][:400]
        for line in lines if "[binance_l2_quality] " in line
    ][-20:]
    result["binance_l2_subscription_tail"] = [
        line.split("[binance_l2] ", 1)[1][:400]
        for line in lines if "[binance_l2] " in line
    ][-20:]
    result["binance_l2_dynamic_tail"] = [
        line.split("[binance_l2_dynamic", 1)[1][:600]
        for line in lines if "[binance_l2_dynamic" in line
    ][-30:]
    result["binance_depth_backlog_tail"] = [
        line.split("Backlog detected: ", 1)[1][:400]
        for line in lines if "Backlog detected: " in line
    ][-40:]
    result["spread_diagnostics_tail"] = [
        line.split("[spreads] ", 1)[1][:600]
        for line in lines if "[spreads] " in line
    ][-20:]
    result["managed_fee_preflight_tail"] = [
        line.split("[managed] fee preflight failed", 1)[1][:800]
        for line in lines if "[managed] fee preflight failed" in line
    ][-20:]
    result["accounting_pending_tail"] = [
        line.split("[accounting] ", 1)[1][:800]
        for line in lines if "[accounting] trade=" in line
    ][-30:]
    result["kucoin_managed_fee_tail"] = [
        line.split("[kucoin] managed fee ", 1)[1][:800]
        for line in lines if "[kucoin] managed fee " in line
    ][-30:]
    # Only transport errors from the unauthenticated public market-data stream;
    # never export private order/account logs or service environments.
    prefix = "[binance_l2_chunk] Receive failed: "
    result["binance_public_transport_errors"] = list(dict.fromkeys(
        line.split(prefix, 1)[1][:400] for line in lines if prefix in line
    ))[:10]
    # Export only service lifecycle/startup lines around deployments. Avoid
    # arbitrary engine messages because private order logs can contain account
    # details that do not belong in the report branch.
    deployment_markers = (
        "Starting CEX Arbitrage Core Node",
        "Started CEX Arbitrage Core Node",
        "Stopping CEX Arbitrage Core Node",
        "Stopped CEX Arbitrage Core Node",
        "core-node.service: Main process exited",
        "core-node.service: Failed with result",
        "[STARTUP]",
        "ENGINE LOOP PANIC",
        "Address already in use",
    )
    result["core_deployment_tail"] = [
        line[:800] for line in lines if any(marker in line for marker in deployment_markers)
    ][-80:]
    result["pending_core_artifacts"] = []
    try:
        for path in sorted(Path("/tmp").glob("arb-core-*")):
            entry = {"name": path.name, "mtime": path.stat().st_mtime}
            if path.is_dir():
                entry["files"] = [
                    {"name": child.name, "size": child.stat().st_size, "mtime": child.stat().st_mtime}
                    for child in sorted(path.iterdir())[:20]
                ]
            else:
                entry["size"] = path.stat().st_size
            result["pending_core_artifacts"].append(entry)
    except OSError as exc:
        result["pending_core_artifacts_error"] = type(exc).__name__
    print("AUDIT: service metadata", file=sys.stderr, flush=True)
    for service in ("core-node", "vps-api", "cloudflared"):
        proc = subprocess.run([
            "systemctl", "show", service, "--property=ActiveState,SubState,MainPID,ActiveEnterTimestamp,NRestarts"
        ], capture_output=True, text=True, timeout=10)
        result[service] = proc.stdout.strip()
    process = subprocess.run(
        ["ps", "-C", "core-node", "-o", "pid=,etimes=,pcpu=,pmem=,nlwp=,rss=,stat="],
        capture_output=True, text=True, timeout=10,
    )
    result["core_process_usage"] = process.stdout.strip()
    result["load_average"] = list(__import__("os").getloadavg())
    print("AUDIT: process usage", file=sys.stderr, flush=True)
    for binary in ("core-node", "vps-api"):
        path = Path("/usr/local/bin") / binary
        try:
            if path.is_file():
                digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                result[binary + "_binary"] = {
                    "sha256": digest.hexdigest(),
                    "mtime": path.stat().st_mtime,
                }
        except OSError as exc:
            result[binary + "_binary_error"] = type(exc).__name__
    print("AUDIT: binary metadata", file=sys.stderr, flush=True)
    result["health_samples"] = []
    for sample_index in range(10):
        try:
            try:
                response = urllib.request.urlopen("http://127.0.0.1:3003/health", timeout=5)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                health = json.load(response)
                result["health_http_status"] = response.status
                result["health"] = health
                result["health_samples"].append({
                    "status": response.status,
                    "trading_ready": health.get("trading_ready"),
                    "fresh_books": {
                        venue.get("exchange"): venue.get("fresh_books")
                        for venue in health.get("venues", [])
                    },
                })
        except Exception as exc:
            result["health_samples"].append({"error": type(exc).__name__})
        if sample_index != 9:
            time.sleep(0.5)
    print("AUDIT: health complete", file=sys.stderr, flush=True)
    root = Path("/home/opc/core-node")
    for filename, kind in (("real-trader.db", "trades"), ("vps-api.db", "profiles"), ("shadow-books.db", "shadow")):
        print("AUDIT: database " + kind, file=sys.stderr, flush=True)
        try:
            result[filename] = inspect_database(root / filename, kind)
        except Exception as exc:
            result[filename] = {"error": str(exc)}
    print("AUDIT: scanner funnel", file=sys.stderr, flush=True)
    try:
        result["scanner_funnel"] = sample_scanner_funnel(root / "vps-api.db")
    except Exception as exc:
        result["scanner_funnel"] = {"error": type(exc).__name__ + ": " + str(exc)}
    print("AUDIT: recorded scanner funnel", file=sys.stderr, flush=True)
    try:
        result["recorded_scanner_funnel"] = summarize_recorded_funnel(
            root / "shadow-books.db", root / "vps-api.db", (result.get("health") or {}).get("build_sha")
        )
    except Exception as exc:
        result["recorded_scanner_funnel"] = {"error": type(exc).__name__ + ": " + str(exc)}
    disk = shutil.disk_usage(root if root.exists() else "/")
    result["disk_free_bytes"] = disk.free
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
