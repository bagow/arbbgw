#!/usr/bin/env python3
"""Replay recorded Binance/KuCoin L2 books through production entry economics.

This is an opportunity audit, not a fill simulator: recorded snapshots cannot
prove queue position, order acceptance, or the price observed between samples.
"""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import sqlite3


SCALE = 100_000_000
EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT = 0.02
FEE_TTL_MS = 3_600_000
MAX_EXECUTION_BOOK_SKEW_MS = 250


def finite_positive(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def quote(levels, quantity, buy):
    if not finite_positive(quantity) or not isinstance(levels, list) or not levels:
        return None
    remaining = quantity
    cost = 0.0
    previous = None
    for level in levels:
        if not isinstance(level, list) or len(level) != 2:
            return None
        price, size = level
        if not finite_positive(price) or not finite_positive(size):
            return None
        if previous is not None and ((buy and price < previous) or (not buy and price > previous)):
            return None
        previous = price
        take = min(size, remaining)
        cost += price * take
        remaining -= take
        if remaining <= quantity * 1e-10:
            filled = quantity - remaining
            return {"vwap": cost / filled, "worst": price}
    return None


def valid_levels(levels, buy):
    if not isinstance(levels, list) or not levels:
        return False
    previous = None
    for level in levels:
        if not isinstance(level, list) or len(level) != 2:
            return False
        price, size = level
        if not finite_positive(price) or not isinstance(size, (int, float)):
            return False
        if not math.isfinite(size) or size < 0:
            return False
        if previous is not None and ((buy and price < previous) or (not buy and price > previous)):
            return False
        previous = price
    return True


def common_quantity_step(a, b):
    def units(value):
        if not finite_positive(value):
            return None
        scaled = value * SCALE
        if scaled < 1 or scaled > 9_000_000_000_000_000:
            return None
        rounded = round(scaled)
        return rounded if abs(scaled - rounded) <= 1e-5 else None

    left, right = units(a), units(b)
    if left is None or right is None:
        return None
    divisor = math.gcd(left, right)
    multiple = left // divisor * right
    if multiple > 9_000_000_000_000_000:
        return None
    return multiple / SCALE


def lot_size(notional, price, step_a, step_b, tolerance_pct):
    step = common_quantity_step(step_a, step_b)
    if step is None or not finite_positive(notional) or not finite_positive(price):
        return None
    maximum = notional * (1 + tolerance_pct / 100) / price
    preferred = max(1, round(notional / price / step))
    steps = min(preferred, math.floor(maximum / step + 1e-10))
    return steps * step if steps >= 1 else None


def break_even_pct(fee_a, fee_b):
    if not all(isinstance(value, (int, float)) and math.isfinite(value) and value >= 0
               for value in (fee_a, fee_b)):
        return None
    return 2 * (fee_a + fee_b) * 100 + 4 * EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT


def directional_structural_regime_is_stable(spread):
    structural_min = spread.get("structural_min")
    structural_max = spread.get("structural_max")
    return (
        spread.get("structural_verified") is True
        and spread.get("has_structural_spread") is True
        and isinstance(structural_min, (int, float))
        and isinstance(structural_max, (int, float))
        and math.isfinite(structural_min)
        and math.isfinite(structural_max)
        and structural_max >= structural_min
    )


def crossing_cost_pct(is_a_higher, entry_a, entry_b, exit_a, exit_b):
    values = (entry_a, entry_b, exit_a, exit_b)
    if not all(finite_positive(value) for value in values):
        return None
    if is_a_higher:
        immediate_gross = entry_a - exit_a + exit_b - entry_b
    else:
        immediate_gross = exit_a - entry_a + entry_b - exit_b
    cost = -immediate_gross / entry_a * 100
    return max(0.0, cost) if math.isfinite(cost) else None


def initial_liquidation_net_pct(
    is_a_higher, entry_a, entry_b, exit_a, exit_b, quantity, fee_a, fee_b
):
    values = (entry_a, entry_b, exit_a, exit_b, quantity)
    if not all(finite_positive(value) for value in values):
        return None
    if not all(
        isinstance(value, (int, float)) and math.isfinite(value) and value >= 0
        for value in (fee_a, fee_b)
    ):
        return None
    gross = (
        entry_a - exit_a + exit_b - entry_b
        if is_a_higher
        else exit_a - entry_a + entry_b - exit_b
    ) * quantity
    fees = quantity * (
        entry_a * fee_a
        + entry_b * fee_b
        + exit_a * fee_a
        + exit_b * fee_b
    )
    percent = (gross - fees) / (entry_a * quantity) * 100
    return percent if math.isfinite(percent) else None


def hard_stop_entry_is_compatible(
    initial_net_pct, hard_stop_percent, entry_slippage_limit_per_leg_pct
):
    values = (
        initial_net_pct,
        hard_stop_percent,
        entry_slippage_limit_per_leg_pct,
    )
    return (
        all(isinstance(value, (int, float)) and math.isfinite(value) for value in values)
        and entry_slippage_limit_per_leg_pct >= 0
        and (
            hard_stop_percent <= 0
            or initial_net_pct
            > -hard_stop_percent + 2 * entry_slippage_limit_per_leg_pct
        )
    )


def valid_book(book, now_ms, freshness_ms=750):
    if not isinstance(book, dict):
        return False
    updated = book.get("updated_ms")
    if not isinstance(updated, (int, float)) or not 0 <= now_ms - updated <= freshness_ms:
        return False
    bids, asks = book.get("bids"), book.get("asks")
    return (
        valid_levels(bids, False)
        and valid_levels(asks, True)
        and bids[0][0] < asks[0][0]
    )


def specification_permits(spec, now_ms, quantity, price, step):
    if not isinstance(spec, dict) or now_ms - spec.get("fetched_ms", 0) > 600_000:
        return False
    minimum = float(spec.get("min_quantity", 0) or 0)
    maximum = float(spec.get("max_quantity", 0) or 0)
    min_notional = float(spec.get("min_notional", 0) or 0)
    on_grid = abs(quantity / step - round(quantity / step)) <= 1e-8
    return (
        finite_positive(quantity)
        and finite_positive(price)
        and finite_positive(step)
        and quantity + 1e-10 >= minimum
        and (maximum <= 0 or quantity <= maximum + 1e-10)
        and quantity * price + 1e-10 >= min_notional
        and on_grid
    )


def evaluate(sample, profile):
    spread = sample.get("spread") or {}
    if {spread.get("exchangeA"), spread.get("exchangeB")} != {"binance", "kucoin"}:
        return None, "venues"
    now_ms = sample.get("recorded_ms")
    if not isinstance(now_ms, int):
        return None, "timestamp"
    book_a, book_b = sample.get("book_a"), sample.get("book_b")
    if not valid_book(book_a, now_ms) or not valid_book(book_b, now_ms):
        return None, "book_freshness"
    if abs(book_a["updated_ms"] - book_b["updated_ms"]) > profile["max_book_skew_ms"]:
        return None, "book_skew"
    if not directional_structural_regime_is_stable(spread):
        return None, "structural_regime"
    gross_l1 = spread.get("spread")
    baseline = spread.get("structural_spread")
    if not all(isinstance(value, (int, float)) and math.isfinite(value)
               for value in (gross_l1, baseline)):
        return None, "spread_values"
    if not profile["min_entry_spread"] <= gross_l1 <= profile["max_entry_spread"]:
        return None, "entry_window"
    if min(float(spread.get("volA", 0)), float(spread.get("volB", 0))) < profile["min_volume_usd"]:
        return None, "volume"
    top_bid_a, top_ask_a = book_a["bids"][0][0], book_a["asks"][0][0]
    top_bid_b, top_ask_b = book_b["bids"][0][0], book_b["asks"][0][0]
    intra_a = (top_ask_a - top_bid_a) / top_bid_a * 100
    intra_b = (top_ask_b - top_bid_b) / top_bid_b * 100
    if intra_a > profile["max_intra_spread_pct"] or intra_b > profile["max_intra_spread_pct"]:
        return None, "intra_spread"
    spec_a, spec_b = sample.get("spec_a"), sample.get("spec_b")
    if not isinstance(spec_a, dict) or not isinstance(spec_b, dict):
        return None, "specification"
    is_a_higher = str(spread.get("direction", "")).startswith(str(spread.get("exchangeA", "")))
    discovery_entry_a = float(spread.get("bidA") if is_a_higher else spread.get("askA"))
    quantity = lot_size(
        profile["notional_usdt"],
        discovery_entry_a,
        float(spec_a.get("quantity_step", 0)),
        float(spec_b.get("quantity_step", 0)),
        profile["volume_tolerance_pct"],
    )
    if quantity is None:
        return None, "lot_grid"
    reserve_qty = profile["notional_usdt"] * profile["min_depth_percent"] / 100 / discovery_entry_a
    reserve_a = quote(book_a["asks" if not is_a_higher else "bids"], reserve_qty, not is_a_higher)
    reserve_b = quote(book_b["asks" if is_a_higher else "bids"], reserve_qty, is_a_higher)
    if reserve_a is None or reserve_b is None:
        return None, "reserve_depth"
    entry_a = quote(book_a["asks" if not is_a_higher else "bids"], quantity, not is_a_higher)
    entry_b = quote(book_b["asks" if is_a_higher else "bids"], quantity, is_a_higher)
    if entry_a is None or entry_b is None:
        return None, "entry_depth"
    unwind_a = quote(book_a["asks" if is_a_higher else "bids"], quantity, is_a_higher)
    unwind_b = quote(book_b["bids" if is_a_higher else "asks"], quantity, not is_a_higher)
    gross = ((entry_a["vwap"] - entry_b["vwap"]) / entry_b["vwap"] * 100
             if is_a_higher else
             (entry_b["vwap"] - entry_a["vwap"]) / entry_a["vwap"] * 100)
    if not profile["min_entry_spread"] <= gross <= profile["max_entry_spread"]:
        return None, "l2_entry_window"
    fee_a, fee_b = sample.get("fee_a"), sample.get("fee_b")
    fee_stamps = (sample.get("fee_a_updated_ms"), sample.get("fee_b_updated_ms"))
    if any(
        not isinstance(stamp, int) or not 0 <= now_ms - stamp <= FEE_TTL_MS
        for stamp in fee_stamps
    ):
        return None, "fee_freshness"
    fixed_costs = break_even_pct(fee_a, fee_b)
    crossing_cost = (
        crossing_cost_pct(
            is_a_higher,
            entry_a["vwap"],
            entry_b["vwap"],
            unwind_a["vwap"],
            unwind_b["vwap"],
        )
        if unwind_a is not None and unwind_b is not None
        else None
    )
    if fixed_costs is None:
        return None, "fees"
    initial_net_pct = (
        initial_liquidation_net_pct(
            is_a_higher,
            entry_a["vwap"],
            entry_b["vwap"],
            unwind_a["vwap"],
            unwind_b["vwap"],
            quantity,
            fee_a,
            fee_b,
        )
        if unwind_a is not None and unwind_b is not None
        else None
    )
    # The recorded structural_spread is already mid-price EMA plus the
    # entry-to-exit half-spreads. Adding crossing_cost again would double count.
    costs = fixed_costs
    structural = max(0.0, baseline)
    required = max(
        profile["min_entry_spread"],
        structural + costs + profile["min_structural_deviation"],
    )
    if gross <= required:
        return None, "entry_budget"
    hard_stop = profile["hard_stop_percent"]
    if hard_stop > 0 and not hard_stop_entry_is_compatible(
        initial_net_pct, hard_stop, profile["slippage_percent"]
    ):
        return None, "hard_stop_compatibility"
    actual_a, actual_b = quantity * entry_a["vwap"], quantity * entry_b["vwap"]
    lower = profile["notional_usdt"] * (1 - profile["volume_tolerance_pct"] / 100)
    upper = profile["notional_usdt"] * (1 + profile["volume_tolerance_pct"] / 100)
    if not lower <= actual_a <= upper or not lower <= actual_b <= upper:
        return None, "actual_volume"
    if not specification_permits(spec_a, now_ms, quantity, entry_a["vwap"], spec_a["quantity_step"]):
        return None, "specification_a"
    if not specification_permits(spec_b, now_ms, quantity, entry_b["vwap"], spec_b["quantity_step"]):
        return None, "specification_b"
    route = (
        spread["pair"], spread["exchangeA"], spread["exchangeB"], spread["direction"]
    )
    return {
        "route": route,
        "recorded_ms": now_ms,
        "gross_pct": gross,
        "required_pct": required,
        "headroom_pct": gross - required,
        "quantity": quantity,
        "entry_a": entry_a["vwap"],
        "entry_b": entry_b["vwap"],
        "fee_a": fee_a,
        "fee_b": fee_b,
        "fixed_cost_pct": fixed_costs,
        "crossing_cost_pct": crossing_cost,
        "initial_liquidation_net_pct": initial_net_pct,
        "is_a_higher": is_a_higher,
        "book_a_updated_ms": book_a["updated_ms"],
        "book_b_updated_ms": book_b["updated_ms"],
        "sample_index": sample.get("_replay_index"),
    }, "eligible"


def group_windows(events, gap_ms):
    windows = []
    by_route = defaultdict(list)
    for event in events:
        by_route[event["route"]].append(event)
    for route, route_events in by_route.items():
        route_events.sort(key=lambda event: event["recorded_ms"])
        current = []
        for event in route_events:
            if current and event["recorded_ms"] - current[-1]["recorded_ms"] > gap_ms:
                windows.append((route, current))
                current = []
            current.append(event)
        if current:
            windows.append((route, current))
    return sorted(windows, key=lambda window: window[1][0]["recorded_ms"])


def confirmed_entry(rows, confirmation_ms, max_observation_gap_ms=250):
    """Model the non-blocking production confirmation state machine.

    Eligibility must be observed continuously at the recorder cadence, both
    venue books must advance, and widening at the confirmation boundary
    restarts the clock.
    """
    initial = None
    previous = None
    for observed in rows:
        timestamps = (
            observed.get("recorded_ms"),
            observed.get("book_a_updated_ms"),
            observed.get("book_b_updated_ms"),
        )
        if not all(isinstance(value, (int, float)) for value in timestamps):
            initial = previous = None
            continue
        discontinuous = (
            previous is not None
            and observed["recorded_ms"] - previous["recorded_ms"] > max_observation_gap_ms
        )
        regressed = (
            initial is not None
            and (
                observed["recorded_ms"] < initial["recorded_ms"]
                or observed["book_a_updated_ms"] < initial["book_a_updated_ms"]
                or observed["book_b_updated_ms"] < initial["book_b_updated_ms"]
            )
        )
        confirmation_due = (
            initial is not None
            and observed["recorded_ms"] >= initial["recorded_ms"] + confirmation_ms
        )
        widened_at_confirmation = (
            confirmation_due
            and observed["gross_pct"] > initial["gross_pct"] + 0.000001
        )
        if initial is None or discontinuous or regressed or widened_at_confirmation:
            initial = observed
        elif (
            confirmation_due
            and observed["book_a_updated_ms"] > initial["book_a_updated_ms"]
            and observed["book_b_updated_ms"] > initial["book_b_updated_ms"]
        ):
            return observed
        previous = observed
    return None


def books_for_route(sample, route):
    spread = sample.get("spread") or {}
    exchange_a, exchange_b = route[1], route[2]
    if spread.get("pair") != route[0]:
        return None
    if spread.get("exchangeA") == exchange_a and spread.get("exchangeB") == exchange_b:
        return sample.get("book_a"), sample.get("book_b")
    if spread.get("exchangeA") == exchange_b and spread.get("exchangeB") == exchange_a:
        return sample.get("book_b"), sample.get("book_a")
    return None


def exit_valuation(entry, sample, max_book_skew_ms=MAX_EXECUTION_BOOK_SKEW_MS):
    books = books_for_route(sample, entry["route"])
    now_ms = sample.get("recorded_ms")
    if books is None or not isinstance(now_ms, int):
        return None
    book_a, book_b = books
    if not valid_book(book_a, now_ms) or not valid_book(book_b, now_ms):
        return None
    if abs(book_a["updated_ms"] - book_b["updated_ms"]) > max_book_skew_ms:
        return None
    quantity = entry["quantity"]
    is_a_higher = entry["is_a_higher"]
    quote_a = quote(
        book_a["asks" if is_a_higher else "bids"], quantity, is_a_higher
    )
    quote_b = quote(
        book_b["bids" if is_a_higher else "asks"], quantity, not is_a_higher
    )
    if quote_a is None or quote_b is None:
        return None

    def net_at(price_a, price_b):
        if is_a_higher:
            gross = (
                entry["entry_a"] - price_a + price_b - entry["entry_b"]
            ) * quantity
        else:
            gross = (
                price_a - entry["entry_a"] + entry["entry_b"] - price_b
            ) * quantity
        fees = quantity * (
            entry["entry_a"] * entry["fee_a"]
            + entry["entry_b"] * entry["fee_b"]
            + price_a * entry["fee_a"]
            + price_b * entry["fee_b"]
        )
        net_usdt = gross - fees
        return {
            "net_usdt": net_usdt,
            "net_pct": net_usdt / max(quantity * entry["entry_a"], 1e-12) * 100,
        }

    return {
        "average": net_at(quote_a["vwap"], quote_b["vwap"]),
        "worst": net_at(quote_a["worst"], quote_b["worst"]),
    }


def simulate_exit(samples, entry, profile):
    first_ms = entry["recorded_ms"]
    deadline_ms = first_ms + profile["max_holding_time_ms"]
    best_pct = -math.inf
    worst_pct = math.inf
    last = None
    for sample in samples:
        stamp = sample.get("recorded_ms")
        if not isinstance(stamp, int) or stamp < first_ms:
            continue
        if stamp > deadline_ms:
            break
        valuation = exit_valuation(entry, sample, profile["max_book_skew_ms"])
        if valuation is None:
            continue
        last = valuation
        net_pct = valuation["average"]["net_pct"]
        best_pct = max(best_pct, net_pct)
        worst_pct = min(worst_pct, net_pct)
        if valuation["worst"]["net_pct"] >= profile["take_profit_percent"]:
            return {
                "outcome": "TP",
                "exit_ms": stamp,
                "net_pct": valuation["average"]["net_pct"],
                "net_usdt": valuation["average"]["net_usdt"],
                "best_pct": best_pct,
                "worst_pct": worst_pct,
            }
        if net_pct <= -profile["hard_stop_percent"]:
            return {
                "outcome": "HARD_STOP",
                "exit_ms": stamp,
                "net_pct": net_pct,
                "net_usdt": valuation["average"]["net_usdt"],
                "best_pct": best_pct,
                "worst_pct": worst_pct,
            }
    return {
        "outcome": "UNRESOLVED",
        "exit_ms": None,
        "net_pct": None if last is None else last["average"]["net_pct"],
        "net_usdt": None if last is None else last["average"]["net_usdt"],
        "best_pct": None if not math.isfinite(best_pct) else best_pct,
        "worst_pct": None if not math.isfinite(worst_pct) else worst_pct,
    }


def select_sequential_cycles(cycles, max_holding_ms):
    """Apply the production invariant of at most one live trade per pair."""
    available_at = defaultdict(int)
    selected = []
    for cycle in sorted(cycles, key=lambda item: item["entry_ms"]):
        pair = cycle["pair"]
        if cycle["entry_ms"] < available_at[pair]:
            continue
        selected.append(cycle)
        if cycle["exit_ms"] is None:
            available_at[pair] = cycle["entry_ms"] + max_holding_ms
        else:
            cooldown = 60_000 if cycle["outcome"] == "TP" else 30 * 60_000
            available_at[pair] = cycle["exit_ms"] + cooldown
    return selected


def summarize(samples, profile, gap_ms=5_000, confirmation_ms=500):
    reasons = Counter()
    events = []
    for sample in samples:
        event, reason = evaluate(sample, profile)
        reasons[reason] += 1
        if event is not None:
            events.append(event)
    windows = group_windows(events, gap_ms)
    durable = []
    cycles = []
    for route, rows in windows:
        duration = rows[-1]["recorded_ms"] - rows[0]["recorded_ms"]
        entry = confirmed_entry(rows, confirmation_ms)
        if entry is not None:
            durable.append({
                "pair": route[0],
                "direction": route[3],
                "first_ms": rows[0]["recorded_ms"],
                "last_ms": rows[-1]["recorded_ms"],
                "duration_ms": duration,
                "samples": len(rows),
                "max_headroom_pct": max(row["headroom_pct"] for row in rows),
                "max_gross_pct": max(row["gross_pct"] for row in rows),
            })
            exit_result = simulate_exit(samples, entry, profile)
            cycles.append({
                "pair": route[0],
                "direction": route[3],
                "entry_ms": entry["recorded_ms"],
                "entry_gross_pct": entry["gross_pct"],
                "entry_headroom_pct": entry["headroom_pct"],
                **exit_result,
            })
    outcome_counts = Counter(cycle["outcome"] for cycle in cycles)
    sequential = select_sequential_cycles(cycles, profile["max_holding_time_ms"])
    sequential_counts = Counter(cycle["outcome"] for cycle in sequential)
    return {
        "samples": len(samples),
        "reasons": dict(reasons),
        "eligible_samples": len(events),
        "independent_windows": len(windows),
        "durable_windows": durable,
        "confirmed_cycle_outcomes": dict(outcome_counts),
        "confirmed_cycles": cycles,
        "sequential_cycle_outcomes": dict(sequential_counts),
        "sequential_cycles": sequential,
        "limitations": [
            "Recorded snapshots cannot prove fills, queue position, or prices between samples.",
            "Listing-age and private-account preflight are not present in shadow snapshots.",
            "A durable opportunity is necessary, not sufficient, evidence of a profitable trade.",
            "Each window is replayed independently; overlapping windows are not simultaneous trades.",
            "Only candidate snapshots were retained, so UNRESOLVED is not proof of a loss.",
            "Entry confirmation resets when eligible observations are more than 250 ms apart; legacy one-hertz samples cannot prove continuous eligibility.",
        ],
    }


def load_samples(path, build=None):
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        rows = db.execute("SELECT snapshot FROM book_samples ORDER BY id").fetchall()
    samples = []
    for index, (raw,) in enumerate(rows):
        sample = json.loads(raw)
        if build is None or sample.get("build_sha") == build:
            sample["_replay_index"] = index
            samples.append(sample)
    return samples


def profile_from_args(args, deviation):
    return {
        "notional_usdt": args.notional,
        "min_entry_spread": args.min_entry,
        "max_entry_spread": args.max_entry,
        "min_structural_deviation": deviation,
        "min_depth_percent": args.min_depth_percent,
        "min_volume_usd": args.min_volume_millions * 1_000_000,
        "max_intra_spread_pct": args.max_intra_spread,
        "volume_tolerance_pct": args.volume_tolerance,
        "take_profit_percent": args.take_profit,
        "hard_stop_percent": args.hard_stop,
        "slippage_percent": args.slippage_percent,
        "max_holding_time_ms": args.max_holding_ms,
        "max_book_skew_ms": args.max_book_skew_ms,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--build")
    parser.add_argument("--notional", type=float, default=6.0)
    parser.add_argument("--min-entry", type=float, default=0.5)
    parser.add_argument("--max-entry", type=float, default=4.0)
    parser.add_argument("--deviations", type=float, nargs="+", default=[0.05, 0.1, 0.2, 0.3])
    parser.add_argument("--min-depth-percent", type=float, default=500.0)
    parser.add_argument("--min-volume-millions", type=float, default=0.5)
    parser.add_argument("--max-intra-spread", type=float, default=1.0)
    parser.add_argument("--volume-tolerance", type=float, default=15.0)
    parser.add_argument("--window-gap-ms", type=int, default=5_000)
    parser.add_argument("--confirmation-ms", type=int, default=500)
    parser.add_argument("--take-profit", type=float, default=0.1)
    parser.add_argument("--hard-stop", type=float, default=1.0)
    parser.add_argument("--slippage-percent", type=float, default=0.05)
    parser.add_argument("--max-holding-ms", type=int, default=1_800_000)
    parser.add_argument("--max-book-skew-ms", type=int, default=MAX_EXECUTION_BOOK_SKEW_MS)
    args = parser.parse_args()
    samples = load_samples(args.database, args.build)
    report = {
        str(deviation): summarize(
            samples,
            profile_from_args(args, deviation),
            args.window_gap_ms,
            args.confirmation_ms,
        )
        for deviation in args.deviations
    }
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
