"""Assess whether recorded books can support replay, without inventing fills."""
from collections import Counter
import math


def valid_book(book, now):
    if not isinstance(book, dict):
        return False
    stamp = book.get("updated_ms")
    if not isinstance(stamp, (int, float)) or not 0 <= now - stamp <= 750:
        return False
    sides = []
    for name in ("bids", "asks"):
        levels = book.get(name)
        if not isinstance(levels, list) or not levels:
            return False
        previous = None
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) != 2:
                return False
            price, quantity = level
            if not all(isinstance(n, (float, int)) and math.isfinite(n) and n > 0 for n in level):
                return False
            if previous is not None and ((name == "bids" and price > previous) or (name == "asks" and price < previous)):
                return False
            previous = price
        sides.append(levels[0][0])
    return sides[0] < sides[1]


def recorded_demand(sample):
    """Missing historical metadata is unknown, never an inactive subscription."""
    books = [sample.get("book_" + side) for side in ("a", "b")]
    requested = [book.get("requested") if isinstance(book, dict) else None for book in books]
    if any(type(value) is not bool for value in requested):
        return "unknown"
    if all(requested):
        return "both_requested"
    return "one_requested" if any(requested) else "neither_requested"


def summarize_shadow(samples):
    count = fresh = verified = 0
    venues = Counter()
    stale = Counter()
    pairs = Counter()
    versions = Counter()
    by_demand = {
        group: {
            "samples": 0,
            "both_books_fresh_and_valid": 0,
            "both_books_valid_and_structural_verified": 0,
            "venue_observations": Counter(),
            "stale_or_invalid_by_venue": Counter(),
        }
        for group in ("both_requested", "one_requested", "neither_requested", "unknown")
    }
    first = last = None
    for sample in samples:
        count += 1
        now = sample["recorded_ms"]
        first = now if first is None else min(first, now)
        last = now if last is None else max(last, now)
        spread = sample["spread"]
        demand = by_demand[recorded_demand(sample)]
        demand["samples"] += 1
        versions[str(sample.get("build_sha"))] += 1
        usable = True
        for side in ("a", "b"):
            ex = spread["exchange" + side.upper()]
            venues[ex] += 1
            demand["venue_observations"][ex] += 1
            if not valid_book(sample["book_" + side], now):
                stale[ex] += 1
                demand["stale_or_invalid_by_venue"][ex] += 1
                usable = False
        if usable:
            fresh += 1
            verified += spread.get("structural_verified") is True
            demand["both_books_fresh_and_valid"] += 1
            demand["both_books_valid_and_structural_verified"] += spread.get("structural_verified") is True
            pairs[spread["pair"]] += 1
    for demand in by_demand.values():
        demand["venue_observations"] = dict(demand["venue_observations"])
        demand["stale_or_invalid_by_venue"] = dict(demand["stale_or_invalid_by_venue"])
        demand["fresh_and_valid_percent"] = (
            100.0 * demand["both_books_fresh_and_valid"] / demand["samples"]
            if demand["samples"] else None
        )
    return {
        "scope": "All recorded books, including cache-only and warm-retained routes; not active-feed latency",
        "samples": count, "first_ms": first, "last_ms": last,
        "both_books_fresh_and_valid": fresh,
        "both_books_valid_and_structural_verified": verified,
        "venue_observations": dict(venues), "stale_or_invalid_by_venue": dict(stale),
        "usable_pairs": dict(pairs), "build_samples": dict(versions),
        "by_recorded_demand": by_demand,
        "demand_definition": "Scanner L2 demand at capture, not exchange subscription acknowledgement; unrequested books may remain streamed during warm retention",
        "replay_limitations": [
            "Historical samples without requested flags have unknown subscription demand.",
            "Aggregate stale counts include unrequested cached books and do not measure active-feed failures.",
            "Funding deadlines and order latency are not reconstructed by this report.",
            "Snapshots cannot prove fills, hedge execution or realized profit.",
            "Sparse or stale samples cannot establish stop/TP execution between observations.",
        ],
    }
