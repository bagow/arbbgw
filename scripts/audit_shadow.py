"""Assess whether recorded books can support replay, without inventing fills."""
from collections import Counter
import math


def valid_book(book, now):
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


def summarize_shadow(samples):
    count = fresh = verified = 0
    venues = Counter()
    stale = Counter()
    pairs = Counter()
    versions = Counter()
    first = last = None
    for sample in samples:
        count += 1
        now = sample["recorded_ms"]
        first = now if first is None else min(first, now)
        last = now if last is None else max(last, now)
        spread = sample["spread"]
        versions[str(sample.get("build_sha"))] += 1
        usable = True
        for side in ("a", "b"):
            ex = spread["exchange" + side.upper()]
            venues[ex] += 1
            if not valid_book(sample["book_" + side], now):
                stale[ex] += 1
                usable = False
        if usable:
            fresh += 1
            verified += spread.get("structural_verified") is True
            pairs[spread["pair"]] += 1
    return {
        "samples": count, "first_ms": first, "last_ms": last,
        "both_books_fresh_and_valid": fresh,
        "both_books_valid_and_structural_verified": verified,
        "venue_observations": dict(venues), "stale_or_invalid_by_venue": dict(stale),
        "usable_pairs": dict(pairs), "build_samples": dict(versions),
        "replay_limitations": [
            "Historical samples omit contract filters, funding deadlines and order latency.",
            "Snapshots cannot prove fills, hedge execution or realized profit.",
            "Sparse or stale samples cannot establish stop/TP execution between observations.",
        ],
    }
