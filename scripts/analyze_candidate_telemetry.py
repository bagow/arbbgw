#!/usr/bin/env python3
"""Summarize read-only canary candidate JSONL across observer restarts."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path


COUNTERFACTUAL_THRESHOLDS = (0.30, 0.35, 0.40, 0.45)


def compact(row):
    return {
        key: row.get(key)
        for key in (
            "pair",
            "spread",
            "structural_spread",
            "break_even_pct",
            "net_excursion_pct",
            "gap_to_entry_pct",
            "stable_regime",
            "structural_min",
            "structural_max",
            "volA",
            "volB",
            "intra_a_pct",
            "intra_b_pct",
        )
    }


def best_by_pair(rows):
    best = {}
    for row in rows:
        pair = row.get("pair") or "?"
        if pair not in best or row.get("net_excursion_pct", -999.0) > best[pair].get(
            "net_excursion_pct", -999.0
        ):
            best[pair] = row
    return [
        compact(row)
        for row in sorted(
            best.values(),
            key=lambda item: item.get("net_excursion_pct", -999.0),
            reverse=True,
        )
    ]


def episode_counts(snapshots, predicate):
    active = set()
    episodes = Counter()
    for record in snapshots:
        current = {
            row.get("pair") or "?"
            for row in record.get("rows") or []
            if predicate(row)
        }
        for pair in current - active:
            episodes[pair] += 1
        active = current
    return dict(episodes)


def summarize(lines):
    starts = 0
    read_errors = []
    observer_summaries = []
    snapshots_by_ts = {}
    malformed = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except (TypeError, ValueError):
            malformed += 1
            continue
        kind = record.get("kind")
        if kind == "start":
            starts += 1
        elif kind == "snapshot" and record.get("ts") is not None:
            snapshots_by_ts[int(record["ts"])] = record
        elif kind == "read_error":
            read_errors.append(record)
        elif kind == "summary":
            observer_summaries.append(record)

    snapshots = [snapshots_by_ts[key] for key in sorted(snapshots_by_ts)]
    rows = [row for record in snapshots for row in record.get("rows") or []]
    health_errors = sum(
        bool((record.get("health") or {}).get("error")) for record in snapshots
    )

    def spread(row):
        return bool(row.get("spread_gate"))

    def volume(row):
        return spread(row) and bool(row.get("volume_gate"))

    def intra(row):
        return volume(row) and bool(row.get("intra_gate"))

    def stable(row):
        return intra(row) and bool(row.get("stable_regime"))

    def candidate(row):
        return stable(row) and bool(row.get("deviation_gate"))

    gates = (
        ("discovery", lambda _: True),
        ("spread", spread),
        ("spread_volume", volume),
        ("spread_volume_intra", intra),
        ("stable", stable),
        ("l1_candidate", candidate),
    )
    funnel = {
        name: {
            "rows": sum(predicate(row) for row in rows),
            "snapshots": sum(
                any(predicate(row) for row in record.get("rows") or [])
                for record in snapshots
            ),
            "pairs": sorted(
                {
                    row.get("pair") or "?"
                    for row in rows
                    if predicate(row)
                }
            ),
        }
        for name, predicate in gates
    }

    liquid = [row for row in rows if intra(row)]
    stable_rows = [row for row in rows if stable(row)]
    unstable_economic = [
        row
        for row in liquid
        if not row.get("stable_regime") and row.get("deviation_gate")
    ]
    counterfactual = {}
    for threshold in COUNTERFACTUAL_THRESHOLDS:
        accepted = [
            row
            for row in stable_rows
            if float(row.get("net_excursion_pct") or -999.0) >= threshold
        ]
        counterfactual[f"{threshold:.2f}"] = {
            "rows": len(accepted),
            "snapshots": sum(
                any(
                    stable(row)
                    and float(row.get("net_excursion_pct") or -999.0) >= threshold
                    for row in record.get("rows") or []
                )
                for record in snapshots
            ),
            "pairs": sorted({row.get("pair") or "?" for row in accepted}),
        }

    first_ts = snapshots[0]["ts"] if snapshots else None
    last_ts = snapshots[-1]["ts"] if snapshots else None
    elapsed_ms = last_ts - first_ts if first_ts is not None else 0
    return {
        "observer_starts": starts,
        "malformed_lines": malformed,
        "read_errors": len(read_errors),
        "health_errors": health_errors,
        "observer_summaries": len(observer_summaries),
        "first_snapshot_ms": first_ts,
        "last_snapshot_ms": last_ts,
        "observed_seconds": round(elapsed_ms / 1000.0, 3),
        "unique_snapshots": len(snapshots),
        "rows": len(rows),
        "funnel": funnel,
        "counterfactual_stable_deviation": counterfactual,
        "stable_episodes": episode_counts(snapshots, stable),
        "candidate_episodes": episode_counts(snapshots, candidate),
        "unstable_economic_episodes": episode_counts(
            snapshots,
            lambda row: intra(row)
            and not row.get("stable_regime")
            and row.get("deviation_gate"),
        ),
        "best_liquid_by_pair": best_by_pair(liquid)[:30],
        "best_stable_by_pair": best_by_pair(stable_rows)[:30],
        "best_blocked_unstable_by_pair": best_by_pair(unstable_economic)[:30],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("telemetry", type=Path)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    with args.telemetry.open() as source:
        result = summarize(source)
    print(json.dumps(result, indent=None if args.compact else 2, allow_nan=False))


if __name__ == "__main__":
    main()
