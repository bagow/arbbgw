import json
import unittest

from analyze_candidate_telemetry import summarize


def row(pair, net, stable=True, **updates):
    value = {
        "pair": pair,
        "spread_gate": True,
        "volume_gate": True,
        "intra_gate": True,
        "stable_regime": stable,
        "deviation_gate": net >= 0.45,
        "l1_candidate": stable and net >= 0.45,
        "net_excursion_pct": net,
        "gap_to_entry_pct": net - 0.45,
    }
    value.update(updates)
    return value


def line(value):
    return json.dumps(value) + "\n"


class CandidateTelemetrySummaryTests(unittest.TestCase):
    def test_deduplicates_snapshots_across_observer_restarts(self):
        rows = [row("ONEUSDT", 0.5)]
        result = summarize(
            [
                line({"kind": "start"}),
                line({"kind": "snapshot", "ts": 1_000, "rows": rows, "health": {}}),
                line({"kind": "start"}),
                line({"kind": "snapshot", "ts": 1_000, "rows": rows, "health": {}}),
            ]
        )
        self.assertEqual(result["observer_starts"], 2)
        self.assertEqual(result["unique_snapshots"], 1)
        self.assertEqual(result["funnel"]["l1_candidate"]["rows"], 1)

    def test_funnel_separates_unstable_economic_edge(self):
        result = summarize(
            [
                line(
                    {
                        "kind": "snapshot",
                        "ts": 1_000,
                        "rows": [row("ARKUSDT", 0.7, stable=False)],
                        "health": {},
                    }
                ),
                line(
                    {
                        "kind": "snapshot",
                        "ts": 2_000,
                        "rows": [row("ONEUSDT", 0.4)],
                        "health": {},
                    }
                ),
            ]
        )
        self.assertEqual(result["funnel"]["spread_volume_intra"]["rows"], 2)
        self.assertEqual(result["funnel"]["stable"]["rows"], 1)
        self.assertEqual(result["funnel"]["l1_candidate"]["rows"], 0)
        self.assertEqual(result["unstable_economic_episodes"], {"ARKUSDT": 1})
        self.assertEqual(
            result["counterfactual_stable_deviation"]["0.40"]["rows"], 1
        )

    def test_counts_new_episode_after_absence(self):
        result = summarize(
            [
                line({"kind": "snapshot", "ts": 1, "rows": [row("ONE", 0.5)]}),
                line({"kind": "snapshot", "ts": 2, "rows": []}),
                line({"kind": "snapshot", "ts": 3, "rows": [row("ONE", 0.5)]}),
            ]
        )
        self.assertEqual(result["candidate_episodes"], {"ONE": 2})


if __name__ == "__main__":
    unittest.main()
