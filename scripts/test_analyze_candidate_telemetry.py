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

    def test_groups_consecutive_health_failures_into_one_outage(self):
        result = summarize(
            [
                line({"kind": "snapshot", "ts": 1_000, "rows": [], "health": {}}),
                line(
                    {
                        "kind": "snapshot",
                        "ts": 2_000,
                        "rows": [],
                        "health": {"error": "HTTPError"},
                    }
                ),
                line(
                    {
                        "kind": "snapshot",
                        "ts": 3_000,
                        "rows": [],
                        "health": {"error": "HTTPError"},
                    }
                ),
                line(
                    {
                        "kind": "snapshot",
                        "ts": 4_500,
                        "rows": [],
                        "health": {"healthy": True},
                    }
                ),
            ]
        )
        self.assertEqual(result["health_errors"], 2)
        self.assertEqual(len(result["readiness_outages"]), 1)
        self.assertEqual(result["readiness_outages"][0]["samples"], 2)
        self.assertEqual(result["readiness_outages"][0]["observed_duration_ms"], 2_500)

    def test_reports_order_lane_outage_when_general_health_stays_true(self):
        result = summarize(
            [
                line(
                    {
                        "kind": "snapshot",
                        "ts": 1_000,
                        "rows": [],
                        "health": {
                            "healthy": True,
                            "order_ws_ready": False,
                            "trading_ready": False,
                        },
                    }
                ),
                line(
                    {
                        "kind": "snapshot",
                        "ts": 2_000,
                        "rows": [],
                        "health": {
                            "healthy": True,
                            "order_ws_ready": True,
                            "trading_ready": True,
                        },
                    }
                ),
            ]
        )
        self.assertEqual(len(result["readiness_outages"]), 1)
        self.assertEqual(
            result["readiness_outages"][0]["errors"],
            ["order_ws_ready=false", "trading_ready=false"],
        )
        self.assertEqual(result["readiness_outages"][0]["observed_duration_ms"], 1_000)

    def test_does_not_report_intentional_terminal_halt_as_outage(self):
        result = summarize(
            [
                line(
                    {
                        "kind": "snapshot",
                        "ts": 1_000,
                        "rows": [],
                        "health": {
                            "healthy": True,
                            "process_healthy": True,
                            "market_ready": True,
                            "private_ready": True,
                            "order_ws_ready": True,
                            "trading_ready": False,
                            "entries_halted": True,
                        },
                    }
                )
            ]
        )
        self.assertEqual(result["readiness_outages"], [])

    def test_summarizes_engine_cycle_tail_latency(self):
        records = []
        for index, cycle_us in enumerate((100, 200, 300, 400, 500), start=1):
            records.append(
                line(
                    {
                        "kind": "snapshot",
                        "ts": index,
                        "rows": [],
                        "health": {"scanner_status": {"engine_cycle_us": cycle_us}},
                    }
                )
            )
        records.append(
            line(
                {
                    "kind": "snapshot",
                    "ts": 6,
                    "rows": [],
                    "health": {"scanner_status": {"engine_cycle_us": None}},
                }
            )
        )
        self.assertEqual(
            summarize(records)["engine_cycle_us"],
            {
                "samples": 5,
                "min": 100.0,
                "p50": 300.0,
                "p95": 500.0,
                "p99": 500.0,
                "max": 500.0,
                "mean": 300.0,
            },
        )


if __name__ == "__main__":
    unittest.main()
