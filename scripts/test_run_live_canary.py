import importlib.util
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "run_live_canary",
    Path(__file__).with_name("run_live_canary.py"),
)
CANARY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CANARY)


class MarketReadinessObservationTests(unittest.TestCase):
    def test_outage_is_recorded_until_recovery(self):
        report = {}
        gap = 0
        for _ in range(35):
            gap = CANARY.observe_market_readiness(report, gap, False)

        self.assertEqual(gap, 35)
        self.assertEqual(report["market_readiness_outages"], 1)
        self.assertEqual(report["market_readiness_max_gap_samples"], 35)

        gap = CANARY.observe_market_readiness(report, gap, True)

        self.assertEqual(gap, 0)
        self.assertEqual(report["market_readiness_recoveries"], 1)
        self.assertEqual(report["last_market_readiness_gap_samples"], 35)

    def test_separate_outages_are_counted(self):
        report = {}
        gap = CANARY.observe_market_readiness(report, 0, False)
        gap = CANARY.observe_market_readiness(report, gap, True)
        gap = CANARY.observe_market_readiness(report, gap, False)

        self.assertEqual(gap, 1)
        self.assertEqual(report["market_readiness_outages"], 2)

    def test_private_outage_is_recorded_until_recovery(self):
        report = {}
        gap = 0
        for _ in range(7):
            gap = CANARY.observe_private_readiness(report, gap, False)
        self.assertEqual(gap, 7)
        self.assertEqual(report["private_readiness_outages"], 1)
        self.assertEqual(report["private_readiness_max_gap_samples"], 7)
        gap = CANARY.observe_private_readiness(report, gap, True)
        self.assertEqual(gap, 0)
        self.assertEqual(report["private_readiness_recoveries"], 1)


if __name__ == "__main__":
    unittest.main()
