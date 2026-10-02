import io
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from capture_canary_candidates import classify, health, supervisor_running


CONFIG = {
    "trade_volume": 6.0,
    "min_entry_spread": 0.5,
    "max_entry_spread": 5.0,
    "min_volume": 500_000.0,
    "min_structural_deviation": 0.45,
    "min_depth_percent": 500.0,
    "intra_spread_max_pct": 0.2,
    "default_fee_rate": 0.0005,
}


def row(**updates):
    value = {
        "pair": "ONEUSDT",
        "exchangeA": "binance",
        "exchangeB": "kucoin",
        "bidA": 101.0,
        "askA": 101.1,
        "bidB": 99.9,
        "askB": 100.0,
        "bidSizeAUsd": 40.0,
        "askSizeAUsd": 40.0,
        "bidSizeBUsd": 40.0,
        "askSizeBUsd": 40.0,
        "sourceMsA": 1_000,
        "sourceMsB": 1_025,
        "spread": 1.0,
        "structural_spread": 0.2,
        "structural_verified": True,
        "has_structural_spread": True,
        "structural_min": 0.1,
        "structural_max": 0.3,
        "volA": 1_000_000.0,
        "volB": 1_000_000.0,
    }
    value.update(updates)
    return value


class CandidateClassificationTests(unittest.TestCase):
    def test_full_candidate_reserves_fees_and_expected_slippage(self):
        result = classify(
            row(), {"binance:ONEUSDT": 0.0005, "kucoin:ONEUSDT": 0.0006}, CONFIG
        )
        self.assertAlmostEqual(result["break_even_pct"], 0.3)
        self.assertAlmostEqual(result["net_excursion_pct"], 0.5)
        self.assertEqual(result["source_skew_ms"], 25)
        self.assertTrue(result["top_depth_gate"])
        self.assertTrue(result["l1_candidate"])

    def test_direction_flip_blocks_large_apparent_edge(self):
        result = classify(
            row(
                spread=2.0,
                has_structural_spread=False,
                structural_min=-0.2,
                structural_max=1.0,
            ),
            {},
            CONFIG,
        )
        self.assertTrue(result["deviation_gate"])
        self.assertFalse(result["stable_regime"])
        self.assertFalse(result["l1_candidate"])

    def test_wide_internal_market_is_not_a_candidate(self):
        result = classify(row(askB=100.3), {}, CONFIG)
        self.assertFalse(result["intra_gate"])
        self.assertFalse(result["l1_candidate"])


class HealthCaptureTests(unittest.TestCase):
    def test_preserves_fail_closed_503_body(self):
        body = {
            "healthy": False,
            "process_healthy": True,
            "market_ready": False,
            "private_ready": True,
            "order_ws_ready": True,
            "trading_ready": False,
            "venues": [{"exchange": "binance", "fresh_books": 0}],
        }
        error = urllib.error.HTTPError(
            "http://127.0.0.1:3003/health",
            503,
            "Service Unavailable",
            None,
            io.BytesIO(json.dumps(body).encode()),
        )
        with patch("urllib.request.urlopen", side_effect=error):
            result = health()
        self.assertEqual(result["http_status"], 503)
        self.assertFalse(result["healthy"])
        self.assertFalse(result["market_ready"])
        self.assertEqual(result["venues"], body["venues"])

    def test_reports_malformed_503_body_without_hiding_status(self):
        error = urllib.error.HTTPError(
            "http://127.0.0.1:3003/health",
            503,
            "Service Unavailable",
            None,
            io.BytesIO(b"not-json"),
        )
        with patch("urllib.request.urlopen", side_effect=error):
            result = health()
        self.assertEqual(result["error"], "HTTPError")
        self.assertEqual(result["http_status"], 503)
        self.assertEqual(result["body_error"], "JSONDecodeError")


class SupervisorLivenessTests(unittest.TestCase):
    def test_uses_supervisor_runner_pid_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_dir = root / "run"
            proc_root = root / "proc"
            run_dir.mkdir()
            proc_root.mkdir()
            (run_dir / "pid").write_text("999")
            self.assertIsNone(supervisor_running(run_dir, proc_root))

            (run_dir / "runner.pid").write_text("123")
            self.assertFalse(supervisor_running(run_dir, proc_root))
            (proc_root / "123").mkdir()
            self.assertTrue(supervisor_running(run_dir, proc_root))

    def test_malformed_runner_pid_is_not_treated_as_alive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_dir = root / "run"
            run_dir.mkdir()
            (run_dir / "runner.pid").write_text("not-a-pid")
            self.assertFalse(supervisor_running(run_dir, root / "proc"))


if __name__ == "__main__":
    unittest.main()
