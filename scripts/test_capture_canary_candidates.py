import unittest

from capture_canary_candidates import classify


CONFIG = {
    "min_entry_spread": 0.5,
    "max_entry_spread": 5.0,
    "min_volume": 500_000.0,
    "min_structural_deviation": 0.45,
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


if __name__ == "__main__":
    unittest.main()
