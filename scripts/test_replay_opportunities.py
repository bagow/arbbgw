import unittest

import replay_opportunities as replay


class ReplayOpportunityTests(unittest.TestCase):
    def test_quote_requires_full_sorted_depth(self):
        self.assertEqual(replay.quote([[10.0, 1.0], [11.0, 1.0]], 2.0, True)["vwap"], 10.5)
        self.assertIsNone(replay.quote([[10.0, 1.0]], 2.0, True))
        self.assertIsNone(replay.quote([[11.0, 1.0], [10.0, 1.0]], 2.0, True))

    def test_common_grid_matches_both_contract_steps(self):
        self.assertEqual(replay.common_quantity_step(100.0, 1.0), 100.0)
        self.assertEqual(replay.common_quantity_step(0.01, 0.001), 0.01)
        self.assertEqual(replay.lot_size(6.0, 0.0035, 100.0, 1.0, 15.0), 1700.0)

    def test_windows_require_duration_and_separate_routes(self):
        route = ("TESTUSDT", "kucoin", "binance", "binance>kucoin")
        events = [
            {"route": route, "recorded_ms": 1_000},
            {"route": route, "recorded_ms": 2_000},
            {"route": route, "recorded_ms": 10_000},
        ]
        windows = replay.group_windows(events, 5_000)
        self.assertEqual([len(rows) for _, rows in windows], [2, 1])

    def test_break_even_reserves_four_fees_and_slippage_legs(self):
        self.assertAlmostEqual(replay.break_even_pct(0.0006, 0.0005), 0.30)

    def test_crossing_cost_reserves_both_internal_spreads(self):
        cost = replay.crossing_cost_pct(True, 101.0, 100.0, 101.2, 99.8)
        self.assertAlmostEqual(cost, 0.4 / 101.0 * 100)

    def test_confirmation_requires_a_non_widening_future_sample(self):
        rows = [
            {"recorded_ms": 1_000, "gross_pct": 1.0},
            {"recorded_ms": 1_600, "gross_pct": 1.1},
            {"recorded_ms": 2_200, "gross_pct": 0.9},
        ]
        self.assertEqual(replay.confirmed_entry(rows, 500), rows[2])

    def test_exit_valuation_includes_all_four_fees(self):
        entry = {
            "route": ("TESTUSDT", "kucoin", "binance", "kucoin>binance"),
            "recorded_ms": 1_000,
            "entry_a": 101.0,
            "entry_b": 100.0,
            "quantity": 1.0,
            "fee_a": 0.001,
            "fee_b": 0.001,
            "is_a_higher": True,
        }
        sample = {
            "recorded_ms": 2_000,
            "spread": {
                "pair": "TESTUSDT",
                "exchangeA": "kucoin",
                "exchangeB": "binance",
            },
            "book_a": {
                "updated_ms": 2_000,
                "bids": [[100.4, 2.0]],
                "asks": [[100.5, 2.0]],
            },
            "book_b": {
                "updated_ms": 2_000,
                "bids": [[100.5, 2.0]],
                "asks": [[100.6, 2.0]],
            },
        }
        value = replay.exit_valuation(entry, sample)
        self.assertAlmostEqual(value["average"]["net_usdt"], 0.598, places=9)

    def test_full_book_validation_rejects_bad_deeper_level(self):
        book = {
            "updated_ms": 1_000,
            "bids": [[10.0, 1.0], [10.5, 1.0]],
            "asks": [[11.0, 1.0], [12.0, 1.0]],
        }
        self.assertFalse(replay.valid_book(book, 1_000))

    def test_sequential_cycles_do_not_double_count_one_live_pair(self):
        cycles = [
            {"pair": "A", "entry_ms": 1_000, "exit_ms": 5_000, "outcome": "TP"},
            {"pair": "A", "entry_ms": 2_000, "exit_ms": 4_000, "outcome": "TP"},
            {"pair": "B", "entry_ms": 2_000, "exit_ms": 4_000, "outcome": "TP"},
            {"pair": "A", "entry_ms": 66_000, "exit_ms": None, "outcome": "UNRESOLVED"},
        ]
        selected = replay.select_sequential_cycles(cycles, 1_800_000)
        self.assertEqual(
            [(row["pair"], row["entry_ms"]) for row in selected],
            [("A", 1_000), ("B", 2_000), ("A", 66_000)],
        )


if __name__ == "__main__":
    unittest.main()
