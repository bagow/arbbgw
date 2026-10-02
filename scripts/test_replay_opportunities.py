import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import replay_opportunities as replay


class ReplayOpportunityTests(unittest.TestCase):
    def test_database_grid_streams_build_filtered_samples_once(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "shadow.db"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE book_samples(id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL)")
                for build in ("old", "new", "new"):
                    db.execute(
                        "INSERT INTO book_samples(snapshot) VALUES(?)",
                        (json.dumps({"build_sha": build}),),
                    )

            seen = []

            def reject(sample, profile):
                seen.append((sample["_replay_index"], profile["label"]))
                return None, "test_rejection"

            profiles = {
                "a": {"label": "a", "max_holding_time_ms": 1_000},
                "b": {"label": "b", "max_holding_time_ms": 1_000},
            }
            with mock.patch.object(replay, "evaluate", side_effect=reject):
                report = replay.summarize_database_profiles(database, "new", profiles)

            self.assertEqual(seen, [(1, "a"), (1, "b"), (2, "a"), (2, "b")])
            self.assertEqual(report["a"]["samples"], 2)
            self.assertEqual(report["b"]["reasons"], {"test_rejection": 2})

    def test_database_grid_reopens_stream_only_for_confirmed_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "shadow.db"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE book_samples(id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL)")
                for recorded_ms in (1_000, 1_250, 1_500):
                    db.execute(
                        "INSERT INTO book_samples(snapshot) VALUES(?)",
                        (json.dumps({"recorded_ms": recorded_ms}),),
                    )

            route = ("TESTUSDT", "kucoin", "binance", "kucoin>binance")

            def eligible(sample, _profile):
                stamp = sample["recorded_ms"]
                return {
                    "route": route,
                    "recorded_ms": stamp,
                    "gross_pct": 1.0,
                    "headroom_pct": 0.1,
                    "book_a_updated_ms": stamp,
                    "book_b_updated_ms": stamp,
                }, "eligible"

            def exit_result(samples, entry, _profile):
                self.assertEqual([row["_replay_index"] for row in samples], [0, 1, 2])
                self.assertEqual(entry["recorded_ms"], 1_500)
                return {
                    "outcome": "TP",
                    "exit_ms": 1_500,
                    "net_pct": 0.1,
                    "net_usdt": 0.006,
                    "best_pct": 0.1,
                    "worst_pct": 0.1,
                }

            profile = {"max_holding_time_ms": 1_000}
            with (
                mock.patch.object(replay, "evaluate", side_effect=eligible),
                mock.patch.object(replay, "simulate_exit", side_effect=exit_result) as simulated,
            ):
                report = replay.summarize_database_profiles(
                    database,
                    None,
                    {"profile": profile},
                )["profile"]

            self.assertEqual(simulated.call_count, 1)
            self.assertEqual(report["confirmed_cycle_outcomes"], {"TP": 1})
            self.assertEqual(report["durable_windows"][0]["samples"], 3)

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

    def test_structural_regime_requires_persistent_verified_direction(self):
        stable = {
            "structural_verified": True,
            "has_structural_spread": True,
            "structural_min": 0.1409,
            "structural_max": 4.6924,
        }
        self.assertTrue(replay.directional_structural_regime_is_stable(stable))
        self.assertFalse(replay.directional_structural_regime_is_stable({
            **stable,
            "has_structural_spread": False,
            "structural_min": -5.1507,
        }))
        self.assertFalse(replay.directional_structural_regime_is_stable({
            **stable,
            "structural_max": float("nan"),
        }))

    def test_crossing_cost_reserves_both_internal_spreads(self):
        cost = replay.crossing_cost_pct(True, 101.0, 100.0, 101.2, 99.8)
        self.assertAlmostEqual(cost, 0.4 / 101.0 * 100)

    def test_initial_liquidation_cannot_begin_inside_hard_stop(self):
        immediate = replay.initial_liquidation_net_pct(
            True, 101.0, 100.0, 101.2, 99.8, 1.0, 0.0005, 0.0005
        )
        self.assertLess(immediate, -0.4)
        self.assertLessEqual(
            immediate,
            -0.4 + 2 * 0.05,
        )
        self.assertFalse(
            replay.hard_stop_entry_is_compatible(immediate, 0.4, 0.05)
        )
        self.assertTrue(
            replay.hard_stop_entry_is_compatible(immediate, 0.7, 0.05)
        )

    def test_confirmation_requires_a_non_widening_future_sample(self):
        rows = [
            {"recorded_ms": 1_000, "gross_pct": 1.0, "book_a_updated_ms": 990, "book_b_updated_ms": 995},
            {"recorded_ms": 1_200, "gross_pct": 1.0, "book_a_updated_ms": 1_190, "book_b_updated_ms": 1_195},
            {"recorded_ms": 1_400, "gross_pct": 1.1, "book_a_updated_ms": 1_390, "book_b_updated_ms": 1_395},
            {"recorded_ms": 1_500, "gross_pct": 1.1, "book_a_updated_ms": 1_490, "book_b_updated_ms": 1_495},
            {"recorded_ms": 1_700, "gross_pct": 1.0, "book_a_updated_ms": 1_690, "book_b_updated_ms": 1_695},
            {"recorded_ms": 1_900, "gross_pct": 0.9, "book_a_updated_ms": 1_890, "book_b_updated_ms": 1_895},
            {"recorded_ms": 2_000, "gross_pct": 0.9, "book_a_updated_ms": 1_990, "book_b_updated_ms": 1_995},
        ]
        self.assertEqual(replay.confirmed_entry(rows, 500), rows[6])

    def test_confirmation_rejects_sparse_or_one_sided_book_updates(self):
        sparse = [
            {"recorded_ms": 1_000, "gross_pct": 1.0, "book_a_updated_ms": 990, "book_b_updated_ms": 995},
            {"recorded_ms": 1_600, "gross_pct": 0.9, "book_a_updated_ms": 1_590, "book_b_updated_ms": 1_595},
        ]
        self.assertIsNone(replay.confirmed_entry(sparse, 500))

        one_sided = [
            {"recorded_ms": 1_000, "gross_pct": 1.0, "book_a_updated_ms": 990, "book_b_updated_ms": 995},
            {"recorded_ms": 1_200, "gross_pct": 0.9, "book_a_updated_ms": 1_190, "book_b_updated_ms": 995},
            {"recorded_ms": 1_400, "gross_pct": 0.9, "book_a_updated_ms": 1_390, "book_b_updated_ms": 995},
            {"recorded_ms": 1_600, "gross_pct": 0.9, "book_a_updated_ms": 1_590, "book_b_updated_ms": 995},
        ]
        self.assertIsNone(replay.confirmed_entry(one_sided, 500))

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

        sample["book_b"]["updated_ms"] = 1_700
        self.assertIsNone(replay.exit_valuation(entry, sample))

    def test_full_book_validation_rejects_bad_deeper_level(self):
        book = {
            "updated_ms": 1_000,
            "bids": [[10.0, 1.0], [10.5, 1.0]],
            "asks": [[11.0, 1.0], [12.0, 1.0]],
        }
        self.assertFalse(replay.valid_book(book, 1_000))

    def test_entry_rejects_books_from_different_market_moments(self):
        sample = {
            "recorded_ms": 2_000,
            "spread": {"exchangeA": "binance", "exchangeB": "kucoin"},
            "book_a": {"updated_ms": 2_000, "bids": [[10.0, 1.0]], "asks": [[11.0, 1.0]]},
            "book_b": {"updated_ms": 1_700, "bids": [[10.0, 1.0]], "asks": [[11.0, 1.0]]},
        }
        event, reason = replay.evaluate(sample, {"max_book_skew_ms": 250})
        self.assertIsNone(event)
        self.assertEqual(reason, "book_skew")

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

    def test_nomination_diagnostics_price_unstable_l1_without_weakening_l2_checks(self):
        sample = {
            "capture_reason": "unstable_l1_nomination",
            "nomination_ms": 1_900,
            "recorded_ms": 2_000,
            "spread": {
                "pair": "TESTUSDT",
                "exchangeA": "kucoin",
                "exchangeB": "binance",
                "direction": "binance>kucoin",
                "spread": 1.0,
                "structural_spread": 0.1,
                "structural_verified": True,
                "has_structural_spread": False,
                "structural_min": -0.2,
                "structural_max": 0.4,
                "volA": 2_000_000.0,
                "volB": 3_000_000.0,
                "askA": 100.0,
                "bidA": 99.8,
                "askB": 101.2,
                "bidB": 101.0,
            },
            "book_a": {
                "updated_ms": 1_990,
                "bids": [[99.8, 2.0]],
                "asks": [[100.0, 2.0]],
            },
            "book_b": {
                "updated_ms": 1_980,
                "bids": [[101.0, 2.0]],
                "asks": [[101.2, 2.0]],
            },
            "spec_a": {
                "fetched_ms": 1_500,
                "min_quantity": 0.01,
                "max_quantity": 100.0,
                "min_notional": 5.0,
                "quantity_step": 0.01,
            },
            "spec_b": {
                "fetched_ms": 1_500,
                "min_quantity": 0.01,
                "max_quantity": 100.0,
                "min_notional": 5.0,
                "quantity_step": 0.01,
            },
            "fee_a": 0.0005,
            "fee_b": 0.0005,
            "fee_a_updated_ms": None,
            "fee_b_updated_ms": None,
        }
        profile = {
            "notional_usdt": 6.0,
            "min_entry_spread": 0.5,
            "max_entry_spread": 5.0,
            "min_structural_deviation": 0.45,
            "min_depth_percent": 500.0,
            "min_volume_usd": 500_000.0,
            "max_intra_spread_pct": 1.0,
            "volume_tolerance_pct": 15.0,
            "hard_stop_percent": 0.4,
            "slippage_percent": 0.05,
            "max_book_skew_ms": 250,
        }
        report = replay.summarize_nomination_diagnostics([sample], profile, 1_000)
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["reasons"], {"priced": 1})
        self.assertEqual(report[0]["priced_samples"], 1)
        self.assertAlmostEqual(report[0]["diagnostic_actual_volume_a_usdt_min"], 6.0)
        self.assertEqual(report[0]["budget_pass_samples"], 1)
        self.assertEqual(report[0]["hard_stop_pass_samples"], 0)
        self.assertEqual(report[0]["both_pass_samples"], 0)
        self.assertAlmostEqual(report[0]["gross_pct_max"], 1.0)
        self.assertAlmostEqual(report[0]["required_pct_max"], 0.83)

        coarse = copy.deepcopy(sample)
        coarse["capture_reason"] = "unstable_l1_nomination"
        coarse["spread"].update({
            "askA": 0.044,
            "bidA": 0.0439,
            "askB": 0.0445,
            "bidB": 0.0444,
        })
        coarse["book_a"] = {
            "updated_ms": 1_990,
            "bids": [[0.0439, 1_000.0]],
            "asks": [[0.044, 1_000.0]],
        }
        coarse["book_b"] = {
            "updated_ms": 1_980,
            "bids": [[0.0444, 1_000.0]],
            "asks": [[0.0445, 1_000.0]],
        }
        coarse["spec_a"].update({
            "quantity_step": 100.0,
            "min_quantity": 100.0,
            "max_quantity": 100_000.0,
        })
        coarse["spec_b"].update({
            "quantity_step": 1.0,
            "min_quantity": 1.0,
            "max_quantity": 100_000.0,
        })
        report = replay.summarize_nomination_diagnostics([coarse], profile, 1_000)
        self.assertEqual(report[0]["reasons"], {"actual_volume": 1})
        self.assertEqual(report[0]["priced_samples"], 0)
        self.assertEqual(report[0]["diagnostic_quantity_min"], 100.0)
        self.assertAlmostEqual(report[0]["diagnostic_actual_volume_a_usdt_min"], 4.4)
        self.assertAlmostEqual(report[0]["diagnostic_actual_volume_b_usdt_min"], 4.44)
        self.assertEqual(report[0]["budget_pass_samples"], 0)
        self.assertEqual(report[0]["hard_stop_pass_samples"], 0)
        self.assertEqual(report[0]["both_pass_samples"], 0)

        sample["capture_reason"] = "low_volume_l1_nomination"
        sample["spread"]["has_structural_spread"] = True
        sample["spread"]["volA"] = 430_000.0
        report = replay.summarize_nomination_diagnostics([sample], profile, 1_000)
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]["capture_reason"], "low_volume_l1_nomination")
        self.assertEqual(report[0]["volume_a"], 430_000.0)
        self.assertEqual(report[0]["reasons"], {"priced": 1})
        self.assertEqual(report[0]["priced_samples"], 1)


if __name__ == "__main__":
    unittest.main()
