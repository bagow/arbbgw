import unittest
from audit_shadow import recorded_demand, summarize_shadow, valid_book


class ShadowTests(unittest.TestCase):
    def test_stale_future_crossed_and_unsorted_books_are_not_executable(self):
        book = {"updated_ms": 1000, "bids": [[100, 1]], "asks": [[101, 1]]}
        self.assertTrue(valid_book(book, 1750))
        self.assertFalse(valid_book(book, 1751))
        self.assertFalse(valid_book(book, 999))
        self.assertFalse(valid_book({**book,"asks":[[99,1]]},1000))
        self.assertFalse(valid_book({**book,"asks":[[102,1],[101,1]]},1000))
        self.assertFalse(valid_book({**book,"asks":[[101,float("nan")]]},1000))
        self.assertFalse(valid_book(None, 1000))

    def test_one_stale_leg_invalidates_the_pair(self):
        book = {"updated_ms":1000,"bids":[[100,1]],"asks":[[101,1]]}
        sample = {"recorded_ms":1000,"spread":{"pair":"BTCUSDT","exchangeA":"binance",
            "exchangeB":"kucoin","structural_verified":True},"book_a":book,"book_b":book}
        report = summarize_shadow([sample,{**sample,"book_a":{**book,"updated_ms":1}}])
        self.assertEqual(report["samples"],2)
        self.assertEqual(report["both_books_fresh_and_valid"],1)
        self.assertEqual(report["both_books_valid_and_structural_verified"],1)
        self.assertEqual(report["stale_or_invalid_by_venue"],{"binance":1})
        self.assertEqual(report["by_recorded_demand"]["unknown"]["samples"], 2)
        self.assertIsNone(report["by_recorded_demand"]["both_requested"]["fresh_and_valid_percent"])

    def test_cached_books_do_not_dilute_requested_route_quality(self):
        book = {"updated_ms": 1000, "bids": [[100, 1]], "asks": [[101, 1]], "requested": True}
        sample = {"recorded_ms": 1000, "spread": {"pair": "BTCUSDT", "exchangeA": "binance",
            "exchangeB": "kucoin", "structural_verified": True}, "book_a": book, "book_b": book}
        stale = {**book, "updated_ms": 1, "requested": False}
        cached = {**sample, "book_a": stale, "book_b": stale}
        partial = {**sample, "book_b": stale}
        report = summarize_shadow([sample, cached, partial])
        groups = report["by_recorded_demand"]
        self.assertEqual(report["samples"], 3)
        self.assertEqual(report["both_books_fresh_and_valid"], 1)
        self.assertEqual(groups["both_requested"]["samples"], 1)
        self.assertEqual(groups["both_requested"]["fresh_and_valid_percent"], 100.0)
        self.assertEqual(groups["both_requested"]["stale_or_invalid_by_venue"], {})
        self.assertEqual(groups["neither_requested"]["stale_or_invalid_by_venue"], {"binance": 1, "kucoin": 1})
        self.assertEqual(groups["one_requested"]["stale_or_invalid_by_venue"], {"kucoin": 1})
        self.assertEqual(sum(group["samples"] for group in groups.values()), report["samples"])

    def test_unknown_demand_is_not_guessed_from_freshness_or_partial_metadata(self):
        for flags in ((None, None), (True, None), (False, None), (1, True), ("false", False)):
            sample = {"book_a": {"requested": flags[0]}, "book_b": {"requested": flags[1]}}
            self.assertEqual(recorded_demand(sample), "unknown")
        self.assertEqual(recorded_demand({}), "unknown")
        self.assertEqual(recorded_demand({"book_a": None, "book_b": None}), "unknown")
        self.assertEqual(recorded_demand({"book_a": [], "book_b": "invalid"}), "unknown")

    def test_empty_report_does_not_claim_zero_percent_feed_health(self):
        report = summarize_shadow([])
        for group in report["by_recorded_demand"].values():
            self.assertEqual(group["samples"], 0)
            self.assertIsNone(group["fresh_and_valid_percent"])


if __name__ == "__main__":
    unittest.main()
