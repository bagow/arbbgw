import unittest
from audit_shadow import summarize_shadow, valid_book


class ShadowTests(unittest.TestCase):
    def test_stale_future_crossed_and_unsorted_books_are_not_executable(self):
        book = {"updated_ms": 1000, "bids": [[100, 1]], "asks": [[101, 1]]}
        self.assertTrue(valid_book(book, 1750))
        self.assertFalse(valid_book(book, 1751))
        self.assertFalse(valid_book(book, 999))
        self.assertFalse(valid_book({**book,"asks":[[99,1]]},1000))
        self.assertFalse(valid_book({**book,"asks":[[102,1],[101,1]]},1000))
        self.assertFalse(valid_book({**book,"asks":[[101,float("nan")]]},1000))

    def test_one_stale_leg_invalidates_the_pair(self):
        book = {"updated_ms":1000,"bids":[[100,1]],"asks":[[101,1]]}
        sample = {"recorded_ms":1000,"spread":{"pair":"BTCUSDT","exchangeA":"binance",
            "exchangeB":"kucoin","structural_verified":True},"book_a":book,"book_b":book}
        report = summarize_shadow([sample,{**sample,"book_a":{**book,"updated_ms":1}}])
        self.assertEqual(report["samples"],2)
        self.assertEqual(report["both_books_fresh_and_valid"],1)
        self.assertEqual(report["both_books_valid_and_structural_verified"],1)
        self.assertEqual(report["stale_or_invalid_by_venue"],{"binance":1})


if __name__ == "__main__":
    unittest.main()
