import unittest

from smoke_core_server import longest_unready_run, market_sample_ready


def sample(binance, kucoin):
    return {"fresh_books": {"binance": binance, "kucoin": kucoin}}


class SmokeCoreServerTests(unittest.TestCase):
    def test_market_sample_requires_both_venues(self):
        self.assertTrue(market_sample_ready(sample(1, 2)))
        self.assertFalse(market_sample_ready(sample(1, 0)))

    def test_longest_unready_run(self):
        samples = [sample(1, 1), sample(0, 1), sample(1, 0), sample(1, 1), sample(0, 0)]
        self.assertEqual(longest_unready_run(samples), 2)


if __name__ == "__main__":
    unittest.main()
