import sqlite3
import tempfile
import unittest
from pathlib import Path

from audit_performance import audit, summarize


def row(i, pnl, status="CLOSED", accounting="VERIFIED"):
    return dict(id=i, pnl=pnl, status=status, accounting_status=accounting, exit_time=i)


class PerformanceTests(unittest.TestCase):
    def test_majority_wins_can_still_lose_money(self):
        report = summarize([row(1, 1), row(2, 1), row(3, -10)])
        self.assertGreater(report["win_rate_percent"], 50)
        self.assertEqual(report["net_pnl_usdt"], -8)
        self.assertEqual(report["profit_factor"], 0.2)
        self.assertEqual(report["realized_pnl_max_drawdown_usdt"], 10)

    def test_pending_legacy_and_open_are_not_profits_or_losses(self):
        report = summarize([
            row(1, 1000, accounting="LEGACY_UNVERIFIED"),
            row(2, None, accounting="FEES_PENDING"),
            row(3, 100, status="OPEN"), row(4, 0), row(5, float("inf")),
        ])
        self.assertEqual(report["verified_closed_count"], 1)
        self.assertEqual(report["wins"], 0)
        self.assertEqual(report["losses"], 0)
        self.assertEqual(report["breakeven"], 1)
        self.assertEqual(report["active_count"], 1)
        self.assertEqual(sum(report["excluded_closed"].values()), 3)

    def test_no_evidence_is_not_zero_profit(self):
        report = summarize([])
        self.assertIsNone(report["net_pnl_usdt"])
        self.assertIsNone(report["win_rate_percent"])

    def test_database_is_read_only_and_profile_is_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.db"
            with self.assertRaises(sqlite3.OperationalError):
                audit(path)
            self.assertFalse(path.exists())
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE trades(id,status,accounting_status,pnl,exit_time,profile_id)")
                db.execute("INSERT INTO trades VALUES(1,'CLOSED','VERIFIED',2,1,'test')")
            before = path.read_bytes()
            self.assertEqual(audit(path, "test")["net_pnl_usdt"], 2)
            self.assertIsNone(audit(path, "' OR 1=1 --")["net_pnl_usdt"])
            self.assertEqual(before, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
