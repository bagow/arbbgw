import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import run_live_canary as canary


PROFILE_SCHEMA = """
CREATE TABLE trading_profiles(
    id TEXT PRIMARY KEY,
    is_active INTEGER,
    pending_commands TEXT,
    trade_volume REAL,
    leverage INTEGER,
    max_active_trades INTEGER,
    min_entry_spread REAL,
    max_entry_spread REAL,
    take_profit_percent REAL,
    trailing_stop_percent REAL,
    hard_stop_percent REAL,
    trailing_activation_percent REAL,
    daily_loss_limit_usdt REAL,
    max_total_exposure_usdt REAL,
    slippage_percent REAL,
    min_structural_deviation REAL,
    min_depth_percent REAL,
    max_holding_time_ms INTEGER,
    disabled_exchanges TEXT,
    updated_at INTEGER
);
CREATE TABLE server_cache(key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER);
CREATE TABLE technical_settings(key TEXT PRIMARY KEY, value TEXT);
INSERT INTO trading_profiles VALUES(
    'real-default',1,NULL,6,10,7,1.2,2.5,2,1,1,.1,5,300,2,0,200,1800000,'[]',1
);
INSERT INTO server_cache VALUES('kill_switch_active','1',0);
INSERT INTO technical_settings VALUES('margin_check_multiplier','1.5');
"""


class CanarySafetyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.profile_db = Path(self.tempdir.name) / "profiles.db"
        with sqlite3.connect(self.profile_db) as db:
            db.executescript(PROFILE_SCHEMA)
        self.patch = mock.patch.object(canary, "PROFILE_DB", self.profile_db)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tempdir.cleanup()

    def test_restore_always_leaves_profile_inactive_and_killed(self):
        snapshot = canary.snapshot_profile()
        with sqlite3.connect(self.profile_db) as db:
            db.execute("UPDATE trading_profiles SET is_active=1,trade_volume=99")
            db.execute("UPDATE server_cache SET value='0' WHERE key='kill_switch_active'")

        canary.restore_profile_halted(snapshot)

        with sqlite3.connect(self.profile_db) as db:
            active, volume = db.execute(
                "SELECT is_active,trade_volume FROM trading_profiles"
            ).fetchone()
            kill = db.execute(
                "SELECT value FROM server_cache WHERE key='kill_switch_active'"
            ).fetchone()[0]
        self.assertEqual(active, 0)
        self.assertEqual(volume, 6)
        self.assertEqual(kill, "1")

    def test_canary_configuration_and_resume_are_one_transaction(self):
        config = {
            "notional_usdt": 6.0,
            "leverage": 2,
            "min_entry_spread": 1.2,
            "max_entry_spread": 2.5,
            "take_profit_percent": 0.2,
            "trailing_stop_percent": 0.1,
            "hard_stop_percent": 0.5,
            "trailing_activation_percent": 0.1,
            "daily_loss_limit_usdt": 0.1,
            "max_total_exposure_usdt": 7.0,
            "slippage_percent": 0.05,
            "min_structural_deviation": 0.5,
            "min_depth_percent": 500.0,
            "max_holding_time_ms": 600_000,
        }

        canary.configure_and_enable(config)

        with sqlite3.connect(self.profile_db) as db:
            row = db.execute(
                "SELECT is_active,leverage,max_active_trades,min_structural_deviation,"
                "disabled_exchanges FROM trading_profiles"
            ).fetchone()
            kill = db.execute(
                "SELECT value FROM server_cache WHERE key='kill_switch_active'"
            ).fetchone()[0]
        self.assertEqual(row[:4], (1, 2, 1, 0.5))
        self.assertEqual(json.loads(row[4]), canary.DISABLED)
        self.assertEqual(kill, "0")


if __name__ == "__main__":
    unittest.main()
