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
    min_volume_millions REAL,
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
    'real-default',1,NULL,6,10,7,.5,1.2,2.5,2,1,1,.1,5,300,2,0,200,1800000,'[]',1
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

    def test_long_detached_monitor_window_remains_bounded(self):
        self.assertEqual(
            canary.configured_monitor_seconds({"monitor_seconds": 21_600}),
            21_600,
        )
        with self.assertRaisesRegex(RuntimeError, "60..21600"):
            canary.configured_monitor_seconds({"monitor_seconds": 21_601})
        with self.assertRaisesRegex(RuntimeError, "60..21600"):
            canary.configured_monitor_seconds({"monitor_seconds": 59})

    def test_canary_configuration_and_resume_are_one_transaction(self):
        config = {
            "notional_usdt": 6.0,
            "leverage": 2,
            "min_volume_millions": 0.5,
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

        canary.configure_and_enable(config, "test-watchdog-token")

        with sqlite3.connect(self.profile_db) as db:
            row = db.execute(
                "SELECT is_active,leverage,max_active_trades,min_structural_deviation,"
                "disabled_exchanges FROM trading_profiles"
            ).fetchone()
            kill = db.execute(
                "SELECT value FROM server_cache WHERE key='kill_switch_active'"
            ).fetchone()[0]
            watchdog = db.execute(
                "SELECT value FROM server_cache WHERE key=?",
                (canary.WATCHDOG_CACHE_KEY,),
            ).fetchone()[0]
        self.assertEqual(row[:4], (1, 2, 1, 0.5))
        self.assertEqual(json.loads(row[4]), canary.DISABLED)
        self.assertEqual(kill, "0")
        self.assertEqual(watchdog, "test-watchdog-token")

    def test_restore_refuses_to_overwrite_newer_watchdog_lease(self):
        snapshot = canary.snapshot_profile()
        with sqlite3.connect(self.profile_db) as db:
            db.execute("UPDATE trading_profiles SET is_active=1")
            db.execute("UPDATE server_cache SET value='0' WHERE key='kill_switch_active'")
            db.execute(
                "INSERT INTO server_cache VALUES(?,?,0)",
                (canary.WATCHDOG_CACHE_KEY, "newer-token"),
            )

        with self.assertRaisesRegex(RuntimeError, "newer run"):
            canary.restore_profile_halted(snapshot, "stale-token")

        with sqlite3.connect(self.profile_db) as db:
            active = db.execute(
                "SELECT is_active FROM trading_profiles WHERE id='real-default'"
            ).fetchone()[0]
            kill = db.execute(
                "SELECT value FROM server_cache WHERE key='kill_switch_active'"
            ).fetchone()[0]
        self.assertEqual(active, 1)
        self.assertEqual(kill, "0")

    @mock.patch.object(canary.subprocess, "Popen")
    @mock.patch.object(canary.secrets, "token_hex", return_value="watchdog-token")
    def test_watchdog_is_detached_before_returning_token(self, _token_hex, popen):
        token = canary.arm_fail_closed_watchdog(2036, 120)

        self.assertEqual(token, "watchdog-token")
        args, kwargs = popen.call_args
        self.assertEqual(args[0][:2], [canary.sys.executable, "-c"])
        self.assertIn("2036", args[0][2])
        self.assertTrue(kwargs["start_new_session"])
        self.assertTrue(kwargs["close_fds"])

    def test_watchdog_halts_profile_and_requests_close(self):
        trade_db = Path(self.tempdir.name) / "trades.db"
        with sqlite3.connect(trade_db) as db:
            db.executescript(
                "CREATE TABLE trades(id INTEGER PRIMARY KEY,status TEXT,manual_close_requested INTEGER);"
                "INSERT INTO trades VALUES(2037,'OPEN',0);"
            )
        with sqlite3.connect(self.profile_db) as db:
            db.execute("UPDATE trading_profiles SET is_active=1")
            db.execute("UPDATE server_cache SET value='0' WHERE key='kill_switch_active'")
            db.execute(
                "INSERT INTO server_cache VALUES(?,?,0)",
                (canary.WATCHDOG_CACHE_KEY, "owned-token"),
            )

        with mock.patch.object(canary, "TRADE_DB", trade_db):
            exec(canary.watchdog_program("owned-token", 2036, 0), {})

        with sqlite3.connect(self.profile_db) as db:
            active = db.execute(
                "SELECT is_active FROM trading_profiles WHERE id='real-default'"
            ).fetchone()[0]
            kill = db.execute(
                "SELECT value FROM server_cache WHERE key='kill_switch_active'"
            ).fetchone()[0]
            token = db.execute(
                "SELECT value FROM server_cache WHERE key=?",
                (canary.WATCHDOG_CACHE_KEY,),
            ).fetchone()
        with sqlite3.connect(trade_db) as db:
            close_requested = db.execute(
                "SELECT manual_close_requested FROM trades WHERE id=2037"
            ).fetchone()[0]
        self.assertEqual(active, 0)
        self.assertEqual(kill, "1")
        self.assertIsNone(token)
        self.assertEqual(close_requested, 1)


if __name__ == "__main__":
    unittest.main()
