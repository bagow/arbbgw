from pathlib import Path
import sqlite3
import tempfile
import unittest
from smoke_maintenance import verify_inactive


class MaintenanceGuardTests(unittest.TestCase):
    def test_only_inactive_empty_engine_is_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with sqlite3.connect(root/'vps-api.db') as db:
                db.execute('CREATE TABLE trading_profiles(is_active,pending_commands)')
                db.execute('INSERT INTO trading_profiles VALUES(0,NULL)')
            with sqlite3.connect(root/'real-trader.db') as db:
                db.execute('CREATE TABLE trades(status)')
                db.execute('CREATE TABLE execution_orders(id)')
            verify_inactive(root)
            for path, insert, clear in [
                ('vps-api.db','UPDATE trading_profiles SET is_active=1','UPDATE trading_profiles SET is_active=0'),
                ('vps-api.db',"UPDATE trading_profiles SET pending_commands='close'",'UPDATE trading_profiles SET pending_commands=NULL'),
                ('real-trader.db',"INSERT INTO trades VALUES('RECONCILING')",'DELETE FROM trades'),
                ('real-trader.db','INSERT INTO execution_orders VALUES(1)','DELETE FROM execution_orders'),
            ]:
                with sqlite3.connect(root/path) as db: db.execute(insert)
                with self.assertRaises(RuntimeError): verify_inactive(root)
                with sqlite3.connect(root/path) as db: db.execute(clear)
