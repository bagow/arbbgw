from pathlib import Path
import sqlite3
import tempfile
import unittest
from smoke_maintenance import verify_inactive


class MaintenanceGuardTests(unittest.TestCase):
    def test_only_inactive_settled_engine_is_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with sqlite3.connect(root/'vps-api.db') as db:
                db.execute('CREATE TABLE trading_profiles(is_active,pending_commands)')
                db.execute('INSERT INTO trading_profiles VALUES(0,NULL)')
            with sqlite3.connect(root/'real-trader.db') as db:
                db.execute('CREATE TABLE trades(id INTEGER PRIMARY KEY,status)')
                db.execute('CREATE TABLE execution_orders(trade_id,observation)')
                db.execute("INSERT INTO trades VALUES(1,'CLOSED')")
                db.execute("INSERT INTO execution_orders VALUES(1,'{\"state\":\"Filled\"}')")
            verify_inactive(root)
            for path, insert, clear in [
                ('vps-api.db','UPDATE trading_profiles SET is_active=1','UPDATE trading_profiles SET is_active=0'),
                ('vps-api.db',"UPDATE trading_profiles SET pending_commands='close'",'UPDATE trading_profiles SET pending_commands=NULL'),
                ('real-trader.db',"INSERT INTO trades VALUES(2,'RECONCILING')",'DELETE FROM trades WHERE id=2'),
                ('real-trader.db',"INSERT INTO execution_orders VALUES(1,'{\"state\":\"Unknown\"}')","DELETE FROM execution_orders WHERE json_extract(observation,'$.state')='Unknown'"),
                ('real-trader.db',"INSERT INTO execution_orders VALUES(999,'{\"state\":\"Cancelled\"}')",'DELETE FROM execution_orders WHERE trade_id=999'),
            ]:
                with sqlite3.connect(root/path) as db: db.execute(insert)
                with self.assertRaises(RuntimeError): verify_inactive(root)
                with sqlite3.connect(root/path) as db: db.execute(clear)
