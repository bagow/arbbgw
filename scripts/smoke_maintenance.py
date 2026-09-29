"""Guarded maintenance for an inactive engine; never changes trading settings."""
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

ROOT = Path('/home/opc/core-node')


def verify_inactive(root=ROOT):
    with sqlite3.connect((root/'vps-api.db').as_uri()+'?mode=ro',uri=True,timeout=5) as db:
        if db.execute('SELECT COUNT(*) FROM trading_profiles WHERE is_active != 0 OR pending_commands IS NOT NULL').fetchone()[0]:
            raise RuntimeError('Refuse maintenance: active profile or pending command')
    with sqlite3.connect((root/'real-trader.db').as_uri()+'?mode=ro',uri=True,timeout=5) as db:
        if db.execute("SELECT COUNT(*) FROM trades WHERE status NOT IN ('CLOSED','CANCELLED')").fetchone()[0]:
            raise RuntimeError('Refuse maintenance: active trade')
        # Historical terminal orders are the durable audit trail and must not
        # permanently disable maintenance after the first real trade. Refuse
        # only orphaned, non-terminal, or active-trade order ownership.
        unsafe_orders = db.execute("""
            SELECT COUNT(*)
            FROM execution_orders e
            LEFT JOIN trades t ON t.id=e.trade_id
            WHERE t.id IS NULL
               OR t.status NOT IN ('CLOSED','CANCELLED')
               OR json_extract(e.observation,'$.state')
                    NOT IN ('Filled','Cancelled','Rejected')
        """).fetchone()[0]
        if unsafe_orders:
            raise RuntimeError('Refuse maintenance: unresolved execution orders')


def main():
    mode = sys.argv[1]
    if mode in ('verify', 'pause'):
        verify_inactive()
        subprocess.run(['systemctl','is-active','--quiet','core-node'],check=True,timeout=10)
        if mode == 'pause':
            # Install an independent restart before stopping the existing service.
            subprocess.run(['sudo','-n','systemd-run','--unit=arb-core-restore-'+str(int(time.time())),
                            '--on-active=420','/bin/systemctl','start','core-node'],check=True,timeout=15)
            subprocess.run(['sudo','-n','systemctl','stop','core-node'],check=True,timeout=15)
    elif mode == 'restore':
        subprocess.run(['sudo','-n','systemctl','start','core-node'],check=True,timeout=20)
        subprocess.run(['systemctl','is-active','--quiet','core-node'],check=True,timeout=10)
        subprocess.run(['sudo','-n','systemctl','stop','arb-core-restore-*.timer'],check=True,timeout=15)
    else:
        raise RuntimeError('Unknown maintenance operation')


if __name__ == '__main__':
    main()
