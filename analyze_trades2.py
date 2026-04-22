import csv
from datetime import datetime

def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0

with open('trades.csv', 'r') as f:
    reader = csv.DictReader(f)
    trades = list(reader)

closed_trades = [t for t in trades if t.get('status') in ['CLOSED', 'EXTERNAL_CLOSE']]

for t in closed_trades:
    gross = safe_float(t.get('curr_gross_pnl'))
    comm = safe_float(t.get('commission_cost'))
    slip = safe_float(t.get('slippage_cost'))
    fund = safe_float(t.get('funding_cost'))
    net = gross - comm - slip - fund
    
    if net <= 0 and gross < 0:
        print(f"ID: {t.get('id')}, Pair: {t.get('pair')}, Dir: {t.get('direction')}, "
              f"Entry Spread: {t.get('entry_spread')}, Exit Spread: {t.get('exit_spread')}, "
              f"Gross PnL: {gross:.4f}, Slippage: {slip:.4f}, Reason: {t.get('close_reason')}")

print("\n--- High Slippage Trades ---")
for t in closed_trades:
    gross = safe_float(t.get('curr_gross_pnl'))
    comm = safe_float(t.get('commission_cost'))
    slip = safe_float(t.get('slippage_cost'))
    fund = safe_float(t.get('funding_cost'))
    net = gross - comm - slip - fund
    
    if net <= 0 and slip > gross and gross >= 0:
        print(f"ID: {t.get('id')}, Pair: {t.get('pair')}, Dir: {t.get('direction')}, "
              f"Gross PnL: {gross:.4f}, Slippage: {slip:.4f}, Reason: {t.get('close_reason')}")

