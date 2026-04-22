import csv

def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0

with open('trades.csv', 'r') as f:
    reader = csv.DictReader(f)
    trades = list(reader)

print(f"Total trades: {len(trades)}")
if not trades:
    exit(0)

print(f"Columns: {list(trades[0].keys())}")

closed_trades = [t for t in trades if t.get('status') in ['CLOSED', 'EXTERNAL_CLOSE']]
print(f"Closed trades: {len(closed_trades)}")

if not closed_trades:
    exit(0)

profitable = 0
unprofitable = 0
total_gross = 0.0
total_comm = 0.0
total_slip = 0.0
total_net = 0.0

loss_reasons = {'Negative Gross PnL': 0, 'High Commission': 0, 'High Slippage': 0, 'Other': 0}
errors = {}

for t in closed_trades:
    gross = safe_float(t.get('curr_gross_pnl'))
    comm = safe_float(t.get('commission'))
    slip = safe_float(t.get('slippage_cost'))
    fund = safe_float(t.get('funding'))
    
    net = gross - comm - slip - fund
    
    total_gross += gross
    total_comm += comm
    total_slip += slip
    total_net += net
    
    if net > 0:
        profitable += 1
    else:
        unprofitable += 1
        
        # Determine reason
        if gross < 0:
            loss_reasons['Negative Gross PnL'] += 1
        elif comm > gross:
            loss_reasons['High Commission'] += 1
        elif slip > gross:
            loss_reasons['High Slippage'] += 1
        else:
            loss_reasons['Other'] += 1
            
        err = t.get('close_error', '')
        if err:
            errors[err] = errors.get(err, 0) + 1

print("\n--- Summary ---")
print(f"Profitable: {profitable} ({profitable/len(closed_trades)*100:.2f}%)")
print(f"Unprofitable: {unprofitable} ({unprofitable/len(closed_trades)*100:.2f}%)")

print("\n--- Averages per Trade ---")
print(f"Avg Gross PnL: {total_gross/len(closed_trades):.4f}")
print(f"Avg Commission: {total_comm/len(closed_trades):.4f}")
print(f"Avg Slippage: {total_slip/len(closed_trades):.4f}")
print(f"Avg Net PnL: {total_net/len(closed_trades):.4f}")

print("\n--- Loss Reasons ---")
for k, v in loss_reasons.items():
    if v > 0:
        print(f"{k}: {v}")
        
print("\n--- Common Errors in Losing Trades ---")
for k, v in sorted(errors.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"{k}: {v}")

# Find extreme outliers in slippage or gross pnl
print("\n--- Examples of huge slippage/negative PnL ---")
sorted_by_slip = sorted(closed_trades, key=lambda x: safe_float(x.get('slippage_cost')), reverse=True)
for t in sorted_by_slip[:3]:
    print(f"ID: {t.get('id')}, Symbol: {t.get('symbol_a')}, Slippage: {t.get('slippage_cost')}, Gross: {t.get('curr_gross_pnl')}")

