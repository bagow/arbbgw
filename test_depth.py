import requests

def check_binance(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=5"
        res = requests.get(url).json()
        bids = res.get('bids', [])
        asks = res.get('asks', [])
        print(f"Binance {symbol}:")
        if bids:
            price, qty = float(bids[0][0]), float(bids[0][1])
            print(f"  Top Bid: {qty} {symbol.replace('USDT','')} at {price} USDT = {qty * price} USD")
        if asks:
            price, qty = float(asks[0][0]), float(asks[0][1])
            print(f"  Top Ask: {qty} {symbol.replace('USDT','')} at {price} USDT = {qty * price} USD")
    except Exception as e:
        print(f"Binance err: {e}")

check_binance("NTRNUSDT")
check_binance("RDNTUSDT")
check_binance("ENJUSDT")
