import urllib.request, urllib.parse, urllib.error
import json
import time
import hashlib
import hmac
import os

with open("/home/bagow/dev/cryptoarb/cex-arb/core-node/test_settings.json", "r") as f:
    s = json.load(f)

k = s["gate"]["key"]
sec = s["gate"]["secret"]

def run_gate(payload_dict):
    method = "POST"
    url = "/api/v4/futures/usdt/orders"
    query = ""
    payload = json.dumps(payload_dict)

    t = str(int(time.time()))
    m = hashlib.sha512()
    m.update(payload.encode('utf-8'))
    hashed_payload = m.hexdigest()
    req_str = f"{method}\n{url}\n{query}\n{hashed_payload}\n{t}"

    sign = hmac.new(sec.encode('utf-8'), req_str.encode('utf-8'), hashlib.sha512).hexdigest()

    req = urllib.request.Request(f"https://api.gateio.ws{url}", method=method)
    req.add_header('KEY', k)
    req.add_header('SIGN', sign)
    req.add_header('Timestamp', t)
    req.add_header('Accept', 'application/json')
    req.add_header('Content-Type', 'application/json')

    try:
        with urllib.request.urlopen(req, data=payload.encode('utf-8')) as response:
            print("SUCCESS:", response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code} - {e.read().decode()}")

print("1. Try dual-mode close=false, reduce_only=true, size < 0")
run_gate({
    "contract": "NOM_USDT",
    "size": -20,
    "price": "0.0039",
    "tif": "ioc",
    "reduce_only": True,
    "close": False
})

print("2. Try dual-mode close=true, size=0")
run_gate({
    "contract": "NOM_USDT",
    "size": 0,
    "price": "0",
    "tif": "ioc",
    "close": True
})
