import urllib.request, urllib.parse, urllib.error
import json
import time
import hashlib
import hmac
import os

with open("/home/opc/core-node/.env", "r") as f:
    for line in f:
        if line.startswith("GATE_KEY="): k = line.split("=")[1].strip()
        if line.startswith("GATE_SECRET="): sec = line.split("=")[1].strip()

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

print("Try dual-mode market order (price=0) without close=true")
run_gate({
    "contract": "NOM_USDT",
    "size": -20,
    "price": "0",
    "tif": "ioc",
    "reduce_only": True,
    "close": False
})
