import hmac
import hashlib
import time
import urllib.request
import urllib.parse
import json

API_KEY = "ktolZxeXkqtr6svQCJm0IoJImQSX9LwKE1yUB23AiffwdJTfdx6OItZT1t1nMvrA"
API_SECRET = "yEHhBTRYkGEHR4lm7ubu5m7nEGh10VDx1QS2sbbvzF6iengF7wqlAIrBUjpUdPsA"

def sign_tradfi():
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = hmac.new(
        API_SECRET.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    url = f"https://fapi.binance.com/fapi/v1/stock/contract?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    print(f"Sending POST to {url}")
    req = urllib.request.Request(url, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req) as resp:
            print(resp.status)
            print(resp.read().decode('utf-8'))
    except urllib.error.URLError as e:
        print("Error:", e.reason)
        if hasattr(e, 'read'):
            print(e.read().decode('utf-8'))

sign_tradfi()
