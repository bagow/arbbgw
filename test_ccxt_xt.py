import time, ccxt, logging
import http.client
http.client.HTTPConnection.debuglevel = 1
x = ccxt.xt({'apiKey': '72872e72-c5ea-4451-83e4-26bca32feb6d', 'secret': 'a490d4e17be5cc3cb542f16d599442e2b5b16ecf'})
x.fetch_balance({'type': 'swap'})
