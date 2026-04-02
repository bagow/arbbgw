import asyncio, json
import websockets

async def test():
    import urllib.request
    req = urllib.request.Request("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    with urllib.request.urlopen(req) as response:
        contracts = json.loads(response.read().decode())
        names = [c["name"] for c in contracts if not c.get("in_delisting")]
    
    async with websockets.connect("wss://fx-ws.gateio.ws/v4/ws/usdt") as ws:
        await ws.send(json.dumps({"time":0,"channel":"futures.tickers","event":"subscribe","payload":names[:10]}))
        print(await ws.recv())
        print(await ws.recv())

asyncio.run(test())
