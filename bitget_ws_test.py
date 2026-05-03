import asyncio
import websockets
import json

async def main():
    async with websockets.connect("wss://ws.bitget.com/v2/ws/public") as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"}]
        }))
        while True:
            msg = await ws.recv()
            print(msg)
            if 'data' in json.loads(msg):
                break

asyncio.run(main())
