import asyncio
import websockets
import json

async def main():
    try:
        async with websockets.connect("wss://stream.toobit.com/swap/ws/v1") as ws:
            print("Connected swap/ws/v1")
    except Exception as e:
        print("swap/ws/v1:", e)
        
    try:
        async with websockets.connect("wss://stream.toobit.com/quote/ws/v1") as ws:
            print("Connected quote/ws/v1")
    except Exception as e:
        print("quote/ws/v1:", e)

asyncio.run(main())
