import asyncio
import aiohttp
import time

async def fetch_bbo(session, url, name):
    try:
        t0 = time.time()
        async with session.get(url, timeout=5) as resp:
            data = await resp.json()
            t1 = time.time()
            return f"{name} ({t1-t0:.3f}s): {str(data)[:200]}"
    except Exception as e:
        return f"{name} Error: {e}"

async def main():
    async with aiohttp.ClientSession() as session:
        urls = [
            ("KuCoin BBO", "https://api.kucoin.com/api/v1/market/allTickers"),
            ("XT BBO", "https://sapi.xt.com/v4/public/ticker/book"),
            ("CoinEx BBO", "https://api.coinex.com/v1/market/ticker/all")
        ]
        tasks = [fetch_bbo(session, url, name) for name, url in urls]
        results = await asyncio.gather(*tasks)
        for r in results:
            print(r)

if __name__ == "__main__":
    asyncio.run(main())
