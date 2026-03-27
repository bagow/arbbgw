const urls = [
    { name: "KuCoin", url: "https://api.kucoin.com/api/v1/market/allTickers" },
    { name: "XT.com", url: "https://sapi.xt.com/v4/public/ticker/book" },
    { name: "CoinEx", url: "https://api.coinex.com/v1/market/ticker/all" },
];

async function fetchBBO() {
    for (const { name, url } of urls) {
        try {
            console.log(`Fetching ${name}...`);
            const start = Date.now();
            const res = await fetch(url);
            const data = await res.json();
            const end = Date.now();
            console.log(`${name} (${end - start}ms):`, JSON.stringify(data).substring(0, 500));
        } catch (e) {
            console.error(`${name} Error:`, e);
        }
    }
}

fetchBBO();
