async function run() {
    const res = await fetch("https://api.arb.bagow.com/api/proxy/binance/fapi/v1/exchangeInfo");
    const json = await res.json();
    for (const c of json.symbols) {
        if (c.symbol.includes("TAG")) {
            console.log(c.symbol, c.filters.find(f => f.filterType === "LOT_SIZE").stepSize);
        }
    }
}
run();
