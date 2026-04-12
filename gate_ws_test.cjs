const WebSocket = require('ws');

const ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');

ws.on('open', () => {
    ws.send(JSON.stringify({
        time: Math.floor(Date.now() / 1000),
        channel: "futures.tickers",
        event: "subscribe",
        payload: ["BTC_USDT", "NOM_USDT"]
    }));
});

ws.on('message', (data) => {
    console.log(data.toString());
    process.exit(0);
});
