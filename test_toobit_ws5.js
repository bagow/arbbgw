const WebSocket = require('ws');
const ws = new WebSocket('wss://stream.toobit.com/quote/ws/v1');
ws.on('open', () => {
    ws.send(JSON.stringify({
        symbol: "BTCUSDT",
        topic: "depth",
        event: "sub"
    }));
});
ws.on('message', (msg) => {
    console.log(msg.toString());
});
