const WebSocket = require('ws');
const ws = new WebSocket('wss://stream.toobit.com/quote/ws/v1');
ws.on('open', () => {
    ws.send(JSON.stringify({
        symbol: "1000PEPEUSDT",
        topic: "realtimes",
        event: "sub"
    }));
});
ws.on('message', (msg) => {
    console.log(msg.toString());
    ws.close();
});
