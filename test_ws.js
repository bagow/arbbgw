const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws');
let count = 0;
ws.on('open', () => {
    ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@depth5@100ms'], id: 1 }));
});
ws.on('message', (data) => {
    console.log(data.toString());
    count++;
    if (count >= 2) process.exit(0);
});
