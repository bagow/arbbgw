const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws');
let count = 0;
ws.on('open', () => {
    ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: ['!ticker@arr'], id: 1 }));
});
ws.on('message', (data) => {
    console.log("DATA RECEIVED:", data.toString().substring(0, 300));
    count++;
    if (count >= 2) process.exit(0);
});
