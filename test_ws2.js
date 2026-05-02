const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws');
let count = 0;
ws.on('open', () => {
    let streams = [];
    for (let i = 0; i < 250; i++) {
        streams.push(`btcusdt@depth5@100ms`); // Just dummy 200 streams to test
    }
    ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: streams, id: 1 }));
});
ws.on('message', (data) => {
    console.log(data.toString());
    count++;
    if (count >= 2) process.exit(0);
});
ws.on('close', () => {
    console.log("CLOSED!");
    process.exit(1);
});
