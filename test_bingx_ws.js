const WebSocket = require('ws');
const ws = new WebSocket('wss://open-api-swap.bingx.com/swap-market');
ws.on('open', () => {
    ws.send('Ping');
});
ws.on('message', (msg) => {
    console.log(msg.toString());
    ws.close();
});
