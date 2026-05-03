const WebSocket = require('ws');
const urls = [
    'wss://fapi.xt.com/ws/market',
    'wss://fstream.xt.com/ws/market',
    'wss://fapi.xt.com/websocket',
    'wss://fstream.xt.com/ws',
    'wss://fapi.xt.com/ws',
    'wss://stream.xt.com/public'
];

urls.forEach(u => {
    const ws = new WebSocket(u);
    ws.on('open', () => { console.log(u, 'CONNECTED'); ws.close(); });
    ws.on('error', (err) => console.log(u, 'error:', err.message));
});
