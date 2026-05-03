const WebSocket = require('ws');
const urls = [
    'wss://stream.toobit.com/quote/ws/v1',
    'wss://stream.toobit.com/swap-market'
];

urls.forEach(u => {
    const ws = new WebSocket(u);
    ws.on('open', () => { console.log(u, 'CONNECTED'); ws.close(); });
    ws.on('error', (err) => console.log(u, 'error:', err.message));
});
