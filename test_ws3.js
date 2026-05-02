const WebSocket = require('ws');
const ws = new WebSocket('wss://fstream.binance.com/ws/!ticker@arr');
ws.on('open', () => console.log('OPENED'));
ws.on('message', (data) => {
    console.log("DATA RECEIVED:", data.toString().substring(0, 300));
    process.exit(0);
});
