const WebSocket = require('ws');
const ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');
ws.on('open', () => {
    ws.send(JSON.stringify({
        time: Math.floor(Date.now() / 1000),
        channel: 'futures.order_book',
        event: 'subscribe',
        payload: ['BABY_USDT', '20', '0']
    }));
});
ws.on('message', (msg) => {
    console.log(msg.toString());
    ws.close();
});
