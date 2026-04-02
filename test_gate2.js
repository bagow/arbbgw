const WebSocket = require('ws');
const https = require('https');

https.get('https://api.gateio.ws/api/v4/futures/usdt/contracts', (res) => {
    let raw = '';
    res.on('data', c => raw += c);
    res.on('end', () => {
        const data = JSON.parse(raw);
        const symbols = data.filter(d => !d.in_delisting).map(d => d.name);
        console.log("Fetched " + symbols.length + " symbols");

        const ws = new WebSocket('wss://fx-ws.gateio.ws/v4/ws/usdt');
        ws.on('open', () => {
            console.log("Connected");
            const payload = JSON.stringify({
                time: Math.floor(Date.now() / 1000),
                channel: "futures.tickers",
                event: "subscribe",
                payload: symbols.slice(0, 10)
            });
            console.log("Sending: " + payload);
            ws.send(payload);
        });

        ws.on('message', (data) => {
            console.log("Received: " + data);
        });

        ws.on('close', () => console.log("Closed"));
        ws.on('error', (e) => console.log("Error: " + e));
    });
});
