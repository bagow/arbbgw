const WebSocket = require('ws');

// Test Bybit
const bybit = new WebSocket('wss://stream.bybit.com/v5/public/linear');
bybit.on('open', () => bybit.send(JSON.stringify({"op": "subscribe", "args": ["tickers.*"]})));
bybit.on('message', data => {
    console.log('[bybit]', data.toString().slice(0, 300));
    bybit.close();
});

// Test OKX
const okx = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
okx.on('open', () => okx.send(JSON.stringify({"op": "subscribe", "args": [{"channel":"tickers","instType":"SWAP"}]})));
okx.on('message', data => {
    if (data.toString() !== 'pong') {
        console.log('[okx]', data.toString().slice(0, 300));
        okx.close();
    }
});

// Test MEXC
const mexc = new WebSocket('wss://contract.mexc.com/edge');
mexc.on('open', () => mexc.send(JSON.stringify({"method": "sub.ticker.batch", "param": {}})));
mexc.on('message', data => {
    console.log('[mexc]', data.toString().slice(0, 300));
    mexc.close();
});

// Test Bitget
const bitget = new WebSocket('wss://ws.bitget.com/v2/ws/public');
bitget.on('open', () => bitget.send(JSON.stringify({"op": "subscribe", "args": [{"instType": "USDT-FUTURES", "channel": "ticker", "instId": "default"}]})));
bitget.on('message', data => {
    if (data.toString() !== 'pong') {
        console.log('[bitget]', data.toString().slice(0, 300));
        bitget.close();
    }
});

// Test HTX
const htx = new WebSocket('wss://api.hbdm.com/linear-swap-ws');
htx.on('open', () => htx.send(JSON.stringify({"sub": "market.*USDT.ticker", "id": "htx1"})));
htx.on('message', data => {
    const zlib = require('zlib');
    try {
        const text = zlib.gunzipSync(data).toString();
        console.log('[htx]', text.slice(0, 300));
        htx.close();
    } catch(e) {}
});

setTimeout(() => process.exit(), 5000);
