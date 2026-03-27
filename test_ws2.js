const WebSocket = require('ws');
const zlib = require('zlib');

// HTX
const htx = new WebSocket('wss://api.hbdm.com/linear-swap-ws');
htx.on('open', () => htx.send(JSON.stringify({"sub": "market.BTC-USDT.ticker", "id": "htx1"})));
htx.on('message', data => {
    try {
        const text = zlib.gunzipSync(data).toString();
        if(!text.includes('ping')) {
            console.log('[HTX]', text.slice(0, 300));
            htx.close();
        } else {
            htx.send(JSON.stringify({ pong: JSON.parse(text).ping }));
        }
    } catch(e) {}
});

// MEXC
const mexc = new WebSocket('wss://contract.mexc.com/edge');
mexc.on('open', () => mexc.send(JSON.stringify({"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}})));
mexc.on('message', data => {
    console.log('[MEXC]', data.toString().slice(0, 300));
    mexc.close();
});

// BingX
const bingx = new WebSocket('wss://open-api-swap.bingx.com/swap-market');
bingx.on('open', () => bingx.send(JSON.stringify({"id": "bingx1", "reqType": "sub", "dataType": "BTC-USDT@ticker"})));
bingx.on('message', data => {
    try {
        const text = zlib.gunzipSync(data).toString();
        if(!text.includes('Ping')) {
            console.log('[BingX gzipped]', text.slice(0, 300));
            bingx.close();
        }
    } catch {
        const text = data.toString();
        if(!text.includes('Ping')) {
            console.log('[BingX raw]', text.slice(0, 300));
            bingx.close();
        }
    }
});

// KuCoin
const crypto = require('crypto');
async function testKucoin() {
    const res = await fetch('https://api-futures.kucoin.com/api/v1/bullet-public', {method: 'POST'});
    const data = await res.json();
    const token = data.data.token;
    const wsUrl = data.data.instanceServers[0].endpoint + '?token=' + token + '&connectId=test1';
    const kucoin = new WebSocket(wsUrl);
    kucoin.on('open', () => kucoin.send(JSON.stringify({
        id: "sub1", type: "subscribe", topic: "/contractMarket/tickerV2:XBTUSDTM", privateChannel: false, response: true
    })));
    kucoin.on('message', d => {
        const text = d.toString();
        if(!text.includes('welcome')) {
            console.log('[KuCoin]', text.slice(0, 300));
            kucoin.close();
        }
    });
}
testKucoin();

setTimeout(() => process.exit(), 8000);
