const WebSocket = require('ws');
const zlib = require('zlib');

// HTX test detail vs bbo vs ticker
const htx = new WebSocket('wss://api.hbdm.com/linear-swap-ws');
htx.on('open', () => {
    htx.send(JSON.stringify({"sub": "market.BTC-USDT.detail", "id": "htx-detail"}));
    htx.send(JSON.stringify({"sub": "market.BTC-USDT.bbo", "id": "htx-bbo"}));
});
htx.on('message', data => {
    try {
        const text = zlib.gunzipSync(data).toString();
        if(text.includes('ping')) htx.send(JSON.stringify({ pong: JSON.parse(text).ping }));
        else if (!text.includes('HTX')) {
            console.log('[HTX]', text.slice(0, 150));
            if(text.includes('tick')) process.exit();
        }
    } catch(e) {}
});

// MEXC batch test
const mexc = new WebSocket('wss://contract.mexc.com/edge');
mexc.on('open', () => {
    mexc.send(JSON.stringify({"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}));
});
mexc.on('message', data => {
    if(!data.toString().includes('success')) {
        console.log('[MEXC]', data.toString().slice(0, 150));
    }
});

// KuCoin
const crypto = require('crypto');
async function testKu() {
    const res = await fetch('https://api-futures.kucoin.com/api/v1/bullet-public', {method: 'POST'});
    const data = await res.json();
    const token = data.data.token;
    const wsUrl = data.data.instanceServers[0].endpoint + '?token=' + token + '&connectId=test2';
    const kucoin = new WebSocket(wsUrl);
    kucoin.on('open', () => kucoin.send(JSON.stringify({
        id: "sub1", type: "subscribe", topic: "/contractMarket/tickerV2:XBTUSDTM", privateChannel: false, response: true
    })));
    kucoin.on('message', d => {
        if(!d.toString().includes('welcome')) console.log('[KuCoin]', d.toString().slice(0, 150));
    });
}
testKu();
