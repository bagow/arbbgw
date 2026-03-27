const WebSocket = require('ws');

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
        if(!d.toString().includes('welcome') && !d.toString().includes('ack')) {
            console.log('[KuCoin]', d.toString().slice(0, 300));
            kucoin.close();
            process.exit();
        }
    });

    setInterval(() => kucoin.send(JSON.stringify({id: "123", type: "ping"})), 10000);
}
testKu();
