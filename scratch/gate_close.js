const crypto = require('crypto');
const fs = require('fs');
const https = require('https');

const s = JSON.parse(fs.readFileSync('/home/opc/core-node/test_settings.json'));
const k = s.gate.key;
const sec = s.gate.secret;

const method = 'POST';
const url = '/api/v4/futures/usdt/orders';
const query = '';

const closePayload = JSON.stringify({
    "contract": "NOM_USDT",
    "size": 0,
    "price": "0",
    "tif": "ioc",
    "close": true
});
const regularLimitPayload = JSON.stringify({
    "contract": "NOM_USDT",
    "size": -20,
    "price": "0.0039",
    "tif": "ioc",
    "reduce_only": true,
    "close": false
});

async function runGate(payload) {
    const t = Math.floor(Date.now() / 1000);
    const hash = crypto.createHash('sha512').update(payload).digest('hex');
    const str = `${method}\n${url}\n${query}\n${hash}\n${t}`;
    const sign = crypto.createHmac('sha512', sec).update(str).digest('hex');

    const options = {
      hostname: 'api.gateio.ws',
      port: 443,
      path: url,
      method: method,
      headers: {
        'KEY': k,
        'Timestamp': t.toString(),
        'SIGN': sign,
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    };

    return new Promise(resolve => {
        const req = https.request(options, res => {
          let data = '';
          res.on('data', chunk => data += chunk);
          res.on('end', () => resolve(data));
        });
        req.write(payload);
        req.end();
    });
}
async function run() {
    console.log("TRYING CLOSE PAYLOAD");
    console.log(await runGate(closePayload));
    console.log("TRYING REGULAR LIMIT");
    console.log(await runGate(regularLimitPayload));
}
run();
