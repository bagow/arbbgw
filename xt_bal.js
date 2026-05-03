const crypto = require('crypto');
const https = require('https');

async function test() {
    const keys = require('./config/keys.json');
    const secret = keys.xt.secret;
    const api_key = keys.xt.key;
    const ts = Date.now();
    const path = "/future/user/v1/balance/list";
    const body = "";
    
    const msg = `xt-validate-appkey=${api_key}&xt-validate-timestamp=${ts}#${path}${body ? '#' + body : ''}`;
    const sig = crypto.createHmac('sha256', secret).update(msg).digest('hex');
    
    const options = {
        hostname: 'fapi.xt.com',
        path: path,
        method: 'GET',
        headers: {
            'xt-validate-appkey': api_key,
            'xt-validate-timestamp': ts.toString(),
            'xt-validate-signature': sig
        }
    };
    
    const req = https.request(options, res => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
            console.log(JSON.stringify(JSON.parse(data), null, 2));
        });
    });
    req.on('error', e => console.error(e));
    req.end();
}
test();
