const crypto = require('crypto');
const fs = require('fs');

async function testBybit() {
    const lines = fs.readFileSync('.env', 'utf8').split('\n');
    let k = '', s = '';
    for (const line of lines) {
        if (line.startsWith('BYBIT_API_KEY=')) k = line.split('=')[1].trim();
        if (line.startsWith('BYBIT_API_SECRET=')) s = line.split('=')[1].trim();
    }
    
    const ts = Date.now().toString();
    const qs = "accountType=UNIFIED";
    const str = ts + k + "5000" + qs;
    const sig = crypto.createHmac('sha256', s).update(str).digest('hex');
    
    const res = await fetch(`https://api.bybit.com/v5/account/wallet-balance?${qs}`, {
        headers: {
            'X-BAPI-API-KEY': k,
            'X-BAPI-SIGN': sig,
            'X-BAPI-TIMESTAMP': ts,
            'X-BAPI-RECV-WINDOW': "5000"
        }
    });
    console.log("UNIFIED:", await res.text());
}
testBybit();
