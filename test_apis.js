const fs = require('fs');

async function testApi(name, url, extractSymbols) {
    try {
        const res = await fetch(url);
        const json = await res.json();
        const syms = extractSymbols(json);
        console.log(`[${name}] OK, ${syms.length} symbols. First 3:`, syms.slice(0, 3));
    } catch (e) {
        console.log(`[${name}] Error:`, e.message);
    }
}

async function run() {
    await testApi('Bybit', 'https://api.bybit.com/v5/market/tickers?category=linear', d => d.result.list.map(i => i.symbol).filter(s => s.endsWith('USDT')));
    await testApi('OKX', 'https://www.okx.com/api/v5/public/instruments?instType=SWAP', d => d.data.map(i => i.instId).filter(s => s.endsWith('-USDT-SWAP')));
    await testApi('Bitget', 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES', d => d.data.map(i => i.symbol).filter(s => s.endsWith('USDT')));
    await testApi('HTX', 'https://api.hbdm.com/linear-swap-api/v1/swap_contract_info', d => d.data.map(i => i.contract_code).filter(s => s.endsWith('-USDT')));
    await testApi('MEXC', 'https://contract.mexc.com/api/v1/contract/detail', d => d.data.map(i => i.symbol).filter(s => s.endsWith('_USDT')));
    await testApi('BingX', 'https://open-api.bingx.com/openApi/swap/v2/quote/ticker', d => d.data.map(i => i.symbol).filter(s => s.endsWith('-USDT')));
}

run();
