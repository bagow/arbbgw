/**
 * engine.js – Spread calculation engine
 *
 * Supported exchanges (11):
 *   binance, bybit, okx, gate, bitget, mexc, htx, bingx, kucoin, xt, coinex
 *
 * All tickers are normalised to the bare USDT pair (e.g. BTCUSDT).
 * buildSnapshot() generates every exchange combination automatically.
 */

export const SPREAD_THRESHOLD = 1.0;
export const SPREAD_MAX = 50; // Keep up to 50% in the global cache so users can customize
export const TOP_N = 1000;   // Store the top 1000 pairs so dynamic filtering has enough candidates

const MAX_PRICE_AGE_MS = 5 * 60 * 1000; // 5 min — filter stale Binance tickers

// ─── Normalise ────────────────────────────────────────────────────────────────
/**
 * Convert a raw ticker list from one exchange into Map<"BTCUSDT", price>.
 */
export function normalizePrices(exchange, rawTickers, rawFunding = [], rawBooks = []) {
  const map = new Map();
  const fMap = new Map();
  const bMap = new Map();

  if (exchange === 'binance') {
    for (const f of rawFunding) {
      if (f.symbol) fMap.set(f.symbol, parseFloat(f.lastFundingRate || 0));
    }
    for (const b of rawBooks) {
      if (b.symbol) bMap.set(b.symbol, { bid: parseFloat(b.bidPrice), ask: parseFloat(b.askPrice) });
    }
  } else if (exchange === 'htx') {
    for (const f of rawFunding) {
      const sym = (f.contract_code || '').replace(/-/g, '');
      if (sym) fMap.set(sym, parseFloat(f.funding_rate || 0));
    }
  } else if (exchange === 'bingx') {
    for (const f of rawFunding) {
      const sym = (f.symbol || '').replace('-', '');
      if (sym) fMap.set(sym, parseFloat(f.lastFundingRate || 0));
    }
  }

  if (exchange === 'binance') {
    const now = Date.now();
    for (const t of rawTickers) {
      if (t.time && (now - t.time) > MAX_PRICE_AGE_MS) continue;
      const price = parseFloat(t.lastPrice || t.price);
      const vol = parseFloat(t.quoteVolume || 0);
      const fundingRate = fMap.get(t.symbol) || 0;
      const book = bMap.get(t.symbol);
      const bid = book ? book.bid : price;
      const ask = book ? book.ask : price;
      if (t.symbol && !isNaN(price) && price > 0) map.set(t.symbol, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'bybit') {
    for (const t of rawTickers) {
      const sym = (t.symbol || '').replace('-', '');
      const price = parseFloat(t.lastPrice);
      const vol = parseFloat(t.turnover24h || 0);
      const fundingRate = parseFloat(t.fundingRate || 0);
      const bid = parseFloat(t.bid1Price || price);
      const ask = parseFloat(t.ask1Price || price);
      if (sym && !isNaN(price) && price > 0) map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'okx') {
    for (const t of rawTickers) {
      const parts = (t.instId || '').split('-');
      if (parts.length < 2) continue;
      const sym = parts[0] + parts[1];
      const price = parseFloat(t.last);
      const vol = parseFloat(t.volCcy24h || 0);
      const fundingRate = 0; // OKX requires separate endpoint per instId, skipping for now
      const bid = parseFloat(t.bidPx || price);
      const ask = parseFloat(t.askPx || price);
      if (sym && !isNaN(price) && price > 0) map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'gate') {
    for (const t of rawTickers) {
      const sym = (t.contract || '').replace('_', '');
      const price = parseFloat(t.last);
      const vol = parseFloat(t.volume_24h_quote || 0);
      const fundingRate = parseFloat(t.funding_rate || 0);
      const bid = parseFloat(t.highest_bid || price);
      const ask = parseFloat(t.lowest_ask || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'bitget') {
    for (const t of rawTickers) {
      const sym = t.symbol || '';
      const price = parseFloat(t.lastPr);
      const vol = parseFloat(t.usdtVolume || t.quoteVolume || 0);
      const fundingRate = parseFloat(t.fundingRate || 0);
      const bid = parseFloat(t.bidPr || price);
      const ask = parseFloat(t.askPr || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'mexc') {
    for (const t of rawTickers) {
      const sym = (t.symbol || '').replace('_', '');
      const price = parseFloat(t.lastPrice);
      const vol = parseFloat(t.amount24 || 0);
      const fundingRate = parseFloat(t.fundingRate || 0);
      const bid = parseFloat(t.bid1 || t.bidPrice || price);
      const ask = parseFloat(t.ask1 || t.askPrice || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'htx') {
    for (const t of rawTickers) {
      const raw = t.contract_code || t.symbol || '';
      const sym = raw.replace(/-/g, '');
      const price = parseFloat(t.close ?? t.last ?? t.lastPrice);
      const vol = parseFloat(t.trade_turnover ?? 0);
      const fundingRate = fMap.get(sym) || 0;
      const bid = (t.bid && t.bid[0]) ? parseFloat(t.bid[0]) : price;
      const ask = (t.ask && t.ask[0]) ? parseFloat(t.ask[0]) : price;
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }

  } else if (exchange === 'bingx') {
    for (const t of rawTickers) {
      const sym = (t.symbol || '').replace('-', '');
      const price = parseFloat(t.lastPrice ?? t.price);
      const vol = parseFloat(t.quoteVolume ?? t.volume ?? 0);
      const fundingRate = fMap.get(sym) || 0;
      const bid = parseFloat(t.bidPrice || price);
      const ask = parseFloat(t.askPrice || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }
  } else if (exchange === 'kucoin') {
    for (const t of rawTickers) {
      const sym = (t.symbolName || t.symbol || '').replace('-', '');
      const price = parseFloat(t.last);
      const vol = parseFloat(t.volValue || 0);
      const fundingRate = 0;
      const bid = parseFloat(t.buy || price);
      const ask = parseFloat(t.sell || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }
  } else if (exchange === 'xt') {
    for (const t of rawTickers) {
      const sym = (t.s || '').toUpperCase().replace('_', '');
      const bid = parseFloat(t.bp || 0);
      const ask = parseFloat(t.ap || 0);
      const price = ask || bid;
      const vol = parseFloat(t.v || t.q || 0);
      const fundingRate = 0;
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }
  } else if (exchange === 'coinex') {
    for (const [sym, t] of Object.entries(rawTickers)) {
      const price = parseFloat(t.last);
      const vol = parseFloat(t.value || t.vol || 0);
      const fundingRate = 0;
      const bid = parseFloat(t.buy || price);
      const ask = parseFloat(t.sell || price);
      if (sym && sym.endsWith('USDT') && !isNaN(price) && price > 0)
        map.set(sym, { price, vol, fundingRate, bid, ask });
    }
  }

  return map;
}

// ─── Spreads ──────────────────────────────────────────────────────────────────
export function computeSpreads(nameA, mapA, nameB, mapB, minVolM = 10) {
  const results = [];
  for (const [sym, dataA] of mapA) {
    if (!mapB.has(sym)) continue;
    const dataB = mapB.get(sym);

    const priceA = dataA.price;
    const priceB = dataB.price;
    const volA = dataA.vol;
    const volB = dataB.vol;
    const fundingA = dataA.fundingRate || 0;
    const fundingB = dataB.fundingRate || 0;

    // A over B: Enter Short A (Sell at Bid), Long B (Buy at Ask)
    const A_over_B = ((dataA.bid - dataB.ask) / dataB.ask) * 100;
    // B over A: Enter Short B (Sell at Bid), Long A (Buy at Ask)
    const B_over_A = ((dataB.bid - dataA.ask) / dataA.ask) * 100;

    let spread = 0;
    let direction = '';

    if (A_over_B > B_over_A) {
      spread = A_over_B;
      direction = `${nameA}>${nameB}`;
    } else {
      spread = B_over_A;
      direction = `${nameB}>${nameA}`;
    }

    if (spread > SPREAD_MAX || spread < -50) continue;

    // Filter out pairs with low liquidity across the analyzed exchanges. Protect against NaN!
    let safeVolA = isNaN(volA) ? 0 : volA;
    let safeVolB = isNaN(volB) ? 0 : volB;
    if (safeVolA < (minVolM * 1000000) || safeVolB < (minVolM * 1000000)) continue;

    results.push({
      pair: sym,
      exchangeA: nameA,
      exchangeB: nameB,
      priceA,
      priceB,
      bidA: dataA.bid,
      askA: dataA.ask,
      bidB: dataB.bid,
      askB: dataB.ask,
      volA,
      volB,
      fundingA,
      fundingB,
      spread: +spread.toFixed(4),
      direction,
    });
  }
  return results;
}

/**
 * Build full snapshot from all exchange maps.
 * Generates every unique pair combination automatically.
 */
export function buildSnapshot(maps, minVolM = 10) {
  const entries = Object.entries(maps).filter(([, m]) => m && m.size > 0);
  const all = [];

  for (let i = 0; i < entries.length; i++) {
    for (let j = i + 1; j < entries.length; j++) {
      const [nameA, mapA] = entries[i];
      const [nameB, mapB] = entries[j];
      all.push(...computeSpreads(nameA, mapA, nameB, mapB, minVolM));
    }
  }

  all.sort((a, b) => b.spread - a.spread);
  return all.slice(0, TOP_N);
}

export function filterForStorage(snapshot) {
  return snapshot.filter(row => row.spread >= SPREAD_THRESHOLD);
}
