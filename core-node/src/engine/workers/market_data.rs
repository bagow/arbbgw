use reqwest::Client;
use std::sync::Arc;
use tracing::info;
use serde_json::Value;
use std::time::Duration;

pub fn start_multiplier_fetcher(
    http: Client,
    e_ctx: Arc<crate::state::EngineContext>,
    settings: Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    // Gate.io
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            loop {
                // Check if gate is enabled
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"gate".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json.as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            for c in arr {
                                let name_stripped =
                                    c["name"].as_str().unwrap_or("").replace("_", "");
                                let key = format!("gate:{}", name_stripped);
                                match crate::engine::specification::Specification::parse("gate", c, crate::engine::utils::now_ms()) {
                                    Ok(spec) => {e_ctx.specifications.insert(key, spec);},
                                    Err(_) => {e_ctx.specifications.remove(&key);},
                                }
                                if let Some(mult_str) = c["quanto_multiplier"].as_str() {
                                    if let Ok(m) = mult_str.parse::<f64>() {
                                        map.insert(format!("gate:{}", name_stripped), m);
                                    }
                                }
                                if let Some(lt) = c["launch_time"].as_i64() {
                                    lmap.insert(format!("gate:{}", name_stripped), lt);
                                }
                                // Gate: order_price_round is the price tick size string
                                if let Some(pr) = c["order_price_round"].as_str() {
                                    if let Ok(tick) = pr.parse::<f64>() {
                                        if tick > 0.0 {
                                            tmap.insert(format!("gate:{}", name_stripped), tick);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("gate:"))
                    .count();
                if loaded > 0 {
                    info!("[multipliers] Gate: loaded {} symbols", loaded);
                    tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                } else {
                    tracing::warn!("[multipliers] Gate: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
            }
        }
    });

    // KuCoin
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            loop {
                // Check if kucoin is enabled
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"kucoin".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://api-futures.kucoin.com/api/v1/contracts/active")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            for c in arr {
                                if let Some(name) = c["symbol"].as_str() {
                                    let mut std_name =
                                        name.replace("USDTM", "USDT").replace("USDM", "USDT");
                                    if std_name == "XBTUSDT" {
                                        std_name = "BTCUSDT".to_string();
                                    }
                                    let full_name = format!("kucoin:{}", std_name);

                                    match crate::engine::specification::Specification::parse("kucoin", c, crate::engine::utils::now_ms()) {
                                        Ok(spec) => {e_ctx.specifications.insert(full_name.clone(), spec);},
                                        Err(_) => {e_ctx.specifications.remove(&full_name);},
                                    }
                                    if let Some(mult) = c["multiplier"].as_f64() {
                                        map.insert(full_name.clone(), mult);
                                    }
                                    // KuCoin: tickSize is the price increment
                                    if let Some(tick) = c["tickSize"].as_f64() {
                                        if tick > 0.0 {
                                            tmap.insert(full_name.clone(), tick);
                                        }
                                    }
                                    if let Some(lt) = c["firstOpenDate"].as_i64() {
                                        lmap.insert(full_name, lt / 1000);
                                    }
                                }
                            }
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("kucoin:"))
                    .count();
                if loaded > 0 {
                    info!("[multipliers] KuCoin: loaded {} symbols", loaded);
                    tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                } else {
                    tracing::warn!("[multipliers] KuCoin: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
            }
        }
    });

    // Binance USDT-M Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            let mut last_http_status: u16 = 0;
            loop {
                // Check if binance is enabled
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                let mut target_url = "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string();
                if last_http_status == 403 || last_http_status == 418 || last_http_status == 429 {
                    target_url = "https://api.codetabs.com/v1/proxy/?quest=https://fapi.binance.com/fapi/v1/exchangeInfo".to_string();
                    tracing::warn!("[multipliers] Binance: Using proxy fallback due to HTTP {}", last_http_status);
                }

                match http
                    .get(&target_url)
                    .timeout(Duration::from_secs(15))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let status = resp.status();
                        last_http_status = status.as_u16();
                        match resp.json::<Value>().await {
                            Ok(json) => {
                                if let Some(symbols) = json["symbols"].as_array() {
                                    let map = &e_ctx.multipliers;
                                    let lmap = &e_ctx.launch_times;
                                    let tmap = &e_ctx.tick_sizes;
                                    let mut count = 0u32;
                                    for s in symbols {
                                        let sym = s["symbol"].as_str().unwrap_or("");
                                        if !sym.ends_with("USDT") {
                                            continue;
                                        }
                                        let key = format!("binance:{}", sym);
                                        match crate::engine::specification::Specification::parse("binance", s, crate::engine::utils::now_ms()) {
                                            Ok(spec) => {e_ctx.specifications.insert(key, spec);},
                                            Err(_) => {e_ctx.specifications.remove(&key);},
                                        }
                                        let ct = s["contractType"].as_str().unwrap_or("");
                                        if ct != "PERPETUAL" && ct != "TRADIFI_PERPETUAL" {
                                            continue;
                                        }
                                        if let Some(filters) = s["filters"].as_array() {
                                            for f in filters {
                                                if f["filterType"].as_str() == Some("LOT_SIZE") {
                                                    if let Some(ss) = f["stepSize"].as_str() {
                                                        if let Ok(step) = ss.parse::<f64>() {
                                                            if step > 0.0 {
                                                                map.insert(
                                                                    format!("binance:{}", sym),
                                                                    step,
                                                                );
                                                                count += 1;
                                                            }
                                                        }
                                                    }
                                                }
                                                if f["filterType"].as_str() == Some("PRICE_FILTER")
                                                {
                                                    if let Some(ts) = f["tickSize"].as_str() {
                                                        if let Ok(tick) = ts.parse::<f64>() {
                                                            if tick > 0.0 {
                                                                tmap.insert(
                                                                    format!("binance:{}", sym),
                                                                    tick,
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if let Some(ot) = s["onboardDate"].as_i64() {
                                            lmap.insert(format!("binance:{}", sym), ot / 1000);
                                        }
                                    }
                                    info!("[multipliers] Binance: loaded {} symbols", count);
                                } else {
                                    tracing::error!("[multipliers] Binance: 'symbols' field missing or not array. http={} keys={}", status, json.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>().join(",")).unwrap_or_default());
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "[multipliers] Binance: JSON parse failed http={}: {}",
                                    status,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[multipliers] Binance: HTTP request failed: {}", e);
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("binance:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                } else if last_http_status == 418 {
                    tracing::error!("[multipliers] Binance: IP BANNED (418). Proxy failed. Backing off 10 min.");
                    tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                } else if last_http_status == 429 {
                    tracing::warn!("[multipliers] Binance: rate limited (429), proxy failed, backing off 5min");
                    tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                } else {
                    tracing::warn!(
                        "[multipliers] Binance: FAILED to load (http={}), retrying in 30s via proxy",
                        last_http_status
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
            }
        }
    });

    // Bybit USDT-M Linear Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            loop {
                // Check if bybit is enabled
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bybit".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http.get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["result"]["list"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            let mut count = 0u32;
                            for c in arr {
                                let sym = c["symbol"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") { continue; }
                                // tickSize = price precision
                                if let Some(ts_str) = c["priceFilter"]["tickSize"].as_str() {
                                    if let Ok(tick) = ts_str.parse::<f64>() {
                                        if tick > 0.0 { 
                                            tmap.insert(format!("bybit:{}", sym), tick); 
                                        }
                                    }
                                }
                                // lotSizeFilter / qtyStep as multiplier/stepSize
                                if let Some(qs_str) = c["lotSizeFilter"]["qtyStep"].as_str() {
                                    if let Ok(qty) = qs_str.parse::<f64>() {
                                        if qty > 0.0 {
                                            map.insert(format!("bybit:{}", sym), qty);
                                            count += 1;
                                        }
                                    }
                                }
                                // launchTime
                                if let Some(lt) = c["launchTime"].as_str() {
                                    if let Ok(ts) = lt.parse::<i64>() {
                                        lmap.insert(format!("bybit:{}", sym), ts / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] Bybit: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("bybit:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Bybit: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // Bitget USDT-FUTURES
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bitget".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            let mut count = 0u32;
                            for c in arr {
                                let sym = c["symbol"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") {
                                    continue;
                                }
                                // sizeMultiplier is the qty step size (e.g. "0.0001" for BTC)
                                if let Some(sm) = c["sizeMultiplier"].as_str() {
                                    if let Ok(m) = sm.parse::<f64>() {
                                        if m > 0.0 {
                                            map.insert(format!("bitget:{}", sym), m);
                                            count += 1;
                                        }
                                    }
                                }
                                // pricePlace = number of decimal places for price → tick = 10^(-pricePlace)
                                if let Some(pp) = c["pricePlace"].as_str() {
                                    if let Ok(places) = pp.parse::<u32>() {
                                        let tick = 10f64.powi(-(places as i32));
                                        if tick > 0.0 {
                                            tmap.insert(format!("bitget:{}", sym), tick);
                                        }
                                    }
                                }
                                // openTime as launch time (ms timestamp string)
                                if let Some(ot) = c["openTime"].as_str() {
                                    if !ot.is_empty() && ot != "-1" {
                                        if let Ok(ts) = ot.parse::<i64>() {
                                            lmap.insert(format!("bitget:{}", sym), ts / 1000);
                                        }
                                    }
                                }
                            }
                            info!("[multipliers] Bitget: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("bitget:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Bitget: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // BingX USDT-M Perpetual Swap
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bingx".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = &e_ctx.multipliers;
                            let tmap = &e_ctx.tick_sizes;
                            let lmap = &e_ctx.launch_times;
                            let mut count = 0u32;
                            for c in arr {
                                let sym_raw = c["symbol"].as_str().unwrap_or("");
                                // BingX uses "BTC-USDT" format, normalize to "BTCUSDT"
                                let sym = sym_raw.replace('-', "");
                                if !sym.ends_with("USDT") {
                                    continue;
                                }
                                // tradeMinQuantity as step size
                                if let Some(tq) = c["tradeMinQuantity"].as_str() {
                                    if let Ok(q) = tq.parse::<f64>() {
                                        if q > 0.0 {
                                            map.insert(format!("bingx:{}", sym), q);
                                            count += 1;
                                        }
                                    }
                                } else if let Some(sz) = c["size"].as_str() {
                                    if let Ok(q) = sz.parse::<f64>() {
                                        if q > 0.0 {
                                            map.insert(format!("bingx:{}", sym), q);
                                            count += 1;
                                        }
                                    }
                                }
                                // tickSize for price precision
                                if let Some(ts) = c["tickSize"].as_str() {
                                    if let Ok(tick) = ts.parse::<f64>() {
                                        if tick > 0.0 {
                                            tmap.insert(format!("bingx:{}", sym), tick);
                                        }
                                    }
                                }
                                // launchTime
                                if let Some(lt) = c["launchTime"].as_i64() {
                                    if lt > 0 {
                                        lmap.insert(format!("bingx:{}", sym), lt / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] BingX: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("bingx:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] BingX: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // BitMEX
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(4)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bitmex".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://www.bitmex.com/api/v1/instrument/active")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json.as_array() {
                            let map = &e_ctx.multipliers;
                            let tmap = &e_ctx.tick_sizes;
                            let lmap = &e_ctx.launch_times;
                            let mut count = 0u32;
                            for c in arr {
                                let sym_raw = c["symbol"].as_str().unwrap_or("");
                                // Normalize XBTUSDT → BTCUSDT
                                let sym = if sym_raw == "XBTUSDT" {
                                    "BTCUSDT".to_string()
                                } else {
                                    sym_raw.to_string()
                                };
                                if !sym.ends_with("USDT") {
                                    continue;
                                }
                                // lotSize = qty step
                                if let Some(ls) = c["lotSize"].as_f64() {
                                    if ls > 0.0 {
                                        map.insert(format!("bitmex:{}", sym), ls);
                                        count += 1;
                                    }
                                }
                                // tickSize = price step
                                if let Some(ts) = c["tickSize"].as_f64() {
                                    if ts > 0.0 {
                                        tmap.insert(format!("bitmex:{}", sym), ts);
                                    }
                                }
                                // listing date
                                if let Some(listing) = c["listing"].as_str() {
                                    // ISO 8601 format "2020-01-01T00:00:00.000Z"
                                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(listing) {
                                        lmap.insert(format!("bitmex:{}", sym), dt.timestamp());
                                    }
                                }
                            }
                            info!("[multipliers] BitMEX: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("bitmex:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] BitMEX: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // AscendEX (BitMax) Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"ascendex".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://ascendex.com/api/pro/v2/futures/contract")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = &e_ctx.multipliers;
                            let tmap = &e_ctx.tick_sizes;
                            let lmap = &e_ctx.launch_times;
                            let mut count = 0u32;
                            for c in arr {
                                // AscendEX: displayName="BTCUSDT", symbol="BTC-PERP"
                                let sym = c["displayName"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") || sym.is_empty() {
                                    continue;
                                }
                                // lotSize is inside lotSizeFilter
                                if let Some(ls) = c["lotSizeFilter"]["lotSize"].as_str() {
                                    if let Ok(lot) = ls.parse::<f64>() {
                                        if lot > 0.0 {
                                            map.insert(format!("ascendex:{}", sym), lot);
                                            count += 1;
                                        }
                                    }
                                }
                                // tickSize is inside priceFilter
                                if let Some(ts) = c["priceFilter"]["tickSize"].as_str() {
                                    if let Ok(tick) = ts.parse::<f64>() {
                                        if tick > 0.0 {
                                            tmap.insert(format!("ascendex:{}", sym), tick);
                                        }
                                    }
                                }
                                // listingTime → tradingStartTime
                                if let Some(lt) = c["tradingStartTime"].as_i64() {
                                    if lt > 0 {
                                        lmap.insert(format!("ascendex:{}", sym), lt / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] AscendEX: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("ascendex:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] AscendEX: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // XT.COM USDT-M Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"xt".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://fapi.xt.com/future/market/v1/public/symbol/list")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["result"].as_array() {
                            let map = &e_ctx.multipliers;
                            let tmap = &e_ctx.tick_sizes;
                            let lmap = &e_ctx.launch_times;
                            let mut count = 0u32;
                            for c in arr {
                                let sym_raw = c["symbol"].as_str().unwrap_or("");
                                // XT uses "btc_usdt" format — normalize to "BTCUSDT"
                                let sym = sym_raw.replace('_', "").to_uppercase();
                                if !sym.ends_with("USDT") {
                                    continue;
                                }
                                // stepSize = qty step
                                if let Some(ss) = c["stepSize"].as_str() {
                                    if let Ok(step) = ss.parse::<f64>() {
                                        if step > 0.0 {
                                            map.insert(format!("xt:{}", sym), step);
                                            count += 1;
                                        }
                                    }
                                } else if let Some(ss) = c["minQty"].as_str() {
                                    if let Ok(step) = ss.parse::<f64>() {
                                        if step > 0.0 {
                                            map.insert(format!("xt:{}", sym), step);
                                            count += 1;
                                        }
                                    }
                                }
                                // priceStep = price tick
                                if let Some(ps) = c["priceStep"].as_str() {
                                    if let Ok(tick) = ps.parse::<f64>() {
                                        if tick > 0.0 {
                                            tmap.insert(format!("xt:{}", sym), tick);
                                        }
                                    }
                                }
                                // listTime / createTime
                                if let Some(lt) = c["listTime"].as_i64() {
                                    if lt > 0 {
                                        lmap.insert(format!("xt:{}", sym), lt / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] XT: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("xt:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] XT: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });

    // Toobit USDT-M
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        let settings = settings.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(7)).await;
            loop {
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"toobit".to_string()))
                };
                if is_disabled {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue;
                }

                if let Ok(resp) = http
                    .get("https://api.toobit.com/api/v1/exchangeInfo")
                    .send()
                    .await
                {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["symbols"].as_array() {
                            let map = &e_ctx.multipliers;
                            let tmap = &e_ctx.tick_sizes;
                            let _lmap = &e_ctx.launch_times;
                            let mut count = 0u32;
                            for c in arr {
                                let sym = c["symbol"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") {
                                    continue;
                                }
                                // Parse filters array for LOT_SIZE and PRICE_FILTER
                                if let Some(filters) = c["filters"].as_array() {
                                    for f in filters {
                                        let ft = f["filterType"].as_str().unwrap_or("");
                                        if ft == "LOT_SIZE" {
                                            if let Some(ss) = f["stepSize"].as_str() {
                                                if let Ok(step) = ss.parse::<f64>() {
                                                    if step > 0.0 {
                                                        map.insert(format!("toobit:{}", sym), step);
                                                        count += 1;
                                                    }
                                                }
                                            }
                                        } else if ft == "PRICE_FILTER" {
                                            if let Some(ts) = f["tickSize"].as_str() {
                                                if let Ok(tick) = ts.parse::<f64>() {
                                                    if tick > 0.0 {
                                                        tmap.insert(format!("toobit:{}", sym), tick);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            info!("[multipliers] Toobit: loaded {} symbols", count);
                        }
                    }
                }
                let loaded = e_ctx
                    .multipliers
                    .iter()
                    .filter(|e| e.key().starts_with("toobit:"))
                    .count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Toobit: FAILED to load, retrying in 90s");
                    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
                }
            }
        }
    });
}

