# Коннекторы и Взаимодействие с Биржами (Exchange Connectors)

Все взаимодействие с криптобиржами изолировано в коннекторах. Движок не знает ни о форматах подписи (HMAC, RSA), ни о структуре JSON-ответов бирж.

> [!CAUTION]
> 🚨 **ЗАПРЕТ REST API [ОБЯЗАТЕЛЬНО К ИСПОЛНЕНИЮ]** 🚨
> **REST API (`http.get()`, `http.post()` к API бирж) СТРОГО ЗАПРЕЩЁН.**
> Все операции (ордера, проверка заливки, закрытие позиций, баланс, позиции, ордербуки) должны выполняться **ИСКЛЮЧИТЕЛЬНО через WebSocket**.
> REST блокируется Cloudflare, создаёт задержку 50-200мс, вызывает рейт-лимиты, и является **единственной причиной** "one-legged" сделок и ASYMMETRIC_CLOSE.
> Для Binance: обязательно использовать `newOrderRespType=RESULT` чтобы WS возвращал данные заливки inline.

## 🔌 Архитектура Коннектора (`ExchangeConnector` trait)

Любая биржа имплементирует единый трейт `ExchangeConnector` (находится в `core-node/src/engine/exchanges/traits.rs`). 

Трейт обязывает реализовать следующие критически важные абстракции:
1. `name()` — возвращает идентификатор (например `"gate"`).
2. `place_order(...)` — Размещает лимитные или рыночные ордера. Должен возвращать стандартизированный `(Option<String>, f64, f64)` (Order ID, Inlined Fill Price, Inlined Fee).
3. `close_position(...)` — Сложная функция, отвечающая за надежное закрытие позиции. Разрешено (и нужно) использовать внутренний Retry Loop (Fallbacks), если биржа начинает выдавать сетевые ошибки во время закрытия сделки.
4. `trim_position(...)` — Сверхбыстрый сброс (Dump) по маркету для ликвидации асимметрии объема ног.
5. `fetch_order(...)` — Опрос API об истории ордера. Используется для верификации IOC ордеров, если `place_order` вернул лишь айдишник.
6. `fetch_positions(...)` — Получение открытых позиций аккаунта (Ghost trade prevention). Возвращает наш внутренний тип `PositionInfo`.
7. `fetch_orderbook(...)` — JIT (Just-In-Time) запрос L2 стакана через REST API для VWAP верификации. Защищает от входа или выхода в пустой L1-стакан ("Ghost depth").

### 🛡️ Инкапсуляция костылей
Главное правило: **Все хаки и баги биржи чинятся только здесь**. 
- Примеры: API биржи `KuCoin` требует `&multiplier` равный лотам (size / lot_size), а `Gate` принимает физический объем `size` для шортов всегда с абсолютным знаком `f64.abs()`.
- Вы не должны менять `cycle.rs`, чтобы научить систему понимать разницу лотов Kucoin. Вся конвертация из физического объема `phys_amount` в строловые лоты происходит внутри конкретной реализации в файле `kucoin.rs`. 
- `KuCoin` возвращает `code="429000"` при rate_limit. Обработка этих уникальных кодов происходит внутри коннектора.

### HTTP Rate Limitations (Smart 429/418 handling)
Коннекторы агрессивно защищают API ключи. При запросах (в частности, Binance) перехватываются заголовки `X-MBX-USED-WEIGHT-1M` и `Retry-After`. 
Если биржа возвращает HTTP `429` (Too Many Requests), коннектор обязан увести процесс в `tokio::time::sleep`, иначе следующий запрос вызовет гнев биржи и приведет к HTTP `418 IP banned`.

### Close Verification Truthfulness (критическое правило)
- `close_position()` НЕ имеет права возвращать успешный `CloseReceipt`, если цена/комиссия закрытия не подтверждены через `fetch_order`.
- Если по deep-verify видно, что позиция исчезла, но fill details не получены, коннектор обязан вернуть ошибку верификации (`unverified close`) вместо фиктивного `final_price=target_price` и `fee=0`.
- Это защищает PnL от скрытого искажения и не позволяет двигателю записывать ложную финансовую реальность в `trades`.
- Сравнение символов при deep-verify выполняется только через нормализацию (например, `BTC_USDT`, `BTCUSDT`, `XBTUSDTM`).

---
## 🌐 WebSocket Stream Collectors (`core-node/src/collector/`)

Коллекторы отвечают за получение рыночных котировок Orderbook в режиме реального времени.

1. **Инициализация и Кэширование (SymMap)**:
   При старте коллектора он отправляет `GET` запрос к REST API биржи (например `/v5/market/instruments-info`), чтобы получить все доступные фьючерсные контракты (Tickers).
   После этого он строит `HashMap<String, String>`, связывая грязные айдишники бирж (напр `XBTUSDM` на KuCoin или `BTC_USDT` на Gate) к единому стандарту `cryptoarb` (например `BTCUSDT`).
   Это кэширование гарантирует отсутствие аллокаций памяти (String formatting bindings) в горячем цикле.

2. **Zero-Copy парсинг**:
   Топики вебсокета обрабатываются потоком с нулевым копированием: 
   Нельзя использовать `let root: serde_json::Value = serde_json::from_str(&msg)`.
   Вместо этого мы пишем жесткие структуры и `#[derive(Deserialize)]` с обилием `IgnoredAny`, парся данные как:
   `let data: FastWsMsg = serde_json::from_str(&msg).unwrap();`
   
3. **Обновление Глобального `PriceMap`**:
   Тики (Bid/Ask) заливаются в глобальную `DashMap` (Thread-Safe таблица). Сразу после обновления коллектор вызывает `update_signal.notify_waiters()`. Этот `signal` мгновенно пробуждает спящий цикл `engine` для анализа новых вероятных спредов без напрасного расхода CPU в режиме жесткого поллинга.

### Изоляция WebSocket DeadLock (Bybit Fix)
Коннектор `bybit.rs` (и другие) оборачиваются в `tokio::spawn` с автоматическим перезапуском в бесконечном цикле `loop {}`. 
Если WS поток по каким-то причинам завис, не прислал пинг за 20 секунд или разорвал соединение с кодом `1006` - процесс выходит из внутреннего цикла и перезапускает HTTP Handshake без падения всего `core-node`.

---
## ⚡ WebSocket Trading API (`core-node/src/engine/exchanges/ws_*.rs`)

Для минимизации задержки при размещении ордеров (2-8ms вместо 50-120ms REST), коннекторы поддерживают persistent WebSocket соединения для исполнения ордеров.

### Поддержка по биржам

| Биржа | WS Trading | Файл | Endpoint |
|---|---|---|---|
| ✅ Binance | Да | `ws_trader.rs` | `wss://ws-fapi.binance.com/ws-fapi/v1` |
| ✅ Bybit | Да | `ws_bybit.rs` | `wss://stream.bybit.com/v5/trade` |
| ✅ Gate.io | Да | `ws_gate.rs` | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| ❌ HTX | Нет | — | API не поддерживает |
| ⏳ KuCoin | Планируется | — | `wss://wsapi.kucoin.com/v1/private` |

### Архитектура WS Trading

1. **Lazy Init**: WS соединение создаётся при первом ордере через `get_or_init_*_ws()`. 
   Используется `OnceLock` для singleton-инициализации.
2. **Persistent Connection**: Одно WS соединение переиспользуется для всех ордеров.
   Auto-reconnect при обрыве (бесконечный loop с задержкой).
3. **WS-ONLY (NO REST FALLBACK)**: Каждый `place_order()` / `close_position()` отправляет 
   ордер **ИСКЛЮЧИТЕЛЬНО через WS**. REST fallback **ЗАПРЕЩЁН**. Если WS недоступен или 
   вернул ошибку → ордер считается неисполненным (fail-fast). Данные о заливке (price, fee, qty)
   извлекаются **из WS-ответа** (`newOrderRespType=RESULT`), а не через отдельный REST-запрос.
4. **Request Correlation**:
   - Binance: поле `id` (UUID)
   - Bybit: поле `reqId` (UUID)  
   - Gate.io: поле `request_id` (UUID)
5. **Auth**:
   - Binance: подпись в params каждого запроса
   - Bybit: отдельный auth handshake при подключении (HMAC-SHA256 `GET/realtime{expires}`)
   - Gate.io: inline auth в каждом запросе (HMAC-SHA512 `channel=&event=&time=`)
6. **Keepalive**: Ping отправляется каждые 10-18с в зависимости от биржи.

### Правило изоляции

Вся WS-специфичная логика (auth, reconnect, signature) находится **строго внутри** 
файлов `ws_*.rs` и соответствующих коннекторов (`binance.rs`, `bybit.rs`, `gate.rs`).
Движок `cycle.rs` и `hft_cycle.rs` **не знают** о том, какой транспорт (WS или REST) 
используется для исполнения ордера.
