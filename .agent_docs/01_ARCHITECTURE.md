# Архитектура и Главные Правила (System Architecture & Core Rules)

## Самое главное правило (CRITICAL RULE)
> [!CAUTION]
> 🚨 **СТРОГОЕ АРХИТЕКТУРНОЕ ПРАВИЛО [ОБЯЗАТЕЛЬНО К ИСПОЛНЕНИЮ]** 🚨
> Никакая логика взаимодействия с биржами (обход багов, костыли для Hedge Mode, расчет fallback-цен для маркет-ордеров) **НЕ ДОЛЖНА ВЫНОСИТЬСЯ ИЗ КОННЕКТОРОВ** (`gate.rs`, `binance.rs` и т.д.) в общие движки вроде `cycle.rs`. Если какому-то коннектору требуются особые хаки для выполнения приказа (например, подмена price=0.0 на агрессивный лимитный ордер), **все делается строго внутри файла этого коннектора**. `cycle.rs` не должен "знать" о проблемах конкретных бирж. Он собирает чистую математику сканера и передает абстрактные команды коннекторам.

---

## 🏗️ Микросервисы системы

Система представляет собой распределенный монолит на Rust, разделенный на две основные ноды: `core-node` (алгоритмическая торговля) и `vps-api` (управление и статистика).

### 1. `core-node` (Торговый движок)
Главный рабочий процесс-монолит, работающий как systemd-демон. 
- Прослушивает WebSockets бирж в режиме реального времени.
- Собирает книгу ордеров (Orderbook/Mids) и расчитывает структурные отклонения (EMA).
- Открывает и закрывает параллельные арбитражные связки.
- **Взаимодействие с БД**: Только чтение настроек из `vps-api.db` (настройки) и активная запись сделок, кэшей и логов в `real-trader.db`.
- **In-Memory State**: Все критические торговые переменные живут исключительно в ОЗУ через `DashMap` (Thread-Safe Concurrent Maps).

### 2. `vps-api` (Бэкенд Интерфейса)
REST/Websocket сервер на фреймворке Axum.
- Обеспечивает связь визуального Дашборда с базами данных.
- Поддерживает endpoints для настроек трейдинга (обновление Trade Volume, Slippage, API ключей и прочего).
- **Взаимодействие с БД**: Эксклюзивно сохраняет настройки профилей в `vps-api.db`, читает статистику прошедших трейдов из `real-trader.db`.

### 3. Базы данных (SQLite WAL Mode)
Используются строго разделенные SQLite DataBases с активным WAL-режимом (Write-Ahead Logging) для предотвращения Database lock'ов.
- **`vps-api.db`**: Принадлежит бэкенду. Хранит аккаунты, настройки ключи. `core-node` читает этот файл 1 раз в пару секунд.
- **`real-trader.db`**: Принадлежит движку. Хранит историю трейдов (`trades`), кэши (`server_cache`) и снапшоты для replay-аналитики (`decision_snapshots`).

---

## Важный recovery-контур (ликвидации и внешние закрытия)

- В `core-node` должен быть активен фоновый `position_sync`: сверка `OPEN` записей с `fetch_positions()` по биржам.
- Если одна нога исчезла на бирже:
  - движок обязан пытаться аварийно закрыть вторую ногу,
  - после попытки сделка не должна оставаться "зависшей" в `OPEN`.
- Если обе ноги отсутствуют, сделка переводится в `CLOSED` с `close_reason='EXTERNAL_CLOSE'`.
- Символы позиций сравниваются после нормализации (например, `BTC_USDT`, `BTCUSDT`, `XBTUSDTM` -> единый canonical), иначе возникают ложные external-close срабатывания.

## Каноника финансовой модели

- Канонический net PnL во всех слоях системы:
  - `net = gross - commission - funding - slippage`
- Где:
  - `gross = pnl_a + pnl_b`
  - `commission` включает вход + выход
  - `funding` накопленный funding cost
  - `slippage` накопленный slippage/trim loss
- Любые API/дашборд агрегаты (`current_pnl`, history totals, stats) должны использовать эту же формулу без исключений.

---

## ⚡ HFT Guidelines (Ограничения Горячего Цикла)

Весь входящий рыночный поток обрабатывается в файлах `core-node/src/collector/`. Любая миллисекундная задержка здесь мультиплицируется на тысячи тиков в секунду, поэтому применяются следующие строгие правила (Zero-Copy JSON Parsing):

1. **Нет `serde_json::Value` (Сырого парсинга)**: В корне горячего цикла (`tokio::select!`) категорически **ЗАПРЕЩЕНО** парсить неизвестные структуры в `serde_json::Value`. Применяются жесткие структуры с `#[derive(Deserialize)]` с игнорированием лишних полей для минимизации выделений памяти.
2. **Нет Аллокациям в Цикле**: ЗАПРЕЩЕНО создавать новые строки (`String::replace`, `.to_string()`, кланирования `clone()`) внутри обработчика тиков на каждый пинг. Вместо этого маппинги и нормализация токенов (например "XBTUSDM" в "BTCUSDT") вычисляются *до* запуска цикла и кладутся в `HashMap<String, String>`. В самом цикле разрешен только хэш-поиск по ссылке.
3. **Zero-Copy Reference Borrowing**: Сырые байты входящего WS сообщения (`ws::Message::Text(t)`) заимствуются напрямую через сырой строковый срез (`&'a str`), чтобы не плодить дубликаты памяти для JSON десериализатора.

---

## Правила архитектурной документации (DOCUMENTATION MANDATE)

> **ЛЮБОЕ АРХИТЕКТУРНОЕ ИЛИ КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ ОБЯЗАНО БЫТЬ ОТРАЖЕНО В `01_ARCHITECTURE.md` И СОСЕДНИХ ДОКУМЕНТАХ `.agent_docs/`**.
Документация для агентов в этой директории является ЕДИНСТВЕННЫМ источником истины (Ground Truth) системы. При добавлении механизмов, изменении форматов БД или конвенций обхода API бирж — сначала обновите мануалы, чтобы следующие агенты не разрушили архитектурную целостность.

---

## 🔀 Dual Strategy Architecture (EMA + HFT)

Система поддерживает два полностью изолированных торговых модуля. Каждый профиль имеет поле `strategy_type` (`"ema"` или `"hft"`), которое определяет, какой модуль обрабатывает этот профиль.

### Dispatch (main.rs)
```
Engine Loop (каждые ~10ms)
  ├── strategy_type == "ema"  → scanner.rs → cycle.rs       (EMA Reversion)
  └── strategy_type == "hft"  → hft_scanner.rs → hft_cycle.rs  (Latency Arb)
```

### Общие ресурсы (shared):
- Exchange коннекторы (`exchanges/`)
- DB слой (`db.rs`, `TradeRepository`)
- Ценовые данные (`PriceMap`)
- `EngineContext` (cooldowns, balances, multipliers)
- `TradingSettings` + `TechnicalSettings`

### Изолированные модули:
| Компонент | EMA (существующий) | HFT (новый) |
|---|---|---|
| Сканер | `scanner.rs` | `hft_scanner.rs` |
| Цикл | `cycle.rs` | `hft_cycle.rs` |
| Вход | structural EMA + deviation | raw spread > break-even |
| Выход | trailing stop + TP% | timeout (`hft_timeout_ms`) или spread ≤ 0 |
| Close reasons | TP, STOP, ZERO_SPREAD | HFT_TIMEOUT, HFT_REVERT |

### Поля в `TradingSettings` (shared/lib.rs):
- `strategy_type: String` — `"ema"` (default) или `"hft"`
- `hft_timeout_ms: i64` — макс. время удержания HFT сделки (default: 10000ms)

### Поля в DB (`trading_profiles` table):
- `strategy_type TEXT DEFAULT 'ema'`
- `hft_timeout_ms INTEGER DEFAULT 10000`

> [!IMPORTANT]
> **ПРАВИЛО ИЗОЛЯЦИИ**: Код EMA модуля (`scanner.rs`, `cycle.rs`) НЕ ДОЛЖЕН содержать `if strategy_type == "hft"`. Код HFT модуля (`hft_scanner.rs`, `hft_cycle.rs`) НЕ ДОЛЖЕН содержать логику EMA/structural spread. Вся маршрутизация происходит ТОЛЬКО в `main.rs`.

---

## 🖥️ Серверы

### Production (Tokyo) — ЕДИНСТВЕННЫЙ АКТИВНЫЙ СЕРВЕР
```
ssh -i "tokyo.pem" ubuntu@ec2-13-158-135-57.ap-northeast-1.compute.amazonaws.com
```
- **OS**: Ubuntu (`user: ubuntu`)
- **Локация**: Tokyo (ap-northeast-1) — минимальная задержка до бирж (Binance, Bybit, BingX)
- **Провайдер**: AWS EC2
- **Cloudflare tunnel** + **Cloudflare front**
- **Сервисы**: `core-node.service`, `vps-api.service`, `cloudflared.service`

---

## 🚫 Правила компиляции

- **ЗАПРЕЩЕНО** компилировать (`cargo build`, `cargo check`) на VPS и на локальной машине
- Только через **GitHub CI/CD** — просто `git push`
- CI собирает бинарники и деплоит на сервер автоматически

