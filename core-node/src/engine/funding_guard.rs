//! New entries must have a known funding boundary. EMA trades exit before it;
//! explicitly admitted funding captures exit just after synchronized payments.
use super::utils::now_ms;
use crate::state::{EngineContext, FundingInfo};
use reqwest::Client;
use serde_json::Value;

pub const EXIT_BUFFER_MS: i64 = 60_000;
pub const CAPTURE_EXIT_DELAY_MS: i64 = 30_000;
const CAPTURE_MIN_LEAD_MS: i64 = 60_000;
const CAPTURE_MAX_LEAD_MS: i64 = 300_000;
const CAPTURE_MAX_SETTLEMENT_SKEW_MS: i64 = 60_000;
// The schedule is stable for hours, but the rate being captured is not. The
// preflight already refreshes open-order state every five seconds, so a
// thirty-second funding refresh adds little load and keeps entry fail-closed.
const CACHE_TTL_MS: i64 = 30_000;

fn cache_key(exchange: &str, pair: &str) -> String {
    format!("{exchange}:{pair}")
}

fn valid_cached_timestamp(info: &FundingInfo, now: i64) -> Option<i64> {
    (now >= info.fetched_at
        && now - info.fetched_at <= CACHE_TTL_MS
        && info.next_ts > now
        && info.next_ts <= now + 86_400_000)
        .then_some(info.next_ts)
}

pub fn is_cached(ctx: &EngineContext, exchange: &str, pair: &str) -> bool {
    let now = now_ms();
    ctx.funding_rates
        .get(&cache_key(exchange, pair))
        .and_then(|info| valid_cached_timestamp(info.value(), now))
        .is_some()
}

/// Read an already-authorized funding boundary without performing network I/O.
/// The execution path uses only this accessor; refreshes happen during candidate
/// preparation, before a signal is eligible for dispatch.
pub fn cached_next(ctx: &EngineContext, exchange: &str, pair: &str) -> Option<i64> {
    let now = now_ms();
    ctx.funding_rates
        .get(&cache_key(exchange, pair))
        .and_then(|info| valid_cached_timestamp(info.value(), now))
}

pub fn cached_info(ctx: &EngineContext, exchange: &str, pair: &str) -> Option<FundingInfo> {
    let now = now_ms();
    ctx.funding_rates
        .get(&cache_key(exchange, pair))
        .and_then(|info| valid_cached_timestamp(info.value(), now).map(|_| info.value().clone()))
}

pub fn deadline(next_a: i64, next_b: i64, now: i64) -> Option<i64> {
    let next = next_a.min(next_b);
    // No new exposure in the five minutes immediately before settlement.
    (next > now + 300_000 && next <= now + 86_400_000).then_some(next - EXIT_BUFFER_MS)
}

/// A funding capture must own both legs before both settlements and must close
/// shortly after the later one. The profile's ordinary holding limit remains
/// the hard upper bound, so this strategy cannot silently become a multi-hour
/// carry trade.
pub fn capture_window(
    next_a: i64,
    next_b: i64,
    now: i64,
    max_holding_ms: i64,
) -> Option<(i64, i64)> {
    let first = next_a.min(next_b);
    let settlement = next_a.max(next_b);
    let exit = settlement.checked_add(CAPTURE_EXIT_DELAY_MS)?;
    (max_holding_ms > CAPTURE_EXIT_DELAY_MS
        && first > now + CAPTURE_MIN_LEAD_MS
        && settlement <= now + CAPTURE_MAX_LEAD_MS
        && next_a.abs_diff(next_b) <= CAPTURE_MAX_SETTLEMENT_SKEW_MS as u64
        && exit > now
        && exit - now <= max_holding_ms)
        .then_some((settlement, exit))
}

fn rate(value: &Value, key: &str) -> Option<f64> {
    value[key]
        .as_f64()
        .or_else(|| value[key].as_str().and_then(|raw| raw.parse().ok()))
        .filter(|rate: &f64| rate.is_finite() && rate.abs() <= 0.1)
}

fn kucoin_current_cycle_rate(data: &Value) -> Option<f64> {
    match data["period"].as_i64() {
        // KuCoin documents period=0 as a cross-period record that is not
        // charged in the current cycle.
        Some(0) => Some(0.0),
        Some(1) => rate(data, "fundingFeeRate"),
        _ => None,
    }
}

async fn fetch_next(http: &Client, exchange: &str, pair: &str) -> anyhow::Result<FundingInfo> {
    anyhow::ensure!(
        pair.ends_with("USDT") && pair.bytes().all(|b| b.is_ascii_alphanumeric()),
        "Invalid funding symbol"
    );
    let (url, symbol) = match exchange {
        "binance" => (
            format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={pair}"),
            pair.to_owned(),
        ),
        "gate" => {
            let s = format!("{}_USDT", pair.strip_suffix("USDT").unwrap());
            (
                format!("https://api.gateio.ws/api/v4/futures/usdt/contracts/{s}"),
                s,
            )
        }
        "kucoin" => {
            let s = if pair == "BTCUSDT" {
                "XBTUSDTM".into()
            } else {
                format!("{pair}M")
            };
            (
                format!("https://api-futures.kucoin.com/api/v1/contracts/{s}"),
                s,
            )
        }
        _ => anyhow::bail!("Unsupported funding schedule"),
    };
    let v: Value = http
        .get(url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let (timestamp, funding_rate) = match exchange {
        "binance" => {
            anyhow::ensure!(
                v["symbol"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            (v["nextFundingTime"].as_i64(), rate(&v, "lastFundingRate"))
        }
        "gate" => {
            anyhow::ensure!(
                v["name"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            (
                v["funding_next_apply"]
                    .as_i64()
                    .and_then(|x| x.checked_mul(1000)),
                rate(&v, "funding_rate"),
            )
        }
        "kucoin" => {
            anyhow::ensure!(
                v["code"].as_str() == Some("200000")
                    && v["data"]["symbol"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            // Absolute epoch milliseconds, not the countdown nextFundingRateTime.
            (
                v["data"]["nextFundingRateDateTime"].as_i64(),
                kucoin_current_cycle_rate(&v["data"]),
            )
        }
        _ => (None, None),
    };
    let timestamp = timestamp
        .ok_or_else(|| anyhow::anyhow!("Missing funding settlement timestamp"))?;
    let funding_rate = funding_rate.ok_or_else(|| anyhow::anyhow!("Missing funding rate"))?;
    anyhow::ensure!(
        timestamp > now_ms() && timestamp <= now_ms() + 86_400_000,
        "Stale funding schedule"
    );
    Ok(FundingInfo {
        rate: funding_rate,
        next_ts: timestamp,
        fetched_at: now_ms(),
    })
}

/// Return an authoritative recent funding boundary. Funding schedules are public
/// and stable for hours, but a short TTL keeps settlement changes fail-closed
/// while allowing candidate prewarming to remove this request from the entry path.
pub async fn next(
    http: &Client,
    exchange: &str,
    pair: &str,
    ctx: &EngineContext,
) -> anyhow::Result<i64> {
    let key = cache_key(exchange, pair);
    let now = now_ms();
    if let Some(timestamp) = ctx
        .funding_rates
        .get(&key)
        .and_then(|info| valid_cached_timestamp(info.value(), now))
    {
        return Ok(timestamp);
    }
    let info = fetch_next(http, exchange, pair).await?;
    let timestamp = info.next_ts;
    ctx.funding_rates.insert(key, info);
    Ok(timestamp)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn earliest_venue_controls_deadline_and_near_settlement_blocks_entry() {
        assert_eq!(deadline(600_000, 900_000, 0), Some(540_000));
        assert_eq!(deadline(300_000, 900_000, 0), None);
        assert_eq!(deadline(600, 900_000, 0), None);
        assert_eq!(deadline(90_000_000, 95_000_000, 0), None);
    }

    #[test]
    fn capture_requires_synchronized_settlement_inside_holding_limit() {
        assert_eq!(
            capture_window(240_000, 240_030, 0, 300_000),
            Some((240_030, 270_030)),
        );
        assert_eq!(capture_window(240_000, 301_000, 0, 700_000), None);
        assert_eq!(capture_window(60_000, 60_000, 0, 700_000), None);
        assert_eq!(capture_window(300_001, 300_001, 0, 700_000), None);
        assert_eq!(capture_window(240_000, 240_000, 0, 240_000), None);
    }

    #[test]
    fn public_rate_parser_rejects_nonfinite_and_implausible_values() {
        assert_eq!(rate(&serde_json::json!({"r":"-0.0025"}), "r"), Some(-0.0025));
        assert_eq!(rate(&serde_json::json!({"r":0.003}), "r"), Some(0.003));
        assert_eq!(rate(&serde_json::json!({"r":"NaN"}), "r"), None);
        assert_eq!(rate(&serde_json::json!({"r":0.2}), "r"), None);
        assert_eq!(
            kucoin_current_cycle_rate(
                &serde_json::json!({"period":1,"fundingFeeRate":-0.002})
            ),
            Some(-0.002),
        );
        assert_eq!(
            kucoin_current_cycle_rate(
                &serde_json::json!({"period":0,"fundingFeeRate":-0.002})
            ),
            Some(0.0),
        );
        assert_eq!(
            kucoin_current_cycle_rate(&serde_json::json!({"fundingFeeRate":-0.002})),
            None,
        );
    }

    #[test]
    fn cached_schedule_must_be_recent_and_in_the_future() {
        let now = 1_000_000;
        let valid = FundingInfo {
            rate: 0.0,
            next_ts: now + 600_000,
            fetched_at: now - CACHE_TTL_MS,
        };
        assert_eq!(valid_cached_timestamp(&valid, now), Some(now + 600_000));
        assert_eq!(
            valid_cached_timestamp(
                &FundingInfo {
                    fetched_at: now - CACHE_TTL_MS - 1,
                    ..valid.clone()
                },
                now,
            ),
            None
        );
        assert_eq!(
            valid_cached_timestamp(
                &FundingInfo {
                    next_ts: now,
                    ..valid
                },
                now,
            ),
            None
        );
    }
}
