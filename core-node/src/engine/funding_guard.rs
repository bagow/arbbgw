//! New entries must have a known funding boundary and exit before it.
use super::utils::now_ms;
use crate::state::{EngineContext, FundingInfo};
use reqwest::Client;
use serde_json::Value;

pub const EXIT_BUFFER_MS: i64 = 60_000;
// Settlement schedules are stable for hours. Match the persisted five-minute
// technical default so one hot candidate cannot monopolize the prewarm lane.
const CACHE_TTL_MS: i64 = 300_000;

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

pub fn deadline(next_a: i64, next_b: i64, now: i64) -> Option<i64> {
    let next = next_a.min(next_b);
    // No new exposure in the five minutes immediately before settlement.
    (next > now + 300_000 && next <= now + 86_400_000).then_some(next - EXIT_BUFFER_MS)
}
async fn fetch_next(http: &Client, exchange: &str, pair: &str) -> anyhow::Result<i64> {
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
    let timestamp = match exchange {
        "binance" => {
            anyhow::ensure!(
                v["symbol"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            v["nextFundingTime"].as_i64()
        }
        "gate" => {
            anyhow::ensure!(
                v["name"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            v["funding_next_apply"]
                .as_i64()
                .and_then(|x| x.checked_mul(1000))
        }
        "kucoin" => {
            anyhow::ensure!(
                v["code"].as_str() == Some("200000")
                    && v["data"]["symbol"].as_str() == Some(&symbol),
                "Funding symbol mismatch"
            );
            // Absolute epoch milliseconds, not the countdown nextFundingRateTime.
            v["data"]["nextFundingRateDateTime"].as_i64()
        }
        _ => None,
    }
    .ok_or_else(|| anyhow::anyhow!("Missing funding settlement timestamp"))?;
    anyhow::ensure!(
        timestamp > now_ms() && timestamp <= now_ms() + 86_400_000,
        "Stale funding schedule"
    );
    Ok(timestamp)
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
    let timestamp = fetch_next(http, exchange, pair).await?;
    ctx.funding_rates.insert(
        key,
        FundingInfo {
            rate: 0.0,
            next_ts: timestamp,
            fetched_at: now_ms(),
        },
    );
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
