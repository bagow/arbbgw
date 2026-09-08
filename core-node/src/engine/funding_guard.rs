//! New entries must have a known funding boundary and exit before it.
use super::utils::now_ms;
use reqwest::Client;
use serde_json::Value;

pub const EXIT_BUFFER_MS: i64 = 60_000;
pub fn deadline(next_a: i64, next_b: i64, now: i64) -> Option<i64> {
    let next = next_a.min(next_b);
    // No new exposure in the five minutes immediately before settlement.
    (next > now + 300_000 && next <= now + 86_400_000).then_some(next - EXIT_BUFFER_MS)
}
pub async fn next(http: &Client, exchange: &str, pair: &str) -> anyhow::Result<i64> {
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
}
