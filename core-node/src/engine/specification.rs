//! Entry constraints are obtained from the venue's instrument metadata.
use super::orders::number;
use serde_json::Value;
#[derive(Clone, Debug)]
pub struct Specification {
    pub fetched_ms: i64,
    pub quantity_step: f64,
    pub price_tick: f64,
    pub min_quantity: f64,
    pub max_quantity: f64,
    pub min_notional: f64,
}
impl Specification {
    pub fn parse(exchange: &str, v: &Value, now: i64) -> Result<Self, String> {
        let (step, tick, min, max, notional) = match exchange {
            "binance" => {
                if v["status"].as_str() != Some("TRADING")
                    || v["contractType"].as_str() != Some("PERPETUAL")
                    || v["marginAsset"].as_str() != Some("USDT")
                {
                    return Err("Unsupported Binance instrument".into());
                }
                let filters = v["filters"].as_array().ok_or("Missing Binance filters")?;
                let f = |kind| {
                    filters
                        .iter()
                        .find(|f| f["filterType"].as_str() == Some(kind))
                        .ok_or("Missing Binance filter")
                };
                let lot = f("LOT_SIZE")?;
                (
                    number(lot, "stepSize")?,
                    number(f("PRICE_FILTER")?, "tickSize")?,
                    number(lot, "minQty")?,
                    number(lot, "maxQty")?,
                    number(f("MIN_NOTIONAL")?, "notional")?,
                )
            }
            "gate" => {
                if v["in_delisting"].as_bool() != Some(false)
                    || v["enable_decimal"].as_bool() == Some(true)
                {
                    return Err("Unsupported Gate instrument".into());
                }
                let m = number(v, "quanto_multiplier")?;
                (
                    m,
                    number(v, "order_price_round")?,
                    number(v, "order_size_min")? * m,
                    number(v, "order_size_max")? * m,
                    0.0,
                )
            }
            "kucoin" => {
                if v["status"].as_str() != Some("Open")
                    || v["isInverse"].as_bool() != Some(false)
                    || v["settleCurrency"].as_str() != Some("USDT")
                    || v["supportCross"].as_bool() != Some(true)
                    || number(v, "lotSize")? != 1.0
                {
                    return Err("Unsupported KuCoin instrument".into());
                }
                let m = number(v, "multiplier")?;
                (
                    m,
                    number(v, "tickSize")?,
                    m,
                    number(v, "maxOrderQty")? * m,
                    0.0,
                )
            }
            _ => return Err("Unsupported venue".into()),
        };
        if ![step, tick, min, max, notional]
            .iter()
            .all(|x| x.is_finite())
            || step <= 0.0
            || tick <= 0.0
            || min <= 0.0
            || max < min
            || notional < 0.0
        {
            return Err("Invalid instrument constraints".into());
        }
        Ok(Self {
            fetched_ms: now,
            quantity_step: step,
            price_tick: tick,
            min_quantity: min,
            max_quantity: max,
            min_notional: notional,
        })
    }
    pub fn permits(&self, now: i64, quantity: f64, price: f64, step: f64, tick: f64) -> bool {
        let age = now - self.fetched_ms;
        (0..=600_000).contains(&age)
            && quantity.is_finite()
            && price.is_finite()
            && price > 0.0
            && quantity >= self.min_quantity
            && quantity <= self.max_quantity
            && quantity * price >= self.min_notional
            && (step - self.quantity_step).abs() <= self.quantity_step * 1e-9
            && (tick - self.price_tick).abs() <= self.price_tick * 1e-9
            && (quantity / self.quantity_step - (quantity / self.quantity_step).round()).abs()
                <= 1e-8
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_exchange_filters_override_generic_five_dollar_floor() {
        let v = serde_json::json!({"status":"TRADING","contractType":"PERPETUAL","marginAsset":"USDT","filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"100"},{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"MIN_NOTIONAL","notional":"100"}]});
        let s = Specification::parse("binance", &v, 1_000).unwrap();
        assert!(!s.permits(1_001, 0.001, 60_000.0, 0.001, 0.1));
        assert!(s.permits(1_001, 0.002, 60_000.0, 0.001, 0.1));
        assert!(!s.permits(601_001, 0.002, 60_000.0, 0.001, 0.1));
        assert!(!s.permits(1_001, 0.002, 60_000.0, 0.001, 0.01));
        assert!(!s.permits(1_001, 101.0, 60_000.0, 0.001, 0.1));
    }
}
