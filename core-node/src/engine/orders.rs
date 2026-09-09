//! Durable order contract. An acknowledgement or timeout is never a fill.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    Submitted,
    Unknown,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}
impl OrderState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_id: String,
    pub pair: String,
    pub is_buy: bool,
    pub quantity: f64,    // base asset, not contracts
    pub limit_price: f64, // exact hard price boundary, zero only for emergency reduce-only
    pub reduce_only: bool,
    pub multiplier: f64,
    pub tick_size: f64,
    pub leverage: f64,
}
impl OrderRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.client_id.is_empty() || self.pair.is_empty() {
            return Err("Missing order identity".into());
        }
        if ![
            self.quantity,
            self.limit_price,
            self.multiplier,
            self.tick_size,
            self.leverage,
        ]
        .iter()
        .all(|x| x.is_finite())
            || self.quantity <= 0.0
            || self.multiplier <= 0.0
            || self.tick_size <= 0.0
            || self.leverage < 1.0
            || self.limit_price < 0.0
            || (!self.reduce_only && self.limit_price == 0.0)
        {
            return Err("Invalid order quantity/price/specification".into());
        }
        let steps = self.quantity / self.multiplier;
        if !steps.is_finite()
            || (steps - steps.round()).abs() > 1e-8
            || (self.limit_price > 0.0
                && bounded_price(self.limit_price, self.tick_size, self.is_buy) <= 0.0)
        {
            return Err("Off-grid quantity or price rounds to zero".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderObservation {
    pub order_id: Option<String>,
    pub state: OrderState,
    pub filled_quantity: f64,
    pub average_price: Option<f64>,
    /// Signed USDT cost; None means not yet confirmed, zero is a valid confirmed fee.
    pub fee: Option<f64>,
}
impl OrderObservation {
    /// Reserve each unconfirmed fee independently. A confirmed fee on another
    /// order must never absorb this order's reserve (including maker rebates).
    pub fn fee_with_reserve(&self, rate: f64) -> f64 {
        self.fee
            .unwrap_or_else(|| self.filled_quantity * self.average_price.unwrap_or(0.0) * rate)
    }

    pub fn rejected() -> Self {
        Self {
            order_id: None,
            state: OrderState::Rejected,
            filled_quantity: 0.0,
            average_price: None,
            fee: Some(0.0),
        }
    }
    pub fn pending(order_id: Option<String>) -> Self {
        Self {
            order_id,
            state: OrderState::Submitted,
            filled_quantity: 0.0,
            average_price: None,
            fee: None,
        }
    }
    pub fn validate(&self, r: &OrderRequest) -> Result<(), String> {
        if !self.filled_quantity.is_finite()
            || self.filled_quantity < 0.0
            || self.filled_quantity > r.quantity + r.quantity * 1e-8
            || self.fee.map(|x| !x.is_finite()).unwrap_or(false)
            || (self.filled_quantity > 0.0
                && !self
                    .average_price
                    .map(|p| p.is_finite() && p > 0.0)
                    .unwrap_or(false))
            || (self.filled_quantity > 0.0
                && self
                    .order_id
                    .as_ref()
                    .map(|id| id.is_empty())
                    .unwrap_or(true))
            || (self.state == OrderState::Rejected && self.filled_quantity != 0.0)
            || (self.state == OrderState::Filled
                && (self.filled_quantity - r.quantity).abs() > r.quantity * 1e-8)
        {
            return Err("Invalid or incomplete exchange execution receipt".into());
        }
        Ok(())
    }
}

/// Round towards safety: a buy cannot exceed its price cap; a sell cannot go below its floor.
pub fn bounded_price(price: f64, tick: f64, buy: bool) -> f64 {
    if buy {
        ((price / tick) + 1e-10).floor() * tick
    } else {
        ((price / tick) - 1e-10).ceil() * tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acknowledgement_and_partial_are_not_terminal() {
        assert!(!OrderState::Submitted.terminal());
        assert!(!OrderState::Unknown.terminal());
        assert!(!OrderState::PartiallyFilled.terminal());
        assert!(OrderState::Cancelled.terminal());
    }
    #[test]
    fn tick_rounding_cannot_spend_extra_budget() {
        assert_eq!(bounded_price(10.09, 0.1, true), 10.0);
        assert!((bounded_price(10.01, 0.1, false) - 10.1).abs() < 1e-9);
    }
    #[test]
    fn positive_price_does_not_prove_full_fill() {
        let r = OrderRequest {
            client_id: "x".into(),
            pair: "XUSDT".into(),
            is_buy: true,
            quantity: 10.0,
            limit_price: 1.0,
            reduce_only: false,
            multiplier: 1.0,
            tick_size: 0.01,
            leverage: 1.0,
        };
        let mut o = OrderObservation {
            order_id: Some("1".into()),
            state: OrderState::Filled,
            filled_quantity: 3.0,
            average_price: Some(1.0),
            fee: None,
        };
        assert!(o.validate(&r).is_err());
        o.state = OrderState::Cancelled;
        assert!(o.validate(&r).is_ok());
    }

    #[test]
    fn mixed_confirmed_and_pending_fees_keep_the_full_reserve() {
        let mut confirmed = OrderObservation {
            order_id: Some("1".into()),
            state: OrderState::Filled,
            filled_quantity: 1.0,
            average_price: Some(100.0),
            fee: Some(0.3),
        };
        let pending = OrderObservation {
            fee: None,
            ..confirmed.clone()
        };
        let total = confirmed.fee_with_reserve(0.001) + pending.fee_with_reserve(0.001);
        assert!((total - 0.4).abs() < 1e-12);
        // The former max(confirmed total, estimated total) reserved only 0.3.
        confirmed.fee = Some(-0.02);
        let total = confirmed.fee_with_reserve(0.001) + pending.fee_with_reserve(0.001);
        assert!((total - 0.08).abs() < 1e-12);
    }

    #[test]
    fn fee_reserve_uses_only_executed_quantity_and_preserves_confirmed_zero() {
        let mut o = OrderObservation::pending(None);
        assert_eq!(o.fee_with_reserve(0.001), 0.0);
        o.filled_quantity = 0.25;
        o.average_price = Some(100.0);
        assert_eq!(o.fee_with_reserve(0.001), 0.025);
        o.fee = Some(0.0);
        assert_eq!(o.fee_with_reserve(0.001), 0.0);
    }
}

pub fn number(v: &serde_json::Value, key: &str) -> Result<f64, String> {
    let x = v[key]
        .as_f64()
        .or_else(|| v[key].as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| format!("Missing numeric field {key}"))?;
    if x.is_finite() {
        Ok(x)
    } else {
        Err(format!("Nonfinite field {key}"))
    }
}
pub fn identity(v: &serde_json::Value, key: &str) -> Option<String> {
    v[key]
        .as_str()
        .map(str::to_string)
        .or_else(|| v[key].as_u64().map(|x| x.to_string()))
}

/// An exchange ledger credit is stored as a negative cost, a debit as positive.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FundingEvent {
    pub id: String,
    pub time_ms: i64,
    pub cost: f64,
}
