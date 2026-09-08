//! Canonical order-book quantities are always in the base asset.
pub fn vwap(bids: &[(f64, f64)], asks: &[(f64, f64)], buy: bool, quantity: f64) -> Option<f64> {
    if !quantity.is_finite() || quantity <= 0.0 {
        return None;
    }
    let levels = if buy { asks } else { bids };
    let mut remaining = quantity;
    let mut cost = 0.0;
    let mut previous = None;
    for &(price, size) in levels {
        if !price.is_finite() || !size.is_finite() || price <= 0.0 || size < 0.0 {
            return None;
        }
        if let Some(p) = previous {
            if (buy && price < p) || (!buy && price > p) {
                return None;
            }
        }
        previous = Some(price);
        let take = size.min(remaining);
        cost += price * take;
        remaining -= take;
        if remaining <= quantity * 1e-10 {
            let price = cost / (quantity - remaining);
            return if price.is_finite() { Some(price) } else { None };
        }
    }
    None
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_coverage_is_required() {
        assert!(vwap(&[], &[(10.0, 9.99)], true, 10.0).is_none());
    }
    #[test]
    fn sizes_are_base_units_without_a_second_contract_multiplier() {
        assert_eq!(
            vwap(&[], &[(10.0, 2.0), (11.0, 2.0)], true, 4.0),
            Some(10.5)
        );
    }
    #[test]
    fn invalid_and_unsorted_books_are_rejected() {
        assert!(vwap(&[], &[(10.0, 1.0), (9.0, 1.0)], true, 2.0).is_none());
        assert!(vwap(&[], &[(f64::NAN, 10.0)], true, 1.0).is_none());
    }
}
