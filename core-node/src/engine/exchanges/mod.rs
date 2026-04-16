pub mod binance;
pub mod bybit;
pub mod gate;
pub mod kucoin;
pub mod traits;
// pub mod context; // We'll put EngineContext here instead of state.rs if needed, or re-export

use std::sync::Arc;
use traits::ExchangeConnector;

pub fn get_connector(exchange: &str) -> Option<Arc<dyn ExchangeConnector>> {
    match exchange {
        "binance" => Some(Arc::new(binance::BinanceConnector)),
        "gate" => Some(Arc::new(gate::GateConnector)),
        "bybit" => Some(Arc::new(bybit::BybitConnector)),
        "kucoin" => Some(Arc::new(kucoin::KucoinConnector)),
        _ => None,
    }
}
