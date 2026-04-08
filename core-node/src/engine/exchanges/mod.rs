pub mod traits;
pub mod binance;
pub mod gate;
pub mod bybit;
pub mod kucoin;
// pub mod context; // We'll put EngineContext here instead of state.rs if needed, or re-export

use traits::ExchangeConnector;
use std::sync::Arc;

pub fn get_connector(exchange: &str) -> Option<Arc<dyn ExchangeConnector>> {
    match exchange {
        "binance" => Some(Arc::new(binance::BinanceConnector)),
        "gate" => Some(Arc::new(gate::GateConnector)),
        "bybit" => Some(Arc::new(bybit::BybitConnector)),
        "kucoin" => Some(Arc::new(kucoin::KucoinConnector)),
        _ => None,
    }
}

