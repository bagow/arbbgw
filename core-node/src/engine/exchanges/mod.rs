pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod gate;
pub mod kucoin;
pub mod bingx;
pub mod bitmex;
pub mod ascendex;
pub mod xt;
pub mod toobit;
pub mod traits;
pub mod ws_trader;
pub mod ws_bybit;
pub mod ws_gate;
pub mod ws_kucoin;

use std::sync::Arc;
use traits::ExchangeConnector;

pub fn get_connector(exchange: &str) -> Option<Arc<dyn ExchangeConnector>> {
    match exchange {
        "binance"  => Some(Arc::new(binance::BinanceConnector)),
        "gate"     => Some(Arc::new(gate::GateConnector)),
        "bybit"    => Some(Arc::new(bybit::BybitConnector)),
        "kucoin"   => Some(Arc::new(kucoin::KucoinConnector)),
        "bitget"   => Some(Arc::new(bitget::BitgetConnector)),
        "bingx"    => Some(Arc::new(bingx::BingxConnector)),
        "bitmex"   => Some(Arc::new(bitmex::BitmexConnector)),
        "ascendex" => Some(Arc::new(ascendex::AscendexConnector)),
        "xt"       => Some(Arc::new(xt::XtConnector)),
        "toobit"   => Some(Arc::new(toobit::ToobitConnector)),
        _ => None,
    }
}

pub mod ws_kucoin_private;
pub mod ws_bitget_private;
pub mod ws_bitmex_private;
pub mod ws_bingx_private;
