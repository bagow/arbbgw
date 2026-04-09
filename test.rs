fn main() {
    let tick_b: f64 = 0.0001;
    let lpb: f64 = 0.11717; 
    let lpb_rounded = (lpb / tick_b).round() * tick_b;

    let mut price_str = format!("{:.8}", lpb_rounded).trim_end_matches('0').to_string();
    if price_str.ends_with('.') { price_str.pop(); }
    if price_str.is_empty() { price_str = "0".to_string(); }
    
    println!("Rounded: {}, Stringified: {}", lpb_rounded, price_str);
}
