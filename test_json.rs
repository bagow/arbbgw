fn main() {
    let v = serde_json::json!(10);
    println!("as_f64: {:?}", v.as_f64());
}
