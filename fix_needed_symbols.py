import os, glob

for file in glob.glob('core-node/src/collector/*.rs'):
    with open(file, 'r') as f:
        content = f.read()

    if 'let mut needed_symbols' not in content:
        continue
        
    # We want to change `let symbols = needed_symbols;` to `let needed_symbols = needed_symbols;`
    # and then carefully replace `symbols.chunks` with `needed_symbols.chunks` ONLY for L2 subscriptions.
    
    # Let's see how each file uses `symbols`.
    # Kucoin:
    # topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
    # topic_l2 = format!("/contractMarket/level2Depth50:{}", chunk.join(","));
    
    print(f"Modifying {file}")
