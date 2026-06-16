import os

# 1. Move functions from market_data.rs to real.rs
market_data_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/workers/market_data.rs"
real_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/real.rs"

with open(market_data_path, 'r') as f:
    lines = f.readlines()

# Find "pub fn now_secs() -> i64 {"
start_idx = -1
for i, line in enumerate(lines):
    if line.startswith("pub fn now_secs() -> i64 {"):
        start_idx = i
        break

if start_idx != -1:
    extracted = lines[start_idx:]
    lines = lines[:start_idx]
    
    with open(market_data_path, 'w') as f:
        f.writelines(lines)
        
    with open(real_path, 'r') as f:
        real_lines = f.readlines()
        
    # Append to real.rs
    real_lines.append("\n// Restored from market_data\n")
    real_lines.extend(extracted)
    
    # Add use base64::Engine;
    if "use base64::Engine;" not in "".join(real_lines):
        real_lines.insert(0, "use base64::Engine;\n")
        
    with open(real_path, 'w') as f:
        f.writelines(real_lines)


# 2. Fix position_sync.rs super::exchanges -> crate::engine::exchanges
sync_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/workers/position_sync.rs"
with open(sync_path, 'r') as f:
    sync_content = f.read()

sync_content = sync_content.replace("super::exchanges", "crate::engine::exchanges")

with open(sync_path, 'w') as f:
    f.write(sync_content)
    
print("Fixes applied.")
