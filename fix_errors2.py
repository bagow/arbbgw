import re

# 1. Fix real.rs outer doc comments
real_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/real.rs"
with open(real_path, 'r') as f:
    real_content = f.read()

# Replace /// with // at the beginning of the file until the first pub fn
lines = real_content.split('\n')
for i, line in enumerate(lines):
    if line.startswith("///") and i < 20:
        lines[i] = line.replace("///", "//", 1)
    if "pub fn" in line:
        break
real_content = '\n'.join(lines)
with open(real_path, 'w') as f:
    f.write(real_content)

# 2. Fix MarketData in pnl.rs
pnl_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/pnl.rs"
with open(pnl_path, 'r') as f:
    pnl_content = f.read()
pnl_content = pnl_content.replace("crate::state::MarketData", "shared::MarketData")
with open(pnl_path, 'w') as f:
    f.write(pnl_content)

# 3. Fix LAST_MARGIN_LOG in entry_manager.rs
entry_manager_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/entry_manager.rs"
with open(entry_manager_path, 'r') as f:
    em_content = f.read()
if "use std::sync::atomic::Ordering;" not in em_content:
    em_content = "use std::sync::atomic::Ordering;\nuse crate::engine::risk_manager::LAST_MARGIN_LOG;\n" + em_content
with open(entry_manager_path, 'w') as f:
    f.write(em_content)

# Make LAST_MARGIN_LOG public in risk_manager.rs
risk_manager_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/risk_manager.rs"
with open(risk_manager_path, 'r') as f:
    rm_content = f.read()
rm_content = rm_content.replace("static LAST_MARGIN_LOG: AtomicI64", "pub static LAST_MARGIN_LOG: AtomicI64")
with open(risk_manager_path, 'w') as f:
    f.write(rm_content)

# 4. Fix Trade import in exit_manager.rs
exit_manager_path = "/home/bagow/dev/cryptoarb/cex-arb/core-node/src/engine/exit_manager.rs"
with open(exit_manager_path, 'r') as f:
    ex_content = f.read()
ex_content = ex_content.replace("crate::engine::db::Trade", "crate::engine::real::Trade")
with open(exit_manager_path, 'w') as f:
    f.write(ex_content)

print("Fixes applied.")
