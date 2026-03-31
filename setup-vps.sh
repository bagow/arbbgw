#!/bin/bash
# setup-vps.sh — полная настройка swiss VPS для cex-arb
# Запуск: bash setup-vps.sh
set -e

echo "=== 1. Проверка swap ==="
free -h
swapon --show

echo "=== 2. Установка зависимостей ==="
sudo dnf install -y git curl gcc openssl-devel chrony sqlite 2>/dev/null || true

echo "=== 3. NTP настройка ==="
sudo systemctl enable --now chronyd
chronyc sources -v || true

echo "=== 4. Rust ==="
if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    source "$HOME/.cargo/env"
else
    echo "Rust уже установлен: $(cargo --version)"
fi
source "$HOME/.cargo/env"
cargo --version

echo "=== 5. GitHub SSH ключ ==="
ssh-keyscan -H github.com >> ~/.ssh/known_hosts 2>/dev/null
echo "github.com добавлен в known_hosts"

echo "=== 6. Clone/Pull репо ==="
if [ -d /home/opc/cex-arb/.git ]; then
    cd /home/opc/cex-arb && git pull origin main
else
    git clone https://github.com/bagow/arbbgw.git /home/opc/cex-arb
fi
cd /home/opc/cex-arb
git log --oneline -3

echo "=== 7. Собираем все бинарники (используем swap) ==="
source "$HOME/.cargo/env"
cd /home/opc/cex-arb

# Одновременная сборка убьёт память — собираем по одному
echo "--- vps-api ---"
cargo build --release -p vps-api 2>&1 | tail -5

echo "--- real-trader ---"
cargo build --release -p real-trader 2>&1 | tail -5

echo "--- core-node ---"
cargo build --release -p core-node 2>&1 | tail -5

echo "--- paper-trader ---"
cargo build --release -p paper-trader 2>&1 | tail -5

echo "--- rust-collector ---"
cargo build --release -p rust-collector 2>&1 | tail -5

echo "=== 8. Копируем бинарники ==="
sudo cp target/release/vps-api      /usr/local/bin/
sudo cp target/release/real-trader  /usr/local/bin/
sudo cp target/release/core-node    /usr/local/bin/
sudo cp target/release/paper-trader /usr/local/bin/
sudo cp target/release/rust-collector /usr/local/bin/
sudo chmod +x /usr/local/bin/{vps-api,real-trader,core-node,paper-trader,rust-collector}
sudo restorecon -v /usr/local/bin/{vps-api,real-trader,core-node,paper-trader,rust-collector} 2>/dev/null || true

echo "=== 9. Systemd сервисы ==="
sudo cp vps-api.service      /etc/systemd/system/ 2>/dev/null || true
sudo cp real-trader.service  /etc/systemd/system/ 2>/dev/null || true
sudo cp core-node.service    /etc/systemd/system/ 2>/dev/null || true
sudo systemctl daemon-reload

echo "=== 10. Инициализация БД ==="
mkdir -p /home/opc/vps-api
mkdir -p /home/opc/real-trader
mkdir -p /home/opc/core-node

echo "=== ГОТОВО ==="
echo "Следующие шаги:"
echo "  1. Заполни /etc/vps-api.env"
echo "  2. Заполни /etc/real-trader.env (или /etc/core-node.env)"
echo "  3. sudo systemctl enable --now vps-api"
echo "  4. DRY_RUN=true sudo systemctl start real-trader"
ls -la /usr/local/bin/{vps-api,real-trader,core-node,paper-trader,rust-collector} 2>/dev/null
