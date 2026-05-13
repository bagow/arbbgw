#!/bin/bash
set -e

LOCAL_DIR="/media/bagow/files/home/bagow/Dev/bagow/cryptoarb/cex-arb"

echo "Stopping service on VPS..."
ssh opc@140.238.217.190 "sudo systemctl stop real-trader || true"

echo "Uploading binary and config..."
scp $LOCAL_DIR/real-trader/target/release/real-trader opc@140.238.217.190:/tmp/real-trader_bin
scp $LOCAL_DIR/real-trader/.env opc@140.238.217.190:/tmp/real-trader_env
scp $LOCAL_DIR/real-trader.service opc@140.238.217.190:/tmp/real-trader.service

echo "Setting permissions and moving files on VPS..."
ssh opc@140.238.217.190 "sudo mv /tmp/real-trader_bin /usr/local/bin/real-trader && \
sudo chmod +x /usr/local/bin/real-trader && \
sudo mv /tmp/real-trader_env /etc/real-trader.env && \
sudo chmod 600 /etc/real-trader.env && \
sudo cp /tmp/real-trader.service /etc/systemd/system/real-trader.service && \
sudo restorecon /etc/systemd/system/real-trader.service && \
sudo systemctl daemon-reload && \
sudo systemctl enable real-trader && \
sudo systemctl start real-trader"

echo "Checking logs..."
ssh opc@140.238.217.190 "sudo journalctl -u real-trader -n 20 --no-pager"
