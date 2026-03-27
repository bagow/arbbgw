#!/bin/bash
# setup.sh – Install Node.js, vps-api dependencies, cloudflared, and configure systemd services
# Run as: bash setup.sh
set -e

echo "=== Step 1: Install Node.js 20 ==="
curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
sudo dnf install -y nodejs

echo "=== Step 2: Install vps-api dependencies ==="
mkdir -p /home/opc/vps-api
cp -r /tmp/vps-api/* /home/opc/vps-api/
cd /home/opc/vps-api
npm install --production

echo "=== Step 3: Install cloudflared ==="
curl -L --output /tmp/cloudflared.rpm \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.rpm
sudo rpm -v --install /tmp/cloudflared.rpm || sudo rpm -v --upgrade /tmp/cloudflared.rpm

echo "=== Step 4: Install cex-api systemd service ==="
sudo cp /home/opc/vps-api/cex-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cex-api
sudo systemctl restart cex-api
sudo systemctl status cex-api --no-pager

echo ""
echo "=== Step 5: Cloudflare Tunnel setup ==="
echo "Run this once and follow the browser link to authenticate:"
echo ""
echo "  cloudflared tunnel login"
echo "  cloudflared tunnel create cex-api"
echo "  cloudflared tunnel route dns cex-api api.arb.bagow.com"
echo "  sudo cloudflared service install"
echo "  sudo systemctl start cloudflared"
echo ""
echo "Update /home/opc/vps-api/tunnel-config.yml with your tunnel ID after 'tunnel create'."
