#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_SOURCE="$SCRIPT_DIR/systemd/alasio.service"

# Also installs bayma's runtime payload, through its postinstall.
npm ci --prefix "$SCRIPT_DIR"

sudo -n install -m 0644 "$UNIT_SOURCE" /etc/systemd/system/alasio.service
sudo -n systemctl daemon-reload
sudo -n systemctl enable alasio.service
