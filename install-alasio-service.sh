#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_SOURCE="$SCRIPT_DIR/systemd/alasio.service"
AGENT_WORKSPACE="${WORKING_DIRECTORY:-/home/operator/monorepo}"
BREADBUTTER_ROOT="$AGENT_WORKSPACE/.agents/skills/monorepo-breadbutter"
BUN_EXECUTABLE="${BUN_EXECUTABLE:-/home/operator/.bun/bin/bun}"

npm ci --prefix "$SCRIPT_DIR"

"$BUN_EXECUTABLE" install \
  --cwd "$BREADBUTTER_ROOT" \
  --frozen-lockfile

uv sync \
  --project "$BREADBUTTER_ROOT" \
  --python 3.12 \
  --all-extras \
  --frozen

node "$BREADBUTTER_ROOT/provision-rust-workbench.mjs"

npm --prefix "$SCRIPT_DIR" run doctor:breadbutter -- \
  --workspace "$AGENT_WORKSPACE"

sudo -n install -m 0644 "$UNIT_SOURCE" /etc/systemd/system/alasio.service
sudo -n systemctl daemon-reload
sudo -n systemctl enable alasio.service
