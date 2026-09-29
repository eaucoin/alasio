#!/usr/bin/env bash
# Install or refresh the standalone alasio bot as alasio-standalone.service,
# which runs it in the container that container/run.sh describes.
# Expects ./.env to exist (see .env.example). Safe to re-run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_SOURCE="$SCRIPT_DIR/systemd/alasio-standalone.service"
UNIT_NAME="alasio-standalone.service"
POLKIT_SOURCE="$SCRIPT_DIR/systemd/alasio-standalone.rules"
POLKIT_TARGET="/etc/polkit-1/rules.d/50-alasio-standalone.rules"
IMAGE="alasio-standalone"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "Missing $SCRIPT_DIR/.env (copy .env.example and fill it in)" >&2
  exit 1
fi
if [[ "$(stat -c %a "$SCRIPT_DIR/.env")" != "600" ]]; then
  echo "Tightening .env permissions to 0600"
  chmod 600 "$SCRIPT_DIR/.env"
fi

npm ci --prefix "$SCRIPT_DIR"

echo "Checking the bot token and Telegram settings..."
DOCTOR_OUTPUT="$(cd "$SCRIPT_DIR" && npm run --silent doctor:telegram)"
echo "$DOCTOR_OUTPUT"
if grep -q '"can_join_groups": true' <<<"$DOCTOR_OUTPUT"; then
  echo "WARNING: the bot can still be added to groups. In BotFather: /mybots -> Bot Settings -> Allow Groups? -> Turn groups off." >&2
fi
if grep -q '"url_set": true' <<<"$DOCTOR_OUTPUT"; then
  echo "WARNING: a webhook is set on this bot; alasio long-polls, so it will delete it at startup." >&2
fi

echo "Building the $IMAGE image the bot runs in..."
docker build --pull --tag "$IMAGE" "$SCRIPT_DIR/container"

# Session filesystems, when enabled, run agents inside gVisor sandboxes on two images
# (alasio/session-host and alasio/agent). Built only when the feature is on, since the
# build fetches gVisor and JuiceFS. The Valkey and "sessions" bucket come up with the
# Neon stack on every start.
if grep -qE '^ALASIO_SANDBOX_ENABLED=1' "$SCRIPT_DIR/.env"; then
  echo "Session filesystems are enabled; building the sandbox images..."
  "$SCRIPT_DIR/sandbox/build.sh"
  # Session hosts reach the credential gateway, which alasio serves on the host, from the
  # Neon stack's network. A host firewall that drops inbound traffic by default (ufw's
  # does) would drop that too, so allow the gateway port on that network's bridge alone;
  # the bridge's name is fixed (neon/control/setup.js DEFAULT_BRIDGE) so this rule
  # outlives the network being recreated.
  GATEWAY_PORT="$(sed -n 's/^ALASIO_SANDBOX_GATEWAY_PORT=//p' "$SCRIPT_DIR/.env" | tail -n 1)"
  GATEWAY_PORT="${GATEWAY_PORT:-8080}"
  if command -v ufw >/dev/null && sudo ufw status | grep -q '^Status: active'; then
    echo "Allowing session hosts to reach the gateway on port $GATEWAY_PORT through ufw..."
    sudo ufw allow in on alasio-neon0 to any port "$GATEWAY_PORT" proto tcp comment 'alasio session gateway'
  fi
fi

# Pulled now, so no agent's first turn waits on it.
BAYMA_IMAGE="$(cd "$SCRIPT_DIR" && node --input-type=module --eval 'import { BAYMA_IMAGE } from "./src/mcp/bayma.js"; console.log(BAYMA_IMAGE)')"
echo "Pulling bayma's image, $BAYMA_IMAGE..."
docker pull "$BAYMA_IMAGE"

# The Neon stack Claude Code's transcripts are kept in; alasio brings it up
# itself on every start, so this only saves its first start the downloads.
echo "Pulling the images of alasio's Neon stack..."
(cd "$SCRIPT_DIR" && node --input-type=module --eval 'import "dotenv/config"; import { loadAlasioConfig } from "./src/config.js"; import { pullNeon } from "./src/neon/stack.js"; await pullNeon({ stateDir: loadAlasioConfig().stateDir });')

sudo install -m 0644 "$UNIT_SOURCE" "/etc/systemd/system/$UNIT_NAME"
# Passwordless restart of this one unit so ./restart-alasio-standalone.sh works from inside the bot.
sudo install -m 0644 "$POLKIT_SOURCE" "$POLKIT_TARGET"
# The sudoers rule that did this before the bot ran in a container.
sudo rm -f /etc/sudoers.d/alasio-standalone
sudo systemctl daemon-reload
if systemctl is-active --quiet "$UNIT_NAME"; then
  echo "$UNIT_NAME is running the previous setup; ./restart-alasio-standalone.sh switches it over."
fi
sudo systemctl enable --now "$UNIT_NAME"
sleep 3
systemctl --no-pager --lines=0 status "$UNIT_NAME" || true
echo
echo "Recent log lines:"
sudo journalctl -u "$UNIT_NAME" -n 25 --no-pager
