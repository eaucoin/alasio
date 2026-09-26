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
