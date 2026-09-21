#!/usr/bin/env bash
# Install or refresh the standalone alasio bot as alasio-standalone.service.
# Expects ./.env to exist (see .env.example). Safe to re-run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_SOURCE="$SCRIPT_DIR/systemd/alasio-standalone.service"
UNIT_NAME="alasio-standalone.service"

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

sudo install -m 0644 "$UNIT_SOURCE" "/etc/systemd/system/$UNIT_NAME"
sudo systemctl daemon-reload
sudo systemctl enable --now "$UNIT_NAME"
sleep 3
systemctl --no-pager --lines=0 status "$UNIT_NAME" || true
echo
echo "Recent log lines:"
sudo journalctl -u "$UNIT_NAME" -n 25 --no-pager
