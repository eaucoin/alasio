#!/bin/sh
# Starts Grafana as alasio runs it: provisioned from the image's files, and alerting
# through the Telegram bot TELEGRAM_BOT_TOKEN names, to each user TELEGRAM_ALLOWED_USER_IDS
# lists (comma-separated, as alasio's bot takes them). Grafana's Telegram integration
# sends to one chat, so the contact point has one for each user, made here, where the
# users are known; then Grafana starts as its image starts it.
set -eu

provisioning="$GF_PATHS_PROVISIONING"
# Grafana reads plugins' provisioning too, of which there is none.
mkdir -p "$provisioning/plugins"
cp -R "$(dirname "$0")/provisioning/." "$provisioning"

users=$(printf '%s' "$TELEGRAM_ALLOWED_USER_IDS" | tr ',' ' ')
if [ -z "$users" ]; then
  echo "TELEGRAM_ALLOWED_USER_IDS names no user to alert" >&2
  exit 1
fi

{
  printf 'apiVersion: 1\ncontactPoints:\n  - orgId: 1\n    name: telegram\n    receivers:\n'
  for user in $users; do
    case "$user" in
      *[!0-9]*)
        echo "TELEGRAM_ALLOWED_USER_IDS holds $user, which is not a Telegram user's id" >&2
        exit 1
        ;;
    esac
    # The bot's token as Grafana's provisioning reads it from the environment, written nowhere.
    printf '      - uid: telegram-%s\n        type: telegram\n        settings:\n          bottoken: $TELEGRAM_BOT_TOKEN\n          chatid: "%s"\n' "$user" "$user"
  done
} >"$provisioning/alerting/contact-points.yaml"

exec /run.sh "$@"
