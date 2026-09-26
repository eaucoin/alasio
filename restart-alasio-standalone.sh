#!/usr/bin/env bash
# Restart alasio-standalone.service while preserving active-turn provenance.
#
# Mirrors restart-alasio-operator.sh for the standalone checkout: records an
# operator_induced restart event for every active turn in this bot's own
# SQLite state (ALASIO_STATE_DIR from ./.env), then restarts the unit through
# the polkit rule that install-alasio-standalone-service.sh provisions. That
# works from the host and from inside the bot's container, whose systemctl
# reaches the host's systemd over the system D-Bus.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_NAME="alasio-standalone.service"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  # Only ALASIO_STATE_DIR is needed here; the token never leaves the file.
  ALASIO_STATE_DIR="$(sed -n 's/^ALASIO_STATE_DIR=//p' "$SCRIPT_DIR/.env" | tail -n1)"
fi
STATE_DIR="${ALASIO_STATE_DIR:-$HOME/.alasio}"
DB_PATH="$STATE_DIR/alasio.sqlite"

RESTART_COMMAND="systemctl restart $UNIT_NAME"
RESTART_TIMESTAMP="$(date +%s)"

(
  cd "$SCRIPT_DIR"
  node --input-type=module - "$DB_PATH" "$RESTART_TIMESTAMP" "$RESTART_COMMAND" <<'NODE'
import { existsSync } from "node:fs";
import Database from "better-sqlite3";

const [dbPath, timestampRaw, command] = process.argv.slice(2);
const timestamp = Number(timestampRaw);

if (!existsSync(dbPath)) {
  console.log(`No alasio SQLite database at ${dbPath}; restarting without recording active turn provenance.`);
  process.exit(0);
}

const db = new Database(dbPath);
let recorded = 0;
const activeTurns = db.prepare("select * from turns where state = 'active'").all();
const existing = db.prepare("select 1 from restart_events where thread_key = ?").pluck();
const insert = db.prepare(`
  insert into restart_events (thread_key, payload_json, created_at)
  values (?, ?, ?)
  on conflict(thread_key) do nothing
`);

for (const turn of activeTurns) {
  if (existing.get(turn.thread_key)) {
    continue;
  }
  const event = {
    cause: "operator_induced",
    thread_key: turn.thread_key,
    channel: turn.channel,
    thread_ts: turn.thread_ts,
    session_id: turn.session_id ?? null,
    command,
    timestamp,
  };
  insert.run(turn.thread_key, JSON.stringify(event), timestamp);
  recorded += 1;
}

db.close();
console.log(`Recorded operator_induced restart events for ${recorded} active turn(s)`);
NODE
)

# --no-ask-password: without the polkit rule, fail rather than prompt.
if ! systemctl --no-ask-password restart "$UNIT_NAME"; then
  echo "systemd refused the restart; run ./install-alasio-standalone-service.sh once to provision the polkit restart rule." >&2
  exit 1
fi
