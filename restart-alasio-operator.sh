#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_WORKSPACE="${WORKING_DIRECTORY:-/home/operator/monorepo}"
DB_PATH="$AGENT_WORKSPACE/.alasio/alasio.sqlite"

CANONICAL_DIR="$(systemctl show alasio.service -p WorkingDirectory --value 2>/dev/null || true)"
if [[ -n "$CANONICAL_DIR" && "$SCRIPT_DIR" != "$CANONICAL_DIR" ]]; then
  CANONICAL_WRAPPER="$CANONICAL_DIR/restart-alasio-operator.sh"
  if [[ ! -x "$CANONICAL_WRAPPER" ]]; then
    echo "Canonical Alasio restart wrapper is unavailable: $CANONICAL_WRAPPER" >&2
    exit 1
  fi
  echo "Delegating Alasio restart to $CANONICAL_WRAPPER"
  exec "$CANONICAL_WRAPPER"
fi

RESTART_COMMAND="sudo -n systemctl restart alasio.service"
RESTART_TIMESTAMP="$(date +%s)"

(
  cd "$SCRIPT_DIR"
  node --input-type=module - "$DB_PATH" "$RESTART_TIMESTAMP" "$RESTART_COMMAND" <<'NODE'
import { existsSync } from "node:fs";
import Database from "better-sqlite3";

const [dbPath, timestampRaw, command] = process.argv.slice(2);
const timestamp = Number(timestampRaw);

if (!existsSync(dbPath)) {
  console.log("No alasio SQLite database yet; restarting without recording active turn provenance.");
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

eval "$RESTART_COMMAND"
