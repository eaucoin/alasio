#!/usr/bin/env bash
# An installation of alasio 8.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 8.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> OUT=<dir> [FAIL_AT=<point>] upgrades/from-8.0.0.sh
#
# UPGRADE runs the release's upgrade as the configuration's owner, which restarts alasio,
# ending the turn it runs; UPGRADE_CHECK proves, before the upgrade, that it can, and that the
# release accepts the installation as it is. On a machine whose node is on the owner's PATH
# alone, as sudo's secure_path drops it:
#
#   as='sudo -u <owner> env HOME=/home/<owner> PATH=<node bin>:/usr/local/bin:/usr/bin:/bin'
#   UPGRADE="$as npx --yes alasio@<v> upgrade --timeout 30m"
#   UPGRADE_CHECK="$as npx --yes alasio@<v> upgrade --check && sudo -u <owner> sudo -n true"
#
# the last proving that the owner's sudo asks for no password, which the upgrade does what it
# does as root through. FAIL_AT=before-the-point-of-no-return fails the run there, once every
# step before the upgrade has run; FAIL_AT=past-the-point-of-no-return past it, before the
# upgrade; and FAIL_AT=after-the-upgrade once the upgrade is through, before what follows it:
# as the upgrade shard rehearses a failure at each.
#
# It upgrades main alone, as alasio upgrade does, which refuses while a branch environment
# 8.0.0 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, each with what makes again what it changed (restore), as a
# run that fails before it leaves the installation as it was; past it, before the upgrade;
# and once the upgrade is through, after it. Each change's steps are a section of their own
# below, between its BEGIN and END lines, which the flow at the end of the script calls.
#
# Run again, it goes on from where it stopped: a failure before the point of no return makes
# again what the steps changed, and a run again starts over. Once past it
# (OUT/point-of-no-return), a run again goes on from there, and once the upgrade went through
# (OUT/upgraded), with what follows it alone.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${OUT:?where to record}"
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
NS=alasio
k() { kubectl -n "$NS" "$@"; }
# Runs the SQL it is given on stdin on alasio's database, as alasio connects to it, from its
# pod: the rows of what it answered, a line each, their values apart by tabs. Its pg is
# alasio's, in its image, wherever the pod's working directory is (a folder workspace's home).
on_database() {
  k exec -i deploy/alasio -c alasio -- node --input-type=module -e '
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
const pg = createRequire("/opt/alasio/package.json")("pg");
const url = process.env.ALASIO_DATABASE_URL ?? readFileSync(process.env.ALASIO_DATABASE_URL_FILE, "utf8");
const client = new pg.Client({ connectionString: url.trim(), connectionTimeoutMillis: 120000 });
await client.connect();
try {
  for (const { rows } of [await client.query({ text: readFileSync(0, "utf8"), rowMode: "array" })].flat()) {
    for (const row of rows ?? []) console.log(row.map((value) => String(value ?? "")).join("\t"));
  }
} finally {
  await client.end();
}'
}
# Makes again what the steps before the point of no return changed, as a run that fails
# before it leaves the installation as it was: each change's own making again, which its
# section below says.
restore() {
  log_statefulset_restore
}

# BEGIN the log's StatefulSet: made anew by the upgrade.
#
# 8.0.0's log, its StatefulSet, has its claims labelled with 8.0.0's version, which this
# release's do not carry, and Kubernetes changes no StatefulSet's claim templates: an
# upgrade's apply of it is refused. So, before the point of no return, it deletes the log's
# StatefulSet, its pod and claim kept running, which the upgrade makes anew and takes back;
# a failure before the point of no return makes it again as 8.0.0 had it.

LOG=alasio-log
# Before the point of no return: the log's StatefulSet, where its claims carry a version,
# deleted, its pod and claim kept, as it is kept in OUT.
log_statefulset_before() {
  local versioned
  versioned=$(k get statefulset "$LOG" --ignore-not-found -o json | jq -r '[.spec.volumeClaimTemplates[]?.metadata.labels["app.kubernetes.io/version"] // empty] | first // empty') ||
    fail "the log's StatefulSet could not be read"
  if [ -n "$versioned" ]; then
    k get statefulset "$LOG" -o json >"$OUT/log-statefulset.json" || fail "the log's StatefulSet could not be kept in $OUT"
    log "deleting the log's StatefulSet, its pod and claim kept, for the upgrade to make it anew without $versioned on its claims (kept in $OUT/log-statefulset.json)"
    k delete statefulset "$LOG" --cascade=orphan --wait=true >/dev/null || fail "the log's StatefulSet could not be deleted"
  fi
}
# The log's StatefulSet, as 8.0.0 had it, from OUT, where it is deleted: it takes its pod back.
log_statefulset_restore() {
  local made
  made=$(k get statefulset "$LOG" --ignore-not-found -o name)
  [ -e "$OUT/log-statefulset.json" ] && [ -z "$made" ] || return 0
  log "making the log's StatefulSet again, as 8.0.0 had it"
  jq 'del(.metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp, .metadata.generation, .metadata.managedFields, .status)' "$OUT/log-statefulset.json" | k create -f - >/dev/null ||
    log "FAILED: the log's StatefulSet is deleted, its pod running unowned, and could not be made again from $OUT/log-statefulset.json: the cut-over run again goes on, the upgrade making it anew; to run 8.0.0 as it was instead, make it from there (kubectl create -f), its metadata.uid and metadata.resourceVersion deleted"
}
# END the log's StatefulSet.

# BEGIN the Telegram messages kept once: 8.0.0's copies of each removed.
#
# 8.0.0 could keep a Telegram message twice, as two of its pods handed over, which this
# release keeps once, by an index its schema makes as alasio starts, refused while such
# copies are kept; and so with the files a message carried. Before anything changes, it finds
# them and says which (OUT/telegram-duplicates.tsv), refusing any but a message's exact copies,
# to be put right by hand. Past the point of no return, it removes those copies, keeping the
# one kept first, on alasio's database as alasio reaches it, from its pod.

# What the Telegram messages kept twice are, a line each: "message", the conversation, the
# direction, the message's id and date, how many keep it, and how many of those differ; and so
# for a file a message carried kept twice: "file", the message, the file's id, how many keep
# it, and how many of their contents differ.
TELEGRAM_COPIES="
select 'message', conversation_id, direction, transport_message_id, coalesce(raw->>'date', ''), count(*),
  count(distinct jsonb_build_array(kind, text, media_group_id, raw::text, session_id, turn_id)::text)
from state.messages where transport_message_id is not null
group by conversation_id, direction, transport_message_id, raw->>'date' having count(*) > 1
union all
select 'file', message_id, telegram_file_unique_id, '', '', count(*),
  count(distinct jsonb_build_array(telegram_file_id, file_name, name, mime_type, file_size, sha256, raw::text)::text)
from state.files where message_id is not null and telegram_file_unique_id is not null
group by message_id, telegram_file_unique_id having count(*) > 1
order by 1, 2, 3, 4, 5"
# Removes the copies of each Telegram message kept twice that are exact copies of it, but for
# their ids and when they were kept, keeping the one kept first. A copy's files go to the
# message kept where it has none, those of its first copy that has any; the rest are let go of,
# as deleting their message lets go of them, kept by their ids for what names them. How many
# it removed.
REMOVE_TELEGRAM_COPIES="
begin;
create temporary table copies on commit drop as
with exact as (
  select conversation_id, direction, transport_message_id, raw->>'date' as date
  from state.messages where transport_message_id is not null
  group by 1, 2, 3, 4
  having count(*) > 1 and count(distinct jsonb_build_array(kind, text, media_group_id, raw::text, session_id, turn_id)::text) = 1
), ranked as (
  select m.id, first_value(m.id) over (partition by m.conversation_id, m.direction, m.transport_message_id, m.raw->>'date' order by m.created_at, m.id) as kept
  from state.messages m join exact e using (conversation_id, direction, transport_message_id)
  where (m.raw->>'date') is not distinct from e.date
)
select id, kept from ranked where id <> kept;
update state.files f set message_id = c.kept from copies c
where f.message_id = c.id
  and not exists (select 1 from state.files held where held.message_id = c.kept)
  and c.id = (
    select earliest.id from copies earliest join state.messages copied on copied.id = earliest.id
    where earliest.kept = c.kept and exists (select 1 from state.files carried where carried.message_id = earliest.id)
    order by copied.created_at, copied.id limit 1
  );
delete from state.messages m using copies c where m.id = c.id;
select count(*) from copies;
commit;"
# Says, before anything changes, which Telegram messages, and files they carried, are kept
# twice, refusing any but a message's exact copies.
telegram_once_check() {
  local differing copies
  log "finding the Telegram messages, and the files they carried, kept twice (telegram-duplicates.tsv)"
  on_database <<<"$TELEGRAM_COPIES" >"$OUT/telegram-duplicates.tsv" ||
    fail "alasio's database could not be read, from its pod, for the Telegram messages it kept twice"
  differing=$(awk -F '\t' '$1 == "file" || $7 > 1 { printf "%s%s %s of %s (%s kept, %s different)", (n++ ? "; " : ""), $1, ($1 == "file" ? $3 : $4), $2, $6, $7 }' "$OUT/telegram-duplicates.tsv")
  [ -z "$differing" ] ||
    fail "alasio kept these twice, which this release refuses to start on, and the cut-over removes exact copies of a message alone: $differing. Put them right by hand (telegram-duplicates.tsv), and run it again"
  copies=$(awk -F '\t' '$1 == "message" { n += $6 - 1 } END { print n + 0 }' "$OUT/telegram-duplicates.tsv")
  [ "$copies" -eq 0 ] || log "$copies exact copies of Telegram messages kept twice, removed past the point of no return"
}
# Past the point of no return, before the upgrade: the exact copies removed, once, as an
# upgrade begun may have stopped the alasio they are removed through.
telegram_once_past() {
  local removed
  if [ ! -e "$OUT/telegram-copies-removed" ] && awk -F '\t' '$1 == "message" { found = 1 } END { exit !found }' "$OUT/telegram-duplicates.tsv"; then
    removed=$(on_database <<<"$REMOVE_TELEGRAM_COPIES") || fail "the exact copies of Telegram messages kept twice could not be removed: the cut-over run again goes on with it"
    printf '%s\n' "$removed" >"$OUT/telegram-copies-removed"
    log "removed $removed exact copies of Telegram messages kept twice"
  fi
}
# END the Telegram messages kept once.

# BEGIN the forks' holds: a holder and an expiry for each.
#
# The forks' holds, a table of 8.0.0's, have a holder and an expiry in this release, which
# alasio's schema defines as the table is now, and evolves no table it has. Past the point of
# no return, the cut-over gives 8.0.0's table the two, with defaults, as 8.0.0 still holds
# without them until the upgrade; once the upgrade is through, it takes the defaults off, as
# this release makes every hold with both. A hold of 8.0.0's comes out expired, and alasio
# lets go of every hold as it starts anyway.

# The holder and the expiry of each of the forks' holds (src/persistence/schema.ts
# conversation_holds), with the defaults 8.0.0's holds are made with until the upgrade.
HOLD_COLUMNS="
alter table state.conversation_holds
  add column if not exists holder text not null default '',
  add column if not exists expires_at timestamptz not null default now();"
# The forks' holds as this release makes them: without the defaults 8.0.0's needed.
HOLD_DEFAULTS="
alter table state.conversation_holds
  alter column holder drop default,
  alter column expires_at drop default;"
# Past the point of no return, before the upgrade: the holder and the expiry given, once.
holds_past() {
  if [ ! -e "$OUT/holds-columns" ]; then
    on_database <<<"$HOLD_COLUMNS" >/dev/null || fail "the forks' holds could not be given their holder and expiry: the cut-over run again goes on with it"
    touch "$OUT/holds-columns"
    log "gave the forks' holds their holder and expiry, which this release keeps"
  fi
}
# Once the upgrade is through: the defaults taken off.
holds_after() {
  on_database <<<"$HOLD_DEFAULTS" >/dev/null || fail "the forks' holds could not be made as this release makes them: the cut-over run again goes on with it"
  log "made the forks' holds as this release makes them, without the defaults 8.0.0's needed"
}
# END the forks' holds.

for tool in jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 8.0.0, recording in $OUT"
  trap 'status=$?; [ "$status" -eq 0 ] || [ -e "$OUT/point-of-no-return" ] || restore' EXIT
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 8.0.0 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 8.0.0 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
  telegram_once_check
  log_statefulset_before
  log "checking that the upgrade can run (upgrade-check.log)"
  bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1 || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  trap - EXIT
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  telegram_once_past
  holds_past
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

holds_after

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
