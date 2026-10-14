#!/usr/bin/env bash
# An installation of alasio 6.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 6.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-6.0.0.sh
#
# UPGRADE runs the release's upgrade as the configuration's owner, which restarts alasio,
# ending the turn it runs; UPGRADE_CHECK proves, before anything changes, that it can, and
# that the release accepts the installation as it is. On a machine whose node is on the
# owner's PATH alone, as sudo's secure_path drops it:
#
#   as='sudo -u <owner> env HOME=/home/<owner> PATH=<node bin>:/usr/local/bin:/usr/bin:/bin'
#   UPGRADE="$as npx --yes alasio@<v> upgrade --timeout 30m"
#   UPGRADE_CHECK="$as npx --yes alasio@<v> upgrade --check && sudo -u <owner> sudo -n true"
#
# the last proving that the owner's sudo asks for no password, which the upgrade does what it
# does as root through. CONFIG is that configuration's file
# (/home/<owner>/.config/alasio/config.json), which the cut-over changes in place before
# UPGRADE reads it. FAIL_AT=before-the-point-of-no-return fails the run there, once every
# step before the upgrade has run, as the upgrade shard rehearses a failure.
#
# It upgrades main alone, as alasio upgrade does, and refuses, before it changes anything,
# while a branch environment exists: this release reads the record of a branch's compute in a
# shape 6.0.0 did not write, so a branch 6.0.0 made would not wake once upgraded. Delete each
# first (alasio branch delete <name>), and make it again once upgraded.
#
# Run again, it goes on from where it stopped: what each step reads is kept in OUT and read
# again from there, and a failure before the upgrade begins starts again what the steps
# stopped. Once UPGRADE has begun (OUT/point-of-no-return), a run again goes on with it, as
# what the steps before it moved is the release's to change from then on.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${CONFIG:?the file of that configuration}" "${OUT:?where to record}"
NS=alasio
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
k() { kubectl -n "$NS" "$@"; }
# Runs "$@" every 2 s until it succeeds, for at most $1 seconds; fails, saying it waited for
# $2, if it never does.
wait_for() {
  local most=$1 what=$2 deadline
  shift 2
  deadline=$((SECONDS + most))
  until "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || fail "waited ${most}s for $what"
    sleep 2
  done
}

# Scales the Deployment $1 to none, and waits until its pods are gone, keeping in OUT the
# replicas it ran the first time, which restart gives it back; the upgrade scales it back
# itself, as the release sets every replica count it does not hand to a scaler.
stop() {
  local selector
  [ -e "$OUT/replicas-$1" ] || { k get deploy "$1" -o jsonpath='{.spec.replicas}' >"$OUT/replicas-$1.part" && mv "$OUT/replicas-$1.part" "$OUT/replicas-$1"; }
  selector=$(k get deploy "$1" -o json | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')
  k scale deploy/"$1" --replicas=0 >/dev/null
  gone() {
    local pods
    pods=$(k get pod -l "$selector" -o name) && [ -z "$pods" ]
  }
  wait_for 300 "$1's pods to be gone" gone
}
# Gives each Deployment stop stopped back the replicas it ran, as a failure before the upgrade
# leaves the installation as 6.0.0 ran it.
restart() {
  local file
  for file in "$OUT"/replicas-*; do
    [ -e "$file" ] && [ "${file%.part}" = "$file" ] || continue
    k scale deploy/"${file#"$OUT/replicas-"}" --replicas="$(cat "$file")" >/dev/null || log "could not scale ${file#"$OUT/replicas-"} back to $(cat "$file")"
  done
}

# neon-control's record of the branches, which 6.0.0's keeps in a file on its volume, read by
# a pod of its own mounting the volume, as neon-control's own pod does, whether that runs or not.
record() {
  local phase
  k delete pod cutover-record --ignore-not-found --wait=true >/dev/null
  k get deploy alasio-neon-control -o json | jq '{
    apiVersion: "v1", kind: "Pod", metadata: {name: "cutover-record", namespace: .metadata.namespace},
    spec: (.spec.template.spec | {
      restartPolicy: "Never", securityContext, nodeSelector, affinity, tolerations,
      volumes: [.volumes[] | select(.name == "state")],
      containers: [.containers[0] | {name: "record", image, securityContext,
        command: ["cat", "/state/branches.json"], volumeMounts: [{name: "state", mountPath: "/state", readOnly: true}]}]
    } | with_entries(select(.value != null)))
  }' | k apply -f - >/dev/null
  ended() {
    phase=$(k get pod cutover-record -o jsonpath='{.status.phase}') && { [ "$phase" = Succeeded ] || [ "$phase" = Failed ]; }
  }
  wait_for 300 "the pod reading neon-control's record to end" ended
  [ "$phase" = Succeeded ] || fail "the pod reading neon-control's record failed: $(k logs cutover-record 2>&1)"
  k logs cutover-record
  k delete pod cutover-record --wait=true >/dev/null
}
# Refuses the record in the file $1 unless it holds main alone, ready: of 6.0.0's records, this
# release reads main's as it is, and no other branch's.
main_alone() {
  local others
  others=$(jq -r '[.branches[].name | select(. != "main")] | map("alasio branch delete \(.)") | join(", ")' "$1")
  [ -z "$others" ] || fail "branch environments exist, which this release would not wake once upgraded from 6.0.0: delete each first ($others), then run the cut-over again"
  jq -e '[.branches[] | select(.name == "main" and .state == "ready")] | length == 1' "$1" >/dev/null ||
    fail "neon-control's record holds no main ready: $(jq -c '[.branches[] | {name, state}]' "$1")"
}

for tool in jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
[ -f "$CONFIG" ] && jq -e '.install | type == "object"' "$CONFIG" >/dev/null 2>&1 || fail "CONFIG, $CONFIG, is not alasio's configuration"

if [ ! -e "$OUT/point-of-no-return" ]; then
  bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1 || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  record >"$OUT/branches-before.json"
  main_alone "$OUT/branches-before.json"
  trap 'status=$?; [ "$status" -eq 0 ] || [ -e "$OUT/point-of-no-return" ] || { log "starting again what the cut-over stopped"; restart; }' EXIT

  # neon-control keeps its record of the branches in the storage controller's database,
  # where 6.0.0's kept it in a file on its volume: the record moves there before the release's
  # neon-control starts, or it would start with none and make main again.
  [ -e "$OUT/storage_controller.sql" ] ||
    { k exec statefulset/alasio-neon-controller-db -- pg_dump -U storage_controller storage_controller >"$OUT/storage_controller.sql.part" &&
      mv "$OUT/storage_controller.sql.part" "$OUT/storage_controller.sql"; }
  log "stopping neon-control, so its record does not change as it moves"
  stop alasio-neon-control
  record >"$OUT/branches.json"
  main_alone "$OUT/branches.json"
  log "moving neon-control's record into the storage controller's database"
  # The record goes in on stdin, dollar-quoted, by a tag it does not hold.
  tag="record_$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
  ! grep -qF "\$$tag\$" "$OUT/branches.json" || fail "the record holds \$$tag\$, which quotes it"
  differing=$({
    printf 'create schema if not exists neon_control;\n'
    printf 'create table if not exists neon_control.branches (name text primary key, branch jsonb not null);\n'
    printf 'create temporary table moved as select b->>%s as name, b as branch from jsonb_array_elements($%s$%s$%s$::jsonb->%s) b;\n' \
      "'name'" "$tag" "$(cat "$OUT/branches.json")" "$tag" "'branches'"
    printf 'insert into neon_control.branches select name, branch from moved on conflict (name) do update set branch = excluded.branch;\n'
    printf 'select count(*) from moved full join neon_control.branches kept using (name) where moved.branch is distinct from kept.branch;\n'
  } | k exec -i statefulset/alasio-neon-controller-db -- psql -U storage_controller -d storage_controller -v ON_ERROR_STOP=1 -Atq)
  [ "$differing" = 0 ] || fail "$differing branches of the storage controller's database differ from neon-control's record"
  log "neon-control's record moved: $(jq -r '[.branches[] | "\(.name) \(.state)"] | join(", ")' "$OUT/branches.json")"

  # alasio's state, in main's database, given what this release keeps that 6.0.0 did not,
  # with alasio stopped, so that nothing writes to the tables as they change, until the
  # upgrade starts this release's alasio.
  log "stopping alasio, so nothing writes to its state as it changes"
  stop alasio
  log "changing alasio's state for this release"
  k exec -i deploy/alasio-neon-compute -c compute -- psql -h 127.0.0.1 -p 55433 -U cloud_admin -d alasio -v ON_ERROR_STOP=1 -q <<'SQL'
-- The outbox carries a turn's status message, the Telegram message it was sent as, and its edits.
alter table state.telegram_outbox add column if not exists edits text, add column if not exists telegram_message_id bigint;
-- A turn is fenced by the epoch its claim took, and says what its harness does, and how it ended.
alter table state.turns add column if not exists epoch bigint not null default 0, add column if not exists status text not null default 'working',
  add column if not exists waiting_on text, add column if not exists outcome text, add column if not exists status_outbox_id text;
update state.turns set status = 'ended' where state = 'completed';
alter table state.responses add column if not exists thread_key text, add column if not exists epoch bigint;
SQL

  # The workflow hook server is gone, and with it what ALASIO_HOOK_PORT set.
  # Rewritten whole, its owner's and mode kept, as it was kept in OUT.
  if jq -e '.install.alasio.env.ALASIO_HOOK_PORT != null' "$CONFIG" >/dev/null; then
    log "removing ALASIO_HOOK_PORT, which nothing reads, from alasio's environment in $CONFIG"
    [ -e "$OUT/config.json" ] || cp -p "$CONFIG" "$OUT/config.json"
    jq 'del(.install.alasio.env.ALASIO_HOOK_PORT)' "$CONFIG" >"$CONFIG.part"
    chown --reference="$CONFIG" "$CONFIG.part"
    chmod --reference="$CONFIG" "$CONFIG.part"
    mv "$CONFIG.part" "$CONFIG"
  fi

  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
fi

log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
  fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it; or, to run 6.0.0 as it was instead, scale each Deployment it stopped back to the replicas OUT/replicas-<name> holds (kubectl -n $NS scale deploy/<name> --replicas=<n>)"

# The upgraded neon-control serves the record moved, before the volume that held it goes.
token=$(k get secret alasio-branches -o jsonpath='{.data.control-token}' | base64 -d)
listed=$(k exec -i deploy/alasio-neon-control -c neon-control -- curl -sS -H @- http://127.0.0.1:8080/branches <<<"authorization: Bearer $token")
jq -e --slurpfile record "$OUT/branches.json" '[.branches[] | {name, state}] == [$record[0].branches[] | {name, state}]' <<<"$listed" >/dev/null ||
  fail "the upgraded neon-control lists $(jq -c '[.branches[] | {name, state}]' <<<"$listed"), not the record moved"
log "removing neon-control's old volume, which the release no longer has, and applying it does not prune"
k delete pvc alasio-neon-control --ignore-not-found --timeout=5m >/dev/null
log "upgraded"
