#!/usr/bin/env bash
# An installation of alasio 10.0.26 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 10.0.26 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-10.0.26.sh
#
# CONFIG is the configuration's file, the owner's alasio/config.json, which UPGRADE upgrades
# by and the steps below read, and change where they say.
#
# UPGRADE runs the release's upgrade as the configuration's owner, which replaces alasio,
# ending the turn it runs, with this release's: started at its count where that is fixed, or,
# where KEDA scales it, once an update, event or bound harness wakes it. UPGRADE_CHECK proves,
# before the upgrade, that it can, and that the release accepts the installation as it is. On
# a machine whose node is on the owner's PATH alone, as sudo's secure_path drops it:
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
# 10.0.26 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, past it, before the upgrade, and once the upgrade is
# through, after it. Each change's steps are a section of their own below, between its BEGIN
# and END lines, which the flow at the end of the script calls. None changes, before the
# point of no return, what a run that fails there would have to make again: a step that does
# brings what makes it again, for the flow to run as such a run ends. One has steps before the
# point of no return and past it: the object store copied whole before SeaweedFS 4.54.
#
# Run again, it goes on from where it stopped: a run that failed before the point of no
# return starts over. Once past it (OUT/point-of-no-return), a run again goes on from there,
# and once the upgrade went through (OUT/upgraded), with what follows it alone.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${CONFIG:?the file of the configuration}" "${OUT:?where to record}"
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
# Whether UPGRADE_CHECK passes now, what it said in OUT/upgrade-check.log.
accepted() { bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1; }

# BEGIN the object store copied whole before SeaweedFS 4.54.
#
# This release runs the bundled object store on SeaweedFS 4.54, where 10.0.26 ran 4.48, and its
# upgrade starts every part of the store again on 4.54, which writes what 4.48 did not (its filer
# configuration's second version, its masters' topology id). The store holds Neon's, the lake's,
# the backups', workspace storage's and the session snapshots' data, of which nothing else keeps
# all. So, past the point of no return, before the upgrade, the store is stopped, and each of its
# parts' volumes copied whole, as 4.48 left them, under STORE_COPY (OUT/object-store unless
# given): SeaweedFS 4.48 run on the copies serves every bucket as it was.
#
# Before the point of no return, changing nothing: each claim of the parts that keep a volume,
# the masters', the volume servers' and the filers' store's, a directory of this machine, as
# local-path keeps a volume; and STORE_COPY's disk, which is to outlive a restart as OUT's is,
# room for what they hold (du) and a tenth more. A claim kept otherwise, or a disk short of room,
# fails the run there, saying which. A store not bundled is neither stopped nor copied.
#
# Past it: the filers stopped first, which write to the other parts, then the volume servers, the
# masters and the filers' store, each its pods gone, so that each volume is copied as its part
# left it; then each claim's directory copied to STORE_COPY/<claim>, into STORE_COPY/<claim>.partial
# while it is copied, renamed once whole, its bytes and how long it took said; and OUT/
# object-store-copied says it all was. Then each part started again, still on 4.48, at the count
# OUT/object-store-replicas kept of it as it was stopped, in the order its peers need: the filers'
# store, the masters, the volume servers, the filers, each ready before the next starts. So the
# upgrade finds the store answering, as it reads what it does of it before it changes it, the
# snapshots the sandbox gateway names among them, and then starts each part again on 4.54, as its
# StatefulSet says. The gateways run on, failing each request while the parts behind them are
# stopped. Meanwhile Neon's safekeepers and pageservers keep on their own volumes what they would
# upload, and upload it once the store answers; what else would write the store fails, and writes
# again: workspace storage's JuiceFS, the node agents' snapshots, the lake's loads; and a read
# fails, a Neon compute's of a layer its pageserver no longer holds among them. Run in a quiet
# hour, nothing is lost. The copies are of the store as it was stopped: what is written once it
# started again is in none. A run again copies again what was not copied whole, and once
# OUT/object-store-copied says it was, nothing, but starts each part again as above, which a part
# running at its count already is; should the upgrade fail, the store runs on 4.48 as before.
#
# It takes as long as the stop, the parts' grace, 30 s, at most; the copy, at the rate this
# machine's disk copies the store's bytes (events.log says each claim's); and the parts' start,
# a minute or so. The copies are kept: STORE_COPY is the owner's to delete once the store is seen
# well on 4.54. Its pods' end, and each part's being ready, is waited for STOP_WAIT_SECONDS
# (10 min) at most, the end looked for every POLL_SECONDS (5).

NS=alasio
STORE_COPY=${STORE_COPY:-$OUT/object-store}
STOP_WAIT_SECONDS=${STOP_WAIT_SECONDS:-600}
POLL_SECONDS=${POLL_SECONDS:-5}
# The store's parts, by their StatefulSets, in the order they are stopped: what writes to the others first. The filers keep no volume.
STORE_PARTS="alasio-seaweedfs-filer alasio-seaweedfs-volume alasio-seaweedfs-master alasio-seaweedfs-filer-store"
k() { kubectl -n "$NS" "$@"; }
# Whether alasio's namespace has no pod of the selector '$1' within '$2' seconds, looking every POLL_SECONDS.
gone() {
  local waited=0 listed
  while :; do
    listed=$(k get pod -l "$1" -o name) || fail "the pods of $1 in $NS could not be listed"
    [ -n "$listed" ] || return 0
    [ "$waited" -lt "$2" ] || return 1
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
}
# Each claim of the store's parts that keep a volume, and the directory of this machine it is, '<claim>\t<directory>' a line:
# the claim of each pod of their StatefulSets, data-<StatefulSet>-<ordinal>; none where the store is not bundled.
store_claims() {
  local claims named claim volume directory
  claims=$(k get pvc -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.volumeName}{"\n"}{end}') ||
    fail "the claims of $NS could not be listed, so the object store's cannot be copied"
  named=$(printf '%s\n' "$claims" | grep -E '^data-alasio-seaweedfs-(master|volume|filer-store)-[0-9]+'$'\t' || true)
  while IFS=$'\t' read -r claim volume; do
    [ -n "$claim" ] || continue
    directory=$(kubectl get pv "$volume" -o jsonpath='{.spec.hostPath.path}{.spec.local.path}') ||
      fail "the volume of the claim $claim, $volume, could not be read, so it cannot be copied"
    [ -n "$directory" ] && [ -d "$directory" ] ||
      fail "the claim $claim is kept in no directory of this machine (its volume $volume says '$directory'), so the cut-over cannot copy it: copy the object store's volumes by hand, its parts stopped, and stop the cut-over from copying them, as a run again past them does (date -u +%FT%TZ >OUT/object-store-copied)"
    printf '%s\t%s\n' "$claim" "$directory"
  done <<<"$named"
}
store_copy_checked() {
  local listed needed=0 room claim directory
  listed=$(store_claims)
  if [ -z "$listed" ]; then
    log "the object store is not the bundled one: none of it to copy"
    return 0
  fi
  log "checking that the object store's volumes can be copied, to $STORE_COPY"
  mkdir -p "$STORE_COPY"
  while IFS=$'\t' read -r claim directory; do
    [ -d "$STORE_COPY/$claim" ] || needed=$((needed + $(du -sb "$directory" | cut -f1)))
  done <<<"$listed"
  room=$(df --output=avail -B1 "$STORE_COPY" | tail -n 1)
  [ "$room" -ge $((needed + needed / 10)) ] ||
    fail "$STORE_COPY has $room bytes free, short of the $needed the object store's volumes hold and a tenth more: give STORE_COPY a disk with room"
  log "the object store's volumes hold $needed bytes, which $STORE_COPY has room for, $room free"
}
store_copied_past() {
  local listed
  listed=$(store_claims)
  [ -n "$listed" ] || return 0
  [ -e "$OUT/object-store-copied" ] || store_copied "$listed"
  store_started
}
store_copied() {
  local listed=$1 part count claim directory started bytes
  log "stopping the object store, to copy its volumes as SeaweedFS 4.48 left them: every request of it fails until it is started again"
  # Each part's count before the cut-over stopped it, kept as first read: a run again after a stop reads none.
  if [ ! -e "$OUT/object-store-replicas" ]; then
    : >"$OUT/object-store-replicas.read"
    for part in $STORE_PARTS; do
      count=$(k get statefulset "$part" -o jsonpath='{.spec.replicas}') || fail "the count of the object store's $part could not be read"
      printf '%s\t%s\n' "$part" "$count" >>"$OUT/object-store-replicas.read"
    done
    mv "$OUT/object-store-replicas.read" "$OUT/object-store-replicas"
  fi
  for part in $STORE_PARTS; do
    k scale statefulset "$part" --replicas=0 >/dev/null || fail "the object store's $part could not be stopped"
    gone "app.kubernetes.io/component=${part#alasio-}" "$STOP_WAIT_SECONDS" || fail "the object store's $part did not stop within ${STOP_WAIT_SECONDS} s"
  done
  while IFS=$'\t' read -r claim directory; do
    [ ! -d "$STORE_COPY/$claim" ] || continue
    rm -rf "$STORE_COPY/$claim.partial"
    started=$(date +%s)
    cp -a "$directory" "$STORE_COPY/$claim.partial" || fail "the claim $claim could not be copied from $directory: the cut-over run again copies it again"
    mv "$STORE_COPY/$claim.partial" "$STORE_COPY/$claim"
    bytes=$(du -sb "$STORE_COPY/$claim" | cut -f1)
    log "copied the object store's $claim, $bytes bytes, in $(($(date +%s) - started)) s, to $STORE_COPY/$claim"
  done <<<"$listed"
  date -u +%FT%TZ >"$OUT/object-store-copied"
  log "the object store's volumes copied"
}
# Each part started again at the count kept of it, those its peers need first: the reverse of the order they are stopped in.
store_started() {
  local part count
  for part in $(printf '%s\n' $STORE_PARTS | tac); do
    count=$(awk -F '\t' -v part="$part" '$1 == part { print $2 }' "$OUT/object-store-replicas")
    [ -n "$count" ] || fail "$OUT/object-store-replicas keeps no count of the object store's $part"
    k scale statefulset "$part" --replicas="$count" >/dev/null || fail "the object store's $part could not be started again: the cut-over run again starts it"
    k rollout status statefulset "$part" --timeout="${STOP_WAIT_SECONDS}s" >/dev/null ||
      fail "the object store's $part was not ready within ${STOP_WAIT_SECONDS} s of its start (kubectl -n $NS describe statefulset $part): the cut-over run again starts it"
  done
  log "the object store started again, each part ready, on SeaweedFS 4.48, as it was, for the upgrade to start it on 4.54"
}
# END the object store copied whole before SeaweedFS 4.54.

command -v kubectl >/dev/null || fail "kubectl is not on root's PATH"
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 10.0.26, recording in $OUT"
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 10.0.26 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 10.0.26 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
  log "checking that the upgrade can run (upgrade-check.log)"
  accepted || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  store_copy_checked
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  store_copied_past
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
