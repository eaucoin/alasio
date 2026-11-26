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
# point of no return and past it: the object store snapshotted before SeaweedFS 4.54.
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

# BEGIN the object store snapshotted before SeaweedFS 4.54.
#
# This release runs the bundled object store on SeaweedFS 4.54, where 10.0.26 ran 4.48, and its
# upgrade starts every part of the store again on 4.54, which writes what 4.48 did not (its filer
# configuration's second version, its masters' topology id). The store holds Neon's, the lake's,
# the backups', workspace storage's and the session snapshots' data, of which nothing else keeps
# all. So, past the point of no return, before the upgrade, the store is stopped and snapshotted,
# as 4.48 left it, under STORE_SNAPSHOT (OUT/object-store unless given), from which the restore
# the snapshot carries (restore.sh) puts every part's volume back as it was, for SeaweedFS 4.48 to
# serve every bucket as it did.
#
# A copy of the volume servers' volumes would take as much again of the disk as they do; the
# snapshot takes next to nothing as it is taken, as SeaweedFS 4.54 changes none of a volume's
# data file (<volume>.dat) but by appending to it, and by writing its first bytes, its superblock,
# as its replication is configured; a compaction writes the volume anew and renames it over the
# old one, and a volume deleted is unlinked. So each data file is hard-linked, which holds it
# whole through a rename or an unlink, and its size and its first HEAD_BYTES are kept beside;
# every other file, each volume's index and its info among them, and the masters' and the filers'
# store's volumes whole, is copied. The restore cuts each data file back to its size and writes
# its head back. The snapshot holds more of the disk as 4.54 compacts or deletes the volumes it
# shares, each then its own, up to what they held, within days as the store compacts them all.
#
# Before the point of no return, changing nothing but making STORE_SNAPSHOT: each claim of the
# parts that keep a volume, the masters', the volume servers' and the filers' store's, a
# directory of this machine, as local-path keeps a volume, on the filesystem STORE_SNAPSHOT is
# on, as a hard link is made on its own; and that filesystem's room for what the snapshot copies
# and a tenth more, leaving STORE_FLOOR_BYTES (60 GiB, the floor this machine's owner keeps free)
# free. A claim kept otherwise, or elsewhere, or too little room, fails the run there, saying
# which. A store not bundled is neither stopped nor snapshotted.
#
# Past it: the filers stopped first, which write to the other parts, then the volume servers, the
# masters and the filers' store, each its pods gone, so that each volume is snapshotted as its
# part left it; the image the masters ran kept (STORE_SNAPSHOT/image), and the count of each part
# (OUT/object-store-replicas, as first read); each claim snapshotted to STORE_SNAPSHOT/<claim>,
# into STORE_SNAPSHOT/<claim>.partial while it is, renamed once whole, how much it linked and
# copied and how long it took said: its files (files/), the size of each data file (sizes) and its
# head (heads/). Then the restore and the deletion written beside (restore.sh, delete.sh), and the
# ConfigMap STORE_SNAPSHOT_CONFIGMAP made, of where the snapshot is, when it was taken, until when
# it is kept, what it copied and the data files it links, by which `alasio status` says it and how
# much of the disk it holds of its own, as the owner, for which OUT, STORE_SNAPSHOT and each
# claim's snapshot are made searchable by others, none listable; OUT/object-store-snapshotted
# says it all was. Then each part started again, still on 4.48, at its count, in the order its
# peers need: the filers' store, the masters, the volume servers, the filers, each ready before
# the next starts. So the upgrade
# finds the store answering, as it reads what it does of it before it changes it, the snapshots
# the sandbox gateway names among them, and then starts each part again on 4.54, as its
# StatefulSet says. The gateways run on, failing each request while the parts behind them are
# stopped. Meanwhile Neon's safekeepers and pageservers keep on their own volumes what they would
# upload, and upload it once the store answers; what else would write the store fails, and writes
# again: workspace storage's JuiceFS, the node agents' snapshots, the lake's loads; and a read
# fails, a Neon compute's of a layer its pageserver no longer holds among them. Run in a quiet
# hour, nothing is lost. The snapshot is of the store as it was stopped: what is written once it
# started again is in none, and a restore drops it, Neon's uploads among it. A run again
# snapshots again what was not snapshotted whole, and once OUT/object-store-snapshotted says it
# was, nothing, but starts each part again as above, which a part running at its count already
# is; should the upgrade fail, the store runs on 4.48 as before.
#
# It takes as long as the stop, the parts' grace, 30 s, at most; the snapshot, seconds and the
# copy of what is not a volume's data, the filers' store's the most; and the parts' start, a minute
# or so. The snapshot is kept STORE_KEEP_SECONDS (48 h), for the store to be seen well on 4.54:
# delete.sh deletes it then, and its ConfigMap, and refuses to before. Its pods' end, and each
# part's being ready, is waited for STOP_WAIT_SECONDS (10 min) at most, the end looked for every
# POLL_SECONDS (5).

NS=alasio
STORE_SNAPSHOT=${STORE_SNAPSHOT:-$OUT/object-store}
STORE_FLOOR_BYTES=${STORE_FLOOR_BYTES:-$((60 * 1024 * 1024 * 1024))}
STORE_KEEP_SECONDS=$((48 * 60 * 60))
STORE_SNAPSHOT_CONFIGMAP=alasio-object-store-snapshot
HEAD_BYTES=8192
STOP_WAIT_SECONDS=${STOP_WAIT_SECONDS:-600}
POLL_SECONDS=${POLL_SECONDS:-5}
# The store's parts, by their StatefulSets, in the order they are stopped: what writes to the others first. The filers keep no volume.
STORE_PARTS="alasio-seaweedfs-filer alasio-seaweedfs-volume alasio-seaweedfs-master alasio-seaweedfs-filer-store"
# Their pods' components, as a selector's set.
STORE_COMPONENTS="seaweedfs-filer,seaweedfs-volume,seaweedfs-master,seaweedfs-filer-store"
k() { kubectl -n "$NS" "$@"; }
# What the restore and the deletion say, and how they fail: on stderr, as they run on their own.
say() { printf '%s\n' "$*" >&2; }
refuse() {
  say "FAILED: $*"
  exit 1
}
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
# The filesystem the path '$1' is on, by where it is mounted.
mounted_at() { df --output=target "$1" | tail -n 1; }
# Each claim of the store's parts that keep a volume, and the directory of this machine it is, '<claim>\t<directory>' a line:
# the claim of each pod of their StatefulSets, data-<StatefulSet>-<ordinal>; none where the store is not bundled.
store_claims() {
  local claims named claim volume directory
  claims=$(k get pvc -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.volumeName}{"\n"}{end}') ||
    fail "the claims of $NS could not be listed, so the object store's cannot be snapshotted"
  named=$(printf '%s\n' "$claims" | grep -E '^data-alasio-seaweedfs-(master|volume|filer-store)-[0-9]+'$'\t' || true)
  while IFS=$'\t' read -r claim volume; do
    [ -n "$claim" ] || continue
    directory=$(kubectl get pv "$volume" -o jsonpath='{.spec.hostPath.path}{.spec.local.path}') ||
      fail "the volume of the claim $claim, $volume, could not be read, so it cannot be snapshotted"
    [ -n "$directory" ] && [ -d "$directory" ] ||
      fail "the claim $claim is kept in no directory of this machine (its volume $volume says '$directory'), so the cut-over cannot snapshot it: copy the object store's volumes by hand, its parts stopped, and stop the cut-over from snapshotting them, as a run again past them does (date -u +%FT%TZ >OUT/object-store-snapshotted)"
    printf '%s\t%s\n' "$claim" "$directory"
  done <<<"$named"
}
store_snapshot_checked() {
  local listed copied=0 volumes=0 copies needed room at claim directory
  listed=$(store_claims)
  if [ -z "$listed" ]; then
    log "the object store is not the bundled one: none of it to snapshot"
    return 0
  fi
  log "checking that the object store can be snapshotted, to $STORE_SNAPSHOT"
  mkdir -p "$STORE_SNAPSHOT"
  at=$(mounted_at "$STORE_SNAPSHOT")
  while IFS=$'\t' read -r claim directory; do
    [ "$(mounted_at "$directory")" = "$at" ] ||
      fail "the claim $claim is kept on $(mounted_at "$directory"), and $STORE_SNAPSHOT on $at, where its volumes cannot be hard-linked: give STORE_SNAPSHOT a directory on $(mounted_at "$directory")"
    [ ! -d "$STORE_SNAPSHOT/$claim" ] || continue
    copied=$((copied + $(find "$directory" -type f ! -name '*.dat' -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')))
    volumes=$((volumes + $(find "$directory" -type f -name '*.dat' | wc -l)))
  done <<<"$listed"
  # What is not a volume's data, and each volume's head.
  copies=$((copied + volumes * HEAD_BYTES))
  needed=$((copies + copies / 10))
  room=$(df --output=avail -B1 "$STORE_SNAPSHOT" | tail -n 1)
  [ $((room - needed)) -ge "$STORE_FLOOR_BYTES" ] ||
    fail "the snapshot of the object store copies $needed bytes with a tenth more, which would leave $((room - needed)) of the $room free on $at, under the $STORE_FLOOR_BYTES kept free (STORE_FLOOR_BYTES): make room there"
  log "the object store's snapshot links $volumes volumes' data and copies $copies bytes, leaving $((room - needed)) free on $at, of $room"
}
# The claim's directory '$1' snapshotted into '$2': its files (files/), each volume's data linked, every other file copied; the
# size of each volume's data (sizes, '<path>\t<bytes>' a line, its path under the directory) and its first HEAD_BYTES (heads/).
store_snapshot_claim() {
  local directory=$1 into=$2 path
  mkdir -p "$into/heads"
  # Every file linked, each directory made as it is; then every file but a volume's data copied over its link.
  cp -al "$directory" "$into/files"
  : >"$into/sizes"
  while IFS= read -r -d '' path; do
    case "$path" in
      *.dat)
        printf '%s\t%s\n' "$path" "$(stat -c %s "$directory/$path")" >>"$into/sizes"
        mkdir -p "$(dirname "$into/heads/$path")"
        head -c "$HEAD_BYTES" "$directory/$path" >"$into/heads/$path"
        ;;
      *) cp -a --remove-destination "$directory/$path" "$into/files/$path" ;;
    esac
  done < <(cd "$directory" && find . ! -type d -printf '%P\0')
}
store_snapshotted_past() {
  local listed
  listed=$(store_claims)
  [ -n "$listed" ] || return 0
  [ -e "$OUT/object-store-snapshotted" ] || store_snapshotted "$listed"
  store_started
}
store_snapshotted() {
  local listed=$1 part count claim directory started linked copied taken
  log "stopping the object store, to snapshot it as SeaweedFS 4.48 left it: every request of it fails until it is started again"
  # Each part's count before the cut-over stopped it, kept as first read: a run again after a stop reads none.
  if [ ! -e "$OUT/object-store-replicas" ]; then
    : >"$OUT/object-store-replicas.read"
    for part in $STORE_PARTS; do
      count=$(k get statefulset "$part" -o jsonpath='{.spec.replicas}') || fail "the count of the object store's $part could not be read"
      printf '%s\t%s\n' "$part" "$count" >>"$OUT/object-store-replicas.read"
    done
    mv "$OUT/object-store-replicas.read" "$OUT/object-store-replicas"
  fi
  mkdir -p "$STORE_SNAPSHOT"
  k get statefulset alasio-seaweedfs-master -o jsonpath='{.spec.template.spec.containers[?(@.name=="master")].image}' >"$STORE_SNAPSHOT/image" ||
    fail "the image of the object store's masters could not be read"
  for part in $STORE_PARTS; do
    k scale statefulset "$part" --replicas=0 >/dev/null || fail "the object store's $part could not be stopped"
    gone "app.kubernetes.io/component=${part#alasio-}" "$STOP_WAIT_SECONDS" || fail "the object store's $part did not stop within ${STOP_WAIT_SECONDS} s"
  done
  while IFS=$'\t' read -r claim directory; do
    [ ! -d "$STORE_SNAPSHOT/$claim" ] || continue
    rm -rf "$STORE_SNAPSHOT/$claim.partial"
    started=$(date +%s)
    store_snapshot_claim "$directory" "$STORE_SNAPSHOT/$claim.partial" || fail "the claim $claim could not be snapshotted from $directory: the cut-over run again snapshots it again"
    mv "$STORE_SNAPSHOT/$claim.partial" "$STORE_SNAPSHOT/$claim"
    linked=$(wc -l <"$STORE_SNAPSHOT/$claim/sizes")
    copied=$(find "$STORE_SNAPSHOT/$claim/files" -type f ! -name '*.dat' -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')
    log "snapshotted the object store's $claim, $linked volumes' data linked and $copied bytes copied, in $(($(date +%s) - started)) s, to $STORE_SNAPSHOT/$claim"
  done <<<"$listed"
  printf '%s\n' "$listed" >"$STORE_SNAPSHOT/claims"
  cp "$OUT/object-store-replicas" "$STORE_SNAPSHOT/counts"
  taken=$(date -u +%FT%TZ)
  printf '%s\n' "$taken" >"$STORE_SNAPSHOT/taken"
  store_snapshot_scripts
  # The data files it links, each by its path, and what it copied: what `alasio status` reads of how much of the disk it holds.
  : >"$STORE_SNAPSHOT/volumes"
  copied=0
  while IFS=$'\t' read -r claim directory; do
    awk -F '\t' -v files="$STORE_SNAPSHOT/$claim/files/" '{ print files $1 }' "$STORE_SNAPSHOT/$claim/sizes" >>"$STORE_SNAPSHOT/volumes"
    copied=$((copied + $(find "$STORE_SNAPSHOT/$claim/files" -type f ! -name '*.dat' -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')))
  done <<<"$listed"
  # Stat-able by the owner, as `alasio status` runs: each directory down to the data files searchable, none listable.
  chmod o+x,g+x "$OUT" "$STORE_SNAPSHOT"
  while IFS=$'\t' read -r claim directory; do chmod o+x,g+x "$STORE_SNAPSHOT/$claim"; done <<<"$listed"
  k delete configmap "$STORE_SNAPSHOT_CONFIGMAP" --ignore-not-found >/dev/null || fail "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be deleted to be made anew"
  k create configmap "$STORE_SNAPSHOT_CONFIGMAP" --from-literal=path="$STORE_SNAPSHOT" --from-literal=taken="$taken" \
    --from-literal=keepUntil="$(date -u -d "@$(($(date -d "$taken" +%s) + STORE_KEEP_SECONDS))" +%FT%TZ)" \
    --from-literal=copiedBytes="$copied" --from-file=volumes="$STORE_SNAPSHOT/volumes" >/dev/null ||
    fail "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP, by which alasio status says the snapshot, could not be made"
  date -u +%FT%TZ >"$OUT/object-store-snapshotted"
  log "the object store snapshotted, to $STORE_SNAPSHOT: restore.sh there puts it back, its parts stopped; delete.sh deletes it from $(date -u -d "@$(($(date -d "$taken" +%s) + STORE_KEEP_SECONDS))" +%FT%TZ)"
}
# The restore and the deletion, written into the snapshot, each a script of its own of what it needs of this one.
store_snapshot_scripts() {
  local name run
  for name in restore delete; do
    case "$name" in
      restore) run=store_restore ;;
      delete) run=store_snapshot_delete ;;
    esac
    {
      printf '#!/usr/bin/env bash\n'
      printf '# %s\n' "Written by the cut-over from 10.0.26 (upgrades/from-10.0.26.sh), which says what it does: run as root on the host, kubectl reaching the cluster."
      printf 'set -euo pipefail\n'
      declare -p NS STORE_PARTS STORE_COMPONENTS STORE_KEEP_SECONDS STORE_SNAPSHOT_CONFIGMAP
      declare -f say refuse store_restore store_snapshot_delete
      printf '%s "$(dirname "$(readlink -f "$0")")"\n' "$run"
    } >"$STORE_SNAPSHOT/$name.sh"
    chmod 0700 "$STORE_SNAPSHOT/$name.sh"
  done
}
# The store's volumes put back as the snapshot '$1' holds them, its parts stopped: what the store made since, in no snapshot,
# removed, the volumes 4.54 made among them, of which the masters as they were know nothing; every file but a volume's data copied
# back; and each volume's data, its file linked back where a compaction replaced it or a deletion removed it, cut to its size,
# past which the store appended, and its head written back.
store_restore() {
  local snapshot=$1 running claim directory files path size
  [ -r "$snapshot/claims" ] || refuse "$snapshot holds no snapshot of the object store"
  running=$(kubectl -n "$NS" get pod -l "app.kubernetes.io/component in ($STORE_COMPONENTS)" -o name) ||
    refuse "the object store's pods could not be listed, so whether it is stopped cannot be told: give kubectl k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
  [ -z "$running" ] ||
    refuse "the object store runs ($(printf '%s ' $running)): stop each of its parts first, $STORE_PARTS (kubectl -n $NS scale statefulset <part> --replicas=0), until none of their pods is left"
  while IFS=$'\t' read -r claim directory; do
    files=$snapshot/$claim/files
    [ -d "$files" ] && [ -d "$directory" ] || refuse "the claim $claim's snapshot ($files) or its directory ($directory) is missing"
    while IFS= read -r -d '' path; do
      [ -e "$files/$path" ] || [ -L "$files/$path" ] || rm -rf -- "${directory:?}/$path"
    done < <(cd "$directory" && find . -mindepth 1 -depth -printf '%P\0')
    while IFS= read -r -d '' path; do
      [ -d "$directory/$path" ] || { mkdir "$directory/$path" && chown --reference="$files/$path" "$directory/$path" && chmod --reference="$files/$path" "$directory/$path"; }
    done < <(cd "$files" && find . -mindepth 1 -type d -printf '%P\0')
    while IFS= read -r -d '' path; do
      cp -a --remove-destination "$files/$path" "$directory/$path"
    done < <(cd "$files" && find . ! -type d ! -name '*.dat' -printf '%P\0')
    while IFS=$'\t' read -r path size; do
      [ "$files/$path" -ef "$directory/$path" ] || ln -f "$files/$path" "$directory/$path"
      truncate -s "$size" "$directory/$path"
      dd if="$snapshot/$claim/heads/$path" of="$directory/$path" conv=notrunc status=none
    done <"$snapshot/$claim/sizes"
    say "restored the object store's $claim, in $directory"
  done <"$snapshot/claims"
  say "the object store restored as it was snapshotted, $(cat "$snapshot/taken"); start it on $(cat "$snapshot/image"), as 10.0.26 ran it: each container of its StatefulSets and of its gateways' Deployment (alasio-seaweedfs) that runs SeaweedFS set to that image (kubectl -n $NS set image), then each part scaled back to its count ($(tr '\t\n' '= ' <"$snapshot/counts")), the filers' store, the masters, the volume servers, the filers in that order"
}
# The snapshot '$1' deleted, and its ConfigMap, once it has been kept STORE_KEEP_SECONDS; refused before.
store_snapshot_delete() {
  local snapshot=$1 taken kept
  [ -r "$snapshot/taken" ] || refuse "$snapshot holds no snapshot of the object store"
  taken=$(date -d "$(cat "$snapshot/taken")" +%s)
  kept=$((taken + STORE_KEEP_SECONDS))
  [ "$(date +%s)" -ge "$kept" ] ||
    refuse "the snapshot $snapshot is kept until $(date -u -d "@$kept" +%FT%TZ), for the store to be seen well on SeaweedFS 4.54 first: run this again then"
  kubectl -n "$NS" delete configmap "$STORE_SNAPSHOT_CONFIGMAP" --ignore-not-found >/dev/null ||
    refuse "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be deleted: give kubectl k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
  rm -rf -- "$snapshot"
  say "deleted the object store's snapshot $snapshot, taken $(date -u -d "@$taken" +%FT%TZ)"
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
# END the object store snapshotted before SeaweedFS 4.54.

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
  store_snapshot_checked
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  store_snapshotted_past
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
