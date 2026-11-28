#!/usr/bin/env bash
# An installation of alasio 10.0.27 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 10.0.27 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-10.0.27.sh
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
# 10.0.27 made exists: it refuses then too, before it changes anything. Delete each first
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
# This release runs the bundled object store on SeaweedFS 4.54, where 10.0.27 ran 4.48, and its
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
# its head back. The snapshot would hold more of the disk as the store compacted or deleted the
# volumes it shares, each then its own, up to what they held: so the store's compaction, which
# its masters ask of the volume servers, deleting what is empty with it, is paused from as the
# snapshot is taken (volume.vacuum.disable), on 4.48 and again once the upgrade has started the
# masters on 4.54, until the snapshot is deleted or restored. A master that restarts meanwhile
# forgets it, its pause held in its memory alone: `alasio status` says so, and that pause.sh,
# beside the snapshot, pauses it again and tells the ConfigMap so.
#
# Before the point of no return, changing nothing but making STORE_SNAPSHOT and pulling SeaweedFS
# 4.54 (STORE_IMAGE) onto this machine, as the upgrade would, so that none of the store's parts
# waits on the pull once it is stopped, nor an image that cannot be pulled fails it there: each
# claim of the parts that keep a volume, the masters', the volume servers' and the filers'
# store's, a directory of this machine, as local-path keeps a volume, on the filesystem
# STORE_SNAPSHOT is on, as a hard link is made on its own; and that filesystem's room for what the
# snapshot copies and a tenth more, leaving STORE_FLOOR_BYTES (60 GiB, the floor this machine's
# owner keeps free) free. A claim kept otherwise, or elsewhere, too little room, or an image not
# pulled, fails the run there, saying which. A store not bundled is neither stopped nor
# snapshotted.
#
# Past it: the filers stopped first, which write to the other parts, then the volume servers, the
# masters and the filers' store, each its pods gone, so that each volume is snapshotted as its
# part left it; the image the masters ran kept (STORE_SNAPSHOT/image), and the count of each part
# (OUT/object-store-replicas, as first read); each claim snapshotted to STORE_SNAPSHOT/<claim>,
# into STORE_SNAPSHOT/<claim>.partial while it is, each step of it failing it, and checked whole
# against the claim before it is renamed: every file there, of its size, each volume's data the
# very file and every other a copy, each data file's size and head kept. How much it linked and
# copied and how long it took said: its files (files/), the size of each data file (sizes) and its
# head (heads/). Then the restore, the deletion and the pause written beside (restore.sh,
# delete.sh, pause.sh), every directory of the snapshot written by root alone, and the ConfigMap
# STORE_SNAPSHOT_CONFIGMAP made, of where the snapshot is, when it was taken, from when it may be deleted, the floor, what
# it copied and the data files it links, by which `alasio status` says it and how much of the disk
# it holds of its own, as the owner, for which OUT and every directory of the snapshot are made
# searchable by the owner's group alone, none listable, and no file it copied readable by any
# other account; OUT/object-store-snapshotted says it all was. Then
# each part started again, at its count, in the order its peers need: the filers' store, the
# masters, the volume servers, the filers, each ready, at the image its StatefulSet says, before
# the next starts, and the compaction paused. So the upgrade finds the store answering, as it
# reads what it does of it before it changes it, the snapshots the sandbox gateway names among
# them, and then starts each part again on 4.54, as its StatefulSet says; once it is through, the
# compaction paused again, and the ConfigMap says from when. The gateways run on, failing each
# request while the parts behind them are stopped. Meanwhile Neon's safekeepers and pageservers
# keep on their own volumes what they would upload, and upload it once the store answers; what
# else would write the store fails, and writes again: workspace storage's JuiceFS, the node
# agents' snapshots, the lake's loads; and a read fails, a Neon compute's of a layer its
# pageserver no longer holds among them. Run in a quiet hour, nothing is lost. The snapshot is of
# the store as it was stopped: what is written once it started again is in none, and a restore
# drops it, Neon's uploads among it. A run again snapshots again what was not snapshotted whole,
# and once OUT/object-store-snapshotted says it was, nothing, but starts each part again as above,
# which a part running at its count already is, at the image it runs then, 4.54's where an upgrade
# that failed got so far; should the upgrade fail, the store runs on what it ran when it did.
#
# The restore (restore.sh --disaster-recovery) puts back the object store alone: Neon's
# safekeepers and pageservers, and workspace storage's metadata, in its Valkey, are ahead of it
# then, holding what the store no longer does. So it refuses unless told it is a disaster's
# recovery, and unless every part of the store is stopped, its StatefulSets at none; it refuses a
# symbolic link in the snapshot or in a claim's directory, which it would write through; and it
# suspends the CronJobs that make the dumps the recovery reads (DUMP_WRITERS), ending any of their
# Jobs that runs, and stops Neon's safekeepers and pageservers, before it changes anything,
# keeping their counts beside (neon-counts), for their recovery, which its last words say, with
# how to resume the CronJobs once it is through.
#
# It takes as long as the stop, the parts' grace, 30 s, at most; the snapshot, seconds and the
# copy of what is not a volume's data, the filers' store's the most; and the parts' start, a minute
# or so. The snapshot is kept STORE_KEEP_SECONDS (48 h), for the store to be seen well on 4.54:
# delete.sh deletes it then, and its ConfigMap, resuming the compaction, and refuses to before,
# but where told (--under-floor) as its filesystem has less free than the floor and every part of
# the store is ready. Its pods' end, and each part's being ready, is waited for STOP_WAIT_SECONDS
# (10 min) at most, the end looked for every POLL_SECONDS (5).

NS=alasio
STORE_SNAPSHOT=${STORE_SNAPSHOT:-$OUT/object-store}
STORE_FLOOR_BYTES=${STORE_FLOOR_BYTES:-$((60 * 1024 * 1024 * 1024))}
STORE_KEEP_SECONDS=$((48 * 60 * 60))
STORE_SNAPSHOT_CONFIGMAP=alasio-object-store-snapshot
# The image of SeaweedFS this release runs the store on, as its configuration pins it (cli/src/manifests/config.ts).
STORE_IMAGE=chrislusf/seaweedfs:4.54@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15
# What pulls an image onto this machine, as the node's container runtime keeps it: k3s's crictl.
CRICTL=${CRICTL:-k3s crictl}
HEAD_BYTES=8192
STOP_WAIT_SECONDS=${STOP_WAIT_SECONDS:-600}
POLL_SECONDS=${POLL_SECONDS:-5}
# The store's parts, by their StatefulSets, in the order they are stopped: what writes to the others first. The filers keep no volume.
STORE_PARTS="alasio-seaweedfs-filer alasio-seaweedfs-volume alasio-seaweedfs-master alasio-seaweedfs-filer-store"
# Their pods' components, as a selector's set.
STORE_COMPONENTS="seaweedfs-filer,seaweedfs-volume,seaweedfs-master,seaweedfs-filer-store"
# The gateways' Deployment, in whose buckets container weed shell runs.
GATEWAYS=alasio-seaweedfs
# Neon's parts that hold what they write to the store, by their StatefulSets, which a restore stops.
NEON_WRITERS="alasio-neon-safekeeper alasio-neon-pageserver"
# The CronJobs that write the dumps a restore's recovery reads, Neon's (alasio.dump, on the backups claim) and workspace storage's
# metadata's (juicefs dump), which a restore suspends before it stops Neon, so none dumps a Neon or a metadata engine halfway.
DUMP_WRITERS="alasio-neon-backup alasio-juicefs-dump"
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
# Runs the store's shell commands '$1', locked, in a gateway's buckets container, as its own shell reaches the masters and the
# filers; fails where it cannot, or the shell says an error.
store_shell() {
  local made reach said
  made=$(kubectl -n "$NS" get deployment "$GATEWAYS" -o jsonpath='{.spec.template.spec.containers[?(@.name=="buckets")].command[2]}') || return 1
  reach=$(printf '%s\n' "$made" | grep -o -- '-master=[^ ]* -filer=[^ ;]*' | head -n 1)
  [ -n "$reach" ] || return 1
  said=$(printf 'lock\n%s\nunlock\n' "$1" | kubectl -n "$NS" exec -i "deployment/$GATEWAYS" -c buckets -- sh -c "weed shell $reach" 2>&1) || return 1
  ! printf '%s\n' "$said" | grep -qi 'error'
}
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
  log "pulling SeaweedFS 4.54 onto this machine ($STORE_IMAGE), for the store's parts to start on it at once"
  $CRICTL pull "$STORE_IMAGE" >/dev/null ||
    fail "SeaweedFS 4.54 ($STORE_IMAGE) could not be pulled onto this machine ($CRICTL pull), which the upgrade starts the object store's parts on: nothing is changed, and the cut-over run again pulls it again"
}
# The claim's directory '$1' snapshotted into '$2': its files (files/), each volume's data linked, every other file copied; the
# size of each volume's data (sizes, '<path>\t<bytes>' a line, its path under the directory) and its first HEAD_BYTES (heads/);
# each step failing it, and checked whole once done (store_snapshot_whole).
store_snapshot_claim() {
  local directory=$1 into=$2 path size
  mkdir -p "$into/heads" || return 1
  # Every file linked, each directory made as it is; then every file but a volume's data copied over its link.
  cp -al "$directory" "$into/files" || return 1
  : >"$into/sizes" || return 1
  while IFS= read -r -d '' path; do
    case "$path" in
      *.dat)
        size=$(stat -c %s "$directory/$path") || return 1
        printf '%s\t%s\n' "$path" "$size" >>"$into/sizes" || return 1
        mkdir -p "$(dirname "$into/heads/$path")" || return 1
        head -c "$HEAD_BYTES" "$directory/$path" >"$into/heads/$path" || return 1
        ;;
      *) cp -a --remove-destination "$directory/$path" "$into/files/$path" || return 1 ;;
    esac
  done < <(cd "$directory" && find . ! -type d -printf '%P\0')
  store_snapshot_whole "$directory" "$into"
}
# Whether the snapshot '$2' of the claim's directory '$1' is whole: every file of the directory there, of its size, and no
# other; each volume's data the very file, its size and its head kept; every other file a copy. Saying what is not.
store_snapshot_whole() {
  local directory=$1 into=$2 listed path size kept volumes=0
  listed=$(cd "$directory" && find . ! -type d -printf '%P\t%s\n' | LC_ALL=C sort) || return 1
  [ "$listed" = "$(cd "$into/files" && find . ! -type d -printf '%P\t%s\n' | LC_ALL=C sort)" ] ||
    { log "the snapshot of $directory does not hold its files, each of its size, and no other"; return 1; }
  while IFS=$'\t' read -r path size; do
    [ -n "$path" ] || continue
    case "$path" in
      *.dat)
        volumes=$((volumes + 1))
        [ "$into/files/$path" -ef "$directory/$path" ] || { log "the snapshot of $directory/$path is not linked to it"; return 1; }
        kept=$(awk -F '\t' -v path="$path" '$1 == path { print $2 }' "$into/sizes")
        [ "$kept" = "$size" ] || { log "the snapshot of $directory/$path keeps its size as '$kept', not $size"; return 1; }
        head -c "$HEAD_BYTES" "$directory/$path" | cmp -s - "$into/heads/$path" || { log "the snapshot of $directory/$path does not keep its head"; return 1; }
        ;;
      *) [ ! "$into/files/$path" -ef "$directory/$path" ] || { log "the snapshot of $directory/$path is linked to it, not a copy"; return 1; } ;;
    esac
  done <<<"$listed"
  [ "$(wc -l <"$into/sizes")" -eq "$volumes" ] || { log "the snapshot of $directory keeps the sizes of other volumes than its $volumes"; return 1; }
}
store_snapshotted_past() {
  local listed
  listed=$(store_claims)
  [ -n "$listed" ] || return 0
  [ -e "$OUT/object-store-snapshotted" ] || store_snapshotted "$listed"
  store_started
  store_shell volume.vacuum.disable || fail "the object store's compaction could not be paused (volume.vacuum.disable, in weed shell): the cut-over run again pauses it"
  log "the object store's compaction paused, so its snapshot holds no more of the disk"
}
store_snapshotted() {
  local listed=$1 part count claim directory started linked copied taken group
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
  [ -s "$STORE_SNAPSHOT/image" ] || k get statefulset alasio-seaweedfs-master -o jsonpath='{.spec.template.spec.containers[?(@.name=="master")].image}' >"$STORE_SNAPSHOT/image" ||
    fail "the image of the object store's masters could not be read"
  for part in $STORE_PARTS; do
    k scale statefulset "$part" --replicas=0 >/dev/null || fail "the object store's $part could not be stopped"
    gone "app.kubernetes.io/component=${part#alasio-}" "$STOP_WAIT_SECONDS" || fail "the object store's $part did not stop within ${STOP_WAIT_SECONDS} s"
  done
  while IFS=$'\t' read -r claim directory; do
    [ ! -d "$STORE_SNAPSHOT/$claim" ] || continue
    rm -rf "$STORE_SNAPSHOT/$claim.partial"
    started=$(date +%s)
    store_snapshot_claim "$directory" "$STORE_SNAPSHOT/$claim.partial" ||
      fail "the claim $claim could not be snapshotted whole from $directory, and its snapshot is not taken: the cut-over run again snapshots it again"
    mv "$STORE_SNAPSHOT/$claim.partial" "$STORE_SNAPSHOT/$claim"
    linked=$(wc -l <"$STORE_SNAPSHOT/$claim/sizes")
    copied=$(find "$STORE_SNAPSHOT/$claim/files" -type f ! -name '*.dat' -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')
    log "snapshotted the object store's $claim, $linked volumes' data linked and $copied bytes copied, in $(($(date +%s) - started)) s, to $STORE_SNAPSHOT/$claim"
  done <<<"$listed"
  printf '%s\n' "$listed" >"$STORE_SNAPSHOT/claims"
  cp "$OUT/object-store-replicas" "$STORE_SNAPSHOT/counts"
  printf '%s\n' "$STORE_FLOOR_BYTES" >"$STORE_SNAPSHOT/floor"
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
  # Every directory of it, and OUT, written by root alone and searchable by the owner's group alone, as `alasio status` stats the
  # data files it links as the owner; none of it read by any other account: every file it copied of its own, but the data files
  # it shares with the store, which no other account reaches through it.
  group=$(id -gn "$(stat -c %U "$CONFIG")") || fail "the group of $CONFIG's owner could not be read"
  find "$STORE_SNAPSHOT" -type d -exec chgrp "$group" {} + -exec chmod go-w,g-r,g+x,o-rwx {} +
  find "$STORE_SNAPSHOT" -type f ! -path '*/files/*.dat' -exec chmod o-rwx {} +
  chgrp "$group" "$OUT"
  chmod g-rw,g+x,o-rwx "$OUT"
  k delete configmap "$STORE_SNAPSHOT_CONFIGMAP" --ignore-not-found >/dev/null || fail "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be deleted to be made anew"
  k create configmap "$STORE_SNAPSHOT_CONFIGMAP" --from-literal=path="$STORE_SNAPSHOT" --from-literal=taken="$taken" \
    --from-literal=keepUntil="$(date -u -d "@$(($(date -d "$taken" +%s) + STORE_KEEP_SECONDS))" +%FT%TZ)" \
    --from-literal=floorBytes="$STORE_FLOOR_BYTES" --from-literal=copiedBytes="$copied" --from-file=volumes="$STORE_SNAPSHOT/volumes" >/dev/null ||
    fail "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP, by which alasio status says the snapshot, could not be made"
  date -u +%FT%TZ >"$OUT/object-store-snapshotted"
  log "the object store snapshotted, to $STORE_SNAPSHOT: restore.sh --disaster-recovery there puts it back, its parts stopped; delete.sh may delete it from $(date -u -d "@$(($(date -d "$taken" +%s) + STORE_KEEP_SECONDS))" +%FT%TZ)"
}
# Once the upgrade is through: the compaction, which the masters it started on 4.54 do not know is paused, paused again, and the
# ConfigMap told from when, which `alasio status` reads it by.
store_vacuum_paused_after() {
  [ -n "$(store_claims)" ] || return 0
  store_vacuum_pause || fail "the object store's compaction could not be paused on SeaweedFS 4.54: the cut-over run again pauses it"
  log "the object store's compaction paused on SeaweedFS 4.54, until its snapshot is deleted"
}
# The store's compaction paused (volume.vacuum.disable), and the ConfigMap told from when, which `alasio status` reads it by;
# saying which failed. As the cut-over pauses it, and pause.sh again, where a master started anew has forgotten it.
store_vacuum_pause() {
  store_shell volume.vacuum.disable || { say "the store's shell did not pause its compaction (volume.vacuum.disable)"; return 1; }
  kubectl -n "$NS" patch configmap "$STORE_SNAPSHOT_CONFIGMAP" --type merge -p "{\"data\":{\"vacuumPausedAt\":\"$(date -u +%FT%TZ)\"}}" >/dev/null ||
    { say "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be told the compaction is paused"; return 1; }
}
# pause.sh: the store's compaction paused again, as a master started since the cut-over has forgotten it, its snapshot '$1' kept.
store_vacuum_paused_again() {
  [ -r "$1/taken" ] || refuse "$1 holds no snapshot of the object store"
  store_vacuum_pause || refuse "the object store's compaction could not be paused: run this again once the store answers"
  say "the object store's compaction paused again, until its snapshot $1 is deleted"
}
# The restore, the deletion and the pause of the compaction again, written into the snapshot, each a script of its own of what it
# needs of this one.
store_snapshot_scripts() {
  local name run
  for name in restore delete pause; do
    case "$name" in
      restore) run=store_restore ;;
      delete) run=store_snapshot_delete ;;
      pause) run=store_vacuum_paused_again ;;
    esac
    {
      printf '#!/usr/bin/env bash\n'
      printf '# %s\n' "Written by the cut-over from 10.0.27 (upgrades/from-10.0.27.sh), which says what it does: run as root on the host, kubectl reaching the cluster."
      printf 'set -euo pipefail\n'
      declare -p NS STORE_PARTS STORE_COMPONENTS STORE_KEEP_SECONDS STORE_SNAPSHOT_CONFIGMAP GATEWAYS NEON_WRITERS DUMP_WRITERS POLL_SECONDS STOP_WAIT_SECONDS
      declare -f say refuse store_shell store_vacuum_pause store_vacuum_paused_again store_stopped_parts store_ready store_restore store_snapshot_delete
      printf '%s "$(dirname "$(readlink -f "$0")")" "$@"\n' "$run"
    } >"$STORE_SNAPSHOT/$name.sh"
    chmod 0700 "$STORE_SNAPSHOT/$name.sh"
  done
}
# Whether each of the StatefulSets '$@' is stopped, at none and its pods gone, within STOP_WAIT_SECONDS; saying which is not.
store_stopped_parts() {
  local part waited
  for part in "$@"; do
    waited=0
    # Its status's count of pods left out where it has none.
    until case "$(kubectl -n "$NS" get statefulset "$part" -o jsonpath='{.spec.replicas}/{.status.replicas}')" in 0/ | 0/0) true ;; *) false ;; esac do
      [ "$waited" -lt "$STOP_WAIT_SECONDS" ] || { say "$part is not stopped within $STOP_WAIT_SECONDS s"; return 1; }
      sleep "$POLL_SECONDS"
      waited=$((waited + POLL_SECONDS))
    done
  done
}
# Whether every part of the store is ready, each StatefulSet's and the gateways' replicas ready; saying which is not.
store_ready() {
  local part counts
  for part in $STORE_PARTS; do
    counts=$(kubectl -n "$NS" get statefulset "$part" -o jsonpath='{.spec.replicas}/{.status.readyReplicas}') || { say "$part could not be read"; return 1; }
    [ "${counts%/*}" -gt 0 ] && [ "${counts%/*}" = "${counts#*/}" ] || { say "$part has $counts of its replicas ready"; return 1; }
  done
  counts=$(kubectl -n "$NS" get deployment "$GATEWAYS" -o jsonpath='{.spec.replicas}/{.status.readyReplicas}') || { say "$GATEWAYS could not be read"; return 1; }
  [ "${counts%/*}" -gt 0 ] && [ "${counts%/*}" = "${counts#*/}" ] || { say "$GATEWAYS has $counts of its replicas ready"; return 1; }
}
# The store's volumes put back as the snapshot '$1' holds them, told '$2' --disaster-recovery, its parts stopped: Neon's writers
# stopped first, their counts kept; what the store made since, in no snapshot, removed, the volumes 4.54 made among them, of which
# the masters as they were know nothing; every file but a volume's data copied back; and each volume's data, its file linked back
# where a compaction replaced it or a deletion removed it, cut to its size, past which the store appended, and its head written back.
store_restore() {
  local snapshot=$1 told=${2:-} part replicas running claim directory files path size count job active
  [ -r "$snapshot/claims" ] || refuse "$snapshot holds no snapshot of the object store"
  [ "$told" = --disaster-recovery ] ||
    refuse "this puts back the object store alone, as it was before its upgrade ($(cat "$snapshot/taken")), dropping all it took since: Neon's safekeepers and pageservers, and workspace storage's metadata, would be ahead of it, holding what it no longer does, so it stops Neon's for their recovery from their backups, and workspace storage's must be recovered too, before either runs again; run it as $0 --disaster-recovery where that is what the store's loss calls for"
  for part in $STORE_PARTS; do
    replicas=$(kubectl -n "$NS" get statefulset "$part" -o jsonpath='{.spec.replicas}') ||
      refuse "the object store's $part could not be read, so whether it is stopped cannot be told: give kubectl k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
    [ "$replicas" = 0 ] || refuse "the object store's $part is at $replicas replicas: stop each of its parts first, $STORE_PARTS (kubectl -n $NS scale statefulset <part> --replicas=0), until none of their pods is left"
  done
  running=$(kubectl -n "$NS" get pod -l "app.kubernetes.io/component in ($STORE_COMPONENTS)" -o name) ||
    refuse "the object store's pods could not be listed, so whether it is stopped cannot be told"
  [ -z "$running" ] || refuse "the object store runs ($(printf '%s ' $running)): wait until none of its parts' pods is left"
  while IFS=$'\t' read -r claim directory; do
    files=$snapshot/$claim/files
    [ -d "$files" ] && [ -d "$directory" ] || refuse "the claim $claim's snapshot ($files) or its directory ($directory) is missing"
    [ ! -L "$directory" ] && [ -z "$(find "$snapshot/$claim" "$directory" -type l -print -quit)" ] ||
      refuse "the claim $claim's snapshot ($snapshot/$claim) or its directory ($directory) holds a symbolic link, which the restore would write through: nothing is changed"
  done <"$snapshot/claims"
  # The dumps the recovery reads made no more, each CronJob that makes one suspended, and any of its Jobs that runs ended: a dump
  # of a Neon whose pageservers are stopped, or of a metadata engine ahead of the store, would replace the one it needs.
  for job in $DUMP_WRITERS; do
    kubectl -n "$NS" patch cronjob "$job" --type merge -p '{"spec":{"suspend":true}}' >/dev/null || refuse "the CronJob $job could not be suspended: nothing of the store is changed"
    active=$(kubectl -n "$NS" get cronjob "$job" -o jsonpath='{range .status.active[*]}{.name}{"\n"}{end}') || refuse "the CronJob $job's Jobs could not be read: nothing of the store is changed"
    for running in $active; do
      kubectl -n "$NS" delete job "$running" --cascade=foreground --wait >/dev/null || refuse "the Job $running of the CronJob $job could not be ended: nothing of the store is changed"
    done
  done
  # Neon's writers stopped, as what they hold is ahead of the store once it is put back; their counts kept as first read, whole.
  if [ ! -s "$snapshot/neon-counts" ]; then
    : >"$snapshot/neon-counts.read"
    for part in $NEON_WRITERS; do
      count=$(kubectl -n "$NS" get statefulset "$part" -o jsonpath='{.spec.replicas}') || refuse "Neon's $part could not be read: nothing of the store is changed"
      printf '%s\t%s\n' "$part" "$count" >>"$snapshot/neon-counts.read"
    done
    mv "$snapshot/neon-counts.read" "$snapshot/neon-counts"
  fi
  for part in $NEON_WRITERS; do kubectl -n "$NS" scale statefulset "$part" --replicas=0 >/dev/null || refuse "Neon's $part could not be stopped: nothing of the store is changed"; done
  store_stopped_parts $NEON_WRITERS || refuse "Neon's writers did not stop: nothing of the store is changed"
  while IFS=$'\t' read -r claim directory; do
    files=$snapshot/$claim/files
    while IFS= read -r -d '' path; do
      [ -e "$files/$path" ] || rm -rf -- "${directory:?}/$path"
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
  say "the object store restored as it was snapshotted, $(cat "$snapshot/taken"), and it alone: start it on $(cat "$snapshot/image"), as 10.0.27 ran it, each container of its StatefulSets and of its gateways' Deployment ($GATEWAYS) that runs SeaweedFS set to that image (kubectl -n $NS set image), then each part scaled back to its count ($(tr '\t\n' '= ' <"$snapshot/counts")), the filers' store, the masters, the volume servers, the filers in that order; its masters, started anew, compact it again. Neon's safekeepers and pageservers are stopped ($(tr '\t\n' '= ' <"$snapshot/neon-counts")): what they hold is ahead of the store, so recover Neon's databases from their newest dump, the backups claim's alasio.dump, newer than any the store holds, before they run again; and workspace storage's metadata, in its Valkey, is ahead of the store too: load its newest JuiceFS dump that the store holds (the backups bucket's) before workspaces are mounted again. The CronJobs that make those dumps ($DUMP_WRITERS) are suspended, so none replaces what the recovery reads: once it is through, resume each (kubectl -n $NS patch cronjob <name> --type merge -p '{\"spec\":{\"suspend\":false}}')"
}
# The snapshot '$1' deleted, and its ConfigMap, the store's compaction resumed, once it has been kept STORE_KEEP_SECONDS; before,
# only told '$2' --under-floor, as its filesystem has less free than the floor and every part of the store is ready.
store_snapshot_delete() {
  local snapshot=$1 told=${2:-} named taken kept free floor
  [ -r "$snapshot/taken" ] || refuse "$snapshot holds no snapshot of the object store"
  named=$(kubectl -n "$NS" get configmap "$STORE_SNAPSHOT_CONFIGMAP" -o jsonpath='{.data.path}' --ignore-not-found) ||
    refuse "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be read: give kubectl k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
  [ -z "$named" ] || [ "$named" = "$snapshot" ] || refuse "the ConfigMap $STORE_SNAPSHOT_CONFIGMAP names the snapshot $named, not $snapshot: nothing is deleted"
  taken=$(date -d "$(cat "$snapshot/taken")" +%s)
  kept=$((taken + STORE_KEEP_SECONDS))
  if [ "$(date +%s)" -lt "$kept" ]; then
    floor=$(cat "$snapshot/floor")
    [ "$told" = --under-floor ] ||
      refuse "the snapshot $snapshot is kept until $(date -u -d "@$kept" +%FT%TZ), for the store to be seen well on SeaweedFS 4.54 first: run this again then, or sooner as $0 --under-floor, which deletes it only while its disk has less than $floor free and every part of the store is ready"
    free=$(df --output=avail -B1 "$snapshot" | tail -n 1)
    [ "$free" -lt "$floor" ] || refuse "--under-floor, but its disk has $free free, not under the floor of $floor: it is kept until $(date -u -d "@$kept" +%FT%TZ)"
    store_ready || refuse "--under-floor, but the object store is not ready, which its snapshot may yet be needed for: it is kept"
  fi
  store_shell volume.vacuum.enable || refuse "the object store's compaction could not be resumed (volume.vacuum.enable, in weed shell), so the snapshot is kept: run this again once the store answers"
  rm -rf -- "$snapshot" || refuse "$snapshot could not be deleted whole"
  kubectl -n "$NS" delete configmap "$STORE_SNAPSHOT_CONFIGMAP" --ignore-not-found >/dev/null ||
    refuse "the snapshot $snapshot is deleted, but its ConfigMap $STORE_SNAPSHOT_CONFIGMAP could not be: delete it (kubectl -n $NS delete configmap $STORE_SNAPSHOT_CONFIGMAP)"
  say "deleted the object store's snapshot $snapshot, taken $(date -u -d "@$taken" +%FT%TZ), its compaction resumed"
}
# Each part started again at the count kept of it, those its peers need first: the reverse of the order they are stopped in; each
# waited on as its StatefulSet says it runs, at the image it says, 4.54's where an upgrade that failed got so far.
store_started() {
  local part count image
  for part in $(printf '%s\n' $STORE_PARTS | tac); do
    count=$(awk -F '\t' -v part="$part" '$1 == part { print $2 }' "$OUT/object-store-replicas")
    [ -n "$count" ] || fail "$OUT/object-store-replicas keeps no count of the object store's $part"
    k scale statefulset "$part" --replicas="$count" >/dev/null || fail "the object store's $part could not be started again: the cut-over run again starts it"
    k rollout status statefulset "$part" --timeout="${STOP_WAIT_SECONDS}s" >/dev/null ||
      fail "the object store's $part was not ready within ${STOP_WAIT_SECONDS} s of its start (kubectl -n $NS describe statefulset $part): the cut-over run again starts it"
  done
  image=$(k get statefulset alasio-seaweedfs-master -o jsonpath='{.spec.template.spec.containers[?(@.name=="master")].image}') ||
    fail "the image of the object store's masters could not be read"
  log "the object store started again, each part ready, on $image"
}
# What an upgrade that failed leaves of the bundled store: the image its masters run, as their StatefulSet says, and its snapshot.
store_said() {
  [ -e "$OUT/object-store-snapshotted" ] || return 0
  printf ', the object store running as its StatefulSets say, its masters on %s, its snapshot kept in %s' "$(k get statefulset alasio-seaweedfs-master -o jsonpath='{.spec.template.spec.containers[?(@.name=="master")].image}' 2>/dev/null || echo 'what could not be read')" "$STORE_SNAPSHOT"
}
# END the object store snapshotted before SeaweedFS 4.54.

command -v kubectl >/dev/null || fail "kubectl is not on root's PATH"
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 10.0.27, recording in $OUT"
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 10.0.27 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 10.0.27 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
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
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log)$(store_said): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"
store_vacuum_paused_after

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
