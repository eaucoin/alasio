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
# brings what makes it again, for the flow to run as such a run ends. One has a step after
# the upgrade: the session image's layers mounted anew, under FUSE passthrough.
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

# BEGIN the session image's layers mounted anew, under FUSE passthrough.
#
# This release has stargz-snapshotter's daemon serve the session image's files through FUSE
# passthrough (cli/src/cluster/stargz.ts), which the daemon sets on a layer as it mounts it. The
# upgrade writes the daemon's configuration and restarts it with k3s, but its FUSE manager, which
# keeps its mounts through every restart of the daemon, serves on each layer it mounted under the
# release before's, and a pod made since reads those as they are. Once the upgrade is through, so,
# the layers are mounted anew, as the daemon mounts them where its manager has gone: the daemon
# stopped; the layers its manager serves unmounted lazily, which the containers on them read on,
# the manager serving them still; the manager's socket and its record of what it mounted removed,
# so that the daemon, started again, starts a manager of its own and mounts each of its snapshots'
# layers again through it, under its configuration (stargz-snapshotter's StartFuseManager and
# restoreRemoteSnapshot; its service's ExecStartPre leaves all as it is, as the manager before
# still listens). Then the pod of each container on the layers before is deleted, as a drain
# deletes it: a session's bound pod saved, its session released, as its bayma holds its stop
# (src/gateway/snapshots.ts saveStopping), and restored at its next bind onto the layers mounted
# anew; a pool's free pod, a standby, a harness's pod and a folder instance made again on them, as
# each is once deleted. Once none of those containers runs, the manager before is killed, with what
# it served. Nothing else restarts: not containerd, nor k3s, which the upgrade restarted, nor a pod
# of an image pulled whole. The daemon serves again within seconds, and the sessions' saves take
# what a drain's take, their pods' grace at the most. Where the daemon's configuration sets no
# passthrough, as the target's imagePassthrough may set it off, or no daemon of alasio's runs on
# this machine, as on a cluster in Docker, whose nodes the upgrade makes anew where it changes
# their configuration, none is mounted anew. Run again, it goes on from where it stopped.
STARGZ_ROOT=/var/lib/containerd-stargz-grpc
# Where the daemon mounts its snapshots' layers, each in a directory of its own.
STARGZ_SNAPSHOTS=$STARGZ_ROOT/snapshotter/snapshots/
FUSE_MANAGER=/usr/local/bin/stargz-fuse-manager
# The machine's root, but for tests.
node_root=${NODE_ROOT:-}
stargz_config=$node_root/etc/containerd-stargz-grpc/config.toml
stargz_run=$node_root/run/containerd-stargz-grpc
# The processes of the daemon's FUSE manager, a PID a line, by the executable each runs.
fuse_managers() {
  local exe
  for exe in "$node_root"/proc/[0-9]*/exe; do
    if [ "$(readlink "$exe" 2>/dev/null)" = "$FUSE_MANAGER" ]; then basename "$(dirname "$exe")"; fi
  done
}
# When the process PID started, in the kernel's ticks since the machine booted, so that a PID used again is told apart; nothing where it has ended.
started_at() { sed 's/^.*) //' "$node_root/proc/$1/stat" 2>/dev/null | cut -d ' ' -f 20; }
# The seconds since MS, milliseconds since the epoch.
seconds_since() { awk -v from="$1" -v to="$(date +%s%3N)" 'BEGIN { printf "%.1f", (to - from) / 1000 }'; }
# The mount points of what is mounted of TYPE, a prefix of it, as /proc/self/mountinfo lists them.
mounted() { awk -v type="$1" '{ for (i = 7; i <= NF; i++) if ($i == "-") { if (index($(i + 1), type) == 1) print $5; next } }' "$node_root/proc/self/mountinfo"; }
# The containers of the layers before (OUT/containers-before) whose roots are still mounted, by their ids: those that run, and
# those made and not started, as a standby's are, until each ends and is removed.
mounted_before() { mounted overlay | sed -n 's|/rootfs$||p' | xargs -r -n 1 basename | { grep -F -x -f "$OUT/containers-before" || true; }; }
layers_mounted_anew() {
  if [ -e "$OUT/remounted" ]; then
    log "the session image's layers mounted anew at $(cat "$OUT/remounted")"
    return 0
  fi
  if [ ! -e "$stargz_config" ] || ! grep -qx 'passthrough = true' "$stargz_config"; then
    log "no stargz-snapshotter daemon of alasio's on this machine serves the session image through FUSE passthrough: no layer to mount anew"
    date -u +%FT%TZ >"$OUT/remounted"
    return 0
  fi
  local began managers before started now on pods waited
  began=$(date +%s%3N)
  if [ ! -e "$OUT/manager-before" ]; then
    managers=$(fuse_managers)
    [ -n "$managers" ] && [ "$(printf '%s\n' "$managers" | wc -l)" -eq 1 ] ||
      fail "stargz-snapshotter's daemon has not one FUSE manager but ${managers:-none}, so what serves its layers cannot be told: the cut-over run again goes on with it"
    log "stopping stargz-snapshotter's daemon, its FUSE manager $managers serving its layers on (remount.log)"
    systemctl stop stargz-snapshotter >>"$OUT/remount.log" 2>&1 || fail "stargz-snapshotter's daemon could not be stopped (remount.log): the cut-over run again goes on with it"
    # The layers its manager serves, as they were before any was unmounted.
    if [ ! -e "$OUT/layers-before" ]; then
      mounted fuse | { grep "^$STARGZ_SNAPSHOTS" || true; } >"$OUT/layers-before.part"
      mv "$OUT/layers-before.part" "$OUT/layers-before"
    fi
    rm -f "$stargz_run/fuse-manager.sock" "$node_root$STARGZ_ROOT/fusestore.db" "$stargz_run/containerd-stargz-grpc.sock"
    # Those still mounted, deepest first.
    mounted fuse | { grep -F -x -f "$OUT/layers-before" || true; } | LC_ALL=C sort -r | xargs -r -n 1 umount -l 2>>"$OUT/remount.log" ||
      fail "the layers of stargz-snapshotter's FUSE manager could not all be unmounted (remount.log): the cut-over run again goes on with them"
    # The containers on them: each whose root is an overlay of them, which holds on to them, unmounted as they are.
    { grep ' - overlay ' "$node_root/proc/self/mountinfo" || true; } | { grep -F -f "$OUT/layers-before" || true; } | cut -d ' ' -f 5 | sed -n 's|/rootfs$||p' | xargs -r -n 1 basename >"$OUT/containers-before.part"
    mv "$OUT/containers-before.part" "$OUT/containers-before"
    printf '%s %s\n' "$managers" "$(started_at "$managers")" >"$OUT/manager-before"
    log "unmounted the $(wc -l <"$OUT/layers-before") layers it served, lazily, which $(wc -l <"$OUT/containers-before") containers read on, the manager serving them still"
  fi
  read -r before started <"$OUT/manager-before"
  if ! systemctl is-active --quiet stargz-snapshotter; then
    log "starting stargz-snapshotter's daemon, which starts a FUSE manager of its own and mounts each layer again through it, under FUSE passthrough"
    systemctl start stargz-snapshotter >>"$OUT/remount.log" 2>&1 || fail "stargz-snapshotter's daemon could not be started (remount.log): the cut-over run again goes on with it"
  fi
  # Its socket, which it makes once it has mounted its snapshots' layers again.
  waited=0
  until [ -S "$stargz_run/containerd-stargz-grpc.sock" ] || [ "$waited" -ge 3000 ]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  [ -S "$stargz_run/containerd-stargz-grpc.sock" ] || fail "stargz-snapshotter's daemon did not serve again within 5 minutes (remount.log): the cut-over run again goes on with it"
  now=$(fuse_managers | { grep -v -x "$before" || true; })
  [ -n "$now" ] && [ "$(printf '%s\n' "$now" | wc -l)" -eq 1 ] ||
    fail "stargz-snapshotter's daemon started not one FUSE manager of its own but ${now:-none}: the cut-over run again goes on with it"
  log "stargz-snapshotter's daemon serves again, $(seconds_since "$began") s after this run of the step began, each layer mounted anew through its FUSE manager $now"
  on=$(mounted_before)
  if [ -n "$on" ]; then
    pods=$(for container in $on; do
      k3s crictl inspect --output go-template --template '{{index .status.labels "io.kubernetes.pod.namespace"}} {{index .status.labels "io.kubernetes.pod.name"}}{{"\n"}}' "$container" 2>>"$OUT/remount.log" || true
    done | sort -u | { grep ' .' || true; })
    log "deleting the $(printf '%s\n' "$pods" | grep -c .) pods of the $(printf '%s\n' "$on" | wc -l) containers on the layers before, as a drain does: a session's bound pod saved and released, the others made again (remount.log)"
    printf '%s\n' "$pods" | while read -r namespace name; do
      [ -z "$name" ] || kubectl -n "$namespace" delete pod "$name" --wait=false --ignore-not-found >>"$OUT/remount.log" 2>&1 || exit 1
    done || fail "the pods on the layers before could not all be deleted (remount.log): the cut-over run again goes on with them"
    waited=0
    while on=$(mounted_before) && [ -n "$on" ]; do
      [ "$waited" -lt 600 ] || fail "$(printf '%s\n' "$on" | wc -l) containers on the layers before are still there 10 minutes after their pods were deleted (remount.log): the cut-over run again goes on with them"
      sleep 1
      waited=$((waited + 1))
    done
    log "the containers on the layers before ended, $waited s after their pods were deleted"
  fi
  # The manager before, as it started, not another given its PID since.
  if [ -n "$started" ] && [ "$(started_at "$before")" = "$started" ]; then kill -KILL "$before" 2>/dev/null || true; fi
  log "killed the FUSE manager before, $before, with the layers it served, which nothing reads"
  date -u +%FT%TZ >"$OUT/remounted"
  log "the session image's layers mounted anew, under FUSE passthrough, $(seconds_since "$began") s after this run of the step began"
}
# END the session image's layers mounted anew, under FUSE passthrough.

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
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"
layers_mounted_anew

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
