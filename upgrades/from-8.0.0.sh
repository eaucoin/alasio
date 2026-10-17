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
# step before the upgrade has run, as the upgrade shard rehearses a failure.
#
# It upgrades main alone, as alasio upgrade does, which refuses while a branch environment
# 8.0.0 made exists: UPGRADE_CHECK fails then. Delete each first (alasio branch delete
# <name>), and make it again once upgraded.
#
# Once upgraded, k3s pulls images through stargz-snapshotter's daemon, which sees none of those
# overlayfs unpacked under 8.0.0, so they would hold the disk for as long as their images stay,
# and every pod not started again would go on running on them. alasio upgrade refuses a cluster
# that does not answer, so they cannot go before it. So, once upgraded, the node's images go,
# and k3s pulls each anew: k3s stops as alasio down stops it, every container and gVisor's
# processes killed, then containerd's whole state and the daemon's are removed while nothing
# can use them, k3s starts again, and the upgrade applied again waits until alasio runs on what
# it pulled. Every image is pulled again, from registries that are to answer meanwhile, as no
# copy is left on the machine.
#
# Run again, it goes on from where it stopped: once UPGRADE has begun
# (OUT/point-of-no-return), a run again goes on with it, and one that finds the move of the
# images begun (OUT/images-moving) ends it first, as k3s is stopped then.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${OUT:?where to record}"
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}

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
CONTAINERD_STATE=/var/lib/rancher/k3s/agent/containerd
STARGZ_ROOT=/var/lib/containerd-stargz-grpc
# Its sockets, its FUSE manager's among them: the daemon starts no manager where that is.
STARGZ_RUN=/run/containerd-stargz-grpc
# Kills each process running one of the executables "$@", as alasio down kills gVisor's.
kill_running() {
  local exe executable wanted
  for exe in /proc/[0-9]*/exe; do
    executable=$(readlink "$exe" 2>/dev/null) || continue
    for wanted in "$@"; do
      # Unquoted, a pattern: each gVisor release alasio keeps is a directory of its own.
      if [[ ${executable% (deleted)} == $wanted ]]; then kill -KILL "${exe:6:-4}" 2>/dev/null || true; fi
    done
  done
}
# Where a mount is, of those under the directory $1.
mounted_under() { awk -v under="$1" '$2 == under || index($2, under "/") == 1 { print $2 }' /proc/self/mounts; }
# Whether UPGRADE_CHECK passes now, what it said in OUT/upgrade-check.log.
accepted() { bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1; }
# The node's images removed, with every snapshot of theirs, overlayfs's and the daemon's, and
# pulled anew through the daemon by k3s started again; OUT/images-moving says it is begun, so
# that a run again, which finds k3s stopped, ends it first, and OUT/images-moved that it ended.
move_images() {
  : >"$OUT/images-moving"
  log "stopping k3s, to remove the node's images, unpacked by overlayfs under 8.0.0: $(du -shx "$CONTAINERD_STATE" | cut -f1) of containerd's state"
  systemctl stop k3s
  kill_running '/usr/local/lib/alasio/gvisor/*'
  /usr/local/bin/k3s-killall.sh >/dev/null 2>&1
  [ -z "$(mounted_under "$CONTAINERD_STATE"; mounted_under /run/k3s/containerd)" ] ||
    fail "k3s-killall.sh left mounts of containerd's: $(mounted_under "$CONTAINERD_STATE"; mounted_under /run/k3s/containerd)"
  # The daemon stopped with k3s; its FUSE manager, which serves its mounts, stops now.
  kill_running /usr/local/bin/stargz-fuse-manager
  mounted_under "$STARGZ_ROOT" | sort -r | while read -r point; do umount --lazy "$point"; done
  rm -rf "$CONTAINERD_STATE" "$STARGZ_ROOT" "$STARGZ_RUN"
  log "starting k3s, which pulls every image again through stargz-snapshotter's daemon"
  systemctl start k3s
  wait_for 600 "k3s to answer" kubectl get --raw=/readyz
  # What the release checks before it applies anything, its storage controller and that
  # controller's database answering among it, back on the images pulled anew.
  wait_for 900 "the release to accept the installation again, its pods started on the images pulled anew (upgrade-check.log)" accepted
  mv "$OUT/images-moving" "$OUT/images-moved"
}

command -v kubectl >/dev/null || fail "kubectl is not on root's PATH"
# A move of the images begun and not ended left k3s stopped, which kubectl and the upgrade find so.
[ ! -e "$OUT/images-moving" ] || move_images
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  accepted || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
fi

log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
if [ ! -e "$OUT/images-moved" ]; then
  move_images
  log "applying the upgrade again, which waits until alasio runs on the images pulled anew"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade applied again failed (upgrade.log): the cut-over run again goes on with it"
fi
log "upgraded"
