#!/usr/bin/env bash
# An installation of alasio 9.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 9.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-9.0.0.sh
#
# CONFIG is the configuration's file, the owner's alasio/config.json, which UPGRADE upgrades
# by and the steps below read, and change where they say.
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
# 9.0.0 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, past it, before the upgrade, and once the upgrade is
# through, after it. Each change's steps are a section of their own below, between its BEGIN
# and END lines, which the flow at the end of the script calls. None changes, before the
# point of no return, what a run that fails there would have to make again: a step that does
# brings what makes it again, for the flow to run as such a run ends.
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
# Where the node's filesystem is: the machine's own, but in the cut-over's unit test.
NODE_ROOT=${NODE_ROOT:-}

# BEGIN the sandbox image: its content whole.
#
# 9.0.0 held the pods' sandbox image unpinned, under the name k3s pins it by, which is not the
# one containerd's CRI pulls it by: a prune of the node's images deleted it while pods ran on
# its snapshot, and the next pod pulled it again onto that snapshot, which containerd fetches
# no layer of, so that it is recorded without its layer's content. This release pins it
# (cli/src/cluster/k3s.ts SANDBOX_IMAGE), which nothing deletes then, but a node whose record
# of it lacks its content keeps it so, and makes no pod once that snapshot is gone. So, before
# anything changes, what containerd lacks of the sandbox image is fetched, by the digest its
# name names, as it was pulled. Of it alone: an image stargz-snapshotter's daemon pulls lazily
# is recorded without its layers' content by design, and runs on the daemon's snapshots.

# k3s's containerd's command line, on the CRI's images.
images_ctr() { "$NODE_ROOT/usr/local/bin/k3s" ctr --namespace k8s.io "$@"; }
CONTAINERD_CONFIG=$NODE_ROOT/var/lib/rancher/k3s/agent/etc/containerd
# The sandbox image's digest, as containerd records it, and whether it holds all its content
# for the node's platform, "complete" if it does: `$1`'s line of containerd's check of it.
sandbox_image_checked() { images_ctr images check "name==$1" | awk -v name="$1" '$1 == name { print $3, $4 }'; }
# Before the point of no return: what containerd lacks of the sandbox image fetched.
sandbox_image_before() {
  local pinned name repository checked digest
  pinned=$(sed -n 's/^ *sandbox = "\(.*\)"$/\1/p' "$CONTAINERD_CONFIG/config.toml") && [ -n "$pinned" ] ||
    fail "containerd's configuration names no sandbox image ($CONTAINERD_CONFIG/config.toml)"
  # Its name as containerd records it, Docker's references normalized: docker.io where it names
  # no registry, and library/ for one of docker.io's own.
  case ${pinned%%/*} in
    "$pinned") name=docker.io/library/$pinned ;;
    *.* | *:* | localhost) name=$pinned ;;
    *) name=docker.io/$pinned ;;
  esac
  checked=$(sandbox_image_checked "$name") || fail "containerd's record of the sandbox image $name could not be checked"
  # None recorded, the CRI pulls it whole.
  [ -n "$checked" ] && [ "${checked#* }" != complete ] || return 0
  digest=${checked%% *}
  # Its tag, where it has one: what follows a colon in its last part.
  repository=${name%@*}
  [[ ${repository##*/} != *:* ]] || repository=${repository%:*}
  log "fetching what containerd lacks of the sandbox image $name, recorded without it, as 9.0.0 let a prune delete it while pods ran on it"
  images_ctr content fetch --hosts-dir "$CONTAINERD_CONFIG/certs.d" "$repository@$digest" >/dev/null ||
    fail "what containerd lacks of the sandbox image $name could not be fetched from its registry"
  checked=$(sandbox_image_checked "$name") || fail "containerd's record of the sandbox image $name could not be checked"
  [ "${checked#* }" = complete ] || fail "containerd lacks content of the sandbox image $name still, fetched"
}
# END the sandbox image.

# BEGIN alasio stopped.
#
# This release's sandbox gateway refuses the caller tokens 9.0.0's alasio mints, good for six
# hours, and answers a bind with 200 and the binding, where 9.0.0's alasio takes 204 alone;
# and the upgrade rolls the two apart: a turn 9.0.0's alasio ran as the gateway rolled would
# lose its workspace's bayma for as long as that alasio ran on. So, past the point of no
# return, before the upgrade, alasio is waited for until it runs no turn, as its busy label
# says, then stopped, so none starts until the upgrade starts this release's. A turn that
# starts between the last look and the stop ends with alasio, as any restart of alasio ends
# one. Once, as OUT/alasio-stopped says it was, the pod gone: a run again after the upgrade
# failed finds alasio the release's, or stopped still. Each wait is bounded, by
# TURN_WAIT_SECONDS (30 min) and STOP_WAIT_SECONDS (10 min), looking every POLL_SECONDS (5).

NS=alasio
TURN_WAIT_SECONDS=${TURN_WAIT_SECONDS:-1800}
STOP_WAIT_SECONDS=${STOP_WAIT_SECONDS:-600}
POLL_SECONDS=${POLL_SECONDS:-5}
k() { kubectl -n "$NS" "$@"; }
# Whether alasio's namespace has no pod of the selector `$1` within `$2` seconds, looking every
# POLL_SECONDS; the run failing where its pods cannot be listed, as nothing can be told then.
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
alasio_stopped_past() {
  [ ! -e "$OUT/alasio-stopped" ] || return 0
  log "waiting for alasio to run no turn, then stopping it, so none runs on as the sandbox gateway rolls"
  gone app.kubernetes.io/component=alasio,alasio.dev/busy=true "$TURN_WAIT_SECONDS" ||
    fail "alasio ran a turn on for ${TURN_WAIT_SECONDS} s: let it end, and run the cut-over again"
  k scale deployment alasio --replicas=0 >/dev/null || fail "alasio could not be stopped"
  gone app.kubernetes.io/component=alasio "$STOP_WAIT_SECONDS" || fail "alasio did not stop within ${STOP_WAIT_SECONDS} s"
  date -u +%FT%TZ >"$OUT/alasio-stopped"
  log "alasio stopped, to be started again by the upgrade"
}
# END alasio stopped.

command -v kubectl >/dev/null || fail "kubectl is not on root's PATH"
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 9.0.0, recording in $OUT"
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 9.0.0 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 9.0.0 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
  sandbox_image_before
  log "checking that the upgrade can run (upgrade-check.log)"
  accepted || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  alasio_stopped_past
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
    fail "the upgrade failed (upgrade.log): alasio stays stopped until an upgrade gets as far as starting it; the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
