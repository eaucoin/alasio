#!/usr/bin/env bash
# An installation of alasio 8.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 8.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-8.0.0.sh
#
# CONFIG is the configuration's file, the owner's alasio/config.json, which UPGRADE upgrades
# by: this release has no agent-sandbox, and refuses a configuration that says how to run it,
# so its install.agentSandbox goes, before UPGRADE_CHECK reads it, a run that fails before
# the point of no return putting it back.
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
# 8.0.0's log, its StatefulSet, has its claims labelled with 8.0.0's version, which this release's
# do not carry, and Kubernetes changes no StatefulSet's claim templates: an upgrade's apply of it
# is refused. So, before the point of no return, it deletes the log's StatefulSet, its pod and
# claim kept running, which the upgrade makes anew and takes back; a failure before the point
# of no return makes it again as 8.0.0 had it.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, each with what makes again what it changed (restore), as a
# run that fails before it leaves the installation as it was; past it, before the upgrade;
# and once the upgrade is through, after it.
#
# A folder conversation's bayma, which 8.0.0 ran as an agent-sandbox Sandbox, is this
# release's sandbox gateway's folder instance, a pod of the gateway's own on the same state
# directory, which its next message makes. Past the point of no return, the cut-over stops
# agent-sandbox's controller, so no pod of a Sandbox's is made again, and alasio once no turn
# runs, so none starts; then each folder's bayma with ten minutes to save its REPL sessions
# whole into its state directory, which it leaves in place for the folder instance to restore
# them from, saying which it saved; then every Sandbox. A machine with a reboot pending is
# refused before anything changes: a reboot between the stop and the folder instance's start
# would refuse every save, its REPLs coming back from their checkpoints alone. Once the
# upgrade is through, what this release has no use for of 8.0.0's goes, its prune having
# deleted most of it already: agent-sandbox's controller and its CRD, alasio's templates of
# its Sandboxes, its role in the folders' namespace, its admission policies of Sandboxes and
# their Secrets, and the network policy that let it reach the folders' bayma.
#
# Run again, it goes on from where it stopped: a failure before the point of no return makes
# again what the steps changed, and a run again starts over. Once past it
# (OUT/point-of-no-return), a run again goes on from there, and once the upgrade went through
# (OUT/upgraded), with what follows it alone.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${CONFIG:?the file of the configuration}" "${OUT:?where to record}"
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
NS=alasio
LOG=alasio-log
k() { kubectl -n "$NS" "$@"; }
# What says this machine has a reboot pending, as Debian's and Ubuntu's packages leave it.
REBOOT_REQUIRED=${REBOOT_REQUIRED:-/run/reboot-required}
SANDBOXES=sandboxes.agents.x-k8s.io
CONTROLLER=alasio-agent-sandbox-controller
# Whether alasio's namespace has no pod of the selector `$1` within `$2` seconds, looking every 5.
gone() {
  local waited=0
  while [ -n "$(k get pod -l "$1" -o name)" ]; do
    [ "$waited" -lt "$2" ] || return 1
    sleep 5
    waited=$((waited + 5))
  done
}
# Makes again what the steps before the point of no return changed, as a run that fails
# before it leaves the installation as it was: each step's own making again, here. The log's
# StatefulSet, as 8.0.0 had it, from OUT, where it is deleted: it takes its pod back. The
# configuration's install.agentSandbox, from OUT, where it is removed.
restore() {
  local made
  made=$(k get statefulset "$LOG" --ignore-not-found -o name)
  if [ -e "$OUT/log-statefulset.json" ] && [ -z "$made" ]; then
    log "making the log's StatefulSet again, as 8.0.0 had it"
    jq 'del(.metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp, .metadata.generation, .metadata.managedFields, .status)' "$OUT/log-statefulset.json" | k create -f - >/dev/null ||
      log "FAILED: the log's StatefulSet is deleted, its pod running unowned, and could not be made again from $OUT/log-statefulset.json: the cut-over run again goes on, the upgrade making it anew; to run 8.0.0 as it was instead, make it from there (kubectl create -f), its metadata.uid and metadata.resourceVersion deleted"
  fi
  if [ -e "$OUT/config.json" ] && ! jq -e '.install | has("agentSandbox")' "$CONFIG" >/dev/null 2>&1; then
    log "putting install.agentSandbox back in $CONFIG, as 8.0.0 had it"
    # Written over, not replaced, so the file keeps its owner and mode.
    cat "$OUT/config.json" >"$CONFIG" || log "FAILED: $CONFIG could not be written as it was: it is in $OUT/config.json"
  fi
}

for tool in jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
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
  [ ! -e "$REBOOT_REQUIRED" ] ||
    fail "this machine has a reboot pending ($REBOOT_REQUIRED): reboot it first, then cut over, as a reboot before the folders' bayma starts again refuses what it saved of their REPLs"
  # The configuration, of no agent-sandbox, as this release reads it.
  if jq -e '.install | has("agentSandbox")' "$CONFIG" >/dev/null; then
    cp -p "$CONFIG" "$OUT/config.json" || fail "$CONFIG could not be kept in $OUT"
    log "removing install.agentSandbox from $CONFIG, as this release runs no agent-sandbox (kept in $OUT/config.json)"
    # Written over, not replaced, so the file keeps its owner and mode.
    jq 'del(.install.agentSandbox)' "$OUT/config.json" >"$OUT/config.next.json" && cat "$OUT/config.next.json" >"$CONFIG" || fail "$CONFIG could not be written"
  fi
  # The log's StatefulSet, where its claims carry a version, deleted, its pod and claim kept.
  versioned=$(k get statefulset "$LOG" --ignore-not-found -o json | jq -r '[.spec.volumeClaimTemplates[]?.metadata.labels["app.kubernetes.io/version"] // empty] | first // empty') ||
    fail "the log's StatefulSet could not be read"
  if [ -n "$versioned" ]; then
    k get statefulset "$LOG" -o json >"$OUT/log-statefulset.json" || fail "the log's StatefulSet could not be kept in $OUT"
    log "deleting the log's StatefulSet, its pod and claim kept, for the upgrade to make it anew without $versioned on its claims (kept in $OUT/log-statefulset.json)"
    k delete statefulset "$LOG" --cascade=orphan --wait=true >/dev/null || fail "the log's StatefulSet could not be deleted"
  fi
  log "checking that the upgrade can run (upgrade-check.log)"
  bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1 || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  trap - EXIT
  log "past the point of no return: a run again goes on from here"
fi

if [ ! -e "$OUT/upgraded" ]; then
  [ "${FAIL_AT:-}" != past-the-point-of-no-return ] || fail "a failure injected past the point of no return, before the upgrade"
  if [ -n "$(kubectl get crd "$SANDBOXES" --ignore-not-found -o name)" ]; then
    log "stopping agent-sandbox's controller, so no Sandbox's pod is made again"
    k scale deployment "$CONTROLLER" --replicas=0 >/dev/null || fail "agent-sandbox's controller could not be stopped"
    gone app.kubernetes.io/name=agent-sandbox-controller 300 || fail "agent-sandbox's controller did not stop within 5m"
    log "waiting for alasio to run no turn, then stopping it, so none starts until the upgrade starts it again"
    gone app.kubernetes.io/component=alasio,alasio.dev/busy=true 1800 || fail "alasio ran a turn for 30m on: let it end, and run the cut-over again"
    k scale deployment alasio --replicas=0 >/dev/null || fail "alasio could not be stopped"
    gone app.kubernetes.io/component=alasio 600 || fail "alasio did not stop within 10m"
    # Each folder's bayma, by its pod's name and its state directory, which stays where it is.
    kubectl get pod -A -l alasio.dev/workload=folder -o json |
      jq -r '.items[] | [.metadata.namespace, .metadata.name, ((.spec.containers[] | select(.name == "bayma") | .args) as $args | $args[($args | index("--state-dir")) + 1])] | @tsv' >"$OUT/folders.tsv" ||
      fail "the folders' bayma could not be listed"
    while IFS="$(printf '\t')" read -r namespace pod state; do
      log "stopping the folder's bayma $namespace/$pod, given 10m to save its REPL sessions whole into $state, which stays for the gateway's folder instance"
      kubectl -n "$namespace" delete pod "$pod" --grace-period=600 --wait=true --timeout=11m >/dev/null || fail "the folder's bayma $namespace/$pod did not stop within 11m"
      saved=$({ ls -1 "$state/snapshots" 2>/dev/null || true; } | paste -sd ' ' -)
      if [ -n "$saved" ]; then
        log "the folder's bayma $namespace/$pod saved the sessions $saved whole; any other comes back from its checkpoint"
      else
        log "the folder's bayma $namespace/$pod saved no session whole: each comes back from its checkpoint"
      fi
    done <"$OUT/folders.tsv"
    log "deleting every Sandbox, which no controller finalizes now"
    kubectl get "$SANDBOXES" -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\n"}{end}' >"$OUT/sandboxes.tsv" || fail "the Sandboxes could not be listed"
    while IFS="$(printf '\t')" read -r namespace name; do
      kubectl -n "$namespace" patch "$SANDBOXES" "$name" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null &&
        kubectl -n "$namespace" delete "$SANDBOXES" "$name" --ignore-not-found --wait=true >/dev/null || fail "the Sandbox $namespace/$name could not be deleted"
    done <"$OUT/sandboxes.tsv"
  fi
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" || fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
[ "${FAIL_AT:-}" != after-the-upgrade ] || fail "a failure injected once the upgrade was through"

log "upgraded at $(cat "$OUT/upgraded")"
host=$(jq -r '.install.host.namespace // "alasio-host"' "$CONFIG")
log "removing what of 8.0.0's this release has no use for: agent-sandbox, alasio's templates and policies of Sandboxes, and its role and network policy in $host"
kubectl delete crd "$SANDBOXES" --ignore-not-found --wait=true >/dev/null || fail "the Sandboxes' CRD could not be deleted"
k delete deployment,serviceaccount,role,rolebinding "$CONTROLLER" --ignore-not-found >/dev/null &&
  kubectl delete clusterrole,clusterrolebinding "$CONTROLLER" --ignore-not-found >/dev/null || fail "agent-sandbox's controller could not be deleted"
k delete configmap alasio-sandbox-templates --ignore-not-found >/dev/null || fail "alasio's templates could not be deleted"
kubectl -n "$host" delete role,rolebinding alasio --ignore-not-found >/dev/null &&
  kubectl -n "$host" delete networkpolicy bayma-from-alasio --ignore-not-found >/dev/null || fail "alasio's role and network policy in $host could not be deleted"
kubectl delete validatingadmissionpolicy,validatingadmissionpolicybinding alasio-sandboxes alasio-secrets --ignore-not-found >/dev/null || fail "alasio's policies of Sandboxes could not be deleted"
log "through"
