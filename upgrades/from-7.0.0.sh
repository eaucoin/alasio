#!/usr/bin/env bash
# An installation of alasio 7.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 7.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-7.0.0.sh
#   upgrades/from-7.0.0.sh release-settings <file>
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
# does as root through. CONFIG is that configuration's file
# (/home/<owner>/.config/alasio/config.json), which the cut-over changes in place before
# UPGRADE_CHECK reads it. FAIL_AT=before-the-point-of-no-return fails the run there, once every
# step before the upgrade has run, FAIL_AT=after-the-storage-controller-stopped past it, with
# the storage controller stopped for node 1 to be registered anew, and
# FAIL_AT=after-the-pageserver-registered-anew once it is, as the upgrade shard rehearses a
# failure at each.
#
# It upgrades main alone, as alasio upgrade does, which refuses while a branch environment
# 7.0.0 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# Each pageserver now renders its own identity as it starts, its id its pod's ordinal plus
# one, at a Service of its pod's own name, where 7.0.0's setup rendered the one pageserver's,
# id 1, at alasio-neon-pageserver; and the pageservers' StatefulSet starts its pods at once,
# a field Kubernetes changes of no StatefulSet. So, before the point of no return, it checks
# that the storage controller has node 1 registered as 7.0.0's setup named it and that the
# one pageserver is pod 0, and deletes 7.0.0's StatefulSet, its pod and claim kept running,
# which the upgrade makes anew and takes back; past it, it registers node 1 anew at the name
# of pod 0's Service, which it makes, with the storage controller stopped, then started again
# with the replicas it ran (kept in OUT), as the storage controller refuses a node that
# registers again under its id at another address (409), and the pageserver then waits at its
# start for ever; and it keeps 7.0.0's Service of the pageservers, which main's compute reads
# pages at, through the upgrade, until neon-control has given the compute pod 0's.
#
# Run again, it goes on from where it stopped: what each step reads is kept in OUT and read
# again from there, and a failure before the point of no return makes again what the steps
# deleted. Once past it (OUT/point-of-no-return), a run again goes on from there.
set -euo pipefail
umask 077
# The configuration $1 in this release's settings, on stdout: the collector's resources gone,
# as this release has its gateway's and its scraper's. What the cut-over gives the configuration
# before UPGRADE_CHECK reads it; and what `from-7.0.0.sh release-settings <file>` says alone, as
# the upgrade shard gives a copy of the configuration it.
release_settings() {
  jq 'del(.install.telemetry.collector.resources)' "$1"
}
if [ "${1:-}" = release-settings ]; then
  release_settings "${2:?the configuration file to give them}"
  exit 0
fi
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${CONFIG:?the configuration UPGRADE reads}" "${OUT:?where to record}"
NS=alasio
PAGESERVERS=alasio-neon-pageserver
CONTROLLER=alasio-neon-storage-controller
# Node 1, as 7.0.0's setup registered it, and as pod 0 registers itself now: its HTTP and
# Postgres addresses and ports, and its zone, which the storage controller matches a node by.
REGISTERED="alasio-neon-pageserver 9898 alasio-neon-pageserver 6400 az-pageserver"
POD0=alasio-neon-pageserver-0
REGISTERED_ANEW="$POD0 9898 $POD0 6400 az-pageserver"
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
# Runs the SQL $1 in the storage controller's database: what it printed.
on_controller_db() { k exec statefulset/alasio-neon-controller-db -- psql -U storage_controller -d storage_controller -v ON_ERROR_STOP=1 -AtF ' ' -c "$1"; }
# Fails, where on_controller_db could not read what the storage controller registered: never
# taken for node 1 registered either way.
unanswered() { fail "the storage controller's database did not answer (kubectl -n $NS get pod alasio-neon-controller-db-0 says why), so what it registers cannot be read"; }
NODE1="select listen_http_addr, listen_http_port, listen_pg_addr, listen_pg_port, availability_zone_id from nodes where node_id = 1"
# Makes the pageservers' StatefulSet again as 7.0.0 had it, from OUT, where a failure before
# the point of no return left it deleted: it takes its pod back.
restore() {
  local made
  made=$(k get statefulset "$PAGESERVERS" --ignore-not-found -o name)
  [ -e "$OUT/pageserver-statefulset.json" ] && [ -z "$made" ] || return 0
  log "making the pageservers' StatefulSet again, as 7.0.0 had it"
  jq 'del(.metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp, .metadata.generation, .metadata.managedFields, .status)' "$OUT/pageserver-statefulset.json" | k create -f - >/dev/null ||
    log "FAILED: the pageservers' StatefulSet is deleted, its pod running unowned, and could not be made again from $OUT/pageserver-statefulset.json: the cut-over run again goes on, the upgrade making it anew; to run 7.0.0 as it was instead, make it from there (kubectl create -f), its metadata.uid and metadata.resourceVersion deleted"
}

for tool in jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
[ -f "$CONFIG" ] && jq -e '.install | type == "object"' "$CONFIG" >/dev/null 2>&1 || fail "CONFIG, $CONFIG, is not alasio's configuration"

if [ ! -e "$OUT/point-of-no-return" ]; then
  # The configuration as 7.0.0 reads it, kept in OUT the first time: what the changes below are
  # made from, so a run again makes the same, and what a failure before the point of no return
  # puts back, as 7.0.0 reads none of them.
  [ -e "$OUT/config.json" ] || cp -p "$CONFIG" "$OUT/config.json"
  trap 'status=$?; [ "$status" -eq 0 ] || [ -e "$OUT/point-of-no-return" ] || { cat "$OUT/config.json" >"$CONFIG"; restore; }' EXIT
  log "checking that the storage controller has node 1 registered as 7.0.0's setup named it"
  [ -e "$OUT/pageserver-registered" ] || { on_controller_db "$NODE1" >"$OUT/pageserver-registered.part" || unanswered; mv "$OUT/pageserver-registered.part" "$OUT/pageserver-registered"; }
  [ "$(cat "$OUT/pageserver-registered")" = "$REGISTERED" ] ||
    fail "the storage controller has node 1 as \"$(cat "$OUT/pageserver-registered")\", not as 7.0.0's setup registered it, \"$REGISTERED\""
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 7.0.0 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  branches=$(kubectl get namespace -l alasio.dev/branch -o json | jq -r '.items[] | .metadata.labels["alasio.dev/branch"] as $branch | select(.metadata.name == "alasio-branch-\($branch)") | $branch')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 7.0.0 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
  statefulset=$(k get statefulset "$PAGESERVERS" --ignore-not-found -o name)
  if [ -n "$statefulset" ]; then
    pods=$(k get pod -l app.kubernetes.io/component=neon-pageserver -o jsonpath='{.items[*].metadata.name}')
    [ "$pods" = "$POD0" ] || fail "the pageserver's pods are \"$pods\", where node 1 is $POD0 alone"
    [ -e "$OUT/pageserver-statefulset.json" ] || { k get statefulset "$PAGESERVERS" -o json >"$OUT/pageserver-statefulset.json.part" && mv "$OUT/pageserver-statefulset.json.part" "$OUT/pageserver-statefulset.json"; }
    policy=$(k get statefulset "$PAGESERVERS" -o jsonpath='{.spec.podManagementPolicy}')
    if [ "$policy" = OrderedReady ]; then
      log "deleting the pageservers' StatefulSet, its pod and claim kept, for the upgrade to make it anew (kept in $OUT/pageserver-statefulset.json)"
      k delete statefulset "$PAGESERVERS" --cascade=orphan >/dev/null
    fi
  fi
  # The configuration in this release's settings, rewritten whole, its owner and mode kept,
  # before UPGRADE_CHECK reads it.
  log "giving $CONFIG this release's settings"
  release_settings "$OUT/config.json" >"$CONFIG.part"
  chown --reference="$CONFIG" "$CONFIG.part"
  chmod --reference="$CONFIG" "$CONFIG.part"
  mv "$CONFIG.part" "$CONFIG"
  bash -c "$UPGRADE_CHECK" >"$OUT/upgrade-check.log" 2>&1 || fail "UPGRADE_CHECK failed, so UPGRADE could not run (upgrade-check.log): $(tail -n 3 "$OUT/upgrade-check.log")"
  [ "${FAIL_AT:-}" != before-the-point-of-no-return ] || fail "a failure injected before the point of no return"
  date -u +%FT%TZ >"$OUT/point-of-no-return"
  trap - EXIT
fi

# Pod 0's Service, as this release makes it, which the storage controller reaches the
# pageserver at once node 1 is registered anew, before the upgrade restarts it there.
service=$(k get service "$POD0" --ignore-not-found -o name)
if [ -z "$service" ]; then
  log "making $POD0's Service"
  k get service "$PAGESERVERS" -o json | jq --arg pod "$POD0" '{
    apiVersion, kind, metadata: {name: $pod, namespace: .metadata.namespace, labels: .metadata.labels},
    spec: {selector: (.spec.selector + {"statefulset.kubernetes.io/pod-name": $pod}), publishNotReadyAddresses: true, ports: [.spec.ports[] | {name, port}]}
  }' | k create -f - >/dev/null
fi
registered=$(on_controller_db "$NODE1") || unanswered
if [ "$registered" = "$REGISTERED" ]; then
  log "registering node 1 anew at $POD0, with the storage controller stopped (as it was: $OUT/pageserver-registered)"
  [ -e "$OUT/controller-replicas" ] || { k get deploy "$CONTROLLER" -o jsonpath='{.spec.replicas}' >"$OUT/controller-replicas.part" && mv "$OUT/controller-replicas.part" "$OUT/controller-replicas"; }
  selector=$(k get deploy "$CONTROLLER" -o json | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')
  k scale deploy/"$CONTROLLER" --replicas=0 >/dev/null
  gone() {
    local left
    left=$(k get pod -l "$selector" -o name) && [ -z "$left" ]
  }
  wait_for 300 "the storage controller's pods to be gone" gone
  [ "${FAIL_AT:-}" != after-the-storage-controller-stopped ] || fail "a failure injected once the storage controller was stopped"
  on_controller_db "update nodes set listen_http_addr = '$POD0', listen_pg_addr = '$POD0' where node_id = 1" >/dev/null
fi
# Started again, whether this run stopped it or one before did, with the replicas it ran.
if [ -e "$OUT/controller-replicas" ] && [ "$(k get deploy "$CONTROLLER" -o jsonpath='{.spec.replicas}')" = 0 ]; then
  log "starting the storage controller again, with the replicas it ran ($OUT/controller-replicas)"
  k scale deploy/"$CONTROLLER" --replicas="$(cat "$OUT/controller-replicas")" >/dev/null
  k rollout status deploy/"$CONTROLLER" --timeout=300s >/dev/null || fail "the storage controller did not start again"
fi
registered=$(on_controller_db "$NODE1") || unanswered
[ "$registered" = "$REGISTERED_ANEW" ] || fail "the storage controller has node 1 as \"$registered\", not registered anew at $POD0, \"$REGISTERED_ANEW\""
[ "${FAIL_AT:-}" != after-the-pageserver-registered-anew ] || fail "a failure injected once node 1 was registered anew"
# 7.0.0's Service of the pageservers, which main's compute reads pages at until neon-control
# gives it pod 0's: left out of the installation, as the upgrade's prune would delete it as soon
# as it has applied this release, before neon-control runs, and deleted once it has.
installation=$(k get service "$PAGESERVERS" --ignore-not-found -o jsonpath='{.metadata.labels.alasio\.dev/installation}')
if [ -n "$installation" ]; then
  log "keeping 7.0.0's pageservers' Service through the upgrade, until main's compute is given pod 0's"
  k label service "$PAGESERVERS" alasio.dev/installation- >/dev/null
fi

log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
  fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it. To run 7.0.0 as it was instead: write $OUT/config.json back to $CONFIG; stop the storage controller (kubectl -n $NS scale deploy/$CONTROLLER --replicas=0), register node 1 back as $OUT/pageserver-registered has it (update nodes set listen_http_addr = 'alasio-neon-pageserver', listen_pg_addr = 'alasio-neon-pageserver' where node_id = 1, in its database), and start it again with the replicas it ran (kubectl -n $NS scale deploy/$CONTROLLER --replicas=$(cat "$OUT/controller-replicas")); where the pageservers' StatefulSet is made anew already (kubectl -n $NS get statefulset $PAGESERVERS finds it), delete it, its pod kept (kubectl -n $NS delete statefulset $PAGESERVERS --cascade=orphan); run 7.0.0's alasio up, which makes it again and takes the pod back; then restart main's compute (kubectl -n $NS rollout restart deploy/alasio-neon-compute), for it to reach the pageserver at 7.0.0's address again"
# Given pod 0's, or asleep, as it then fetches its spec, with it, as it wakes.
given() {
  local replicas layout
  replicas=$(k get deploy alasio-neon-compute -o jsonpath='{.spec.replicas}') || return 1
  [ "$replicas" != 0 ] || return 0
  layout=$(on_controller_db "select layout from neon_control.compute_layouts where id = 'alasio'") || return 1
  [[ $layout == *"$POD0"* ]]
}
kept=$(k get service "$PAGESERVERS" --ignore-not-found -o name)
if [ -n "$kept" ]; then
  wait_for 600 "neon-control to give main's compute pod 0's address" given
  log "deleting 7.0.0's pageservers' Service, main's compute given pod 0's"
  k delete service "$PAGESERVERS" >/dev/null
fi
kept=$(k get service "$PAGESERVERS" --ignore-not-found -o name)
[ -z "$kept" ] || fail "7.0.0's pageservers' Service is there still, where nothing is to read pages at it now: delete it (kubectl -n $NS delete service $PAGESERVERS)"
log "upgraded"
