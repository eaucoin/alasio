#!/usr/bin/env bash
# An installation of alasio 7.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 7.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> OUT=<dir> [FAIL_AT=<point>] upgrades/from-7.0.0.sh
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
# step before the upgrade has run, FAIL_AT=after-the-storage-controller-stopped past it, with
# the storage controller stopped for node 1 to be registered anew, and
# FAIL_AT=after-the-pageserver-registered-anew once it is, and
# FAIL_AT=midway-through-the-old-snapshots-deleted once the upgrade is through, as the upgrade
# shard rehearses a failure at each. MOVE_BACK=1, with the OUT of a run that moved the sessions'
# snapshots and did not finish the upgrade, binds each session back to its snapshot of 7.0.0's
# layout, for a return to 7.0.0, and does nothing else.
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
# Past it too, the sessions' snapshots are moved to this release's layout, alasio stopped
# meanwhile, which the upgrade starts again (the step's own comment says how).
#
# Run again, it goes on from where it stopped: what each step reads is kept in OUT and read
# again from there, and a failure before the point of no return makes again what the steps
# deleted. Once past it (OUT/point-of-no-return), a run again goes on from there.
set -euo pipefail
umask 077
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${OUT:?where to record}"
NS=alasio
RELEASE=alasio
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

# snapshots MODE runs the moves, the deletes or the moving back in a node agent, which reaches
# the bucket and holds its credentials, by 7.0.0's client of it or this release's, as runs:
# what it prints, the old id and the new of each workspace's snapshot, a line each.
snapshots() {
  local agent
  agent=$(k get pod -l app.kubernetes.io/component=node-agent --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}')
  agent=${agent%% *}
  [ -n "$agent" ] || fail "no node agent runs to reach the snapshots bucket with"
  k exec -i "$agent" -c node-agent -- env MODE="$1" ENVIRONMENTS="$environments" FAIL_AT="${FAIL_AT:-}" MOVED="$(cat "$OUT/snapshots-moved" 2>/dev/null)" node --input-type=module - <<'JS'
import { createHash } from "node:crypto";
import { request } from "node:http";
import { isDeepStrictEqual } from "node:util";
import { Effect } from "effect";
import { makeS3Bucket, sigV4Authorization } from "/opt/alasio/src/node-agent/s3.ts";

const required = (name) => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
};
const options = {
  endpoint: required("S3_ENDPOINT"),
  region: required("S3_REGION"),
  bucket: required("S3_BUCKET"),
  accessKeyId: required("AWS_ACCESS_KEY_ID"),
  secretAccessKey: required("AWS_SECRET_ACCESS_KEY"),
};
const bucket = makeS3Bucket(options);
const run = (effect) => Effect.runPromise(effect);
const endpoint = new URL(options.endpoint);
const encoded = (value) => encodeURIComponent(value).replace(/[!'()*]/gu, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
// The object's size and ETag, as the bucket holds it: 7.0.0's client asks no HEAD.
const head = (key) =>
  new Promise((resolve, reject) => {
    const path = `${endpoint.pathname.replace(/\/+$/u, "")}/${encoded(options.bucket)}/${key.split("/").map(encoded).join("/")}`;
    const payloadHash = createHash("sha256").update("").digest("hex");
    const headers = { host: endpoint.host, "x-amz-content-sha256": payloadHash, "x-amz-date": new Date().toISOString().replace(/[-:]|\.\d{3}/gu, "") };
    const authorization = sigV4Authorization({ method: "HEAD", path, query: {}, headers, payloadHash }, { region: options.region, service: "s3" }, options);
    const asked = request({ method: "HEAD", host: endpoint.hostname, port: endpoint.port, path, headers: { ...headers, authorization } }, (response) => {
      response.resume();
      if (response.statusCode === 200) resolve({ bytes: Number(response.headers["content-length"]), etag: String(response.headers.etag) });
      else reject(new Error(`the bucket answered ${response.statusCode} to a HEAD of ${key}`));
    });
    asked.setTimeout(60_000, () => asked.destroy(new Error(`no answer to a HEAD of ${key} within 60 s`)));
    asked.once("error", reject);
    asked.end();
  });
// 7.0.0's id, to the second, as this release's, to the millisecond it was taken at; and back.
const anew = (snapshot, takenAt) => `${snapshot.slice(0, 15)}${String(Date.parse(takenAt) % 1000).padStart(3, "0")}${snapshot.slice(15)}`;
const before = (snapshot) => `${snapshot.slice(0, 15)}${snapshot.slice(18)}`;
// What 7.0.0 kept of a workspace: all but this release's layers, manifests and current.json.
const ofOld = (prefix) => (key) => key.startsWith(`${prefix}/`) && key !== `${prefix}/current.json` && !key.startsWith(`${prefix}/layers/`) && !key.startsWith(`${prefix}/manifests/`);
const written = async (key, value) => {
  await run(bucket.putJson(key, value));
  if (!isDeepStrictEqual(await run(bucket.getJson(key)), JSON.parse(JSON.stringify(value)))) throw new Error(`${key} does not read back as it was written`);
};
// The keys under `prefix`: 7.0.0's client lists them, this release's the objects of them.
const keysUnder = async (prefix) => (await run(bucket.list(prefix))).map((each) => (typeof each === "string" ? each : each.key));
const moved = (process.env.MOVED ?? "").split("\n").filter(Boolean).map((line) => line.split(" "));

const mode = required("MODE");
if (mode === "move") {
  for (const environment of required("ENVIRONMENTS").split(" ")) {
    const keys = await keysUnder(`${environment}/`);
    for (const workspace of [...new Set(keys.map((key) => key.split("/")[1]))]) {
      const prefix = `${environment}/${workspace}`;
      const current = await run(bucket.getJson(`${prefix}/current.json`));
      if (current && Array.isArray(current.layers)) {
        process.stdout.write(`${environment} ${workspace} ${before(current.snapshot)} ${current.snapshot}\n`);
      } else if (current) {
        const { snapshot: was, image, files, environment: _environment, workspace: _workspace, ...rest } = current;
        const snapshot = anew(was, current.takenAt);
        const copied = [];
        for (const file of files) {
          const to = `${prefix}/layers/${image}/${file.path}`;
          await run(bucket.copy(`${prefix}/${was}/${file.path}`, to, file.bytes));
          const stored = await head(to);
          if (stored.bytes !== file.bytes) throw new Error(`${to} is ${stored.bytes} bytes in the bucket, where the manifest of ${was} says ${file.bytes}`);
          copied.push({ ...file, etag: stored.etag });
        }
        const manifest = { environment, workspace, snapshot, ...rest, layers: [{ digest: image, files: copied }] };
        await written(`${prefix}/manifests/${snapshot}.json`, manifest);
        await written(`${prefix}/current.json`, manifest);
        process.stdout.write(`${environment} ${workspace} ${was} ${snapshot}\n`);
      }
    }
  }
} else if (mode === "delete") {
  let deleted = 0;
  for (const environment of required("ENVIRONMENTS").split(" ")) {
    const keys = await keysUnder(`${environment}/`);
    for (const workspace of [...new Set(keys.map((key) => key.split("/")[1]))]) {
      for (const key of keys.filter(ofOld(`${environment}/${workspace}`))) {
        await run(bucket.remove(key));
        deleted += 1;
        if (process.env.FAIL_AT === "midway-through-the-old-snapshots-deleted") throw new Error("a failure injected midway through the deletion of what 7.0.0 kept of the snapshots");
      }
    }
  }
  process.stderr.write(`deleted ${deleted} objects 7.0.0 kept of the sessions' snapshots\n`);
} else if (mode === "back") {
  for (const [environment, workspace, was, snapshot] of moved) {
    const prefix = `${environment}/${workspace}`;
    const kept = await run(bucket.getJson(`${prefix}/${was}/manifest.json`));
    if (!kept) throw new Error(`${prefix}/${was}/manifest.json is gone: 7.0.0's layout of the snapshot is deleted already`);
    await written(`${prefix}/current.json`, kept);
    process.stdout.write(`${environment} ${workspace} ${snapshot} ${was}\n`);
  }
} else {
  throw new Error(`no mode ${mode}`);
}
JS
}
# Binds each Sandbox released into the snapshot whose id `snapshots` printed first on a line to
# the one it printed second.
rebind() {
  local environment workspace from to bound
  while read -r environment workspace from to; do
    bound=$(kubectl -n "$environment" get sandboxes.agents.x-k8s.io "$workspace" --ignore-not-found -o jsonpath='{.metadata.annotations.alasio\.dev/snapshot}')
    if [ "$bound" = "$from" ]; then
      log "binding the Sandbox $environment/$workspace to its snapshot $to, which was $from"
      kubectl -n "$environment" annotate sandboxes.agents.x-k8s.io "$workspace" alasio.dev/snapshot="$to" --overwrite >/dev/null
    fi
  done
}
environments=$(kubectl get sandboxes.agents.x-k8s.io -A -l alasio.dev/workload=session -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' | sort -u | paste -sd ' ')
if [ "${MOVE_BACK:-}" = 1 ]; then
  [ -e "$OUT/snapshots-moved" ] || fail "no snapshot was moved ($OUT/snapshots-moved is not there), so none is moved back"
  [ ! -e "$OUT/snapshots-deleting" ] || fail "what 7.0.0 kept of the snapshots is being deleted already, the upgrade through: none can be moved back, and the run again goes on with the deletion"
  log "binding each session back to its snapshot of 7.0.0's layout, for 7.0.0 to restore"
  snapshots back >"$OUT/snapshots-moved-back.part" || fail "the snapshots could not all be moved back: run this again"
  mv "$OUT/snapshots-moved-back.part" "$OUT/snapshots-moved-back"
  rebind <"$OUT/snapshots-moved-back"
  rm -f "$OUT/caches-emptied"
  log "the sessions are bound back to their snapshots of 7.0.0's layout"
  exit 0
fi
if [ ! -e "$OUT/point-of-no-return" ]; then
  trap 'status=$?; [ "$status" -eq 0 ] || [ -e "$OUT/point-of-no-return" ] || restore' EXIT
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

# The sessions' snapshots: 7.0.0 kept a workspace's one snapshot in the snapshots bucket as
# <environment>/<workspace>/<snapshot>/, its manifest beside its files and current.json, by an
# id to the second; this release keeps each image once, by its digest, under layers/, each
# manifest under manifests/, and names snapshots to the millisecond. alasio, which restores and
# releases sessions, is stopped first, and is down until the upgrade starts it again, so that
# none is restored or released meanwhile, as one would fall back from a snapshot it could not
# read. Then each workspace's current snapshot is moved (snapshots move): its files copied
# under layers/<image>/, each looked at then, its manifest written anew under its new id, then
# its current.json, both read back; the old ids and the new kept in OUT/snapshots-moved; each
# Sandbox released into one bound to its new id; the nodes' caches, of 7.0.0's layout,
# emptied. What 7.0.0 kept is held in the bucket until the upgrade is through, then deleted
# (snapshots delete), so that a return to 7.0.0 before then, MOVE_BACK=1, binds each session
# back to its snapshot of 7.0.0's layout (snapshots back).
if [ ! -e "$OUT/snapshots-moved" ] && [ -n "$environments" ]; then
  replicas=$(k get deploy "$RELEASE" -o jsonpath='{.spec.replicas}')
  if [ "$replicas" != 0 ]; then
    log "stopping alasio, down from now until the upgrade starts it again, so that no session is restored or released as the snapshots move"
    k scale deploy/"$RELEASE" --replicas=0 >/dev/null
  fi
  stopped() {
    local left
    left=$(k get pod -l app.kubernetes.io/component=alasio -o name) && [ -z "$left" ]
  }
  wait_for 300 "alasio's pods to be gone" stopped
  log "moving the snapshots of the sessions of $environments to this release's layout, what 7.0.0 kept held until the upgrade is through"
  snapshots move >"$OUT/snapshots-moved.part" || fail "the snapshots could not all be moved, alasio down meanwhile: the run again goes on with them"
  mv "$OUT/snapshots-moved.part" "$OUT/snapshots-moved"
fi
if [ -e "$OUT/snapshots-moved" ] && [ ! -e "$OUT/caches-emptied" ]; then
  rebind <"$OUT/snapshots-moved"
  for agent in $(k get pod -l app.kubernetes.io/component=node-agent -o jsonpath='{.items[*].metadata.name}'); do
    log "emptying the snapshot cache of $agent, of 7.0.0's layout"
    k exec "$agent" -c node-agent -- sh -c 'rm -rf /var/lib/alasio/snapshots/* /var/lib/alasio/snapshots/.[!.]*' || fail "the snapshot cache of $agent could not be emptied"
  done
  touch "$OUT/caches-emptied"
fi

# 7.0.0's sessions' profile, which this release's alasio up carries as the template before
# its own, for the sessions' snapshots taken on it to restore onto it, as each moved does:
# this release reads a profile only where it says how often a session is snapshotted live,
# which 7.0.0's does not, so it is given this release's default, 10 minutes, which 7.0.0's
# alasio, reading it again, ignores.
templates=$(k get configmap alasio-sandbox-templates --ignore-not-found -o json)
lacking='.data["templates.json"] | fromjson | any((.sessions, .previousSessions) | select(. != null and .snapshots != null) | .snapshots; has("liveEverySeconds") | not)'
if [ -n "$templates" ] && [ "$(jq "$lacking" <<<"$templates")" = true ]; then
  log "giving 7.0.0's sessions' profile in its sandbox templates how often a session is snapshotted live, for this release's alasio up to carry it"
  jq '.data["templates.json"] |= (fromjson | reduce ("sessions", "previousSessions") as $profile (.; if .[$profile].snapshots? != null and (.[$profile].snapshots | has("liveEverySeconds") | not) then .[$profile].snapshots.liveEverySeconds = 600 else . end) | tojson)' <<<"$templates" | k replace -f - >/dev/null ||
    fail "7.0.0's sandbox templates could not be given how often a session is snapshotted live: the run again goes on with it"
fi

log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
  fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it. To run 7.0.0 as it was instead: stop the storage controller (kubectl -n $NS scale deploy/$CONTROLLER --replicas=0), register node 1 back as $OUT/pageserver-registered has it (update nodes set listen_http_addr = 'alasio-neon-pageserver', listen_pg_addr = 'alasio-neon-pageserver' where node_id = 1, in its database), and start it again with the replicas it ran (kubectl -n $NS scale deploy/$CONTROLLER --replicas=$(cat "$OUT/controller-replicas")); where the pageservers' StatefulSet is made anew already (kubectl -n $NS get statefulset $PAGESERVERS finds it), delete it, its pod kept (kubectl -n $NS delete statefulset $PAGESERVERS --cascade=orphan); run 7.0.0's alasio up, which makes it again and takes the pod back; then restart main's compute (kubectl -n $NS rollout restart deploy/alasio-neon-compute), for it to reach the pageserver at 7.0.0's address again. alasio is down meanwhile, stopped as the snapshots moved: 7.0.0's alasio up starts it again (or kubectl -n $NS scale deploy/$RELEASE --replicas=1); but first, as 7.0.0 reads none of the snapshots moved to this release's layout ($OUT/snapshots-moved), run this script again with MOVE_BACK=1 and OUT as now, which binds each session back to its snapshot of 7.0.0's layout, held until the upgrade is through: without, those sessions come back from bayma's checkpoints, their live state lost"
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
# What 7.0.0 kept of the snapshots moved, held until now that the upgrade is through.
if [ -e "$OUT/snapshots-moved" ] && [ ! -e "$OUT/snapshots-deleted" ]; then
  log "deleting what 7.0.0 kept of the sessions' snapshots, the upgrade through"
  touch "$OUT/snapshots-deleting"
  snapshots delete || fail "what 7.0.0 kept of the snapshots could not all be deleted: the run again goes on with it"
  touch "$OUT/snapshots-deleted"
fi
log "upgraded"
