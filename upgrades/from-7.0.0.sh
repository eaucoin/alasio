#!/usr/bin/env bash
# An installation of alasio 7.0.0 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 7.0.0 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] [PRIVATE_ADDRESS=allowed] upgrades/from-7.0.0.sh
#   upgrades/from-7.0.0.sh release-settings <file>
#   upgrades/from-7.0.0.sh private-address <address>
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
# failure at each. PRIVATE_ADDRESS=allowed lets Telegram be told a private address, as the
# upgrade shard's Telegram, a stand-in in the cluster, reaches one; `private-address` exits 0
# where the address is one the cut-over refuses as private, and 1 where not.
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
# Telegram delivers to the webhook intake now, at an address 7.0.0 has no setting of, to a bot
# this release admits its allowed users alone to. So, before the point of no return, it gives
# the configuration that address where it names none, this machine's on its route to the
# internet, saying which, and, where the bot allows no user, allows the one 7.0.0 let in as the
# first to write to it, refusing before it changes anything where it can find neither, or where
# the address is private, which Telegram cannot reach; once upgraded, with main's bot given its
# route, it deletes what 7.0.0 kept of polling Telegram.
#
# Past the point of no return, the sessions' snapshots are moved to this release's layout,
# alasio stopped meanwhile, which the upgrade starts again (the step's own comment says how).
#
# Run again, it goes on from where it stopped: what each step reads is kept in OUT and read
# again from there, and a failure before the point of no return makes again what the steps
# deleted. Once past it (OUT/point-of-no-return), a run again goes on from there, and once the
# upgrade went through (OUT/upgraded), with what follows it alone.
set -euo pipefail
umask 077
# This machine's address on its route to the internet, as alasio init offers it as the address
# Telegram delivers to; none where it has no such route.
routed() { ip -4 -o route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p'; }
# The address Telegram delivers to, as the configuration $1 names it, or this machine's on its
# route to the internet where it names none.
delivered_to() {
  local named
  named=$(jq -r '.install.webhook.publicAddress // ""' "$1")
  printf '%s\n' "${named:-$(routed)}"
}
# Whether the IPv4 address $1 is one Telegram cannot reach, being private: RFC 1918's, the
# shared space carrier-grade NAT and tailnets use (100.64.0.0/10), link-local, loopback,
# "this network" (0.0.0.0/8), the benchmarking space (198.18.0.0/15), or multicast and the
# reserved space past it (224.0.0.0/3). A host name is none.
private_address() {
  [[ $1 =~ ^([0-9]{1,3})\.([0-9]{1,3})\.[0-9]{1,3}\.[0-9]{1,3}$ ]] || return 1
  local a=$((10#${BASH_REMATCH[1]})) b=$((10#${BASH_REMATCH[2]}))
  ((a == 0 || a == 10 || a == 127 || (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168) || (a == 100 && b >= 64 && b <= 127) || (a == 169 && b == 254) || (a == 198 && (b == 18 || b == 19)) || a >= 224))
}
# The configuration $1 in this release's settings, on stdout: the collector's resources
# gone, as this release has its gateway's and its scraper's; the lake's its loader's,
# which loads as 7.0.0's one pod did, where it sets them; the address Telegram delivers to
# $2. What the cut-over gives the configuration before UPGRADE_CHECK reads it; and what
# `from-7.0.0.sh release-settings <file>` says alone, the address the configuration's or
# this machine's on its route, as the upgrade shard gives a copy of the configuration it.
release_settings() {
  jq --arg address "$2" 'del(.install.telemetry.collector.resources) | (if .install.lake.resources == null then . else .install.lake.loader.resources = .install.lake.resources end) | del(.install.lake.resources) | .install.webhook.publicAddress = $address' "$1"
}
case "${1:-}" in
  release-settings)
    release_settings "${2:?the configuration file to give them}" "$(delivered_to "$2")"
    exit 0
    ;;
  private-address)
    private_address "${2:?the address to classify}"
    exit
    ;;
esac
: "${UPGRADE:?the upgrade of the release, run as the owner of the configuration}" "${UPGRADE_CHECK:?a command proving UPGRADE can run}" "${CONFIG:?the configuration UPGRADE reads}" "${OUT:?where to record}"
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

for tool in ip jq kubectl; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
[ -f "$CONFIG" ] && jq -e '.install | type == "object"' "$CONFIG" >/dev/null 2>&1 || fail "CONFIG, $CONFIG, is not alasio's configuration"
# alasio's state in main's compute, as SQL $1 answers it: the compute awake, as alasio holds
# its sessions on it. It waits for the compute to be awake, and wakes it not: a cut-over runs
# with alasio running, which keeps main's compute awake while it holds sessions on it.
in_state() {
  awake() { [ "$(k get deploy alasio-neon-compute -o jsonpath='{.status.readyReplicas}')" = 1 ]; }
  wait_for 300 "main's compute to be awake" awake
  k exec deploy/alasio-neon-compute -c compute -- psql -h 127.0.0.1 -p 55433 -U cloud_admin -d alasio -v ON_ERROR_STOP=1 -Atqc "$1"
}

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
  # The address Telegram delivers to, which 7.0.0 has no setting of: the one CONFIG names, as
  # its owner sets it where a run before refused this machine's, or else this machine's on its
  # route to the internet, as alasio init offers it; kept in OUT once taken, as a failure gives
  # CONFIG back as 7.0.0 reads it.
  if [ ! -e "$OUT/public-address" ]; then
    address=$(delivered_to "$CONFIG")
    [ -n "$address" ] ||
      fail "this machine has no route to the internet whose address Telegram could deliver to: set install.webhook.publicAddress in $CONFIG, and run the cut-over again (it reads the address there until it has kept one in $OUT/public-address, and an edit of $CONFIG after that is ignored)"
    if private_address "$address" && [ "${PRIVATE_ADDRESS:-}" != allowed ]; then
      fail "Telegram would deliver to $address, a private address, which it cannot reach: set install.webhook.publicAddress in $CONFIG to the address Telegram reaches this machine at, and run the cut-over again (it reads the address there until it has kept one in $OUT/public-address, and an edit of $CONFIG after that is ignored)"
    fi
    printf '%s\n' "$address" >"$OUT/public-address.part"
    mv "$OUT/public-address.part" "$OUT/public-address"
  fi
  address=$(cat "$OUT/public-address")
  log "Telegram delivers to $address once upgraded (kept in $OUT/public-address)"
  # The users the bot allows, whom this release admits alone: where 7.0.0's allows none, as
  # 7.0.0 let in the first to write to it, that user, as 7.0.0 kept it in alasio's state.
  first=
  if [ -z "$(k get secret alasio-telegram -o jsonpath='{.data.allowedUserIds}' | base64 -d)" ]; then
    first=$(in_state "select value from state.bot_state where key = 'telegram_bootstrap_user_id'")
    [[ $first =~ ^[0-9]+$ ]] || fail "the bot allows no user, and 7.0.0 let none in: give it its users (alasio init --allowed-user-ids), and run the cut-over again"
  fi
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
  release_settings "$OUT/config.json" "$address" >"$CONFIG.part"
  chown --reference="$CONFIG" "$CONFIG.part"
  chmod --reference="$CONFIG" "$CONFIG.part"
  mv "$CONFIG.part" "$CONFIG"
  # Left so on a failure, as 7.0.0 admits that user alone either way.
  if [ -n "$first" ]; then
    log "allowing the user 7.0.0 let in, $first, as the bot allows none"
    k patch secret alasio-telegram --type merge -p "{\"data\":{\"allowedUserIds\":\"$(printf %s "$first" | base64 -w0)\"}}" >/dev/null
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
# releases sessions, is stopped first, so that none is restored or released meanwhile, as one
# would fall back from a snapshot it could not read: the upgrade starts it again. In a node
# agent of 7.0.0's, which reaches the bucket, each workspace's current snapshot is moved: its
# files copied under layers/<image>/, each then looked at, as the agent checks what it puts;
# its manifest written anew, under its new id, then its current.json; what 7.0.0 left of the
# workspace's then deleted. Each Sandbox released into one is then bound to its new id. Run
# again, a workspace moved already is told again, and what is left of it deleted. The nodes'
# caches, of 7.0.0's layout, are emptied: the agents fill them anew as they restore.
if [ ! -e "$OUT/snapshots-moved" ]; then
  replicas=$(k get deploy "$RELEASE" -o jsonpath='{.spec.replicas}')
  if [ "$replicas" != 0 ]; then
    log "stopping alasio, which the upgrade starts again, so that no session is restored or released as the snapshots move"
    k scale deploy/"$RELEASE" --replicas=0 >/dev/null
  fi
  stopped() {
    local left
    left=$(k get pod -l app.kubernetes.io/component=alasio -o name) && [ -z "$left" ]
  }
  wait_for 300 "alasio's pods to be gone" stopped
  environments=$(kubectl get sandboxes.agents.x-k8s.io -A -l alasio.dev/workload=session -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' | sort -u | paste -sd ' ')
  if [ -n "$environments" ]; then
    log "moving the snapshots of the sessions of $environments to this release's layout"
    agent=$(k get pod -l app.kubernetes.io/component=node-agent --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}')
    agent=${agent%% *}
    [ -n "$agent" ] || fail "no node agent runs to move the snapshots with"
    k exec -i "$agent" -c node-agent -- env ENVIRONMENTS="$environments" node --input-type=module - >"$OUT/snapshots-moved.part" <<'JS' || fail "the snapshots could not all be moved (the run again goes on with them): $(tail -n 3 "$OUT/snapshots-moved.part" 2>/dev/null)"
import { createHash } from "node:crypto";
import { request } from "node:http";
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
// 7.0.0's id, to the second, as this release's, to the millisecond it was taken at.
const anew = (snapshot, takenAt) => `${snapshot.slice(0, 15)}${String(Date.parse(takenAt) % 1000).padStart(3, "0")}${snapshot.slice(15)}`;
const before = (snapshot) => `${snapshot.slice(0, 15)}${snapshot.slice(18)}`;

for (const environment of required("ENVIRONMENTS").split(" ")) {
  const keys = await run(bucket.list(`${environment}/`));
  for (const workspace of [...new Set(keys.map((key) => key.split("/")[1]))]) {
    const prefix = `${environment}/${workspace}`;
    const left = keys.filter((key) => key.startsWith(`${prefix}/`) && key !== `${prefix}/current.json` && !key.startsWith(`${prefix}/layers/`) && !key.startsWith(`${prefix}/manifests/`));
    const current = await run(bucket.getJson(`${prefix}/current.json`));
    if (current && Array.isArray(current.layers)) {
      process.stdout.write(`${environment} ${workspace} ${before(current.snapshot)} ${current.snapshot}\n`);
    } else if (current) {
      const { snapshot: was, image, files, environment: _environment, workspace: _workspace, ...rest } = current;
      const snapshot = anew(was, current.takenAt);
      const moved = [];
      for (const file of files) {
        const to = `${prefix}/layers/${image}/${file.path}`;
        await run(bucket.copy(`${prefix}/${was}/${file.path}`, to, file.bytes));
        const stored = await head(to);
        if (stored.bytes !== file.bytes) throw new Error(`${to} is ${stored.bytes} bytes in the bucket, where the manifest of ${was} says ${file.bytes}`);
        moved.push({ ...file, etag: stored.etag });
      }
      const manifest = { environment, workspace, snapshot, ...rest, layers: [{ digest: image, files: moved }] };
      await run(bucket.putJson(`${prefix}/manifests/${snapshot}.json`, manifest));
      await run(bucket.putJson(`${prefix}/current.json`, manifest));
      process.stdout.write(`${environment} ${workspace} ${was} ${snapshot}\n`);
    }
    for (const key of left) await run(bucket.remove(key));
  }
}
JS
    while read -r environment workspace was snapshot; do
      bound=$(kubectl -n "$environment" get sandboxes.agents.x-k8s.io "$workspace" --ignore-not-found -o jsonpath='{.metadata.annotations.alasio\.dev/snapshot}')
      if [ "$bound" = "$was" ]; then
        log "binding the Sandbox $environment/$workspace to its snapshot $snapshot, $was in 7.0.0's"
        kubectl -n "$environment" annotate sandboxes.agents.x-k8s.io "$workspace" alasio.dev/snapshot="$snapshot" --overwrite >/dev/null
      fi
    done <"$OUT/snapshots-moved.part"
  fi
  for agent in $(k get pod -l app.kubernetes.io/component=node-agent -o jsonpath='{.items[*].metadata.name}'); do
    log "emptying the snapshot cache of $agent, of 7.0.0's layout"
    k exec "$agent" -c node-agent -- sh -c 'rm -rf /var/lib/alasio/snapshots/* /var/lib/alasio/snapshots/.[!.]*' || fail "the snapshot cache of $agent could not be emptied"
  done
  # Moved, the Sandboxes bound, the caches emptied: a run again goes on past it.
  touch "$OUT/snapshots-moved.part" && mv "$OUT/snapshots-moved.part" "$OUT/snapshots-moved"
fi

# The upgrade, once: a run again once it went through goes on with what follows it alone.
if [ ! -e "$OUT/upgraded" ]; then
  log "upgrading, from the cut-over begun at $(cat "$OUT/point-of-no-return"): alasio restarts"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  bash -c "$UPGRADE" 2>&1 | tee -a "$OUT/upgrade.log" ||
    fail "the upgrade failed (upgrade.log): the cut-over run again goes on with it. To run 7.0.0 as it was instead: write $OUT/config.json back to $CONFIG; stop the storage controller (kubectl -n $NS scale deploy/$CONTROLLER --replicas=0), register node 1 back as $OUT/pageserver-registered has it (update nodes set listen_http_addr = 'alasio-neon-pageserver', listen_pg_addr = 'alasio-neon-pageserver' where node_id = 1, in its database), and start it again with the replicas it ran (kubectl -n $NS scale deploy/$CONTROLLER --replicas=$(cat "$OUT/controller-replicas")); where the pageservers' StatefulSet is made anew already (kubectl -n $NS get statefulset $PAGESERVERS finds it), delete it, its pod kept (kubectl -n $NS delete statefulset $PAGESERVERS --cascade=orphan); run 7.0.0's alasio up, which makes it again and takes the pod back; then restart main's compute (kubectl -n $NS rollout restart deploy/alasio-neon-compute), for it to reach the pageserver at 7.0.0's address again; 7.0.0 restores none of the snapshots moved to this release's layout ($OUT/snapshots-moved), whose sessions come back from bayma's checkpoints"
  date -u +%FT%TZ >"$OUT/upgraded"
fi
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
log "upgraded at $(cat "$OUT/upgraded")"

# What 7.0.0 kept of polling Telegram, which nothing reads now that Telegram delivers to the
# webhook intake: where it was in the bot's updates, and the user an empty allowlist let in,
# once the upgrade has given main's bot its route.
[ -n "$(k get secret alasio-webhook-routes -o jsonpath='{.data.main}')" ] || fail "main's bot has no route to the webhook intake: run alasio up as the owner of $CONFIG, which gives it one now that $OUT/upgraded is there, then the cut-over again, which upgrades no more"
log "deleting what alasio kept of polling Telegram"
in_state "delete from state.bot_state where key in ('telegram_update_offset', 'telegram_bootstrap_user_id')" >/dev/null
log "through"
