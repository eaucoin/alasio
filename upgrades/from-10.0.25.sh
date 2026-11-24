#!/usr/bin/env bash
# An installation of alasio 10.0.25 upgraded to this release, with what the release needs done
# by hand around its upgrade: what the box's cut-over runs, and what the end-to-end run's
# upgrade shard proves, on an installation of 10.0.25 it put data in (test/e2e/alasio.test.ts).
# Run as root on the host, kubectl reaching the cluster (KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# for k3s on this machine). Everything is recorded in OUT, which is to be on a disk that
# outlives a restart of the machine, as /tmp may not.
#
#   UPGRADE=<command> UPGRADE_CHECK=<command> CONFIG=<file> OUT=<dir> [FAIL_AT=<point>] upgrades/from-10.0.25.sh
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
# 10.0.25 made exists: it refuses then too, before it changes anything. Delete each first
# (alasio branch delete <name>), and make it again once upgraded.
#
# The changes this release is made of add their steps to it, with what those steps need:
# before the point of no return, past it, before the upgrade, and once the upgrade is
# through, after it. Each change's steps are a section of their own below, between its BEGIN
# and END lines, which the flow at the end of the script calls. None changes, before the
# point of no return, what a run that fails there would have to make again: a step that does
# brings what makes it again, for the flow to run as such a run ends. One has a step after
# the upgrade: the records of releases given their manifests.
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

# BEGIN the records of releases given their manifests.
#
# This release's gateway restores a workspace from the manifest its record of its release holds
# (src/node-agent/workspaces.ts Released), reading nothing of the bucket; the records the release
# before wrote hold none, as its gateway read the workspace's current snapshot from the bucket
# instead. Once the upgrade is through, each record that holds none is given one, by a program run
# in a node agent of this release, which reads each record as it is on workspace storage (the agent's
# own API refuses one without a manifest), reads the bucket, and writes the records through the
# agent's API: the workspace's current snapshot, where it is the one the record names or one
# compacted from it, as the release before restored; where it is neither, or there is none, the
# record is removed, as the release before fell back from it; and so is one that is no record, which
# no bind of the release before restored either. A bind meanwhile finds a record it cannot read, and
# is deferred, to be tried again, for 15 minutes from its first (src/gateway/snapshots.ts
# DEFER_FOR_MS), past which it falls back: within that, as the wait for the node agents' rollout
# below takes 10 minutes at the most, and the records' rewrite a few seconds. The gateway writes a
# record as a binding ends meanwhile, holding its Lease, which this does not: each record is read
# again just before it is written, and left be where it has changed, as one the gateway wrote is.
# Run again, it gives none a manifest twice.
RECORDS_PROGRAM=$(
  cat <<'PROGRAM'
const [root, agent, workspaces] = process.argv.slice(2);
const { readFile } = await import("node:fs/promises");
const { constants } = await import("node:fs");
// As the agent reads it: a link is not followed.
// A link is read as no record, which is removed.
const readRecord = (file) => readFile(file, { encoding: "utf8", flag: constants.O_RDONLY | constants.O_NOFOLLOW }).catch((error) => (error.code === "ENOENT" ? null : error.code === "ELOOP" ? "" : Promise.reject(error)));
const { makeS3Bucket } = await import(`${root}/src/node-agent/s3.ts`);
const bucket = makeS3Bucket({ endpoint: process.env.S3_ENDPOINT, region: process.env.S3_REGION, bucket: process.env.S3_BUCKET, accessKeyId: process.env.AWS_ACCESS_KEY_ID, secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY });
const { createRequire } = await import("node:module");
const { Effect } = createRequire(`${root}/package.json`)("effect");
const headers = { authorization: `Bearer ${process.env.ALASIO_NODE_AGENT_TOKEN}` };
const ask = async (method, path, body) => {
  const response = await fetch(`${agent}${path}`, { method, headers: body === undefined ? headers : { ...headers, "content-type": "application/json" }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  if (!response.ok && response.status !== 404) throw new Error(`${method} ${path} was answered ${response.status}: ${await response.text()}`);
  return response.status === 404 ? null : response.status === 204 ? true : response.json();
};
const counted = { given: 0, removed: 0, holding: 0, changed: 0 };
for (const environment of (await ask("GET", "/workspaces")).environments) {
  for (const workspace of (await ask("GET", `/workspaces/${encodeURIComponent(environment)}`)).workspaces) {
    const path = `/workspaces/${encodeURIComponent(environment)}/${encodeURIComponent(workspace)}/released`;
    // As it is beside the workspace's files: one written by the release before has no manifest, which this release's API refuses.
    const file = `${workspaces}/${environment}/${workspace}/released.json`;
    const text = await readRecord(file);
    if (text === null) continue;
    const record = (() => {
      try {
        return JSON.parse(text);
      } catch {
        return null;
      }
    })();
    if (record?.manifest !== undefined) {
      counted.holding += 1;
      continue;
    }
    const current = record === null ? null : await Effect.runPromise(bucket.getJson(`${environment}/${workspace}/current.json`));
    // Written meanwhile, by the gateway as a binding ended: left as it is.
    if ((await readRecord(file)) !== text) {
      counted.changed += 1;
      continue;
    }
    if (record !== null && current !== null && (current.snapshot === record.snapshot || current.compactedFrom === record.snapshot) && current.release === record.release) {
      await ask("PUT", path, { ...record, manifest: current });
      counted.given += 1;
    } else {
      await ask("DELETE", path);
      counted.removed += 1;
      console.log(record === null
        ? `${environment}/${workspace}'s record is no record: removed, as the release before fell back from it`
        : `${environment}/${workspace}'s record names ${record.snapshot}, which its current snapshot is not, nor was compacted from: removed, as the release before fell back from it`);
    }
  }
}
console.log(`records of releases given their manifests: ${counted.given}; removed: ${counted.removed}; holding one already: ${counted.holding}; written meanwhile: ${counted.changed}`);
PROGRAM
)
records_given_manifests() {
  log "waiting for this release's node agents (records.log)"
  kubectl -n alasio rollout status daemonset/alasio-node-agent --timeout=10m >>"$OUT/records.log" 2>&1 ||
    fail "the node agents of this release did not roll out within 10 minutes (records.log): the cut-over run again goes on with it"
  local pod
  pod=$(kubectl -n alasio get pods -l app.kubernetes.io/instance=alasio,app.kubernetes.io/component=node-agent --field-selector=status.phase=Running -o 'jsonpath={.items[0].metadata.name}') ||
    fail "the node agents' pods could not be listed: the cut-over run again goes on with it"
  [ -n "$pod" ] || fail "no node agent runs: the cut-over run again goes on with it"
  log "giving each record of a release its manifest, through the node agent $pod (records.log)"
  printf '%s\n' "$RECORDS_PROGRAM" | kubectl -n alasio exec -i "$pod" -c node-agent -- node --input-type=module - /opt/alasio http://127.0.0.1:8484 /var/lib/alasio/workspaces >>"$OUT/records.log" 2>&1 ||
    fail "the records of releases could not all be given their manifests (records.log): the cut-over run again goes on with them"
  log "$(tail -n 1 "$OUT/records.log")"
}
# END the records of releases given their manifests.

command -v kubectl >/dev/null || fail "kubectl is not on root's PATH"
[ -r "$CONFIG" ] || fail "CONFIG $CONFIG cannot be read"
kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"

if [ ! -e "$OUT/point-of-no-return" ]; then
  log "cutting over from 10.0.25, recording in $OUT"
  # What UPGRADE_CHECK refuses that needs no release to read, read before anything changes.
  log "checking that no branch environment exists, as the upgrade refuses one 10.0.25 made"
  # Each by its own namespace, alasio-branch-<name>, which its sessions' is labelled as too.
  namespaces=$(kubectl get namespace -l alasio.dev/branch -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.alasio\.dev/branch}{"\n"}{end}') ||
    fail "the cluster's namespaces could not be listed, so whether a branch environment exists cannot be told"
  branches=$(printf '%s\n' "$namespaces" | awk -F '\t' 'NF == 2 && $1 == "alasio-branch-" $2 { print $2 }')
  [ -z "$branches" ] ||
    fail "alasio upgrades main alone, and the branch environments 10.0.25 made would not run on what upgrading changes: delete each first ($(printf '%s\n' "$branches" | awk 'NR > 1 { printf ", " } { printf "alasio branch delete %s", $0 }')), and make them again once alasio is upgraded"
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
records_given_manifests

log "upgraded at $(cat "$OUT/upgraded")"
log "through"
