#!/usr/bin/env bash
# 08's cut-over undone (gate/08/cutover.sh), from the step it reached, as it recorded in OUT:
# what the release made deleted and what it overwrote put back, the volume rebound to the
# old store's claim, the old store started alone and checked against what the cut-over
# recorded before it touched anything; only then the previous release applied, and every
# writer started again as it was. Once the cut-over's upgrade has begun, the writers write
# to the new store alone, which the old one lacks: it refuses then, and says to roll forward.
#
#   RELEASE=<file of gate/08/render.ts's List> OUT=<the cut-over's OUT> PREVIOUS=<command>
#     PREVIOUS_CHECK=<command> [READ_BACK=50] [WAIT=900] [CHECKS_WAIT=300] [JOBS_WAIT=2700]
#     [EXEC_TIMEOUT=1800] [UPGRADE_TIMEOUT=3600] gate/08/rollback.sh
#
# PREVIOUS applies the previous release again, as the configuration's owner does (`sudo -u
# <owner> env HOME=<their home> npx --yes alasio@<previous> upgrade`); PREVIOUS_CHECK, which
# must succeed before anything changes, proves it can (the same with `--version`). As it
# starts it records OUT/rolling-back, which the cut-over then refuses; run again, it goes on.
# It stops, the writers stopped and the old store sealed, where the old store would write or
# allocate wrongly: the recorded PV on another claim, a volume the old store had missing from
# it (by id), the old store not starting, its masters not listing every volume it had, a
# volume id it would make again, no bucket listed, or versions lost since the cut-over
# recorded them. What else it answers otherwise than then, its volumes and versions whole (an
# inventory that differs, the bytes of a version read back, no new object taken), is
# recorded in OUT/discrepancies.txt, each run's under a line of its own; the stack comes
# back, the old store's lifecycle CronJob held suspended until someone looks (the hold
# recorded in OUT/lifecycle-held from the first run that found anything, so a run again holds
# it too), and the rollback, done, exits 3 rather than 0 (and never 3 otherwise). A run again
# after one that lifted the seal stops the writers and seals the old store again before it
# checks it. Each check of the old store just started is waited for, at most CHECKS_WAIT. The
# filer store's Secrets and the root Secret's keys for them, which the cut-over made, are
# left: the previous release reads neither, and the next cut-over takes them as they are.
set -euo pipefail
umask 077
here=$(cd "$(dirname "$0")" && pwd)
: "${RELEASE:?the objects of the release, as gate/08/render.ts prints them}" "${OUT:?where the cut-over recorded}" "${PREVIOUS:?how the previous release is applied again}"
: "${PREVIOUS_CHECK:?how the command line of the previous release is proven runnable}"
READ_BACK=${READ_BACK:-50}
UPGRADE_TIMEOUT=${UPGRADE_TIMEOUT:-3600}
JOBS_WAIT=${JOBS_WAIT:-2700}
# The longest each check of the old store just started waits for it to answer as it should.
CHECKS_WAIT=${CHECKS_WAIT:-300}
# shellcheck source=gate/08/lib.sh
. "$here/lib.sh"
# 3 says a rollback done with discrepancies alone: any other 3, as a command in a pod may
# pass through kubectl, is a failure.
rolled_back_differing=0
trap 'status=$?; [ "$status" != 3 ] || { [ "$rolled_back_differing" = 1 ] && [ -e "$OUT/rolled-back-from-step" ]; } || exit 1' EXIT
[ -d "$OUT" ] || fail "$OUT, where the cut-over recorded, is not there"
hold
[ ! -e "$OUT/rolled-back-from-step" ] || fail "the cut-over recorded in $OUT was rolled back already, from step $(cat "$OUT/rolled-back-from-step")"
[ -e "$OUT/step" ] || fail "the cut-over recorded no step in $OUT: there is nothing of it to roll back"
if [ -e "$OUT/point-of-no-return" ]; then
  fail "the cut-over's upgrade began at $(cat "$OUT/point-of-no-return"): what the writers wrote since is in the new store alone, so it is not rolled back; roll it forward, running the upgrade again: $(cat "$OUT/upgrade")"
fi
prerequisites "$PREVIOUS_CHECK"
[ -e "$OUT/rolling-back" ] || date -u +%FT%TZ >"$OUT/rolling-back"
reached=$(cat "$OUT/step")
log "rolling back the cut-over, which reached step $reached"
pv=""
[ ! -e "$OUT/volume.tsv" ] || read -r pv dir _ <"$OUT/volume.tsv"

if [ "$reached" -ge 5 ]; then
  log "1. the release's workloads deleted, their pods gone"
  jq -c '[.items[] | select(.kind == "Deployment" or .kind == "StatefulSet") | {kind, name: .metadata.name}]' "$RELEASE" >"$OUT/release-workloads.json"
  jq -r '.[] | "\(.kind) \(.name)"' "$OUT/release-workloads.json" | while read -r kind name; do
    k delete "$kind" "$name" --ignore-not-found --wait=false >/dev/null
  done
  # Their pods by what owns them, as the gateways' select the old store's pod too.
  release_pods() {
    k get pods -o json | jq -r --slurpfile workloads "$OUT/release-workloads.json" '.items[]
      | select(any(.metadata.ownerReferences[]?; . as $owner | any($workloads[0][]; . as $workload |
          ($owner.kind == "StatefulSet" and $workload.kind == "StatefulSet" and $owner.name == $workload.name)
          or ($owner.kind == "ReplicaSet" and $workload.kind == "Deployment" and ($owner.name | startswith($workload.name + "-"))))))
      | .metadata.name'
  }
  wait_for "$WAIT" "the release's pods to be gone ($(release_pods | tr '\n' ' '))" none release_pods
  if [ -n "$pv" ] && [ "$reached" -ge 7 ]; then
    # What the release's parts wrote into the volumes' directory: their filers' log of changes
    # is written to volumes, some of which the release's masters may have made.
    find "$dir" -maxdepth 1 -name '*.dat' -printf '%f\n' | sed 's/\.dat$//; s/.*_//' | sort -u >"$OUT/volumes-on-disk.txt"
    tr ' ' '\n' <"$OUT/volumes-stopped.txt" | sed -n 's/^\([0-9]*\):.*/\1/p' | sort -u >"$OUT/volumes-stopped-ids.txt"
    log "volumes the release made beside the old ones: $(comm -13 "$OUT/volumes-stopped-ids.txt" "$OUT/volumes-on-disk.txt" | sort -n | tr '\n' ' ')"
  fi

  log "2. the release's own objects deleted, with the claims its StatefulSets made, and what it overwrote put back as it was"
  # What the cut-over recorded before it applied anything, if it got that far.
  [ ! -e "$OUT/release-only.jsonl" ] || while read -r object; do
    kind=$(jq -r .kind <<<"$object") name=$(jq -r .name <<<"$object") ns=$(jq -r '.namespace // empty' <<<"$object")
    kubectl ${ns:+-n "$ns"} delete "$kind" "$name" --ignore-not-found --wait=true --timeout="${WAIT}s" >/dev/null
  done <"$OUT/release-only.jsonl"
  for claim in $(jq -r '.items[] | select(.kind == "StatefulSet") | .metadata.name as $set | .spec.volumeClaimTemplates[]?.metadata.name | "^\(.)-\($set)-[0-9]+$"' "$RELEASE" |
    while read -r pattern; do k get pvc -o name | sed 's|^persistentvolumeclaim/||' | grep -E "$pattern" || true; done); do
    [ "$claim" = "$NEW_CLAIM" ] || k delete pvc "$claim" --wait=true --timeout="${WAIT}s" >/dev/null
  done
  [ ! -e "$OUT/overwritten.json" ] || jq -c '.items[] | del(.metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.managedFields, .metadata.generation, .metadata.selfLink, .status)' "$OUT/overwritten.json" |
    while read -r object; do kubectl replace -f - <<<"$object" >/dev/null 2>&1 || kubectl create -f - <<<"$object" >/dev/null; done
fi

# The volume back on the old store's claim wherever it is not, whatever step was recorded: on
# the release's, or on neither, between a rebind's two claims; never from another's.
if [ -n "$pv" ]; then
  holder=$(claim_of "$pv")
  [ "$holder" = "$OLD_CLAIM" ] || [ "$holder" = "$NEW_CLAIM" ] || [ -z "$holder" ] ||
    fail "the PV $pv the cut-over recorded is held by '$holder', neither $OLD_CLAIM nor $NEW_CLAIM: the old store is not started"
  if [ "$holder" != "$OLD_CLAIM" ]; then
    log "3. the volume rebound to the old store's claim"
    rebind "$pv" "$NEW_CLAIM" "$OLD_CLAIM" "$(k get statefulset "$OLD" -o json | jq -c '.spec.volumeClaimTemplates[0]')"
  fi
fi

log "4. the old store alone, started and checked before any writer starts"
# Never on another volume than the one the cut-over recorded: one made anew is empty.
[ -z "$pv" ] || [ "$(claim_of "$pv")" = "$OLD_CLAIM" ] || fail "$OLD_CLAIM does not hold the PV $pv the cut-over recorded: the old store is not started"
# Never without each volume the old store had, by its id, on every run: the release's
# volumes beside them would hide one lost from a count of the data files. Those its masters
# listed before the cut-over touched it, and those on the disk once it stopped it, each where
# the cut-over recorded them.
had=$(for list in volumes-before.txt volumes-stopped.txt; do [ ! -s "$OUT/$list" ] || tr ' ' '\n' <"$OUT/$list"; done | sed '/^$/d' | sort -u | sort -t: -k1,1n | tr '\n' ' ')
if [ -n "$pv" ] && [ -n "$had" ]; then
  on_disk=$(volumes_on_disk "$pv")
  for volume in $had; do
    grep -qw -- "$volume" <<<"$on_disk" || fail "the PV $pv lacks the volume $volume the old store had: the old store is not started"
  done
fi
# A run that lifted the seal, then stopped, may have started writers: they are stopped, and
# the old store sealed, again, as every check below takes them to be.
if [ -e "$OUT/unsealed" ]; then
  log "a run before this one lifted the seal: what writes to the store stopped again, and the old store sealed again, before it is checked"
  [ -s "$OUT/csi-namespace" ] || fail "the cut-over recorded no JuiceFS driver's namespace (csi-namespace), which stopping the writers again needs"
  stop_writers "$(cat "$OUT/csi-namespace")" "$JOBS_WAIT"
  seal_old
  rm "$OUT/unsealed"
fi
was=$(marked statefulset "$OLD" "$NS")
if [ -n "$was" ]; then
  k scale statefulset/"$OLD" --replicas="$(jq -r .replicas <<<"$was")" >/dev/null
  unmark statefulset "$OLD" "$NS"
fi
k rollout status statefulset/"$OLD" --timeout="${WAIT}s" || fail "the old store is not ready within ${WAIT}s"
# What would have the old store write or allocate wrongly stops the rollback, its writers
# stopped: no volumes known, a volume id it would make again, versions lost. What else it
# answers otherwise than before the cut-over, its volumes and versions whole, is recorded, for
# the operator to look into, and the stack brought back, its lifecycle passes held until then.
printf '# the rollback of %s\n' "$(date -u +%FT%TZ)" >>"$OUT/discrepancies.txt"
differing=0
differs() {
  log "DIFFERS: $*"
  printf '%s\n' "$*" >>"$OUT/discrepancies.txt"
  differing=$((differing + 1))
}
# Each of these waited for, for at most CHECKS_WAIT, as a store just started answers in parts:
# its masters as its volume servers' first heartbeats reach them, an S3 call that failed once.
if [ -n "$pv" ]; then
  highest=$(find "$dir" -maxdepth 1 -name '*.dat' -printf '%f\n' | sed 's/\.dat$//; s/.*_//' | sort -n | tail -n 1)
  newest=""
  above() {
    newest=$(kx "$OLD_POD" -c seaweedfs -- wget -q -O - http://127.0.0.1:9333/cluster/status | jq -r '.MaxVolumeId // 0') && [[ "$newest" =~ ^[0-9]+$ ]] && [ "$newest" -ge "${highest:-0}" ]
  }
  deadline=$((SECONDS + CHECKS_WAIT))
  until above; do
    [ "$SECONDS" -lt "$deadline" ] || fail "the old master would make volume $((${newest:-0} + 1)) again: the disk holds volume $highest (its status said '$newest' for ${CHECKS_WAIT}s)"
    sleep 5
  done
fi
if [ -e "$OUT/inventory-before.json" ]; then
  # The old store's masters list every volume it had, as its volume servers report them.
  listed() {
    local now volume
    now=$(volumes old_shell volume-list-rolled-back.txt) && [ -n "$now" ] || return 1
    for volume in $had; do grep -qw -- "$volume" <<<"$now" || return 1; done
  }
  wait_for "$CHECKS_WAIT" "the old store's masters to list every volume it had, $had (volume-list-rolled-back.txt)" listed
  read_whole() { inventory "$OLD_POD" lifecycle >"$OUT/inventory-rolled-back.json"; }
  wait_for "$CHECKS_WAIT" "the old store's inventory to be read" read_whole
  # The versions lost, by bucket, key and version, as sorted lists compare: a list of the
  # box's tens of thousands of versions takes jq's array subtraction seconds to minutes.
  keys() { jq -r '.[] | .bucket as $bucket | .versions[] | [$bucket, .Key, (.VersionId // "null")] | @json' "$1" | LC_ALL=C sort; }
  keys "$OUT/inventory-before.json" >"$OUT/versions-before.txt"
  keys "$OUT/inventory-rolled-back.json" >"$OUT/versions-rolled-back.txt"
  lost=$(LC_ALL=C comm -23 "$OUT/versions-before.txt" "$OUT/versions-rolled-back.txt" | wc -l)
  [ "$(jq 'length' "$OUT/inventory-rolled-back.json")" -gt 0 ] || fail "the old store lists no bucket (inventory-rolled-back.json)"
  [ "$lost" = 0 ] || fail "the old store lists $lost fewer versions than before the cut-over (inventory-rolled-back.json)"
  diff -u "$OUT/inventory-before.json" "$OUT/inventory-rolled-back.json" >"$OUT/inventory-diff-rolled-back.txt" ||
    differs "the old store's inventory differs from before the cut-over, every version there (inventory-diff-rolled-back.txt)"
  if contents "$OLD_POD" <"$OUT/read-back.tsv" >"$OUT/contents-rolled-back.tsv"; then
    diff -u "$OUT/contents-before.tsv" "$OUT/contents-rolled-back.tsv" >"$OUT/contents-diff-rolled-back.txt" ||
      differs "versions read back hold other bytes than before the cut-over (contents-diff-rolled-back.txt)"
  else
    differs "the old store does not read every version it read before the cut-over (read-back.tsv)"
  fi
  checked="its inventory against before the cut-over, $(wc -l <"$OUT/read-back.tsv") versions read back against their bytes then"
else
  # The cut-over stopped before its inventory, before anything of the store was touched: it is
  # read as it answers now.
  read_whole() { inventory "$OLD_POD" lifecycle >"$OUT/inventory-rolled-back.json"; }
  wait_for "$CHECKS_WAIT" "the old store's inventory to be read" read_whole
  read_back_set "$OUT/inventory-rolled-back.json" "$READ_BACK" >"$OUT/read-back-rolled-back.tsv"
  if ! contents "$OLD_POD" <"$OUT/read-back-rolled-back.tsv" >"$OUT/contents-rolled-back.tsv"; then
    differs "the old store does not read every version it lists (read-back-rolled-back.tsv)"
  fi
  checked="no inventory before (the cut-over stopped at step $reached), $(wc -l <"$OUT/contents-rolled-back.tsv") versions read now"
fi
probe() {
  [ "$(kx "$OLD_POD" -c lifecycle -- sh -c 'set -e
  s3api() { aws --endpoint-url http://127.0.0.1:8333 s3api "$@"; }
  bucket=$(s3api list-buckets --query "Buckets[0].Name" --output text)
  head -c 4096 /dev/urandom > /tmp/probe
  s3api put-object --bucket "$bucket" --key rollback-probe --body /tmp/probe >/dev/null
  s3api get-object --bucket "$bucket" --key rollback-probe /tmp/probe-back >/dev/null
  cmp -s /tmp/probe /tmp/probe-back && s3api delete-object --bucket "$bucket" --key rollback-probe >/dev/null && echo written')" = written ]
}
taken=0
deadline=$((SECONDS + CHECKS_WAIT))
while [ "$taken" = 0 ] && [ "$SECONDS" -lt "$deadline" ]; do
  if probe; then taken=1; else sleep 5; fi
done
[ "$taken" = 1 ] || differs "the old store takes no new object within ${CHECKS_WAIT}s"
log "the old store: $checked; $differing discrepancies"

# The lifecycle passes, which delete versions, wait for whoever looks into what differs: held
# from the first run that found anything, recorded with how the CronJob was, until a run ends.
if [ "$differing" != 0 ] && [ ! -e "$OUT/lifecycle-held" ]; then
  was=$(marked cronjob "$OLD-lifecycle" "$NS")
  printf '%s\n' "${was:-"{\"suspend\":false}"}" >"$OUT/lifecycle-held.part"
  sync "$OUT/lifecycle-held.part"
  mv "$OUT/lifecycle-held.part" "$OUT/lifecycle-held"
fi

log "5. the seal lifted, the previous release applied, and every writer started as it was"
date -u +%FT%TZ >"$OUT/unsealed"
unseal_old
timeout "$UPGRADE_TIMEOUT" bash -c "$PREVIOUS" || fail "the previous release was not applied again: rollback.sh run again goes on"
held=()
[ ! -e "$OUT/lifecycle-held" ] || held=("$NS/$OLD-lifecycle")
start_writers "${held[@]}"
[ ! -e "$OUT/lifecycle-held" ] || k patch cronjob "$OLD-lifecycle" -p '{"spec":{"suspend":true}}' >/dev/null
if [ -e "$OUT/writers.txt" ]; then
  while read -r ns kind; do
    kubectl -n "$ns" rollout status "$kind" --timeout="${WAIT}s" >/dev/null || fail "$ns $kind is not ready within ${WAIT}s"
  done <"$OUT/writers.txt"
fi
mv "$OUT/step" "$OUT/rolled-back-from-step"
rm "$OUT/rolling-back" "$OUT/unsealed"
if [ -e "$OUT/lifecycle-held" ]; then
  log "rolled back from step $reached; the old store answered otherwise than before the cut-over (discrepancies.txt), and its lifecycle CronJob $OLD-lifecycle is held suspended until that is looked into: kubectl -n $NS patch cronjob $OLD-lifecycle -p '$(jq -c '{spec: .}' "$OUT/lifecycle-held")' resumes it"
  rolled_back_differing=1
  exit 3
fi
log "rolled back from step $reached"
