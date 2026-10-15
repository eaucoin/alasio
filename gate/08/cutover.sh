#!/usr/bin/env bash
# 08's cut-over (program/progress/object-store.md, C1): the bundled object store, all in one
# as the release before 08 runs it, moved onto 08's parts without starting them empty, and
# checked whole before anything of the stack writes to it again. On the box, run detached
# as root on the host, as its upgrades are (it reads the volume's directory there); in CI,
# by gate/08/rehearse.ts. Everything is recorded in OUT.
#
#   RELEASE=<file of gate/08/render.ts's List> CSI_NAMESPACE=<workspaceStorage.csi.namespace>
#     OUT=<dir> [UPGRADE=<command> UPGRADE_CHECK=<command>] [MIN_FREE_GB=15] [READ_BACK=50]
#     [WAIT=900] [JOBS_WAIT=2700] [EXEC_TIMEOUT=1800] [UPGRADE_TIMEOUT=3600] gate/08/cutover.sh
#
# UPGRADE applies the release, as the configuration's owner does (`sudo -u <owner> env
# HOME=<their home> npx --yes alasio@<v> upgrade`); UPGRADE_CHECK, which must succeed before
# any writer stops, proves it can (the same with `--version`). Without UPGRADE the cut-over
# stops once the store is checked, before any writer starts. Of each bucket, READ_BACK
# versions are read through the old store before anything of it is touched, and read back
# through each of the release's gateways: the same bytes, by their SHA-256.
#
# Run again, it goes on from the step it reached, as recorded in OUT/step, which only grows:
# what each step changes is marked or recorded once, and the metadata's copy, once the
# volume has moved, is never made again. Until UPGRADE begins, gate/08/rollback.sh puts the
# old store back from wherever it stopped. Once it has begun (OUT/point-of-no-return), the
# writers write to the new store alone: the cut-over is rolled forward, a run again going on
# with the upgrade, and not back.
set -euo pipefail
umask 077
here=$(cd "$(dirname "$0")" && pwd)
: "${RELEASE:?the objects of the release, as gate/08/render.ts prints them}" "${OUT:?where to record}"
: "${CSI_NAMESPACE:?the namespace of the JuiceFS driver, workspaceStorage.csi.namespace}"
MIN_FREE_GB=${MIN_FREE_GB:-15}
READ_BACK=${READ_BACK:-50}
JOBS_WAIT=${JOBS_WAIT:-2700}
UPGRADE_TIMEOUT=${UPGRADE_TIMEOUT:-3600}
mkdir -p "$OUT"
# shellcheck source=gate/08/lib.sh
. "$here/lib.sh"
hold
[ ! -e "$OUT/rolled-back-from-step" ] || fail "the cut-over recorded in $OUT was rolled back: a cut-over again records in an OUT of its own"
[ ! -e "$OUT/rolling-back" ] || fail "a rollback of the cut-over recorded in $OUT began at $(cat "$OUT/rolling-back"): gate/08/rollback.sh finishes it"
[ -z "${UPGRADE:-}" ] || [ -n "${UPGRADE_CHECK:-}" ] || fail "UPGRADE is given without UPGRADE_CHECK, which proves it can run before any writer stops"
prerequisites "${UPGRADE:+$UPGRADE_CHECK}"
reached=$(reached)

# What the cut-over leaves behind should it stop: nothing of the metadata's copy.
cleanup() {
  k delete pod cutover-meta-backup --ignore-not-found --wait=false >/dev/null 2>&1 || true
  k delete secret cutover-filer-toml --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The upgrade, which starts the writers; then the writers as they were, and a lifecycle pass
# on the new store. From its start the cut-over is rolled forward: a run again goes on here.
upgrade() {
  step 9 "the upgrade, which starts the writers: from here the cut-over is rolled forward, not back"
  [ -e "$OUT/point-of-no-return" ] || date -u +%FT%TZ >"$OUT/point-of-no-return"
  printf '%s\n' "$UPGRADE" >"$OUT/upgrade"
  timeout "$UPGRADE_TIMEOUT" bash -c "$UPGRADE" || fail "the upgrade failed: the cut-over run again goes on with it"
  start_writers
  k delete job cutover-lifecycle --ignore-not-found --wait=true --timeout="${WAIT}s" >/dev/null
  k create job cutover-lifecycle --from=cronjob/alasio-seaweedfs-lifecycle >/dev/null
  conditions() { k get job cutover-lifecycle -o jsonpath='{.status.conditions[?(@.status=="True")].type}'; }
  ended() { [ -n "$(conditions)" ]; }
  wait_for 2760 "the lifecycle pass on the new store to end" ended
  k logs job/cutover-lifecycle >"$OUT/lifecycle-after.log" || true
  # Complete among its conditions, beside SuccessCriteriaMet where the cluster sets that too.
  grep -qw Complete <<<"$(conditions)" || fail "the lifecycle pass on the new store failed (lifecycle-after.log)"
  log "the lifecycle pass on the new store: $(grep -o 'shards [0-9-]* complete.*' "$OUT/lifecycle-after.log" | tail -n 1)"
  injected 9
  log "cut over"
}

if [ -e "$OUT/point-of-no-return" ]; then
  [ -n "${UPGRADE:-}" ] || fail "the upgrade began at $(cat "$OUT/point-of-no-return"): give UPGRADE, which the cut-over goes on with"
  log "the upgrade began at $(cat "$OUT/point-of-no-return"): going on with it"
  upgrade
  exit 0
fi

k get statefulset "$OLD" >/dev/null || fail "no $OLD runs: this cluster's store is not the one the cut-over is for"
# The volume: the one recorded, on whichever claim holds it now, or the old store's.
if [ -e "$OUT/volume.tsv" ]; then
  read -r pv _ _ <"$OUT/volume.tsv"
  holder=$(claim_of "$pv")
  # Held by neither only between the rebind's deleting one claim and binding the other, at step 6.
  [ "$holder" = "$OLD_CLAIM" ] || [ "$holder" = "$NEW_CLAIM" ] || { [ -z "$holder" ] && [ "$reached" -ge 6 ]; } ||
    fail "the PV $pv the cut-over recorded is held by '${holder}', neither $OLD_CLAIM nor $NEW_CLAIM"
else
  pv=$(k get pvc "$OLD_CLAIM" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
  [ -n "$pv" ] || fail "$OLD_CLAIM is bound to no PV"
fi
dir=$(host_path "$pv")
[ -n "$dir" ] && [ -d "$dir" ] || fail "the PV $pv's directory, '$dir', is not on this machine"
[ "$reached" -eq 0 ] || log "going on from step $reached"

step 1 "the disk: what is free where the volumes are"
free_gb=$(df --output=avail -BG "$dir" | tail -n 1 | tr -dc 0-9)
[ "$free_gb" -ge "$MIN_FREE_GB" ] || fail "$free_gb GB free under $dir, less than $MIN_FREE_GB"
injected 1

step 2 "every writer stopped, the old store reached by nothing: the CronJobs suspended, their Jobs ended, the scalers paused, the workloads at none, the sessions suspended"
stop_writers "$CSI_NAMESPACE" "$JOBS_WAIT"
seal_old
# The old filer writes its log of changes out within a minute of them: none is left unwritten.
[ "$reached" -ge 2 ] || sleep 70
injected 2

step 3 "the volume, before anything of it is touched: kept on deletion, and its data files counted"
kubectl patch pv "$pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}' >/dev/null
[ "$(kubectl get pv "$pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" = Retain ] || fail "the PV $pv is not kept on deletion"
[ -e "$OUT/volume.tsv" ] || printf '%s\t%s\t%s\n' "$pv" "$dir" "$(dat_files "$pv")" >"$OUT/volume.tsv"
read -r _ _ dat_before <"$OUT/volume.tsv"
ls -la "$dir" >"$OUT/volume-files-at-$(date -u +%H%M%S).txt"
log "the PV $pv at $dir, its reclaim policy Retain, $dat_before data files"
injected 3

step 4 "the inventory, through the old store"
if [ ! -e "$OUT/inventory-before.json" ]; then
  inventory "$OLD_POD" lifecycle >"$OUT/inventory-before.part.json"
  volumes old_shell volume-list-before.txt >"$OUT/volumes-before.txt"
  [ -n "$(cat "$OUT/volumes-before.txt")" ] || fail "the old store's masters list no volumes (volume-list-before.txt)"
  [ "$(cat "$OUT/volumes-before.txt")" = "$(volumes_on_disk "$pv")" ] || fail "the old store's masters know volumes $(cat "$OUT/volumes-before.txt"), the disk holds $(volumes_on_disk "$pv")"
  entries old_shell /buckets entries-before-saved.txt >"$OUT/entries-before.txt"
  log_files old_shell /topics/.system/log >"$OUT/log-before.txt"
  printf 'fs.meta.save -o /tmp/meta-before.meta /\n' | old_shell >"$OUT/fs-meta-save.txt"
  kx "$OLD_POD" -c seaweedfs -- cat /tmp/meta-before.meta >"$OUT/meta-before.meta"
  kx "$OLD_POD" -c seaweedfs -- tar -C /data -cf - filerldb2 m9333 >"$OUT/old-filer-and-master.tar"
  # What the versions read back hold, as the old store reads them: what each store is read
  # back against, as it answered before anything of it was touched.
  read_back_set "$OUT/inventory-before.part.json" "$READ_BACK" >"$OUT/read-back.tsv"
  contents "$OLD_POD" <"$OUT/read-back.tsv" >"$OUT/contents-before.tsv" || fail "the old store does not read every version read back (read-back.tsv)"
  mv "$OUT/inventory-before.part.json" "$OUT/inventory-before.json"
fi
volumes_before=$(cat "$OUT/volumes-before.txt")
entries_before=$(cat "$OUT/entries-before.txt")
log "before: $(jq '[.[].versions[]] | length' "$OUT/inventory-before.json") versions and markers; $entries_before under /buckets; $(wc -l <"$OUT/log-before.txt") files of the log of changes; volumes $volumes_before"
on_disk=$(volumes_on_disk "$pv")
for volume in $volumes_before; do grep -qw -- "$volume" <<<"$on_disk" || fail "the masters knew the volume $volume, which is not on the disk"; done
injected 4

step 5 "the filer store, the release's own, at one replica: the old filer's metadata copied into it"
if [ "$reached" -ge 6 ]; then
  # The old store stopped and its volume moved: its metadata was copied, and is not again.
  log "copied before the volume moved, at the step this cut-over went on from"
else
  # What the release's objects would overwrite, as it is now, and which are the release's own,
  # for the rollback to put back and to delete: recorded once, before anything is applied.
  if [ ! -e "$OUT/overwritten.json" ]; then
    applicable "$RELEASE" | jq -c '.items[] | {kind, name: .metadata.name, namespace: .metadata.namespace}' | while read -r object; do
      kind=$(jq -r .kind <<<"$object") name=$(jq -r .name <<<"$object") ns=$(jq -r '.namespace // empty' <<<"$object")
      if kubectl ${ns:+-n "$ns"} get "$kind" "$name" -o json >"$OUT/object.json" 2>/dev/null; then
        cat "$OUT/object.json"
      else
        jq -c . <<<"$object" >>"$OUT/release-only.part.jsonl"
      fi
    done | jq -s '{apiVersion: "v1", kind: "List", items: .}' >"$OUT/overwritten.part.json"
    rm -f "$OUT/object.json"
    touch "$OUT/release-only.part.jsonl"
    mv "$OUT/release-only.part.jsonl" "$OUT/release-only.jsonl"
    mv "$OUT/overwritten.part.json" "$OUT/overwritten.json"
  fi
  # The filer store's users' passwords, kept by the stack's setup in its root Secret, which the
  # release's keeps: made where missing, and given to kubectl on its input, in no argument.
  secrets=$(k get secret alasio-neon-root -o jsonpath='{.data.secrets\.json}' | base64 -d)
  for key in filerStoreFilerPassword filerStoreCollectorPassword filerStoreBackupPassword; do
    jq -e --arg key "$key" 'has($key)' <<<"$secrets" >/dev/null && continue
    secrets=$(VALUE=$(head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=') jq --arg key "$key" '.[$key] = env.VALUE' <<<"$secrets")
  done
  jq . <<<"$secrets" | jq -Rs '{data: {"secrets.json": (. | @base64)}}' | k patch secret alasio-neon-root --patch-file /dev/stdin >/dev/null
  for user in filer collector backup; do
    key=filerStore$(tr '[:lower:]' '[:upper:]' <<<"${user:0:1}")${user:1}Password
    jq -j --arg key "$key" '.[$key]' <<<"$secrets" | k create secret generic "$FILER_STORE-$user" --from-file=password=/dev/stdin --dry-run=client -o json |
      jq '.metadata.labels = {"app.kubernetes.io/managed-by": "alasio-neon-setup", "app.kubernetes.io/part-of": "alasio-neon"}' | k apply -f - >/dev/null
  done
  unset secrets
  # The release's policies, some of which would select the old store's pod as they do the new
  # gateways', applied as not selecting it, as the seal has the old ones; applied as alasio
  # applies, so its own apply in the upgrade owns every field of them again.
  applicable "$RELEASE" '(.kind == "PriorityClass") or (.kind == "NetworkPolicy") or (.metadata.name == "'"$FILER_STORE"'") or (.kind == "ConfigMap" and .metadata.name == "'"$FILER"'")' |
    jq --arg pod "$OLD_POD" '.items |= map(if .kind == "NetworkPolicy" then .spec.podSelector.matchExpressions = ((.spec.podSelector.matchExpressions // []) + [{key: "statefulset.kubernetes.io/pod-name", operator: "NotIn", values: [$pod]}]) else . end)' |
    apply_as_alasio
  seal_old
  store_ready() { [ "$(k get statefulset "$FILER_STORE" -o jsonpath='{.status.readyReplicas}')" = 1 ]; }
  wait_for "$WAIT" "the filer store to be ready" store_ready
  injected 5a
  image=$(jq -r '.items[] | select(.kind == "Deployment" and .metadata.name == "'"$GATEWAY"'") | .spec.template.spec.containers[0].image' "$RELEASE")
  labels=$(jq -c --arg copier "$COPIER" '.items[] | select(.kind == "StatefulSet" and .metadata.name == "'"$FILER"'") | .spec.template.metadata.labels + {($copier | split("=")[0]): ($copier | split("=")[1])}' "$RELEASE")
  # filer.meta.backup reads its store's configuration from the file alone, not the filers'
  # environment: the release's filer.toml, with the filers' user's password in it, in a Secret,
  # written from a pipe, never to a file, the password in no command's arguments.
  k delete pod cutover-meta-backup --ignore-not-found --wait=true --timeout="${WAIT}s" >/dev/null
  jq -r '.items[] | select(.kind == "ConfigMap" and .metadata.name == "'"$FILER"'") | .data["filer.toml"]' "$RELEASE" |
    PASSWORD=$(k get secret "$FILER_STORE-filer" -o jsonpath='{.data.password}' | base64 -d) awk '{ print } /^username = / { print "password = \"" ENVIRON["PASSWORD"] "\"" }' |
    k create secret generic cutover-filer-toml --from-file=filer.toml=/dev/stdin --dry-run=client -o json | k apply -f - >/dev/null
  k run cutover-meta-backup --image="$image" --restart=Never --labels="$(jq -r 'to_entries | map("\(.key)=\(.value)") | join(",")' <<<"$labels")" --overrides="$(jq -n --arg image "$image" --arg filer "$OLD_FILER" '{
    spec: {
      securityContext: {runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, seccompProfile: {type: "RuntimeDefault"}},
      containers: [{
        name: "backup", image: $image, args: ["filer.meta.backup", "-filer=\($filer)", "-config=/etc/backup/filer.toml", "-restart"],
        securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}},
        volumeMounts: [{name: "config", mountPath: "/etc/backup", readOnly: true}]
      }],
      volumes: [{name: "config", secret: {secretName: "cutover-filer-toml"}}]
    }}')" >/dev/null
  copied() {
    [ "$(k get pod cutover-meta-backup -o jsonpath='{.status.phase}')" != Failed ] || { k logs cutover-meta-backup >"$OUT/filer-meta-backup.log" || true; fail "filer.meta.backup failed (filer-meta-backup.log)"; }
    k logs cutover-meta-backup 2>/dev/null | grep -q 'metadata copied up to'
  }
  wait_for "$WAIT" "filer.meta.backup to copy the old filer's metadata" copied
  # Nothing is written to the old store, whose log has long been written out: what follows
  # the copy is the tail of the subscription, which it has reached in the gate's 12 s.
  sleep 30
  k logs cutover-meta-backup >"$OUT/filer-meta-backup.log"
  cleanup
  log "metadata copied: $(grep -o 'metadata copied up to.*' "$OUT/filer-meta-backup.log" | tail -n 1)"
  mark statefulset "$FILER_STORE" "$NS" '{"replicas":1}'
  k scale statefulset/"$FILER_STORE" --replicas=0 >/dev/null
  wait_for "$WAIT" "the filer store's pod to be gone" none k get pods --field-selector "metadata.name=$FILER_STORE-0" -o name
fi
injected 5

step 6 "the volume rebound to the release's volume server, its data files read back"
mark statefulset "$OLD" "$NS" "$(k get statefulset "$OLD" -o json | jq -c '{replicas: (.spec.replicas // 1)}')"
k scale statefulset/"$OLD" --replicas=0 >/dev/null
wait_for "$WAIT" "the old store's pod to be gone" none k get pods --field-selector "metadata.name=$OLD_POD" -o name
unseal_old
injected 6a
# The old store stopped, its volumes are those on the disk: its masters may have made empty
# ones since the inventory, for a collection with too few to write to.
[ -e "$OUT/volumes-stopped.txt" ] || volumes_on_disk "$pv" >"$OUT/volumes-stopped.txt"
stopped=$(cat "$OUT/volumes-stopped.txt")
for volume in $volumes_before; do grep -qw -- "$volume" <<<"$stopped" || fail "the disk lacks the volume $volume the masters knew"; done
[ "$stopped" = "$volumes_before" ] || log "the old masters made volumes since the inventory: $stopped (were $volumes_before)"
rebind "$pv" "$OLD_CLAIM" "$NEW_CLAIM" "$(jq -c '.items[] | select(.kind == "StatefulSet" and .metadata.name == "'"$VOLUME_SERVER"'") | .spec.volumeClaimTemplates[0]' "$RELEASE")"
on_disk=$(volumes_on_disk "$pv")
for volume in $stopped; do grep -qw -- "$volume" <<<"$on_disk" || fail "the PV $pv lacks the volume $volume the old store left"; done
log "$NEW_CLAIM bound to $pv, its $(dat_files "$pv") data files there, every one the old store left"
injected 6

step 7 "the release's store, its CronJobs suspended, each marked to be on once the writers are"
applicable "$RELEASE" | jq '.items |= map(if .kind == "CronJob" then .spec.suspend = true | .metadata.annotations["'"$MARK"'"] = "{\"suspend\":false}" else . end)' | apply_as_alasio
unmark statefulset "$FILER_STORE" "$NS"
for workload in statefulset/"$FILER_STORE" statefulset/"$MASTER" statefulset/"$VOLUME_SERVER" statefulset/"$FILER" deployment/"$GATEWAY"; do
  k rollout status "$workload" --timeout="${WAIT}s" || fail "$workload is not ready within ${WAIT}s"
done
injected 7

step 8 "the store checked, before any writer starts"
volumes_after=$(volumes new_shell volume-list-after.txt)
[ -n "$volumes_after" ] || fail "the release's masters list no volumes (volume-list-after.txt)"
[ "$volumes_after" = "$(volumes_on_disk "$pv")" ] || fail "the release's masters know volumes $volumes_after, the disk holds $(volumes_on_disk "$pv")"
highest=$(tr ' ' '\n' <<<"$stopped" | sed -n 's/^\([0-9]*\):.*/\1/p' | sort -n | tail -n 1)
for volume in $stopped; do grep -qw -- "$volume" <<<"$volumes_after" || fail "the masters do not know the volume $volume"; done
# Volumes made since, for the filers' log of changes as they start, are above every old one.
extra=$(for volume in $volumes_after; do grep -qw -- "$volume" <<<"$stopped" || printf '%s ' "$volume"; done)
for volume in $extra; do [ "${volume%%:*}" -gt "${highest:-0}" ] || fail "the masters know the volume $volume, which the old store had not, below its highest, $highest"; done
printf '%s\n' "$extra" >"$OUT/volumes-made.txt"
entries_after=$(entries new_shell /buckets entries-after-saved.txt)
[ "$entries_after" = "$entries_before" ] || fail "the filers count $entries_after under /buckets, not $entries_before"
log_files new_shell /topics/.system/log >"$OUT/log-after.txt"
comm -23 "$OUT/log-before.txt" "$OUT/log-after.txt" >"$OUT/log-missing.txt"
[ ! -s "$OUT/log-missing.txt" ] || fail "the filers' log of changes lacks $(wc -l <"$OUT/log-missing.txt") of the old one's files (log-missing.txt)"
# The inventory, a listing, against the old store's listing, which the same code answers in
# both releases; each version read back against what the old store read it as.
for gateway in $(k get pods -l app.kubernetes.io/component=seaweedfs -o jsonpath='{.items[*].metadata.name}'); do
  inventory "$gateway" lifecycle >"$OUT/inventory-after-$gateway.json"
  diff -u "$OUT/inventory-before.json" "$OUT/inventory-after-$gateway.json" >"$OUT/inventory-diff-$gateway.txt" || fail "the inventory through $gateway differs (inventory-diff-$gateway.txt)"
  contents "$gateway" <"$OUT/read-back.tsv" >"$OUT/contents-after-$gateway.tsv" || fail "$gateway does not read every version read back (read-back.tsv)"
  diff -u "$OUT/contents-before.tsv" "$OUT/contents-after-$gateway.tsv" >"$OUT/contents-diff-$gateway.txt" ||
    fail "through $gateway, versions read back hold other bytes than the old store's (contents-diff-$gateway.txt)"
  log "through $gateway: the inventory identical, $(wc -l <"$OUT/contents-before.tsv") versions read back, each its bytes as before"
done
log "checked: volumes $volumes_after (made since: ${extra:-none}); $entries_after under /buckets; the log of changes whole"
injected 8

if [ -z "${UPGRADE:-}" ]; then
  log "stopped before the upgrade: gate/08/rollback.sh puts the old store back from here"
  exit 0
fi
upgrade
