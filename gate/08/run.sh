#!/usr/bin/env bash
# 08's gate, on the cluster kubectl reaches, with the bundled object store as alasio 5.1.0
# deploys it (SeaweedFS 4.47 all in one, gate/store.sh): the box's cut-over rehearsed, then
# the new layout put through what 08 needs of it.
#
# The rehearsal (program/08-object-store.md, Box cut-over): the store seeded with
# versioned objects, delete markers, a multipart object, tags and the lifecycle pass's
# cursors; inventoried; its metadata copied by `weed filer.meta.backup` into the new
# filers' Valkey; its volumes' PV rebound to the new volume server; the new components
# started (gate/08/components.yaml); everything inventoried again through each gateway
# and compared, objects read back and checked, and the lifecycle pass run on the new
# targets.
#
# Then: read-after-write across the two filers under concurrent writers, versioned
# listings through each, a filer that waits while no volume server is registered, the
# filers' keys confined to their prefix, and Valkey's memory per entry.
#
#   IMAGE=chrislusf/seaweedfs:4.48@sha256:… OUT=/tmp/gate-08 gate/08/run.sh
set -euo pipefail

here=$(dirname "$0")
OUT=${OUT:-$RUNNER_TEMP/gate-08}
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
k() { kubectl -n alasio "$@"; }
fail() { log "FAILED: $*"; exit 1; }

secret() { k get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }
AWS_ACCESS_KEY_ID=$(secret alasio-neon-s3-admin AWS_ACCESS_KEY_ID)
AWS_SECRET_ACCESS_KEY=$(secret alasio-neon-s3-admin AWS_SECRET_ACCESS_KEY)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=us-east-1
endpoint() { echo "http://$(k get svc "$1" -o jsonpath='{.spec.clusterIP}'):8333"; }
s3api() { local at=$1; shift; aws --endpoint-url "$at" s3api "$@"; }
BUCKETS=(neon lake backups workspaces gate)

# Every object of every bucket, through the gateway at $1: versions and delete markers
# where versioning is on, with sizes and ETags; and Neon's bucket's versioning and rules.
inventory() {
  for bucket in "${BUCKETS[@]}"; do
    s3api "$1" list-object-versions --bucket "$bucket" --output json |
      jq -S --arg bucket "$bucket" '[(.Versions // [])[] | {bucket: $bucket, Key, VersionId, Size, ETag, IsLatest}] + [(.DeleteMarkers // [])[] | {bucket: $bucket, Key, VersionId, IsLatest, deleteMarker: true}] | sort_by(.Key, .VersionId) | .[]'
  done
  s3api "$1" get-bucket-versioning --bucket neon --output json | jq -S .
  s3api "$1" get-bucket-lifecycle-configuration --bucket neon --output json | jq -S .
}

old_shell() { k exec -i alasio-seaweedfs-0 -c seaweedfs -- weed shell -master=127.0.0.1:9333 -filer=127.0.0.1:8888; }
new_shell() { k exec -i seaweedfs-master-0 -- weed shell -master=127.0.0.1:9333 -filer=seaweedfs-filer-0.seaweedfs-filer.alasio.svc.cluster.local:8888; }
# The volumes the master knows, by id and collection, as volume.list says them (its
# listing written to $OUT/$2).
volumes() { printf 'volume.list\n' | "$1" >"$OUT/$2"; sed -n 's/.*volume Id:\([0-9]*\), .*Collection:\([^,]*\),.*/\1:\2/p' "$OUT/$2" | sort -n | tr '\n' ' '; }

OLD=$(endpoint alasio-seaweedfs)

log "seeding the 4.47 store"
s3api "$OLD" put-bucket-versioning --bucket gate --versioning-configuration Status=Enabled >/dev/null
head -c 20000000 /dev/urandom >"$OUT/large.bin"
for i in $(seq 1 30); do
  for version in 1 2 3; do
    head -c $((1000 * i + version)) /dev/urandom >"$OUT/small.bin"
    s3api "$OLD" put-object --bucket neon --key "pageserver/tenants/t/timelines/l/layer-$i" --body "$OUT/small.bin" >/dev/null
  done
  s3api "$OLD" put-object --bucket neon --key "safekeeper/t/l/000000010000000000000$i" --body "$OUT/small.bin" >/dev/null
  s3api "$OLD" put-object --bucket lake --key "otel/spans/part-$i.parquet" --body "$OUT/small.bin" --tagging "seed=$i" >/dev/null
  s3api "$OLD" put-object --bucket gate --key "versioned-$((i % 5))" --body "$OUT/small.bin" >/dev/null
done
for i in $(seq 1 5); do
  s3api "$OLD" delete-object --bucket neon --key "pageserver/tenants/t/timelines/l/layer-$i" >/dev/null
  s3api "$OLD" put-object --bucket backups --key "alasio-$i.dump" --body "$OUT/small.bin" >/dev/null
  s3api "$OLD" put-object --bucket workspaces --key "fs/chunks/$i" --body "$OUT/small.bin" >/dev/null
done
aws --endpoint-url "$OLD" s3 cp "$OUT/large.bin" s3://neon/pageserver/tenants/t/timelines/l/large-layer >/dev/null
log "the lifecycle pass, once, for its cursors"
k create job gate-lifecycle --from=cronjob/alasio-seaweedfs-lifecycle
k wait --for=condition=complete job/gate-lifecycle --timeout=900s

log "inventory before"
inventory "$OLD" >"$OUT/inventory-before.json"
before_volumes=$(volumes old_shell volume-list-before.txt)
printf 'fs.meta.save -o /tmp/meta-before.meta\n' | old_shell >"$OUT/fs-meta-save.txt"
log "before: $(grep -c '"Key"' "$OUT/inventory-before.json") versions and markers; volumes: $before_volumes"
[ -n "$before_volumes" ] || fail "the old store lists no volumes"

started=$(date +%s)
log "cut-over: the filer store, its metadata copied by filer.meta.backup from the old filer"
envsubst '$IMAGE $VALKEY_IMAGE $FILER_PASSWORD' <"$here/components.yaml" >"$OUT/components.yaml"
kubectl apply -f <(yq 'select(.metadata.name == "seaweedfs-filer-store")' "$OUT/components.yaml")
k rollout status statefulset/seaweedfs-filer-store --timeout=300s
k run gate-meta-backup --image="$IMAGE" --restart=Never \
  --overrides="$(jq -n --arg image "$IMAGE" '{spec: {securityContext: {runAsNonRoot: true, runAsUser: 1000, seccompProfile: {type: "RuntimeDefault"}}, containers: [{name: "backup", image: $image, args: ["filer.meta.backup", "-filer=alasio-seaweedfs-0.alasio-seaweedfs-peers.alasio.svc.cluster.local:8888", "-config=/etc/backup/filer.toml", "-restart"], securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}, volumeMounts: [{name: "toml", mountPath: "/etc/backup"}]}], volumes: [{name: "toml", secret: {secretName: "seaweedfs-filer-store", items: [{key: "filer.toml", path: "filer.toml"}]}}]}}')"
until k logs gate-meta-backup 2>/dev/null | grep -q 'metadata copied up to'; do
  [ "$(k get pod gate-meta-backup -o jsonpath='{.status.phase}')" != Failed ] || { k logs gate-meta-backup; fail "filer.meta.backup failed"; }
  sleep 2
done
sleep 10
k logs gate-meta-backup >"$OUT/filer-meta-backup.log"
k delete pod gate-meta-backup --wait=true
log "metadata copied: $(grep -o 'metadata copied up to.*' "$OUT/filer-meta-backup.log")"
k scale statefulset/seaweedfs-filer-store --replicas=0
k wait --for=delete pod/seaweedfs-filer-store-0 --timeout=120s

log "cut-over: the volumes' PV rebound to the new volume server"
k scale statefulset/alasio-seaweedfs --replicas=0
k wait --for=delete pod/alasio-seaweedfs-0 --timeout=120s
pv=$(k get pvc data-alasio-seaweedfs-0 -o jsonpath='{.spec.volumeName}')
kubectl patch pv "$pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
k delete pvc data-alasio-seaweedfs-0 --wait=true
kubectl patch pv "$pv" --type json -p '[{"op":"remove","path":"/spec/claimRef"}]'
k apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data-seaweedfs-volume-0, namespace: alasio }
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  volumeName: $pv
  resources: { requests: { storage: 100Gi } }
EOF
until [ "$(k get pvc data-seaweedfs-volume-0 -o jsonpath='{.status.phase}')" = Bound ]; do sleep 1; done

log "cut-over: the new components"
kubectl apply -f "$OUT/components.yaml"
for workload in statefulset/seaweedfs-filer-store statefulset/seaweedfs-master statefulset/seaweedfs-volume statefulset/seaweedfs-filer deploy/s3-a deploy/s3-b; do
  k rollout status "$workload" --timeout=600s
done
log "cut-over done in $(($(date +%s) - started)) s (without the old store's stop and the images' pulls in a real one)"

A=$(endpoint s3-a)
B=$(endpoint s3-b)
after_volumes=$(volumes new_shell volume-list-after.txt)
log "after, volumes: $after_volumes(before: $before_volumes)"
[ "$after_volumes" = "$before_volumes" ] || fail "the volume server holds volumes $after_volumes, not $before_volumes"
for gateway in A B; do
  inventory "${!gateway}" >"$OUT/inventory-after-$gateway.json"
  if diff -u "$OUT/inventory-before.json" "$OUT/inventory-after-$gateway.json" >"$OUT/inventory-diff-$gateway.txt"; then
    log "inventory through gateway $gateway: identical"
  else
    fail "inventory through gateway $gateway differs (inventory-diff-$gateway.txt)"
  fi
done
log "objects read back, each version's ETag"
checked=0
while read -r line; do
  bucket=$(jq -r .bucket <<<"$line")
  key=$(jq -r .Key <<<"$line")
  version=$(jq -r .VersionId <<<"$line")
  etag=$(jq -r .ETag <<<"$line")
  args=(--bucket "$bucket" --key "$key")
  [ "$version" = null ] || args+=(--version-id "$version")
  got=$(s3api "$B" get-object "${args[@]}" "$OUT/got.bin" --query ETag --output text)
  [ "$got" = "$etag" ] || fail "$bucket/$key $version reads back as $got, not $etag"
  checked=$((checked + 1))
done < <(jq -c 'select(.Key != null and .deleteMarker != true)' "$OUT/inventory-before.json")
cmp "$OUT/large.bin" <(aws --endpoint-url "$A" s3 cp s3://neon/pageserver/tenants/t/timelines/l/large-layer -) || fail "the multipart object reads back changed"
log "read back: $checked versions, each with its ETag, and the multipart object byte for byte"
s3api "$A" get-object-tagging --bucket lake --key otel/spans/part-7.parquet --output json | jq -e '.TagSet[] | select(.Key == "seed" and .Value == "7")' >/dev/null || fail "a tag was lost"

log "the lifecycle pass on the new targets"
k create job gate-lifecycle-after --image="$IMAGE" -- sh -c "printf 's3.lifecycle.run-shard -shards 0-15 -s3 s3-a:18333 -events 0 -runtime 2m\n' | weed shell -master=seaweedfs-master-0.seaweedfs-master.alasio.svc.cluster.local:9333 -filer=seaweedfs-filer-0.seaweedfs-filer.alasio.svc.cluster.local:8888"
k wait --for=condition=complete job/gate-lifecycle-after --timeout=600s
k logs job/gate-lifecycle-after >"$OUT/lifecycle-after.log"

log "read-after-write across the filers, four writers at once, 100 objects each"
write_read() { # <writer> <put through> <read through>
  local misses=0
  for i in $(seq 1 100); do
    printf '%s-%s' "$1" "$i" >"$OUT/raw-$1"
    s3api "$2" put-object --bucket gate --key "raw/$1/$i" --body "$OUT/raw-$1" >/dev/null
    s3api "$3" head-object --bucket gate --key "raw/$1/$i" >/dev/null 2>&1 || misses=$((misses + 1))
  done
  echo "$misses"
}
write_read 1 "$A" "$B" >"$OUT/raw-misses-1" &
write_read 2 "$B" "$A" >"$OUT/raw-misses-2" &
write_read 3 "$A" "$B" >"$OUT/raw-misses-3" &
write_read 4 "$B" "$A" >"$OUT/raw-misses-4" &
wait
misses=$(cat "$OUT"/raw-misses-* | paste -sd+ | bc)
log "read-after-write misses across filers: $misses of 400"
versions_a=$(s3api "$A" list-object-versions --bucket gate --prefix versioned- --query 'length(Versions)')
versions_b=$(s3api "$B" list-object-versions --bucket gate --prefix versioned- --query 'length(Versions)')
log "a versioned listing: $versions_a versions through A, $versions_b through B"
[ "$misses" = 0 ] && [ "$versions_a" = "$versions_b" ] || fail "the filers disagree"

log "a filer waits while no volume server is registered"
k scale statefulset/seaweedfs-volume --replicas=0
k wait --for=delete pod/seaweedfs-volume-0 --timeout=120s
sleep 20
k delete pod seaweedfs-filer-1 --wait=true
sleep 60
waiting=$(k get pod seaweedfs-filer-1 -o jsonpath='{.status.initContainerStatuses[0].state}')
log "filer-1, a minute on with no volume server: $waiting"
grep -q running <<<"$waiting" || fail "filer-1 did not wait for a volume server"
k scale statefulset/seaweedfs-volume --replicas=1
k rollout status statefulset/seaweedfs-filer --timeout=300s
log "filer-1 ready once the volume server registered"

password=$FILER_PASSWORD
valkey() { k exec seaweedfs-filer-store-0 -- valkey-cli --no-auth-warning "$@"; }
log "the filers' keys under their prefix, the ACL user confined to it"
outside=$(valkey --user filer --pass "$password" --scan --pattern '*' | grep -vc '^seaweedfs:' || true)
denied=$(valkey --user filer --pass "$password" set gate-outside 1 2>&1 || true)
log "keys outside the prefix: $outside; a write outside it: $denied"
[ "$outside" = 0 ] && grep -qi 'no permissions' <<<"$denied" || fail "the ACL user is not confined to its prefix"

log "Valkey's memory per entry, 2,000 small objects added"
memory() { valkey --user filer --pass "$password" info memory | sed -n 's/^used_memory:\([0-9]*\).*/\1/p'; }
keys() { valkey --user filer --pass "$password" dbsize | tr -d '\r'; }
memory_before=$(memory)
keys_before=$(keys)
printf x >"$OUT/one-byte"
seq 1 2000 | xargs -P 16 -I{} aws --endpoint-url "$A" s3api put-object --bucket gate --key many/{} --body "$OUT/one-byte" --output text --query ETag >/dev/null
memory_after=$(memory)
keys_after=$(keys)
log "Valkey: $keys_before → $keys_after keys, $memory_before → $memory_after bytes: $(((memory_after - memory_before) / 2000)) bytes an object, so as many MB a million"
valkey --user filer --pass "$password" info memory >"$OUT/valkey-memory.txt"
kubectl top pod -n alasio --containers >"$OUT/top.txt" 2>&1 || true
log "gate passed"
