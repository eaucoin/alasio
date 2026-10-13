# 08's cut-over (gate/08/cutover.sh) and its rollback (gate/08/rollback.sh): what both use.
# Sourced, each script having set `umask 077` first; OUT is where each records what it finds.
# shellcheck disable=SC2034 # the names are the scripts' that source this

NS=${NS:-alasio}
# The longest a wait takes, in seconds, unless it says another.
WAIT=${WAIT:-900}
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log" >&2; }
fail() {
  log "FAILED: $*"
  exit 1
}
k() { kubectl -n "$NS" "$@"; }
# The longest a command run in a pod takes, in seconds.
EXEC_TIMEOUT=${EXEC_TIMEOUT:-1800}
kx() { timeout "$EXEC_TIMEOUT" kubectl -n "$NS" exec "$@"; }
# Applies the objects of the List on stdin as alasio applies its own, server-side as its field
# manager, so its next apply owns every field of them, those set here among them.
apply_as_alasio() { kubectl apply --server-side --field-manager=alasio --force-conflicts -f - >/dev/null; }
# The claim, in the namespace, that holds the PV $1: its name, or nothing.
claim_of() { kubectl get pv "$1" -o json | jq -r --arg ns "$NS" 'select(.status.phase == "Bound" and .spec.claimRef.namespace == $ns) | .spec.claimRef.name'; }

# The release before 08's store, all in one, and the release's parts, by their names.
OLD=alasio-seaweedfs
OLD_POD=alasio-seaweedfs-0
OLD_CLAIM=data-alasio-seaweedfs-0
OLD_FILER=alasio-seaweedfs-0.alasio-seaweedfs-peers.$NS.svc.cluster.local:8888
MASTER=alasio-seaweedfs-master
VOLUME_SERVER=alasio-seaweedfs-volume
FILER=alasio-seaweedfs-filer
FILER_STORE=alasio-seaweedfs-filer-store
GATEWAY=alasio-seaweedfs
NEW_CLAIM=data-alasio-seaweedfs-volume-0

# What the scripts mark each object they change with, as JSON of what it was, to put it
# back as it was; a mark there already is kept, so a script run again changes nothing of it.
MARK=alasio.dev/cutover
# The label of the pod that copies the old filer's metadata, which the old store's seal admits alone.
COPIER=alasio.dev/cutover=meta-backup
# The installation's objects, main's and its branch environments'.
INSTANCE=app.kubernetes.io/instance=alasio

# Runs "$@" every 2 s until it succeeds, for at most $1 seconds; fails, saying it waited for
# $2, if it never does.
wait_for() {
  local most=$1 what=$2
  shift 2
  local deadline=$((SECONDS + most))
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || fail "waited ${most}s for $what"
    sleep 2
  done
}
# Whether "$@" succeeds and prints nothing.
none() {
  local out
  out=$("$@") || return 1
  [ -z "$out" ]
}

# The step the cut-over recorded it reached, 0 before any.
reached() { cat "$OUT/step" 2>/dev/null || echo 0; }
# What each script needs of the host, before it changes anything: its tools on root's PATH,
# node new enough for render.ts, the cluster reached by kubectl, and the release's command
# line runnable as the configuration's owner, which the command $1 proves (as its
# `npx alasio@<v> --version`), if given.
prerequisites() {
  local tool
  for tool in jq kubectl node numfmt flock timeout sudo; do command -v "$tool" >/dev/null || fail "$tool is not on root's PATH"; done
  node -e 'const [major, minor] = process.versions.node.split(".").map(Number); process.exit(major > 22 || (major === 22 && minor >= 18) ? 0 : 1)' ||
    fail "node $(node --version) is older than 22.18, which render.ts needs"
  kubectl get --raw=/readyz >/dev/null 2>&1 || fail "kubectl does not reach the cluster: give it k3s's kubeconfig (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)"
  if [ -n "${1:-}" ]; then
    said=$(timeout 600 bash -c "$1" 2>&1) || fail "the release's command line does not run as the configuration's owner: $1 said: $said"
    log "the release's command line runs as the configuration's owner: $said"
  fi
}

# Holds OUT for this script alone, while it runs: a cut-over and a rollback, or two of
# either, never act on one OUT at once.
hold() {
  exec 9>"$OUT/lock"
  flock -n 9 || fail "another cut-over or rollback holds $OUT"
}

# Records that the script reached step $1, saying $2, unless it reached a later one before: the
# step recorded only grows, so a run again records no less than the one before it reached. A
# failure is injected once step $1 has run where FAIL_AT is it, as the rehearsal asks
# (gate/08/rehearse.ts).
step() {
  [ "$(reached)" -ge "$1" ] || printf '%s\n' "$1" >"$OUT/step"
  log "$1. $2"
}
injected() { [ "${FAIL_AT:-}" != "$1" ] || fail "a failure injected after step $1"; }

# The items of the List in the file $1 whose kind the cluster serves (no VerticalPodAutoscaler
# or ScaledObject where it has no VPA or KEDA), and that the jq condition $2 holds of, as a List.
applicable() {
  local kinds
  kinds=$(kubectl api-resources --no-headers | awk '{print $NF}' | sort -u | jq -R . | jq -s .)
  jq --argjson kinds "$kinds" "{apiVersion: \"v1\", kind: \"List\", items: [.items[] | select(.kind as \$kind | \$kinds | index(\$kind)) | select(${2:-true})]}" "$1"
}
# Whether the cluster serves the kind $1.
served() { kubectl api-resources --no-headers | awk '{print $NF}' | grep -qx "$1"; }

# The label selector a LabelSelector, as JSON on stdin, says, as kubectl's -l takes it.
selector_of() {
  jq -r '[(.matchLabels // {} | to_entries[] | "\(.key)=\(.value)"),
    ((.matchExpressions // [])[] | if .operator == "In" then "\(.key) in (\(.values | join(",")))"
      elif .operator == "NotIn" then "\(.key) notin (\(.values | join(",")))"
      elif .operator == "Exists" then .key else "!\(.key)" end)] | join(",")'
}

# --- what writes to the store, stopped and started again ---

# The objects of kind $1 of the installation in every namespace, as namespace/name per line.
installation() { kubectl get "$1" -A -l "$INSTANCE" -o json | jq -r '.items[] | "\(.metadata.namespace)/\(.metadata.name)"'; }
# Marks the object $2 of kind $1, in the namespace $3, with the JSON $4, unless it is marked.
mark() {
  kubectl -n "$3" get "$1" "$2" -o json | jq -e --arg mark "$MARK" '.metadata.annotations[$mark] != null' >/dev/null ||
    kubectl -n "$3" annotate "$1" "$2" "$MARK=$4" >/dev/null
}
# What the mark of the object $2 of kind $1, in the namespace $3, says, or nothing.
marked() { kubectl -n "$3" get "$1" "$2" -o json | jq -c --arg mark "$MARK" '.metadata.annotations[$mark] // empty | fromjson'; }
unmark() { kubectl -n "$3" annotate "$1" "$2" "$MARK-" >/dev/null; }

# The workloads that write to the store, through it, as kind/name in namespace per line
# (namespace kind/name): alasio and the data stack, main's and each branch environment's,
# but the store itself, the old one and the release's.
writers() {
  local kind
  for kind in deployment statefulset; do
    kubectl get "$kind" -A -l "$INSTANCE" -o json | jq -r --arg kind "$kind" --arg ns "$NS" \
      '.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "alasio" or .metadata.labels["alasio.dev/stack"] == "neon")
        | select((.metadata.labels["app.kubernetes.io/component"] // "") | test("^seaweedfs") | not)
        | "\(.metadata.namespace) \($kind)/\(.metadata.name)"'
  done
}

# Stops every writer, and waits until none runs: the installation's CronJobs suspended and
# the Jobs running, theirs, JuiceFS's in the driver's namespace $1, waited for, for at most
# $2 seconds; KEDA's ScaledObjects paused at none; alasio and the data stack scaled to none;
# and every Sandbox suspended, its pod gone and with it the JuiceFS mount pods of $1, but
# folder workspaces' bayma's, which reaches no store, Neon nor JuiceFS, and is the operator's
# own session on the machine. Each marked with how it was. The Jobs are looked for again as the
# pods are, as one may start between the wait for them and the CronJobs' suspension taking.
stop_writers() {
  local csi=$1 jobs_wait=$2 ref ns name kind
  for ref in $(installation cronjob); do
    ns=${ref%%/*} name=${ref#*/}
    mark cronjob "$name" "$ns" "$(kubectl -n "$ns" get cronjob "$name" -o json | jq -c '{suspend: (.spec.suspend // false)}')"
    kubectl -n "$ns" patch cronjob "$name" -p '{"spec":{"suspend":true}}' >/dev/null
  done
  running_jobs() {
    { kubectl get job -A -l "$INSTANCE" -o json; kubectl -n "$csi" get job -o json; } |
      jq -r '.items[] | select((.status.active // 0) > 0) | "\(.metadata.namespace)/\(.metadata.name)"'
  }
  log "the Jobs that run, waited for: $(running_jobs | tr '\n' ' ')"
  wait_for "$jobs_wait" "the Jobs that run to end ($(running_jobs | tr '\n' ' '))" none running_jobs
  if served ScaledObject; then
    for ref in $(installation scaledobject); do
      ns=${ref%%/*} name=${ref#*/}
      mark scaledobject "$name" "$ns" "$(kubectl -n "$ns" get scaledobject "$name" -o json |
        jq -c '.metadata.annotations // {} | {"autoscaling.keda.sh/paused-replicas": .["autoscaling.keda.sh/paused-replicas"], "autoscaling.keda.sh/paused": .["autoscaling.keda.sh/paused"]}')"
      kubectl -n "$ns" annotate scaledobject "$name" autoscaling.keda.sh/paused-replicas=0 autoscaling.keda.sh/paused- --overwrite >/dev/null
    done
  fi
  writers >"$OUT/writers.txt"
  while read -r ns kind; do
    mark "${kind%%/*}" "${kind#*/}" "$ns" "$(kubectl -n "$ns" get "$kind" -o json | jq -c '{replicas: (.spec.replicas // 1)}')"
    kubectl -n "$ns" scale "$kind" --replicas=0 >/dev/null
  done <"$OUT/writers.txt"
  if served Sandbox; then
    kubectl get sandbox -A -o json | jq -r '.items[] | select(.spec.podTemplate.metadata.labels["app.kubernetes.io/component"] != "folder-bayma") | "\(.metadata.namespace)/\(.metadata.name)"' >"$OUT/sandboxes.txt"
    while read -r ref; do
      ns=${ref%%/*} name=${ref#*/}
      mark sandbox "$name" "$ns" "$(kubectl -n "$ns" get sandbox "$name" -o json | jq -c '{operatingMode: (.spec.operatingMode // "Running")}')"
      kubectl -n "$ns" patch sandbox "$name" --type merge -p '{"spec":{"operatingMode":"Suspended"}}' >/dev/null
    done <"$OUT/sandboxes.txt"
  fi
  writer_pods() {
    while read -r ns kind; do
      kubectl -n "$ns" get pods -l "$(kubectl -n "$ns" get "$kind" -o json | jq '.spec.selector' | selector_of)" -o name
    done <"$OUT/writers.txt"
    kubectl get pods -A -o json | jq -r '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Sandbox"))
      | select(.metadata.labels["app.kubernetes.io/component"] != "folder-bayma") | "\(.metadata.namespace)/\(.metadata.name)"'
    kubectl -n "$csi" get pods -l app.kubernetes.io/name=juicefs-mount -o name
    running_jobs
  }
  wait_for "$WAIT" "the writers' pods, the sessions' and JuiceFS's mount pods, and the Jobs, to be gone ($(writer_pods | tr '\n' ' '))" none writer_pods
}

# Starts again what stop_writers stopped, as each was, from its mark.
start_writers() {
  local ref ns name was kind
  if served Sandbox; then
    kubectl get sandbox -A -o json | jq -r '.items[] | "\(.metadata.namespace)/\(.metadata.name)"' | while read -r ref; do
      ns=${ref%%/*} name=${ref#*/}
      was=$(marked sandbox "$name" "$ns") || continue
      [ -n "$was" ] || continue
      kubectl -n "$ns" patch sandbox "$name" --type merge -p "$(jq -c '{spec: .}' <<<"$was")" >/dev/null
      unmark sandbox "$name" "$ns"
    done
  fi
  for kind in deployment statefulset; do
    for ref in $(installation "$kind"); do
      ns=${ref%%/*} name=${ref#*/}
      was=$(marked "$kind" "$name" "$ns") || continue
      [ -n "$was" ] || continue
      kubectl -n "$ns" scale "$kind/$name" --replicas="$(jq -r .replicas <<<"$was")" >/dev/null
      unmark "$kind" "$name" "$ns"
    done
  done
  if served ScaledObject; then
    for ref in $(installation scaledobject); do
      ns=${ref%%/*} name=${ref#*/}
      was=$(marked scaledobject "$name" "$ns") || continue
      [ -n "$was" ] || continue
      kubectl -n "$ns" patch scaledobject "$name" --type merge -p "$(jq -c '{metadata: {annotations: .}}' <<<"$was")" >/dev/null
      unmark scaledobject "$name" "$ns"
    done
  fi
  for ref in $(installation cronjob); do
    ns=${ref%%/*} name=${ref#*/}
    was=$(marked cronjob "$name" "$ns") || continue
    [ -n "$was" ] || continue
    kubectl -n "$ns" patch cronjob "$name" -p "$(jq -c '{spec: .}' <<<"$was")" >/dev/null
    unmark cronjob "$name" "$ns"
  done
}

# --- the old store sealed: its pod reached by the metadata's copier alone ---

# Every NetworkPolicy of the namespace that selects the old store's pod is made not to,
# marked with its selector before, and a policy of its own admits the copier alone to its
# filer: NetworkPolicies add to each other, so no other may select it. Run again after
# anything that applies NetworkPolicies while the pod is there.
seal_old() {
  local policy selector
  k get pod "$OLD_POD" >/dev/null 2>&1 || return 0
  for policy in $(k get networkpolicy -o name); do
    [ "$policy" != networkpolicy.networking.k8s.io/alasio-cutover-seal ] || continue
    selector=$(k get "$policy" -o json | jq '.spec.podSelector' | selector_of)
    if [ -z "$selector" ] || [ -n "$(k get pods -l "$selector" --field-selector "metadata.name=$OLD_POD" -o name)" ]; then
      mark "${policy%%/*}" "${policy#*/}" "$NS" "$(k get "$policy" -o json | jq -c '{podSelector: .spec.podSelector}')"
      k patch "$policy" --type json -p "$(k get "$policy" -o json | jq -c --arg pod "$OLD_POD" \
        '[{op: "add", path: "/spec/podSelector/matchExpressions", value: ((.spec.podSelector.matchExpressions // []) + [{key: "statefulset.kubernetes.io/pod-name", operator: "NotIn", values: [$pod]}])}]')" >/dev/null
    fi
  done
  jq -n --arg ns "$NS" --arg pod "$OLD_POD" --arg copier "$COPIER" '{
    apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy",
    metadata: {name: "alasio-cutover-seal", namespace: $ns},
    spec: {
      podSelector: {matchLabels: {"statefulset.kubernetes.io/pod-name": $pod}},
      policyTypes: ["Ingress"],
      ingress: [{from: [{podSelector: {matchLabels: {($copier | split("=")[0]): ($copier | split("=")[1])}}}], ports: [{protocol: "TCP", port: 8888}, {protocol: "TCP", port: 18888}]}]
    }}' | kubectl apply -f - >/dev/null
}
# The seal lifted: its policy deleted, and every policy it narrowed given its selector back,
# unless what was applied since has given it another.
unseal_old() {
  local policy was
  k delete networkpolicy alasio-cutover-seal --ignore-not-found >/dev/null
  for policy in $(k get networkpolicy -o name); do
    was=$(marked "${policy%%/*}" "${policy#*/}" "$NS")
    [ -n "$was" ] || continue
    if k get "$policy" -o json | jq -e --argjson was "$was" --arg pod "$OLD_POD" \
      '.spec.podSelector == ($was.podSelector + {matchExpressions: (($was.podSelector.matchExpressions // []) + [{key: "statefulset.kubernetes.io/pod-name", operator: "NotIn", values: [$pod]}])})' >/dev/null; then
      k patch "$policy" --type json -p "$(jq -c '[{op: "replace", path: "/spec/podSelector", value: .podSelector}]' <<<"$was")" >/dev/null
    fi
    unmark "${policy%%/*}" "${policy#*/}" "$NS"
  done
}

# --- the volume ---

# Rebinds the PV $1, kept on deletion, from the claim $2 to the claim $3, made from the claim
# template, as JSON, $4: checked to fit the PV first (its class, size and access modes); the
# PV's reclaim policy read back as Retain right before $2 is deleted; the PV reserved for $3
# by its claimRef; $3 made, and checked bound to it.
rebind() {
  local pv=$1 from=$2 to=$3 template=$4 volume claim
  bound() { [ "$(k get pvc "$to" -o jsonpath='{.status.phase}' 2>/dev/null)" = Bound ]; }
  if bound && [ "$(k get pvc "$to" -o jsonpath='{.spec.volumeName}')" = "$pv" ]; then
    log "$to holds $pv already"
    return 0
  fi
  volume=$(kubectl get pv "$pv" -o json)
  claim=$(jq -c --argjson pv "$volume" --arg name "$to" --arg ns "$NS" '{apiVersion: "v1", kind: "PersistentVolumeClaim",
    metadata: {name: $name, namespace: $ns},
    spec: (.spec + {volumeName: $pv.metadata.name, storageClassName: (if (.spec.storageClassName // "") == "" then $pv.spec.storageClassName else .spec.storageClassName end)})}' <<<"$template")
  jq -e --argjson pv "$volume" '.spec.storageClassName == $pv.spec.storageClassName' <<<"$claim" >/dev/null ||
    fail "$to's class, $(jq -r .spec.storageClassName <<<"$claim"), is not the PV $pv's, $(jq -r .spec.storageClassName <<<"$volume")"
  jq -e --argjson pv "$volume" '(.spec.accessModes - $pv.spec.accessModes) == []' <<<"$claim" >/dev/null ||
    fail "$to asks for access modes $(jq -c .spec.accessModes <<<"$claim") the PV $pv has not ($(jq -c .spec.accessModes <<<"$volume"))"
  [ "$(numfmt --from=iec-i "$(jq -r '.spec.resources.requests.storage' <<<"$claim")")" -le "$(numfmt --from=iec-i "$(jq -r '.spec.capacity.storage' <<<"$volume")")" ] ||
    fail "$to asks for $(jq -r '.spec.resources.requests.storage' <<<"$claim"), more than the PV $pv's $(jq -r '.spec.capacity.storage' <<<"$volume")"
  if k get pvc "$from" >/dev/null 2>&1; then
    [ "$(kubectl get pv "$pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" = Retain ] || fail "the PV $pv is not kept on deletion: $from is not deleted"
    k delete pvc "$from" --wait=true --timeout="${WAIT}s" >/dev/null
  fi
  claimed_by() { [ "$(kubectl get pv "$pv" -o jsonpath='{.spec.claimRef.name}')" = "$to" ]; }
  # Free for $3: released by $2; or, where a run stopped between reserving it and making the
  # claim it reserved it for, Available and reserved for $3 already, for no claim, or for one
  # there is not, as a cut-over that stopped there leaves it reserved for its claim, which
  # its rollback then finds, never made.
  free() {
    local phase reserved
    phase=$(kubectl get pv "$pv" -o jsonpath='{.status.phase}')
    reserved=$(kubectl get pv "$pv" -o jsonpath='{.spec.claimRef.name}')
    [ "$phase" = Released ] && return 0
    [ "$phase" = Available ] || return 1
    [ -z "$reserved" ] || [ "$reserved" = "$to" ] || ! k get pvc "$reserved" >/dev/null 2>&1
  }
  wait_for "$WAIT" "the PV $pv to be released by $from, or free for $to" free
  kubectl patch pv "$pv" --type merge -p "$(jq -nc --arg ns "$NS" --arg name "$to" '{spec: {claimRef: {apiVersion: "v1", kind: "PersistentVolumeClaim", namespace: $ns, name: $name, uid: null, resourceVersion: null}}}')" >/dev/null
  injected 6b
  kubectl apply -f - <<<"$claim" >/dev/null
  wait_for "$WAIT" "$to to be bound" bound
  [ "$(k get pvc "$to" -o jsonpath='{.spec.volumeName}')" = "$pv" ] || fail "$to is bound to another PV than $pv"
  claimed_by || fail "the PV $pv is claimed by another than $to"
}

# --- what a store holds ---

# A shell of the old store, in its pod; one of the new, in a gateway's buckets container,
# which its NetworkPolicies let reach its masters, volume servers and filers.
old_shell() { kx -i "$OLD_POD" -c seaweedfs -- weed shell -master=127.0.0.1:9333 -filer=127.0.0.1:8888; }
new_shell() { kx -i deployment/"$GATEWAY" -c buckets -- weed shell "-master=$MASTER-0.$MASTER.$NS.svc.cluster.local:9333" "-filer=$FILER:8888"; }

# The volumes a shell's masters know, as id:collection, sorted, the listing written to $OUT/$2.
volumes() { printf 'volume.list\n' | "$1" >"$OUT/$2"; sed -n 's/.*volume Id:\([0-9]*\), .*Collection:\([^,]*\),.*/\1:\2/p' "$OUT/$2" | sort -n | tr '\n' ' '; }

# The entries under $2, as a shell $1 counts them, through a filer: "total N directories, M files".
entries() { printf 'fs.meta.save -o /dev/null %s\n' "$2" | "$1" >"$OUT/$3"; grep -o 'total [0-9]* directories, [0-9]* files' "$OUT/$3" | tail -n 1; }
# The files of the log of changes under $2, its days' directories, each as its size and path,
# sorted, as a shell $1 lists them.
log_files() {
  local day
  for day in $(printf 'fs.ls %s\n' "$2" | "$1" | tr -d '\r' | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' || true); do
    printf 'fs.ls -l %s/%s\n' "$2" "$day" | "$1" | awk 'NF >= 6 && $NF ~ /^\// { print $5, $NF }'
  done | sort
}

# What a gateway's pod $1, in its container $2, answers at its own S3 port: every bucket's
# every version and delete marker, with sizes and ETags, its versioning and lifecycle rules.
# Each answer goes through a file, as a bucket's versions are megabytes, more than an
# argument holds.
inventory() {
  local pod=$1 container=$2 bucket work
  work=$(mktemp -d "$OUT/inventory.XXXXXX")
  mkdir "$work/buckets"
  s3api() { kx "$pod" -c "$container" -- aws --endpoint-url http://127.0.0.1:8333 s3api "$@"; }
  for bucket in $(s3api list-buckets --query 'Buckets[].Name' --output text); do
    s3api list-object-versions --bucket "$bucket" --output json >"$work/versions.json"
    s3api get-bucket-versioning --bucket "$bucket" --output json >"$work/versioning.json"
    s3api get-bucket-lifecycle-configuration --bucket "$bucket" --output json >"$work/lifecycle.json" 2>/dev/null || true
    for file in versions versioning lifecycle; do [ -s "$work/$file.json" ] || echo '{}' >"$work/$file.json"; done
    jq -n --arg bucket "$bucket" --slurpfile versions "$work/versions.json" --slurpfile versioning "$work/versioning.json" --slurpfile lifecycle "$work/lifecycle.json" \
      '{bucket: $bucket,
        versions: ([($versions[0].Versions // [])[] | {Key, VersionId, Size, ETag, IsLatest}] + [($versions[0].DeleteMarkers // [])[] | {Key, VersionId, IsLatest, deleteMarker: true}] | sort_by(.Key, .VersionId)),
        versioning: $versioning[0], lifecycle: $lifecycle[0]}' >"$work/buckets/$bucket.json"
  done
  find "$work/buckets" -name '*.json' -print0 | sort -z | xargs -0 cat | jq -S -s .
  rm -rf "$work"
}

# Every version of every bucket of $1, an inventory, read back through the gateway's pod $2,
# each its ETag; at most $3 of each bucket, the first in the inventory's order, by key and
# version.
read_back() {
  local inventory=$1 pod=$2 most=$3
  jq -r --argjson most "$most" '.[] | .bucket as $bucket | [.versions[] | select(.deleteMarker != true)][:$most][] | [$bucket, .Key, (.VersionId // "null"), .ETag] | @tsv' "$inventory" |
    kx -i "$pod" -c lifecycle -- sh -c '
      checked=0
      while IFS="	" read -r bucket key version etag; do
        set -- --bucket "$bucket" --key "$key"
        [ "$version" = null ] || set -- "$@" --version-id "$version"
        got=$(aws --endpoint-url http://127.0.0.1:8333 s3api get-object "$@" /tmp/read-back --query ETag --output text)
        [ "$got" = "$etag" ] || { echo "$bucket/$key $version reads back as $got, not $etag"; exit 1; }
        checked=$((checked + 1))
      done
      echo "$checked"'
}

# The host path of the PV $1, as local-path or a hostPath one says it.
host_path() { kubectl get pv "$1" -o json | jq -r '.spec.hostPath.path // .spec.local.path // empty'; }
# The count of volumes' data files at the PV $1's host path.
dat_files() { find "$(host_path "$1")" -maxdepth 1 -name '*.dat' | wc -l; }
# The volumes whose data files are at the PV $1's host path, as id:collection, sorted, as
# volumes() says them: <collection>_<id>.dat, or <id>.dat of no collection.
volumes_on_disk() {
  find "$(host_path "$1")" -maxdepth 1 -name '*.dat' -printf '%f\n' | sed -E 's/\.dat$//; s/^(.*)_([0-9]+)$/\2:\1/; s/^([0-9]+)$/\1:/' | sort -n | tr '\n' ' '
}
