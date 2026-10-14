#!/usr/bin/env bash
# 09's gate, on the runner itself, as alasio's host target makes a node (k3s at alasio's
# pin, gVisor from alasio's fork as runsc): stargz-snapshotter's standalone daemon, with
# its FUSE manager, beside k3s's containerd as a proxy snapshotter (program/09-lazy-
# pulling.md, CS2), and eStargz images from a registry of the run's own.
#
# - G1: gVisor pods start from eStargz images, their layers mounted remotely, with
#   directfs on and off; how long a sandbox image takes to start lazily and whole.
# - G2: lazily pulled pods, gVisor's and runc's, keep serving through a k3s restart and a
#   restart of the daemon itself.
# - G4: a gVisor pod checkpointed, its image removed from the node, and restored, by the
#   fork's restore annotations, onto a rootfs pulled lazily anew.
# - G5: the daemon's caches against the content store, and its and containerd's memory.
# - G6: the caches once the images are removed from the node, and once the daemon restarted.
# - G7: what the FUSE manager's memory is, at each stage: its own heap, or files it maps,
#   as /proc and its service's cgroup count them.
#
# Needs root through sudo, Docker and buildx. Measurements go to $OUT. STARGZ_OPTIONS,
# when set, are top-level lines of the daemon's configuration besides, as G5 compares.
#
#   OUT=/tmp/gate-09 STARGZ_OPTIONS='no_background_fetch = true' gate/09/run.sh
#
# STARGZ_ENVIRONMENT, when set, is the daemon's service's Environment= besides, which the
# FUSE manager it starts inherits (GOMEMLIMIT=..., say).
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$RUNNER_TEMP/gate-09}
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
fail() { log "FAILED: $*"; exit 1; }
pins=$here/../../cluster/node/pins.json
STARGZ_VERSION=v0.19.0
SNAPSHOTTER=stargz-daemon
REGISTRY=localhost:5000
CONTAINERD=/var/lib/rancher/k3s/agent/etc/containerd
STARGZ_ROOT=/var/lib/containerd-stargz-grpc

log "stargz-snapshotter $STARGZ_VERSION, its daemon and FUSE manager"
archive=stargz-snapshotter-$STARGZ_VERSION-linux-amd64.tar.gz
curl -fsSL -o "$RUNNER_TEMP/$archive" "https://github.com/containerd/stargz-snapshotter/releases/download/$STARGZ_VERSION/$archive"
curl -fsSL "https://github.com/containerd/stargz-snapshotter/releases/download/$STARGZ_VERSION/$archive.sha256sum" | sed "s|  .*|  $RUNNER_TEMP/$archive|" | sha256sum --check
sha256sum "$RUNNER_TEMP/$archive" | tee -a "$OUT/events.log"
sudo tar -xzf "$RUNNER_TEMP/$archive" -C /usr/local/bin
sudo mkdir -p /etc/containerd-stargz-grpc
sudo tee /etc/containerd-stargz-grpc/config.toml >/dev/null <<EOF
# The daemon's FUSE mounts held by its FUSE manager, so they outlive the daemon's own
# restarts; the run's registry over HTTP.
${STARGZ_OPTIONS:-}

[fuse_manager]
enable = true
address = "/run/containerd-stargz-grpc/fuse-manager.sock"
path = "/usr/local/bin/stargz-fuse-manager"

[[resolver.host."$REGISTRY".mirrors]]
host = "$REGISTRY"
insecure = true
EOF
sudo tee /etc/systemd/system/stargz-snapshotter.service >/dev/null <<EOF
[Unit]
Description=stargz snapshotter, beside k3s's containerd
Before=k3s.service

[Service]
${STARGZ_ENVIRONMENT:+Environment=$STARGZ_ENVIRONMENT}
ExecStart=/usr/local/bin/containerd-stargz-grpc --address=/run/containerd-stargz-grpc/containerd-stargz-grpc.sock --config=/etc/containerd-stargz-grpc/config.toml --root=$STARGZ_ROOT --log-level=info
Restart=always
# Its FUSE mounts stay with the FUSE manager when it stops.
KillMode=process

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now stargz-snapshotter
for (( i = 0; ; i++ )); do
  ! sudo test -S /run/containerd-stargz-grpc/containerd-stargz-grpc.sock || break
  [ "$i" -lt 60 ] || { sudo systemctl status stargz-snapshotter --no-pager || true; sudo journalctl -u stargz-snapshotter --no-pager | tail -50; fail "the daemon did not listen"; }
  sleep 1
done

log "the run's registry, and k3s"
docker run -d --restart=always --name registry -p 5000:5000 mirror.gcr.io/library/registry:2 >/dev/null
sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/registries.yaml >/dev/null <<EOF
mirrors:
  "$REGISTRY":
    endpoint: ["http://$REGISTRY"]
EOF
"$here/../k3s.sh"
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# After k3s's first start, which renders its containerd configuration without it.
log "gVisor $(jq -r .gvisor.release "$pins") from alasio's fork"
release=$(jq -r .gvisor.release "$pins")
curl -fsSL -o "$RUNNER_TEMP/gvisor.tar.zstd" "https://github.com/eaucoin/gvisor/releases/download/release-$release/gvisor-x86_64.tar.zstd"
echo "$(jq -r .gvisor.sha512 "$pins")  $RUNNER_TEMP/gvisor.tar.zstd" | sha512sum --check
mkdir -p "$RUNNER_TEMP/gvisor"
tar --zstd -xf "$RUNNER_TEMP/gvisor.tar.zstd" -C "$RUNNER_TEMP/gvisor"
for binary in runsc containerd-shim-runsc-v1 gvisor-bin; do
  sudo cp -r "$(find "$RUNNER_TEMP/gvisor" -maxdepth 2 -name "$binary" | head -1)" /usr/local/bin/
done
runsc --version | head -1 | tee -a "$OUT/events.log"


# k3s's containerd configuration, as k3s renders it for this machine, with the daemon as
# the CRI's snapshotter, its remote-snapshot annotations passed on, and runsc as alasio's
# template registers it, with the fork's restore annotations passed to the shim. A whole
# template rather than k3s's base and a table more: the base already has the CRI's image
# table, which TOML takes once (program/09-lazy-pulling.md, Corrections 5).
rendered=$(sudo cat "$CONTAINERD/config.toml")
grep -q 'snapshotter = "overlayfs"' <<<"$rendered" || fail "k3s's containerd configuration names no overlayfs snapshotter"
sudo mkdir -p /etc/containerd/runsc
sudo tee /etc/containerd/runsc/runsc.toml >/dev/null <<'EOF'
restore_background = true
[runsc_config]
  systemd-cgroup = "true"
EOF
sudo tee /etc/containerd/runsc/runsc-nodirectfs.toml >/dev/null <<'EOF'
restore_background = true
[runsc_config]
  systemd-cgroup = "true"
  directfs = "false"
EOF
{
  sed -e "s/snapshotter = \"overlayfs\"/snapshotter = \"$SNAPSHOTTER\"/" \
    -e 's/disable_snapshot_annotations = true/disable_snapshot_annotations = false/' <<<"$rendered"
  cat <<EOF

[proxy_plugins.$SNAPSHOTTER]
  type = "snapshot"
  address = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"
  [proxy_plugins.$SNAPSHOTTER.exports]
    root = "$STARGZ_ROOT/"
    enable_remote_snapshot_annotations = "true"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
  pod_annotations = ["dev.gvisor.internal.restore.*"]
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc.options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "/etc/containerd/runsc/runsc.toml"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-nodirectfs]
  runtime_type = "io.containerd.runsc.v1"
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-nodirectfs.options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "/etc/containerd/runsc/runsc-nodirectfs.toml"
EOF
} | sudo tee "$CONTAINERD/config-v3.toml.tmpl" >/dev/null
sudo systemctl restart k3s
until kubectl get node -o name 2>/dev/null | grep -q node; do sleep 2; done
kubectl wait --for=condition=Ready node --all --timeout=300s
sudo grep -q "$SNAPSHOTTER" "$CONTAINERD/config.toml" || fail "k3s did not take the template"
kubectl apply -f - <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: { name: gvisor }
handler: runsc
---
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: { name: gvisor-nodirectfs }
handler: runsc-nodirectfs
---
apiVersion: v1
kind: Namespace
metadata: { name: gate }
EOF
until kubectl -n gate get serviceaccount default >/dev/null 2>&1; do sleep 1; done

log "eStargz images, built by buildx and pushed to the run's registry"
docker buildx create --name estargz --driver docker-container --driver-opt network=host --use >/dev/null
estargz() { # <name> <context>: the image built as eStargz, every layer
  docker buildx build --quiet --output "type=image,name=$REGISTRY/$1:estargz,push=true,registry.insecure=true,oci-mediatypes=true,compression=estargz,force-compression=true" \
    --provenance=false --metadata-file "$OUT/$1-estargz.json" "$2" >/dev/null
}
plain() { # <name> <context>: the image built whole, gzip
  docker buildx build --quiet --output "type=image,name=$REGISTRY/$1:plain,push=true,registry.insecure=true,oci-mediatypes=true,compression=gzip,force-compression=true" \
    --provenance=false --metadata-file "$OUT/$1-plain.json" "$2" >/dev/null
}
estargz counter "$here/counter"
plain counter "$here/counter"
estargz sandbox "$here/sandbox"
plain sandbox "$here/sandbox"
curl -fsS "http://$REGISTRY/v2/sandbox/manifests/$(jq -r '."containerimage.digest"' "$OUT/sandbox-estargz.json")" -H 'Accept: application/vnd.oci.image.manifest.v1+json' >"$OUT/sandbox-estargz-manifest.json"
jq -e '[.layers[].annotations["containerd.io/snapshot/stargz/toc.digest"]] | all' "$OUT/sandbox-estargz-manifest.json" >/dev/null || fail "a layer of the eStargz image has no TOC digest"
log "every eStargz layer carries its TOC digest; sizes: estargz $(jq '[.layers[].size] | add' "$OUT/sandbox-estargz-manifest.json") B"

# pod <name> <image> <runtime class> [restore image path]: a pod that counts in memory
# (gate/09/counter), restored from the checkpoint at the path when given.
pod() {
  local annotations=""
  [ -z "${4:-}" ] || annotations="annotations: { dev.gvisor.internal.restore.host-image-path: \"$4\", dev.gvisor.internal.restore.background: \"true\" }"
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: gate
  $annotations
spec:
  runtimeClassName: $3
  restartPolicy: Never
  automountServiceAccountToken: false
  enableServiceLinks: false
  terminationGracePeriodSeconds: 0
  containers:
    - name: app
      image: $2
      resources: { limits: { memory: 512Mi } }
EOF
}
running() { # <name>: waits until it runs, and says how long that took in ms
  local start
  start=$(date +%s%N)
  until [ "$(kubectl -n gate get pod "$1" -o 'jsonpath={.status.phase} {.status.containerStatuses[0].ready}' 2>/dev/null)" = "Running true" ]; do
    [ $((($(date +%s%N) - start) / 1000000)) -lt 600000 ] || { kubectl -n gate describe pod "$1"; return 1; }
    sleep 0.1
  done
  echo $((($(date +%s%N) - start) / 1000000))
}
started() { # <name>: waits until its container has started, whatever it does then, and says how long that took in ms
  local start
  start=$(date +%s%N)
  until kubectl -n gate get pod "$1" -o 'jsonpath={.status.containerStatuses[0].state}' 2>/dev/null | grep -Eq '"(running|terminated)"'; do
    [ $((($(date +%s%N) - start) / 1000000)) -lt 600000 ] || { kubectl -n gate describe pod "$1"; return 1; }
    sleep 0.1
  done
  echo $((($(date +%s%N) - start) / 1000000))
}
remote_layers() { mount | grep -c "$STARGZ_ROOT" || true; }
count() { kubectl -n gate exec "$1" -- cat /count; }
# memory <stage>: the FUSE manager's memory now, as /proc and its service's cgroup count it,
# to memory-stages.txt: its resident set, anonymous (its heap) and of files, and its
# proportional share of each; and the cgroup's anon and file pages.
memory() {
  local pid
  pid=$(pgrep -x stargz-fuse-man | head -1 || true)
  [ -n "$pid" ] || { echo "$1 no FUSE manager runs" | tee -a "$OUT/memory-stages.txt"; return; }
  {
    printf '%s ' "$1"
    sudo awk '/^(VmRSS|RssAnon|RssFile|RssShmem):/ { printf "%s %s kB, ", $1, $2 }' "/proc/$pid/status"
    sudo awk '/^(Pss_Anon|Pss_File):/ { printf "%s %s kB, ", $1, $2 }' "/proc/$pid/smaps_rollup"
    cgroup=$(awk -F: '{ print $3 }' "/proc/$pid/cgroup")
    sudo awk '/^(anon|file) / { printf "cgroup %s %d MB, ", $1, $2 / 1048576 }' "/sys/fs/cgroup$cgroup/memory.stat"
    printf 'GOMEMLIMIT=%s\n' "$(sudo tr '\0' '\n' <"/proc/$pid/environ" | sed -n 's/^GOMEMLIMIT=//p')"
  } | tee -a "$OUT/memory-stages.txt"
}
counting() { # <name>: waits until it has counted for a second
  until count "$1" >/dev/null 2>&1; do sleep 0.2; done
  sleep 1
}

memory started
log "G1: gVisor pods from eStargz images, directfs on and off"
pod lazy-directfs "$REGISTRY/counter:estargz" gvisor
log "  directfs on: running after $(running lazy-directfs) ms"
pod lazy-nodirectfs "$REGISTRY/counter:estargz" gvisor-nodirectfs
log "  directfs off: running after $(running lazy-nodirectfs) ms"
kubectl -n gate exec lazy-directfs -- md5sum /blob >"$OUT/blob-directfs.txt"
kubectl -n gate exec lazy-nodirectfs -- md5sum /blob >"$OUT/blob-nodirectfs.txt"
cmp "$OUT/blob-directfs.txt" "$OUT/blob-nodirectfs.txt" || fail "the two read the image's file differently"
log "  remote layer mounts: $(remote_layers)"
memory counters-mounted
sudo k3s ctr -n k8s.io snapshots --snapshotter "$SNAPSHOTTER" ls >"$OUT/snapshots.txt"
# start <image> <name>: starts the sandbox image, not on the node, and says how long it took.
start() {
  pod "$2" "$REGISTRY/$1" gvisor
  ms=$(started "$2")
  log "  a sandbox image ($1), not on the node, started under gVisor after $ms ms"
  echo "$1 $ms" >>"$OUT/start-times.txt"
}
# starts <image>: starts it three times, each on a node it is gone from again, its caches
# with it once its resolved layers expire (120 s); the last is left running.
starts() {
  for run in 1 2 3; do
    start "$1" "start-${1#*:}-$run"
    [ "$run" = 3 ] && break
    kubectl -n gate delete pod "start-${1#*:}-$run" --grace-period=0 --force --wait=true >/dev/null 2>&1 || true
    # Its layers unmounted as it goes, which can take longer than crictl's 2 s.
    sudo k3s crictl --timeout=120s rmi "$REGISTRY/$1" >/dev/null
    sleep 150
  done
}
start sandbox:plain start-plain
starts sandbox:estargz
memory sandbox-started

log "G2: a k3s restart, lazily pulled pods running"
pod survivor-runc "$REGISTRY/counter:estargz" ""
running survivor-runc >/dev/null
counting survivor-runc
counting lazy-directfs
declare -A before
for name in lazy-directfs survivor-runc; do before[$name]=$(count "$name"); done
sudo systemctl restart k3s
kubectl wait --for=condition=Ready node --all --timeout=300s
sleep 10
for name in lazy-directfs survivor-runc; do
  restarts=$(kubectl -n gate get pod "$name" -o 'jsonpath={.status.containerStatuses[0].restartCount}')
  after=$(count "$name") || fail "$name does not answer after k3s's restart"
  kubectl -n gate exec "$name" -- md5sum /blob >/dev/null || fail "$name cannot read its image after k3s's restart"
  log "  $name after k3s's restart: restarts $restarts, count $after (was ${before[$name]})"
  [ "$restarts" = 0 ] || fail "$name restarted"
done
log "G2: the daemon itself restarted"
sudo systemctl restart stargz-snapshotter
sleep 5
for name in lazy-directfs survivor-runc; do
  kubectl -n gate exec "$name" -- md5sum /blob >/dev/null || fail "$name cannot read its image after the daemon's restart"
done
log "  both still read their images"

log "G4: a checkpoint restored onto a rootfs pulled lazily anew"
pod source "$REGISTRY/counter:estargz" gvisor
running source >/dev/null
counting source
sandbox=$(sudo k3s crictl pods --quiet --name '^source$' --state ready)
checkpointed=$(count source)
sudo mkdir -p /checkpoints
sudo k3s ctr --namespace k8s.io task checkpoint --exit --image-path /checkpoints/source "$sandbox"
kubectl -n gate delete pod source --grace-period=0 --force --wait=true >/dev/null 2>&1 || true
for name in lazy-directfs lazy-nodirectfs survivor-runc; do kubectl -n gate delete pod "$name" --grace-period=0 --force --wait=true >/dev/null 2>&1 || true; done
sleep 5
sudo k3s crictl rmi "$REGISTRY/counter:estargz" || fail "the image could not be removed from the node"
log "  image removed from the node; remote layer mounts now: $(remote_layers)"
pod restored "$REGISTRY/counter:estargz" gvisor /checkpoints/source
log "  restored pod running after $(running restored) ms"
sleep 2
after=$(count restored)
later=$(sleep 1 && count restored)
log "  at the checkpoint: $checkpointed; restored: $after, then $later"
[ "${after%% *}" = "${checkpointed%% *}" ] || fail "the restored pod started afresh"
[ "${after##* }" -ge "${checkpointed##* }" ] && [ "${later##* }" -gt "${after##* }" ] || fail "the restored pod does not count on"
kubectl -n gate exec restored -- md5sum /blob >/dev/null || fail "the restored pod cannot read its lazily pulled image"
log "  the restored pod counts on from its checkpoint and reads its image"

log "G5: caches and memory, once the sandbox image's background fetch had time (options: ${STARGZ_OPTIONS:-none})"
sleep 120
# On their own file systems only (-x): the snapshots' FUSE mounts, read, would fetch.
sudo sh -c "du -shx $STARGZ_ROOT/* $STARGZ_ROOT/stargz/* /var/lib/rancher/k3s/agent/containerd/io.containerd.content.v1.content /var/lib/rancher/k3s/agent/containerd/io.containerd.snapshotter.v1.overlayfs; true" | tee "$OUT/disk.txt"
ps -o rss=,comm= -C containerd-stargz-grpc,stargz-fuse-manager,containerd | tee "$OUT/memory.txt"
memory background-fetched

# The caches are a directory of each layer's, under httpcache/ and fscache/, which the
# daemon removes when it closes the layer (cache/cache.go's directoryCache.Close): once no
# snapshot mounts it and its resolved entry has expired (resolve_result_entry_ttl_sec, 120 s).
log "G6: the caches once the images are gone from the node"
kubectl -n gate delete pods --all --grace-period=0 --force --wait=true >/dev/null 2>&1 || true
for image in sandbox:plain sandbox:estargz counter:estargz; do sudo k3s crictl rmi "$REGISTRY/$image" >/dev/null 2>&1 || true; done
sleep 180
sudo sh -c "du -shx $STARGZ_ROOT/snapshotter $STARGZ_ROOT/stargz/*; true" | tee "$OUT/disk-after-rmi.txt"
log "G6: and once the daemon restarted"
sudo systemctl restart stargz-snapshotter
sleep 10
sudo sh -c "du -shx $STARGZ_ROOT/snapshotter $STARGZ_ROOT/stargz/*; true" | tee "$OUT/disk-after-restart.txt"
log "gate done"
