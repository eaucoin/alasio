#!/usr/bin/env bash
# 09's box cut-over, rehearsed on the runner: a node of k3s on overlayfs, as the box is,
# its pods running, moved onto stargz-snapshotter's daemon with every image unpacked anew.
# The old stores go while nothing can use them: k3s stopped, every container killed and
# unmounted by k3s-killall.sh, then containerd's whole state removed, so no image is left
# for kubelet to restart a pod from, and no overlayfs snapshot holds the disk. k3s then
# starts through the daemon, and pulls every image again.
#
# Needs root through sudo and Docker. Measurements go to $OUT.
#
#   OUT=/tmp/gate-09-cutover gate/09/cutover.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$RUNNER_TEMP/gate-09-cutover}
mkdir -p "$OUT"
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$OUT/events.log"; }
fail() { log "FAILED: $*"; exit 1; }
STARGZ_VERSION=v0.19.0
SNAPSHOTTER=stargz-daemon
CONTAINERD_STATE=/var/lib/rancher/k3s/agent/containerd
CONTAINERD=/var/lib/rancher/k3s/agent/etc/containerd
STARGZ_ROOT=/var/lib/containerd-stargz-grpc

log "k3s on overlayfs, as the box runs, with pods of images pulled whole"
"$here/../k3s.sh"
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl create namespace cutover
for image in mirror.gcr.io/library/busybox:1.37 mirror.gcr.io/library/python:3.13-slim mirror.gcr.io/library/node:22-slim; do
  name=$(basename "${image%%:*}")
  kubectl -n cutover create deployment "$name" --image="$image" -- sleep infinity
done
kubectl -n cutover rollout status deployment --timeout=300s
# Expanded by root's shell: the directory is root's alone.
sudo sh -c "du -shx $CONTAINERD_STATE/io.containerd.*" | tee "$OUT/before.txt"

log "the cut-over: k3s stopped, its containers killed and unmounted, containerd's state removed"
started=$(date +%s)
sudo systemctl stop k3s
sudo /usr/local/bin/k3s-killall.sh >/dev/null 2>&1
if mount | grep -E "$CONTAINERD_STATE|/run/k3s/containerd" >"$OUT/mounts-left.txt"; then
  cat "$OUT/mounts-left.txt"; fail "k3s-killall.sh left mounts of containerd's"
fi
sudo rm -rf "$CONTAINERD_STATE"
log "  containerd's state removed after $(( $(date +%s) - started )) s"

log "stargz-snapshotter's daemon, and k3s's containerd through it, as alasio's upgrade writes them"
archive=stargz-snapshotter-$STARGZ_VERSION-linux-amd64.tar.gz
curl -fsSL -o "$RUNNER_TEMP/$archive" "https://github.com/containerd/stargz-snapshotter/releases/download/$STARGZ_VERSION/$archive"
curl -fsSL "https://github.com/containerd/stargz-snapshotter/releases/download/$STARGZ_VERSION/$archive.sha256sum" | sed "s|  .*|  $RUNNER_TEMP/$archive|" | sha256sum --check
sudo tar -xzf "$RUNNER_TEMP/$archive" -C /usr/local/bin
sudo mkdir -p /etc/containerd-stargz-grpc
sudo tee /etc/containerd-stargz-grpc/config.toml >/dev/null <<EOF
max_concurrency = 1

[fuse_manager]
enable = true
address = "/run/containerd-stargz-grpc/fuse-manager.sock"
path = "/usr/local/bin/stargz-fuse-manager"
EOF
sudo tee /etc/systemd/system/stargz-snapshotter.service >/dev/null <<EOF
[Unit]
Description=stargz snapshotter, beside k3s's containerd
PartOf=k3s.service

[Service]
ExecStart=/usr/local/bin/containerd-stargz-grpc --address=/run/containerd-stargz-grpc/containerd-stargz-grpc.sock --config=/etc/containerd-stargz-grpc/config.toml --root=$STARGZ_ROOT
Restart=always
KillMode=process
EOF
sudo mkdir -p /etc/systemd/system/k3s.service.d
sudo tee /etc/systemd/system/k3s.service.d/stargz.conf >/dev/null <<EOF
[Unit]
Wants=stargz-snapshotter.service
After=stargz-snapshotter.service
EOF
# k3s's own configuration as it renders it, with the daemon as the CRI's snapshotter.
{
  sed -e "s/snapshotter = \"overlayfs\"/snapshotter = \"$SNAPSHOTTER\"/" \
    -e 's/disable_snapshot_annotations = true/disable_snapshot_annotations = false/' <<<"$(sudo cat "$CONTAINERD/config.toml")"
  cat <<EOF

[proxy_plugins.$SNAPSHOTTER]
  type = "snapshot"
  address = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"
  [proxy_plugins.$SNAPSHOTTER.exports]
    root = "$STARGZ_ROOT/"
    enable_remote_snapshot_annotations = "true"
EOF
} | sudo tee "$CONTAINERD/config-v3.toml.tmpl" >/dev/null
sudo systemctl daemon-reload
sudo systemctl start k3s
until kubectl get node -o name >/dev/null 2>&1; do sleep 2; done
kubectl wait --for=condition=Ready node --all --timeout=300s
sudo grep -q "$SNAPSHOTTER" "$CONTAINERD/config.toml" || fail "k3s did not take the template"
# Each pod's status says it ran before until kubelet reports it anew: each counts as back
# once its container started after the cut-over began, and is ready.
since=$(date -u -d "@$started" +%Y-%m-%dT%H:%M:%SZ)
back() {
  kubectl get pods -A -o json | jq -e --arg since "$since" '
    [.items[] | select(.metadata.namespace == "cutover" or (.metadata.ownerReferences // [] | any(.kind == "ReplicaSet")))
      | (.status.containerStatuses // [])[0] | (.state.running.startedAt // "") >= $since and .ready] | all' >/dev/null
}
for (( i = 0; ; i++ )); do
  ! back || break
  [ "$i" -lt 300 ] || { kubectl get pods -A -o wide; fail "the pods did not come back within ten minutes"; }
  sleep 2
done
log "  every pod ready again $(( $(date +%s) - started )) s after k3s stopped"

restarts=$(kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.status.containerStatuses[0].restartCount}{"\n"}{end}')
printf '%s\n' "$restarts" | tee "$OUT/restarts.txt"
sudo test ! -e "$CONTAINERD_STATE/io.containerd.snapshotter.v1.overlayfs/snapshots" || [ -z "$(sudo ls "$CONTAINERD_STATE/io.containerd.snapshotter.v1.overlayfs/snapshots")" ] ||
  fail "overlayfs holds snapshots again"
sudo k3s ctr -n k8s.io snapshots --snapshotter "$SNAPSHOTTER" ls >"$OUT/snapshots.txt"
snapshots=$(($(wc -l <"$OUT/snapshots.txt") - 1))
log "  the daemon holds $snapshots snapshots"
[ "$snapshots" -gt 0 ] || fail "the daemon holds no snapshot"
sudo sh -c "du -shx $CONTAINERD_STATE/io.containerd.* $STARGZ_ROOT/*; true" | tee "$OUT/after.txt"
log "cut-over done"
