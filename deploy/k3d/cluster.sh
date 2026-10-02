#!/usr/bin/env bash
# Creates a k3d cluster that runs alasio's chart: k3s nodes with gVisor (node/), the
# gvisor RuntimeClass, and kubelet thresholds for a machine whose disk the cluster
# shares with everything else on it, so images in use are not evicted.
#
#   deploy/k3d/cluster.sh NAME [k3d cluster create flags...]
#
# Environment:
#   AGENTS           agent nodes beside the server (0 unless set)
#   REGISTRY_CONFIG  a k3s registries.yaml, for a private registry's credentials
#   KUBECONFIG_OUT   where to write the cluster's kubeconfig (./kubeconfig-NAME unless set)
# Further flags go to `k3d cluster create` as they are: volumes the host profile mounts,
# for instance.
set -euo pipefail

name=${1:?usage: cluster.sh NAME [k3d flags...]}
shift
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
image=alasio-k3s-gvisor:v1.37.1-k3s1
kubeconfig=${KUBECONFIG_OUT:-$PWD/kubeconfig-$name}

docker image inspect "$image" >/dev/null 2>&1 || docker build -t "$image" "$here/node"

args=(
  --image "$image"
  --agents "${AGENTS:-0}"
  --no-lb
  --k3s-arg "--disable=traefik@server:*"
  # Evicts only when the disk is nearly full, and collects unused images only then.
  --k3s-arg "--kubelet-arg=eviction-hard=imagefs.available<5%,nodefs.available<5%@all"
  --k3s-arg "--kubelet-arg=eviction-minimum-reclaim=imagefs.available=1%,nodefs.available=1%@all"
  --k3s-arg "--kubelet-arg=image-gc-high-threshold=98@all"
  --k3s-arg "--kubelet-arg=image-gc-low-threshold=95@all"
  --kubeconfig-update-default=false
  --kubeconfig-switch-context=false
  --wait
)
[[ -n "${REGISTRY_CONFIG:-}" ]] && args+=(--registry-config "$REGISTRY_CONFIG")

k3d cluster create "$name" "${args[@]}" "$@"
(umask 077 && k3d kubeconfig get "$name" >"$kubeconfig")
KUBECONFIG=$kubeconfig kubectl apply -f "$here/runtimeclass.yaml"
echo "cluster $name is up; KUBECONFIG=$kubeconfig"
