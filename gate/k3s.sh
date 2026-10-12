#!/usr/bin/env bash
# k3s on the runner itself, at alasio's pinned release and install script
# (cluster/node/pins.json), one node, as the host target makes it; kubectl reads its
# kubeconfig from KUBECONFIG, which this exports for the job's later steps.
#
#   gate/k3s.sh [k3s server flags...]
set -euo pipefail

pins=$(dirname "$0")/../cluster/node/pins.json
version=$(jq -r .k3s.version "$pins")
commit=$(jq -r .k3s.installScript.commit "$pins")
sha=$(jq -r .k3s.installScript.sha256 "$pins")

curl -fsSL -o "$RUNNER_TEMP/k3s-install.sh" "https://raw.githubusercontent.com/k3s-io/k3s/$commit/install.sh"
echo "$sha  $RUNNER_TEMP/k3s-install.sh" | sha256sum --check
INSTALL_K3S_VERSION=$version sh "$RUNNER_TEMP/k3s-install.sh" --disable traefik --write-kubeconfig-mode 644 "$@"
echo "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" >>"$GITHUB_ENV"
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
until kubectl get node -o name | grep -q node; do sleep 2; done
kubectl wait --for=condition=Ready node --all --timeout=180s
# Its packaged components, which it applies once it runs.
for deployment in metrics-server local-path-provisioner; do
  until kubectl -n kube-system get deploy "$deployment" >/dev/null 2>&1; do sleep 2; done
  kubectl -n kube-system rollout status deploy "$deployment" --timeout=180s
done
