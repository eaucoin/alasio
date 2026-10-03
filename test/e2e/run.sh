#!/usr/bin/env bash
# alasio's end-to-end run: builds its images, creates a k3d cluster with gVisor
# (deploy/k3d/cluster.sh) that pulls them from a registry beside it, installs the chart
# with Telegram and OTLP stand-ins, and runs the end-to-end and Neon suites against it.
#
#   test/e2e/run.sh
#
# Environment: AGENTS, agent nodes beside the server (0 unless set); KEEP=1 leaves the
# cluster and registry up afterwards. Needs docker, k3d, kubectl, helm and node.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
agents=${AGENTS:-0}
name=alasio-e2e-$agents
registry=alasio-e2e-registry
port=5050
work=$(mktemp -d)
export KUBECONFIG=$work/kubeconfig

# What the cluster was doing, when something did not come up.
diagnose() {
  echo "::group::cluster state"
  kubectl get nodes -o wide || true
  kubectl get pods --all-namespaces -o wide || true
  kubectl get events --all-namespaces --sort-by=.lastTimestamp | tail -60 || true
  for pod in $(kubectl get pods --all-namespaces --no-headers 2>/dev/null | awk '$3 !~ /^([0-9]+)\/\1$/ && $4 != "Completed" {print $1 "/" $2}'); do
    echo "--- $pod"
    kubectl --namespace "${pod%%/*}" describe pod "${pod#*/}" | tail -25 || true
    kubectl --namespace "${pod%%/*}" logs "${pod#*/}" --all-containers --tail 40 || true
  done
  echo "::endgroup::"
}

cleanup() {
  status=$?
  [[ $status -ne 0 ]] && diagnose
  if [[ "${KEEP:-}" != 1 ]]; then
    k3d cluster delete "$name" >/dev/null 2>&1 || true
    k3d registry delete "k3d-$registry" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

k3d registry list -o json | grep -q "\"k3d-$registry\"" || k3d registry create "$registry" --port "$port"

# The images, pushed where the cluster pulls them from, as a release publishes them, and
# then dropped here, with what they were built from: the cluster keeps its own copies.
for image in "alasio:." "alasio-agent:sandbox/agent" "alasio-lake:neon/lake"; do
  repository=${image%%:*}
  docker build --quiet --tag "localhost:$port/$repository:e2e" "$root/${image#*:}"
  docker push --quiet "localhost:$port/$repository:e2e"
  docker image rm "localhost:$port/$repository:e2e" >/dev/null
  docker image prune --all --force >/dev/null
  docker builder prune --all --force >/dev/null
done

KUBECONFIG_OUT=$KUBECONFIG AGENTS=$agents "$root/deploy/k3d/cluster.sh" "$name" --registry-use "k3d-$registry:$port"

# The stand-ins alasio talks to instead of Telegram and a telemetry backend.
kubectl create namespace alasio-test
kubectl --namespace alasio-test create configmap telegram-stub --from-file="$root/test/e2e/telegram-stub.ts"
kubectl --namespace alasio-test create configmap otlp-sink --from-file="$root/test/e2e/otlp-sink.ts"
kubectl --namespace alasio-test apply -f "$root/test/e2e/telegram-stub.yaml" -f "$root/test/e2e/otlp-sink.yaml"
kubectl --namespace alasio-test rollout status deployment/telegram-stub deployment/otlp-sink --timeout=300s

kubectl create namespace alasio
kubectl --namespace alasio create secret generic alasio-telegram --from-literal=token=123:e2e --from-literal=allowedUserIds=1001

# On several nodes, each pulls the images its own pods run, so the heavy ones are placed
# once each: Neon and its object store on the server, sessions on one agent and alasio on
# another, which still crosses nodes everywhere alasio reaches. The host profile, for a
# single machine, is off there.
placement=()
if (( agents >= 2 )); then
  placement=(
    --set-string "neon.nodeSelector.kubernetes\\.io/hostname=k3d-$name-server-0"
    --set-string "sessions.nodeSelector.kubernetes\\.io/hostname=k3d-$name-agent-0"
    --set-string "alasio.nodeSelector.kubernetes\\.io/hostname=k3d-$name-agent-1"
    --set "host.enabled=false"
  )
else
  export ALASIO_E2E_HOST=1
fi

images=k3d-$registry:$port
helm dependency build "$root/charts/alasio" >/dev/null
helm install alasio "$root/charts/alasio" --namespace alasio --wait --timeout 20m \
  --values "$root/test/e2e/values.yaml" "${placement[@]}" \
  --set-string "images.alasio.repository=$images/alasio,images.alasio.tag=e2e" \
  --set-string "images.agent.repository=$images/alasio-agent,images.agent.tag=e2e" \
  --set-string "images.lake.repository=$images/alasio-lake,images.lake.tag=e2e"

cd "$root"
ALASIO_E2E_TELEMETRY=1 node --test --test-concurrency=1 --test-timeout=1800000 test/e2e/alasio.test.ts
npm run test:neon
