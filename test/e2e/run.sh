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

cleanup() {
  if [[ "${KEEP:-}" != 1 ]]; then
    k3d cluster delete "$name" >/dev/null 2>&1 || true
    k3d registry delete "k3d-$registry" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

k3d registry list -o json | grep -q "\"k3d-$registry\"" || k3d registry create "$registry" --port "$port"

# The images, pushed where the cluster pulls them from, as a release publishes them.
for image in "alasio:." "alasio-agent:sandbox/agent" "alasio-lake:neon/lake"; do
  repository=${image%%:*}
  docker build --quiet --tag "localhost:$port/$repository:e2e" "$root/${image#*:}"
  docker push --quiet "localhost:$port/$repository:e2e"
done

KUBECONFIG_OUT=$KUBECONFIG AGENTS=$agents "$root/deploy/k3d/cluster.sh" "$name" --registry-use "k3d-$registry:$port"

# The stand-ins alasio talks to instead of Telegram and a telemetry backend.
kubectl create namespace alasio-test
kubectl --namespace alasio-test create configmap telegram-stub --from-file="$root/test/e2e/telegram-stub.mjs"
kubectl --namespace alasio-test create configmap otlp-sink --from-file="$root/test/e2e/otlp-sink.mjs"
kubectl --namespace alasio-test apply -f "$root/test/e2e/telegram-stub.yaml" -f "$root/test/e2e/otlp-sink.yaml"
kubectl --namespace alasio-test rollout status deployment/telegram-stub deployment/otlp-sink --timeout=300s

kubectl create namespace alasio
kubectl --namespace alasio create secret generic alasio-telegram --from-literal=token=123:e2e --from-literal=allowedUserIds=1001

images=k3d-$registry:$port
helm dependency build "$root/charts/alasio" >/dev/null
helm install alasio "$root/charts/alasio" --namespace alasio --wait --timeout 20m \
  --set-string "images.alasio.repository=$images/alasio,images.alasio.tag=e2e" \
  --set-string "images.agent.repository=$images/alasio-agent,images.agent.tag=e2e" \
  --set-string "images.lake.repository=$images/alasio-lake,images.lake.tag=e2e" \
  --set-string "alasio.telegram.existingSecret=alasio-telegram" \
  --set-string "alasio.env.TELEGRAM_API_ROOT=http://telegram.alasio-test.svc:8081" \
  --set-string "telemetry.otlpEndpoint=http://otlp.alasio-test.svc:4318" \
  --set-string "neon.safekeepers.storage.size=2Gi,neon.pageserver.storage.size=5Gi" \
  --set-string "objectStore.bundled.storage.size=10Gi,sessions.storage.size=1Gi"

cd "$root"
ALASIO_E2E_TELEMETRY=1 node --test --test-concurrency=1 --test-timeout=1800000 test/e2e/alasio.test.mjs
npm run test:neon
