#!/usr/bin/env bash
# The criu system's job (DESIGN.md section 8): installs CRIU and runc (install.sh), exports the
# instance's image to a root filesystem, makes the instances' network namespace, and runs the
# harness over the cases asked, which writes $BENCH_DIR/results.jsonl.
#
#   bench/criu/run.sh <target> <profile>
#
# With BENCH_DIR, BENCH_RUNS, BENCH_CASES and GITHUB_WORKSPACE as the workflow gives them.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
target=$1
profile=$2
[ "$target" = host ] || { echo "criu runs on the host target only, not $target" >&2; exit 1; }

IMAGE=ghcr.io/eaucoin/alasio-agent:10.0.0@sha256:b90f8164f77e9a56bd0da0fc1869bc76c1c541742a07751764ffc156fa8fe30a
work=$BENCH_DIR/staging/criu
logs=$BENCH_DIR/logs/criu
mkdir -p "$logs"

"$here/install.sh" 2>&1 | tee "$logs/install.log"

# The instance's root filesystem: the image's, whole, which the instances mount read-only.
sudo mkdir -p "$work/rootfs"
docker pull --quiet "$IMAGE"
container=$(docker create "$IMAGE")
docker export "$container" | sudo tar --extract --numeric-owner --directory "$work/rootfs"
docker rm "$container" >/dev/null

# The instances' network: a namespace runc is given, which CRIU leaves to it as external, and a
# veth to the runner, as runc's checkpoint tests and Podman give a container one.
sudo ip netns add criu-bench
sudo ip link add criu-bench0 type veth peer name eth0 netns criu-bench
sudo ip address add 10.89.0.1/30 dev criu-bench0
sudo ip link set criu-bench0 up
sudo ip -n criu-bench address add 10.89.0.2/30 dev eth0
sudo ip -n criu-bench link set eth0 up
sudo ip -n criu-bench link set lo up

set -a
. "$BENCH_DIR/store/store.env"
set +a
sudo --preserve-env=S3_ENDPOINT,S3_BUCKET,S3_REGION,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,GITHUB_RUN_ID,GITHUB_JOB,GITHUB_SHA \
  "$(command -v node)" "$here/harness.ts" \
  --profile "$profile" --target "$target" --work "$work" --logs "$logs" \
  --host "$BENCH_DIR/host.json" --store "$BENCH_DIR/store.json" --dist "$GITHUB_WORKSPACE/bench/common/dist" \
  --out "$BENCH_DIR/results.jsonl" --runs "${BENCH_RUNS:-5}" --cases "${BENCH_CASES:-}" \
  2>&1 | tee "$logs/harness.log"
