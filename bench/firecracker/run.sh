#!/usr/bin/env bash
# Firecracker's job (DESIGN.md section 8): installs Firecracker v1.17.0 and the guest kernel its
# CI tests that release with, builds the guest's root filesystem from the session image and the
# page-fault handler, readies the runner (hugepages, the network, cgroups), then runs every case
# (bench.ts), writing $BENCH_DIR/results.jsonl. The workflow has set up the store, its profile's
# toxics and bench/common before, and written $BENCH_DIR/host.json and store.json.
#
#   bench/firecracker/run.sh <target> <profile>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
target=$1
profile=$2
[ "$target" = host ] || { echo "firecracker runs on the host alone, not $target" >&2; exit 1; }

FIRECRACKER_VERSION=v1.17.0
FIRECRACKER_SHA256=06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558
# The guest kernel of Firecracker's CI artifacts, the 6.1 series E2B's sandboxes run
# (vmlinux-6.1.158 there), at its latest build.
KERNEL_URL=https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261023-dd7c24b0d165-0/x86_64/vmlinux-6.1.188
KERNEL_SHA256=fc7a33d23a629d64db61ddf12789c737c12edb1702a5ce9d5546b95ca21dc6ca
# E2B's runtime and the Firecracker of theirs its handler needs, for the reference restores
# (e2b/main.go): the commit studied, and the release its v1.14 line serves
# (packages/shared/pkg/featureflags/flags.go DefaultFirecrackerV1_14Version).
E2B_RUNTIME_COMMIT=f551e44addb4acce285aa290fba39c260dd522fe
E2B_FIRECRACKER=v1.14.1_431f1fc
E2B_FIRECRACKER_SHA256=d81fd733be7e027406b4d5241442c447a2b5878b06dfa63dc236e68f3536d689
GO_VERSION=go1.27.3
GO_SHA256=65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40
IMAGE=ghcr.io/eaucoin/alasio-agent:10.0.0@sha256:b90f8164f77e9a56bd0da0fc1869bc76c1c541742a07751764ffc156fa8fe30a

work=$BENCH_DIR/firecracker
mkdir -p "$work" "$BENCH_DIR/logs"

# Guest memory on 2 MiB hugetlbfs pages: the 5 GiB of one instance, which the runner holds one
# of at a time, and a few to spare; taken first, from memory the page cache gives back and
# compacted, before the builds below fragment it.
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
echo 1 | sudo tee /proc/sys/vm/compact_memory >/dev/null
echo 2600 | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
[ "$(cat /proc/sys/vm/nr_hugepages)" -ge 2560 ] || { echo "the runner gave $(cat /proc/sys/vm/nr_hugepages) of 2560 hugepages" >&2; exit 1; }

curl -fsSLo "$work/firecracker.tgz" "https://github.com/firecracker-microvm/firecracker/releases/download/$FIRECRACKER_VERSION/firecracker-$FIRECRACKER_VERSION-x86_64.tgz"
echo "$FIRECRACKER_SHA256  $work/firecracker.tgz" | sha256sum --check --strict
tar -xzf "$work/firecracker.tgz" -C "$work"
sudo install "$work/release-$FIRECRACKER_VERSION-x86_64/firecracker-$FIRECRACKER_VERSION-x86_64" /usr/local/bin/firecracker
firecracker --version
curl -fsSLo "$work/vmlinux" "$KERNEL_URL"
echo "$KERNEL_SHA256  $work/vmlinux" | sha256sum --check --strict

# The handler and the guest's init, static, as the guest has no Go of its own.
curl -fsSLo "$work/go.tgz" "https://go.dev/dl/$GO_VERSION.linux-amd64.tar.gz"
echo "$GO_SHA256  $work/go.tgz" | sha256sum --check --strict
tar -xzf "$work/go.tgz" -C "$work"
export PATH="$work/go/bin:$PATH" GOTOOLCHAIN=local
(cd "$here" && go test -count=1 ./... && CGO_ENABLED=0 go build -trimpath -o "$work/fcmem" ./fcmem && CGO_ENABLED=0 go build -trimpath -o "$work/bench-init" ./guest)

# The reference restores, in the local and S3 Express jobs: E2B's handler, built in E2B's tree
# with E2B's go.mod, and E2B's Firecracker.
reference=()
if [ "$profile" = local ] || [ "$profile" = s3-express ]; then
  git init -q "$work/e2b-runtime"
  git -C "$work/e2b-runtime" fetch -q --depth 1 https://github.com/e2b-dev/runtime.git "$E2B_RUNTIME_COMMIT"
  git -C "$work/e2b-runtime" checkout -q FETCH_HEAD
  mkdir -p "$work/e2b-runtime/packages/orchestrator/cmd/bench-e2b-serve"
  cp "$here/e2b/main.go" "$work/e2b-runtime/packages/orchestrator/cmd/bench-e2b-serve/"
  (cd "$work/e2b-runtime/packages/orchestrator" && go build -trimpath -o "$work/e2b-serve" ./cmd/bench-e2b-serve)
  curl -fsSLo "$work/firecracker-e2b" "https://github.com/e2b-dev/fc-versions/releases/download/$E2B_FIRECRACKER/firecracker-amd64"
  echo "$E2B_FIRECRACKER_SHA256  $work/firecracker-e2b" | sha256sum --check --strict
  chmod +x "$work/firecracker-e2b"
  "$work/firecracker-e2b" --version
  reference=(--e2b-reference "$E2B_FIRECRACKER")
fi

# The root filesystem: the session image's, whole, with the init beside it, read-only to every run.
docker pull --quiet "$IMAGE"
container=$(docker create "$IMAGE")
sudo rm -rf "$work/rootfs" && mkdir "$work/rootfs"
docker export "$container" | sudo tar -x -C "$work/rootfs"
docker rm "$container" >/dev/null
docker rmi "$IMAGE" >/dev/null
sudo install -m 0755 "$work/bench-init" "$work/rootfs/sbin/bench-init"
size=$(( $(sudo du -sm "$work/rootfs" | cut -f1) * 11 / 10 + 512 ))
truncate -s "${size}M" "$work/rootfs.ext4"
sudo mkfs.ext4 -q -F -d "$work/rootfs" "$work/rootfs.ext4"
sudo rm -rf "$work/rootfs"

# The guest's network: one tap, which every microVM of the job uses in turn, as a restored one
# needs the device its snapshot names.
sudo ip tuntap add dev tap0 mode tap
sudo ip addr add 172.16.0.1/30 dev tap0
sudo ip link set tap0 up

# Each instance and each handler in a cgroup of its own, for their memory and hugetlb counters.
echo "+memory +hugetlb" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p /sys/fs/cgroup/bench
echo "+memory +hugetlb" | sudo tee /sys/fs/cgroup/bench/cgroup.subtree_control >/dev/null

# Firecracker's default page size beside the 2 MiB pages, in one profile's job, the store's own.
small_pages=()
[ "$profile" != local ] || small_pages=(--small-pages)
set -a
. "$BENCH_DIR/store/store.env"
set +a
sudo --preserve-env "$(command -v node)" "$here/bench.ts" \
  --work "$work" --profile "$profile" --out "$BENCH_DIR/results.jsonl" --logs "$BENCH_DIR/logs" \
  --host "$BENCH_DIR/host.json" --store "$BENCH_DIR/store.json" --driver "$here/../common/dist/driver.mjs" \
  --s3 "$here/../common/store/s3.ts" --cases "${BENCH_CASES:-}" --runs "${BENCH_RUNS:-5}" "${small_pages[@]}" "${reference[@]}"
