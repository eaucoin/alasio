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
IMAGE=ghcr.io/eaucoin/alasio-agent:10.0.0@sha256:b90f8164f77e9a56bd0da0fc1869bc76c1c541742a07751764ffc156fa8fe30a

work=$BENCH_DIR/firecracker
mkdir -p "$work" "$BENCH_DIR/logs"

curl -fsSLo "$work/firecracker.tgz" "https://github.com/firecracker-microvm/firecracker/releases/download/$FIRECRACKER_VERSION/firecracker-$FIRECRACKER_VERSION-x86_64.tgz"
echo "$FIRECRACKER_SHA256  $work/firecracker.tgz" | sha256sum --check --strict
tar -xzf "$work/firecracker.tgz" -C "$work"
sudo install "$work/release-$FIRECRACKER_VERSION-x86_64/firecracker-$FIRECRACKER_VERSION-x86_64" /usr/local/bin/firecracker
firecracker --version
curl -fsSLo "$work/vmlinux" "$KERNEL_URL"
echo "$KERNEL_SHA256  $work/vmlinux" | sha256sum --check --strict

# The handler and the guest's init, static, as the guest has no Go of its own.
(cd "$here" && CGO_ENABLED=0 go build -trimpath -o "$work/fcmem" ./fcmem && CGO_ENABLED=0 go build -trimpath -o "$work/bench-init" ./guest)

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

# Guest memory on 2 MiB hugetlbfs pages: the 5 GiB of one instance, which the runner holds one
# of at a time, and a few to spare.
echo 2600 | sudo tee /proc/sys/vm/nr_hugepages >/dev/null
[ "$(cat /proc/sys/vm/nr_hugepages)" -ge 2560 ] || { echo "the runner gave $(cat /proc/sys/vm/nr_hugepages) of 2560 hugepages" >&2; exit 1; }

# The guest's network: one tap, which every microVM of the job uses in turn, as a restored one
# needs the device its snapshot names.
sudo ip tuntap add dev tap0 mode tap
sudo ip addr add 172.16.0.1/30 dev tap0
sudo ip link set tap0 up

# Each instance and each handler in a cgroup of its own, for their memory and hugetlb counters.
echo "+memory +hugetlb" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p /sys/fs/cgroup/bench
echo "+memory +hugetlb" | sudo tee /sys/fs/cgroup/bench/cgroup.subtree_control >/dev/null

set -a
. "$BENCH_DIR/store/store.env"
set +a
sudo --preserve-env "$(command -v node)" "$here/bench.ts" \
  --work "$work" --profile "$profile" --out "$BENCH_DIR/results.jsonl" --logs "$BENCH_DIR/logs" \
  --host "$BENCH_DIR/host.json" --store "$BENCH_DIR/store.json" --driver "$here/../common/dist/driver.mjs" \
  --s3 "$here/../common/store/s3.ts" --cases "${BENCH_CASES:-}" --runs "${BENCH_RUNS:-5}"
