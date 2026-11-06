#!/usr/bin/env bash
# Installs what the gvisor system runs on a CI runner: upstream gVisor's release (runsc, its
# shim and its gvisor-bin/ sidecars, from the release tarball, checked against its SHA-512, as
# gvisor.dev's install guide does it), and rclone, whose FUSE mount of the bucket the `lazy`
# variant restores through. Run as root.
set -euo pipefail

GVISOR_RELEASE=20261019.0
RCLONE_VERSION=v1.75.2

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

gvisor="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE}/x86_64"
curl -fsSLO "$gvisor/gvisor.tar.zstd"
curl -fsSLO "$gvisor/gvisor.tar.zstd.sha512"
sha512sum -c gvisor.tar.zstd.sha512
tar --zstd -xf gvisor.tar.zstd -C /usr/local/bin
/usr/local/bin/runsc --version

rclone="rclone-${RCLONE_VERSION}-linux-amd64"
curl -fsSLO "https://downloads.rclone.org/${RCLONE_VERSION}/${rclone}.zip"
curl -fsSLO "https://downloads.rclone.org/${RCLONE_VERSION}/SHA256SUMS"
grep " ${rclone}.zip\$" SHA256SUMS | sha256sum -c
unzip -q "${rclone}.zip"
install -m 0755 "${rclone}/rclone" /usr/local/bin/rclone
/usr/local/bin/rclone version

# rclone mounts through the kernel's FUSE device, which the runner's image has; fuse3 gives
# the fusermount3 that unmounts it.
command -v fusermount3 >/dev/null || { apt-get update -q && apt-get install -yq fuse3; }
test -c /dev/fuse
