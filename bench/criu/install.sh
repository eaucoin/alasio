#!/usr/bin/env bash
# Installs the criu system on a GitHub-hosted ubuntu-24.04 runner (DESIGN.md section 8): CRIU
# built from its v4.3 tag (the project publishes no binaries), runc v1.5.2's release binary,
# and Mountpoint for Amazon S3 1.24.0's package (the lazy cold restore's reads of the bucket).
# Then `criu check`, which fails the job on a kernel that lacks what the saves and restores need.
#
#   bench/criu/install.sh
set -euo pipefail
CRIU_TAG=v4.3
CRIU_COMMIT=2b7ac603cda31d015d91c856c34c557cb2300e7d
RUNC_VERSION=v1.5.2
RUNC_SHA256=599f6f94ff8c5057241eff0d54c3c74f95c34935b6457b33fe545defc61e9488
MOUNTPOINT_VERSION=1.24.0
MOUNTPOINT_SHA256=410e9e1cf25d45d0b26cb19fa53475ba22f179ae46b636d0345a3cd08a4d4a3b

# CRIU's build dependencies, as its contrib/dependencies/apt-packages.sh names them, less the
# test suite's and the GPU plugins'; fuse3 for Mountpoint, time for the helpers' peak RSS.
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends \
  build-essential pkg-config git iproute2 iptables fuse3 time \
  libaio-dev libbsd-dev libcap-dev libgnutls28-dev liblz4-dev libnet-dev libnftables-dev \
  libnl-3-dev libnl-route-3-dev libprotobuf-c-dev libprotobuf-dev protobuf-c-compiler \
  protobuf-compiler python3-protobuf uuid-dev

build=$(mktemp -d)
git -C "$build" init -q
git -C "$build" fetch -q --depth 1 https://github.com/checkpoint-restore/criu.git "refs/tags/$CRIU_TAG"
[ "$(git -C "$build" rev-parse 'FETCH_HEAD^{commit}')" = "$CRIU_COMMIT" ]
git -C "$build" checkout -q FETCH_HEAD
make -C "$build" -j"$(nproc)" criu
sudo install -m 0755 "$build/criu/criu" /usr/local/sbin/criu
rm -rf "$build"

curl -fsSL -o /tmp/runc.amd64 "https://github.com/opencontainers/runc/releases/download/$RUNC_VERSION/runc.amd64"
echo "$RUNC_SHA256  /tmp/runc.amd64" | sha256sum --check --quiet
sudo install -m 0755 /tmp/runc.amd64 /usr/local/sbin/runc
rm -f /tmp/runc.amd64

curl -fsSL -o /tmp/mount-s3.deb "https://s3.amazonaws.com/mountpoint-s3-release/$MOUNTPOINT_VERSION/x86_64/mount-s3-$MOUNTPOINT_VERSION-x86_64.deb"
echo "$MOUNTPOINT_SHA256  /tmp/mount-s3.deb" | sha256sum --check --quiet
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q /tmp/mount-s3.deb
rm -f /tmp/mount-s3.deb

criu --version
runc --version
mount-s3 --version
# Category 1, and the features this system's saves and restores use: dirty-page tracking for
# the pre-dumps, non-cooperative userfaultfd for the lazy restores.
sudo criu check
sudo criu check --feature mem_dirty_track
sudo criu check --feature uffd-noncoop
