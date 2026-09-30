#!/bin/bash
# Builds the two session-filesystem images (see sandbox/README.md):
#   alasio/session-host  gVisor + JuiceFS + the entrypoint
#   alasio/agent         bayma's image, run as the unprivileged agent
# It fetches gVisor and JuiceFS pinned by checksum. Run from anywhere; the installer
# calls it. No host install results.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
GVISOR_RELEASE=${GVISOR_RELEASE:-release-20260921.0}
JUICEFS_VERSION=${JUICEFS_VERSION:-1.4.1}
ARCH=x86_64

fetch() { # url dest  (Node, which the installer already requires, so no curl/wget needed)
  node -e 'const fs=require("fs");const[,u,d]=process.argv;fetch(u).then(async r=>{if(!r.ok)throw new Error(r.status+" "+u);fs.writeFileSync(d,Buffer.from(await r.arrayBuffer()))}).catch(e=>{console.error(String(e));process.exit(1)})' "$1" "$2"
}

echo "sandbox: fetching gVisor $GVISOR_RELEASE"
sh=$HERE/session-host
rm -rf "$sh/gvisor" && mkdir -p "$sh/gvisor"
base="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE#release-}/$ARCH"
fetch "$base/gvisor.tar.zstd" "$sh/gvisor/gvisor.tar.zstd"
fetch "$base/gvisor.tar.zstd.sha512" "$sh/gvisor/gvisor.tar.zstd.sha512"
( cd "$sh/gvisor" && sha512sum -c gvisor.tar.zstd.sha512 )
# zstd may be absent; Node's zlib always has it.
node -e 'const fs=require("fs");fs.writeFileSync(process.argv[2],require("zlib").zstdDecompressSync(fs.readFileSync(process.argv[1])))' \
  "$sh/gvisor/gvisor.tar.zstd" "$sh/gvisor/gvisor.tar"
tar -xf "$sh/gvisor/gvisor.tar" -C "$sh/gvisor"
rm -f "$sh/gvisor/gvisor.tar" "$sh/gvisor/gvisor.tar.zstd" "$sh/gvisor/gvisor.tar.zstd.sha512"

echo "sandbox: fetching JuiceFS $JUICEFS_VERSION"
tmp=$(mktemp -d)
fetch "https://github.com/juicedata/juicefs/releases/download/v$JUICEFS_VERSION/juicefs-$JUICEFS_VERSION-linux-amd64.tar.gz" "$tmp/j.tgz"
tar -xzf "$tmp/j.tgz" -C "$tmp" juicefs
install -m755 "$tmp/juicefs" "$sh/juicefs"
rm -rf "$tmp"

echo "sandbox: building alasio/session-host"
docker build -t alasio/session-host "$sh"
echo "sandbox: building alasio/agent"
docker build -t alasio/agent "$HERE/agent"
echo "sandbox: images built"
