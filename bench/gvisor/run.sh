#!/usr/bin/env bash
# The gvisor system's job (DESIGN.md section 8): upstream gVisor's release and rclone installed,
# then every case's runs, as root, which runsc, the network namespace and dropping the page
# cache need (harness.ts), against the job's store (common/store/up.sh).
#
#   bench/gvisor/run.sh <target> <profile>
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
target=$1
profile=$2
if [ "$target" != host ]; then
  echo "gvisor runs on the host target alone, not $target" >&2
  exit 2
fi
sudo "$here/install.sh"
set -a
. "$BENCH_DIR/store/store.env"
set +a
# What the harness wrote as root, the job's own user uploads.
trap 'sudo chown -R "$(id -u):$(id -g)" "$BENCH_DIR"' EXIT
sudo --preserve-env "$(command -v node)" "$here/harness.ts" "$profile"
