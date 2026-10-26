#!/usr/bin/env bash
# The runner, as every result records it (DESIGN.md sections 2, 3 and 6): swap off, so every
# system runs without; then its CPU model, kernel, cores, memory, image, and its copy bandwidth,
# the sequential O_DIRECT write bandwidth of the disk images are staged on ($1), which the
# saturating fixture rewrites memory at. Prints the result's `host` object.
#
#   bench/common/host.sh <staging dir>
set -euo pipefail
staging=$1
sudo swapoff -a
mkdir -p "$staging"
probe="$staging/.copy-bandwidth"
said=$(dd if=/dev/zero of="$probe" bs=16M count=64 oflag=direct conv=fsync 2>&1 | tail -n 1)
rm -f "$probe"
# "1073741824 bytes (1.1 GB, 1.0 GiB) copied, 2.1 s, 511 MB/s"
seconds=$(sed -E 's/.* copied, ([0-9.]+) s.*/\1/' <<<"$said")
jq --null-input \
  --arg cpu "$(lscpu | sed -n 's/^Model name: *//p' | head -n 1)" \
  --arg kernel "$(uname -r)" \
  --argjson cores "$(nproc)" \
  --argjson memory "$(($(sed -n 's/^MemTotal: *\([0-9]*\) kB/\1/p' /proc/meminfo) * 1024))" \
  --argjson copy "$(awk -v s="$seconds" 'BEGIN { printf "%.1f", 1024 / s }')" \
  --arg image "${ImageOS:-unknown}/${ImageVersion:-unknown}" \
  '{cpu_model: $cpu, kernel: $kernel, cores: $cores, memory_bytes: $memory, copy_mib_s: $copy, runner_image: $image}'
