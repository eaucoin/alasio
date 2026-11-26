#!/usr/bin/env bash
# The benchmark of alasio on the job's runner: alasio installed from the images job's images as the
# end-to-end runs install it, on the runner itself (the host target), its sessions saved and restored
# through its own path, through the store profile $BENCH_PROFILE, in the variant $BENCH_VARIANT
# (bench/alasio/bench.test.ts).
#
#   bench/alasio/run.sh
set -euo pipefail
# The runner's memory every 10 s, the stack and the sessions on it together, to tell a run that ran out of it.
mkdir -p "$BENCH_DIR/logs"
(while sleep 10; do echo "$(date -u +%T) $(free -m | sed -n 2p)"; done) >"$BENCH_DIR/logs/memory.log" &
# The runner's CPU, its /proc/stat cpu line (user nice system idle iowait irq softirq steal, in ticks), its stalls (each
# /proc/pressure line: some, and full, their avg10 and total in µs) and its disks' counters (/proc/diskstats), every 5 s,
# as the kernel counts them: to tell a restore slow of the whole host, its disk or a neighbour, from one slow of its own.
(set +e; while sleep 5; do
  at=$(date -u +%T.%3N)
  echo "$at cpu $(sed -n 's/^cpu  *//p' /proc/stat)"
  for kind in cpu io memory; do sed "s/^/$at psi-$kind /" "/proc/pressure/$kind"; done
  grep -E ' (sd[a-z]+|nvme[0-9]+n[0-9]+|vd[a-z]+) ' /proc/diskstats | sed "s/^ */$at disk /"
done) >"$BENCH_DIR/logs/node.log" 2>/dev/null &
# k3s's journal, its kubelet's and containerd's, and the image snapshotter's, over the job, as it ends however it ends.
since=$(date -u '+%F %T')
trap 'sudo journalctl -u k3s -u "containerd-stargz-grpc*" --since "$since" --utc --no-pager -o short-iso-precise 2>/dev/null | gzip >"$BENCH_DIR/logs/journal.txt.gz"' EXIT
# The runner's writeback held to 256 MiB dirty, 64 MiB before it starts: a save of 3 GiB left gigabytes dirty on the
# disk the store and the node's cache share with k3s, and the stall of the fsyncs behind them ended k3s ("Transaction
# commit failed"). Said in logs/writeback.txt.
sudo sysctl -w vm.dirty_bytes=268435456 vm.dirty_background_bytes=67108864 | tee "$BENCH_DIR/logs/writeback.txt"
ALASIO_E2E_TARGET=host ALASIO_E2E_AGENTS=0 ALASIO_E2E_SHARD=bench \
  node --test --test-timeout=21600000 --test-force-exit --test-reporter=spec bench/alasio/bench.test.ts
