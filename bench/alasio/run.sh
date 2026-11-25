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
# Spike: the CPU time of each command on the runner, in clock ticks, summed over its processes, every second, to
# place what a cold restore waits on.
(while sleep 1; do
  t=$(date -u +%T.%N)
  { sudo cat /proc/[0-9]*/stat 2>/dev/null || true; } | awk -v t="$t" '{ cpu[$2] += $14 + $15 } END { for (c in cpu) if (cpu[c] > 0) print t, c, cpu[c] }' || true
done) >"$BENCH_DIR/logs/cpu.log" &
# Spike: each Node.js process's CPU time apart, by its pid, every second, and what each pid runs every 10 s, to tell the
# node agent's from the bench's.
(while sleep 1; do
  t=$(date -u +%T.%N)
  { sudo cat /proc/[0-9]*/stat 2>/dev/null || true; } | awk -v t="$t" '$2 == "(MainThread)" || $2 == "(node)" { print t, $1, $14 + $15 }' || true
done) >"$BENCH_DIR/logs/node-cpu.log" &
(while sleep 10; do echo "== $(date -u +%T)"; ps -eo pid,args --no-headers | awk '$2 ~ /node/' || true; done) >"$BENCH_DIR/logs/node-pids.log" &
# Spike: the node's own counters of the store's port, and its nat table's OUTPUT, every minute.
(while sleep 60; do echo "== $(date -u +%T)"; sudo iptables -L BENCH-STORE-OUT -v -x -n 2>&1; sudo iptables -L BENCH-STORE-IN -v -x -n 2>&1; sudo iptables -t nat -L OUTPUT -v -n 2>&1 | head -8; done) >"$BENCH_DIR/logs/iptables.log" &
# The runner's writeback held to 256 MiB dirty, 64 MiB before it starts: a save of 3 GiB left gigabytes dirty on the
# disk the store and the node's cache share with k3s, and the stall of the fsyncs behind them ended k3s ("Transaction
# commit failed"). Said in logs/writeback.txt.
sudo sysctl -w vm.dirty_bytes=268435456 vm.dirty_background_bytes=67108864 | tee "$BENCH_DIR/logs/writeback.txt"
ALASIO_E2E_TARGET=host ALASIO_E2E_AGENTS=0 ALASIO_E2E_SHARD=bench \
  node --test --test-timeout=21600000 --test-force-exit --test-reporter=spec bench/alasio/bench.test.ts
