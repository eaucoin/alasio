#!/usr/bin/env bash
# The benchmark's alasio system on the job's runner: alasio installed from the images job's images
# as the end-to-end runs install it, on target $1, its sessions saved and restored through its own
# path, through the store profile $2 (test/e2e/snapshot-bench.test.ts).
#
#   bench/alasio/run.sh <host|docker> <profile>
set -euo pipefail
# The runner's memory every 10 s, the stack and the sessions on it together, to tell a run that ran out of it.
mkdir -p "$BENCH_DIR/logs"
(while sleep 10; do echo "$(date -u +%T) $(free -m | sed -n 2p)"; done) >"$BENCH_DIR/logs/memory.log" &
# The runner's writeback held to 256 MiB dirty, 64 MiB before it starts: a save of 3 GiB left gigabytes dirty on the
# disk the store and the node's cache share with k3s, and the stall of the fsyncs behind them ended k3s ("Transaction
# commit failed", run 40712100149). Said in logs/writeback.txt.
sudo sysctl -w vm.dirty_bytes=268435456 vm.dirty_background_bytes=67108864 | tee "$BENCH_DIR/logs/writeback.txt"
npm ci
ALASIO_E2E_TARGET=$1 ALASIO_E2E_AGENTS=0 ALASIO_E2E_SHARD=bench BENCH_PROFILE=$2 \
  node --test --test-timeout=21600000 --test-force-exit --test-reporter=spec test/e2e/snapshot-bench.test.ts
