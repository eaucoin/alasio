#!/bin/bash

# Copyright 2026 The gVisor Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# save_memory_test.sh checks that a sandbox's Sentry gives back to the host the
# memory its saves take: a container holding much memory of its own is
# checkpointed, left running, again and again, and the Sentry's own
# (anonymous) memory, once the saves have ended, must come back near what it
# held before them, rather than stay at what a save held. It also measures how
# long the running container is held up just after each save, as the Sentry
# collects the save's garbage, and how long the Sentry stopped the world to,
# as its log says, and says both.
#
# It needs root (through sudo), Docker, for a busybox root filesystem, and a C
# compiler that links statically.
#
# Usage: tools/fork/save_memory_test.sh RUNSC [RUNSC_FLAG...]

set -euo pipefail

readonly RUNSC="${1:?usage: save_memory_test.sh RUNSC [RUNSC_FLAG...]}"
shift
declare -ra FLAGS=("$@")

# What the container holds of its own, in MiB, and how many saves it takes.
readonly HOLD_MIB=1536 SAVES=6
# What the Sentry may keep, past what it held before the saves, once they
# have ended: what a save allocates is about a twentieth of what it saves.
readonly KEPT_LIMIT_MIB=32
# The longest the container may be held up after a save, in ms.
readonly STALL_LIMIT_MS=100

declare tmp
tmp="$(mktemp -d)"
readonly tmp
readonly root="${tmp}/root" bundle="${tmp}/bundle" image="${tmp}/image" logs="${tmp}/logs"

runsc() {
  sudo "${RUNSC}" --root="${root}" --network=none --debug --debug-log="${logs}/" "${FLAGS[@]}" "$@"
}

cleanup() {
  local status=$?
  if (( status != 0 )); then
    sudo find "${logs}" -type f -name '*.boot.txt' -exec tail -n 40 {} + >&2 || true
  fi
  runsc delete --force holder >/dev/null 2>&1 || true
  findmnt -rn -o TARGET | { grep "^${tmp}/" || true; } | sort -r | xargs -r sudo umount
  sudo rm -rf "${tmp}"
}
trap cleanup EXIT

mkdir -p "${bundle}/rootfs" "${image}"
# Docker Hub's busybox, from Google's mirror of Docker Hub, as smoke_test.sh's.
container="$(docker create mirror.gcr.io/library/busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e)"
docker export "${container}" | tar -C "${bundle}/rootfs" -x
docker rm "${container}" >/dev/null
# The holder touches every page of what it holds, then wakes every
# millisecond, writing to /tmp/stall the longest it has slept past a wake
# since /tmp/reset was last removed, in microseconds.
cat >"${tmp}/holder.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static long long now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

int main(int argc, char **argv) {
  size_t bytes = (size_t)atol(argv[1]) << 20;
  char *held = malloc(bytes);
  if (held == NULL) return 1;
  memset(held, 1, bytes);
  FILE *ready = fopen("/tmp/ready", "w");
  if (ready == NULL) return 1;
  fclose(ready);
  long long stall = 0, last = now_us(), written = last;
  const struct timespec ms = {0, 1000000};
  for (;;) {
    nanosleep(&ms, NULL);
    long long t = now_us();
    if (t - last - 1000 > stall) stall = t - last - 1000;
    last = t;
    if (access("/tmp/reset", F_OK) == 0) {
      unlink("/tmp/reset");
      stall = 0;
    }
    if (t - written >= 100000) {
      FILE *out = fopen("/tmp/stall.part", "w");
      if (out != NULL) {
        fprintf(out, "%lld\n", stall);
        fclose(out);
        rename("/tmp/stall.part", "/tmp/stall");
      }
      written = t;
    }
  }
}
EOF
cc -O2 -static -o "${bundle}/rootfs/holder" "${tmp}/holder.c"
(cd "${bundle}" && "${RUNSC}" spec -- /holder "${HOLD_MIB}")
sed -i 's/"terminal": true/"terminal": false/' "${bundle}/config.json"

# The Sentry's own memory, its anonymous resident pages, in MiB: of the
# processes named for it, the largest, as the others are its stubs.
sentry_anon_mib() {
  local pid
  for pid in $(pgrep -x gvisor_sentry); do
    sudo awk '/^VmRSS:/ { rss = $2 } /^RssAnon:/ { anon = $2 } END { print rss, int(anon / 1024) }' "/proc/${pid}/status" 2>/dev/null || true
  done | sort -n | tail -1 | cut -d" " -f2
}

runsc run --bundle="${bundle}" --detach holder
for _ in $(seq 1 120); do
  runsc exec holder cat /tmp/ready >/dev/null 2>&1 && break
  sleep 1
done
runsc exec holder cat /tmp/ready >/dev/null
# What it holds settles before the saves.
sleep 5
before="$(sentry_anon_mib)"
echo "the Sentry holds ${before} MiB of its own before the saves, the container ${HOLD_MIB} MiB"

worst=0
for save in $(seq 1 "${SAVES}"); do
  sudo rm -rf "${image}"
  runsc checkpoint --image-path="${image}" --leave-running holder
  # How long the container is held up in the seconds after the save.
  runsc exec holder touch /tmp/reset
  sleep 3
  stall_us="$(runsc exec holder cat /tmp/stall)"
  (( stall_us > worst )) && worst="${stall_us}"
  echo "save ${save}: the Sentry holds $(sentry_anon_mib) MiB; the container was held up for at most ${stall_us} us after it"
done

# The scavenger returns what the last save's collection freed at its own pace.
kept=0
for _ in $(seq 1 60); do
  kept=$(( $(sentry_anon_mib) - before ))
  (( kept <= KEPT_LIMIT_MIB )) && break
  sleep 1
done
echo "once the saves have ended, the Sentry keeps ${kept} MiB more than before them; the longest the container was held up after a save: ${worst} us"
# What the Sentry says of each collection after a save: how long it took, and
# how long the world stopped, as Go prints durations.
collections="$(sudo find "${logs}" -type f -name '*.boot.txt' -exec grep -h "Collected the save's garbage" {} + || true)"
echo "${collections}"
stopped_us="$(sed -n "s/.*the world stopped for \([0-9.]*\)\([a-zµ]*\) of it.*/\1 \2/p" <<<"${collections}" |
  awk '{ unit = $2; v = $1; if (unit == "ns") v /= 1000; else if (unit == "ms") v *= 1000; else if (unit == "s") v *= 1000000; if (v > max) max = v } END { printf "%d", max }')"
echo "the longest the Sentry stopped the world to collect a save's garbage: ${stopped_us} us, over $(grep -c . <<<"${collections}") collections"
if (( kept > KEPT_LIMIT_MIB )); then
  echo "the Sentry keeps the memory of its saves: ${kept} MiB more than before them, past ${KEPT_LIMIT_MIB} MiB" >&2
  exit 1
fi
if (( $(grep -c . <<<"${collections}") < SAVES )); then
  echo "the Sentry collected the garbage of fewer saves than ${SAVES}, as its log says" >&2
  exit 1
fi
if (( stopped_us > STALL_LIMIT_MS * 1000 || worst > STALL_LIMIT_MS * 1000 )); then
  echo "the container was held up for ${worst} us after a save, the world stopped for ${stopped_us} us, past ${STALL_LIMIT_MS} ms" >&2
  exit 1
fi
echo "a save's memory given back: ok"
