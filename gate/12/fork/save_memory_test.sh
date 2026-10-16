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
# memory its saves take. A container whose memory grows, in many small pieces
# as a busy application's does, is checkpointed, left running, after each step
# of its growth; once the saves have ended, the Sentry's own (anonymous) memory
# must come back near what it holds for a container grown the same way without
# a save, its baseline, rather than stay at what the largest save held. It
# also says how long the Sentry stopped the world to collect each save's
# garbage, as its log says, and how long the container was held up after each
# save.
#
# It needs root (through sudo), Docker, for a busybox root filesystem, and a C
# compiler that links statically.
#
# Usage: tools/fork/save_memory_test.sh RUNSC [RUNSC_FLAG...]

set -euo pipefail

readonly RUNSC="${1:?usage: save_memory_test.sh RUNSC [RUNSC_FLAG...]}"
shift
declare -ra FLAGS=("$@")

# Each step of the container's growth, in MiB of address space, of which it
# touches every other page, and how many steps, one save after each.
readonly STEP_MIB=512 STEPS=6
# What the Sentry may keep past its baseline once the saves have ended.
readonly KEPT_LIMIT_MIB=32
# The longest the Sentry may stop the world to collect a save's garbage, and
# the container may be held up after a save, in ms.
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
# The holder grows by a step each time /tmp/grow appears, removing it: a
# mapping of the step's size, every other page of which it touches, so that
# what it holds is in as many pieces as pages it touches; it writes how many
# steps it holds to /tmp/steps. Meanwhile it wakes every millisecond, and
# writes to /tmp/stall the longest it slept past a wake since /tmp/reset last
# appeared, in microseconds.
cat >"${tmp}/holder.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static long long now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

static void write_number(const char *path, long long n) {
  char part[64];
  snprintf(part, sizeof(part), "%s.part", path);
  FILE *out = fopen(part, "w");
  if (out == NULL) exit(1);
  fprintf(out, "%lld\n", n);
  fclose(out);
  rename(part, path);
}

int main(int argc, char **argv) {
  size_t step = (size_t)atol(argv[1]) << 20;
  long page = sysconf(_SC_PAGESIZE);
  long long steps = 0, stall = 0, last = now_us(), written = last;
  const struct timespec ms = {0, 1000000};
  write_number("/tmp/steps", steps);
  for (;;) {
    if (access("/tmp/grow", F_OK) == 0) {
      unlink("/tmp/grow");
      char *grown = mmap(NULL, step, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (grown == MAP_FAILED) return 1;
      for (size_t off = 0; off < step; off += 2 * page) grown[off] = 1;
      write_number("/tmp/steps", ++steps);
      last = now_us();
    }
    nanosleep(&ms, NULL);
    long long t = now_us();
    if (t - last - 1000 > stall) stall = t - last - 1000;
    last = t;
    if (access("/tmp/reset", F_OK) == 0) {
      unlink("/tmp/reset");
      stall = 0;
    }
    if (t - written >= 100000) {
      write_number("/tmp/stall", stall);
      written = t;
    }
  }
}
EOF
cc -O2 -static -o "${bundle}/rootfs/holder" "${tmp}/holder.c"
(cd "${bundle}" && "${RUNSC}" spec -- /holder "${STEP_MIB}")
sed -i 's/"terminal": true/"terminal": false/' "${bundle}/config.json"

# The Sentry's own memory, its anonymous resident pages, in MiB: of the
# processes named for it, the largest, as the others are its stubs.
sentry_anon_mib() {
  local pid
  for pid in $(pgrep -x gvisor_sentry); do
    sudo awk '/^VmRSS:/ { rss = $2 } /^RssAnon:/ { anon = $2 } END { print rss, int(anon / 1024) }' "/proc/${pid}/status" 2>/dev/null || true
  done | sort -n | tail -1 | cut -d" " -f2
}

# Has the holder grow to `steps` steps.
grow_to() {
  runsc exec holder touch /tmp/grow
  for _ in $(seq 1 120); do
    [[ "$(runsc exec holder cat /tmp/steps 2>/dev/null)" == "$1" ]] && return
    sleep 0.5
  done
  echo "the holder did not grow to ${1} steps" >&2
  exit 1
}

# What the Sentry holds of its own, in MiB, once the container has grown by
# STEPS steps, saved after each step when told `saved`, and once what it holds
# has settled: the scavenger returns memory at its own pace.
grown() {
  local saved="$1" step stall settled=0 now
  runsc run --bundle="${bundle}" --detach holder
  for _ in $(seq 1 60); do
    runsc exec holder cat /tmp/steps >/dev/null 2>&1 && break
    sleep 0.5
  done
  for step in $(seq 1 "${STEPS}"); do
    grow_to "${step}"
    if [[ "${saved}" == saved ]]; then
      sudo rm -rf "${image}"
      runsc checkpoint --image-path="${image}" --leave-running holder
      # How long the container is held up in the seconds after the save.
      runsc exec holder touch /tmp/reset
      sleep 3
      stall="$(runsc exec holder cat /tmp/stall)"
      echo "${stall}" >>"${tmp}/stalls"
      echo "after step ${step}'s save, the Sentry holds $(sentry_anon_mib) MiB; the container was held up for at most ${stall} us after it" >&2
    fi
  done
  # Settled once it has not fallen for 10 s.
  settled="$(sentry_anon_mib)"
  for _ in $(seq 1 12); do
    sleep 10
    now="$(sentry_anon_mib)"
    (( now >= settled )) && break
    settled="${now}"
  done
  runsc delete --force holder
  echo "${settled}"
}

baseline="$(grown unsaved)"
echo "grown by ${STEPS} steps of ${STEP_MIB} MiB without a save, the Sentry holds ${baseline} MiB of its own"
saved="$(grown saved)"
kept=$(( saved - baseline ))
echo "grown so with a save after each step, it holds ${saved} MiB: ${kept} MiB past its baseline"

worst_stall_us="$(sort -n "${tmp}/stalls" | tail -1)"
# What the Sentry says of each collection after a save: how long it took, and
# how long the world stopped, as Go prints durations.
collections="$(sudo find "${logs}" -type f -name '*.boot.txt' -exec grep -h "Collected the save's garbage" {} + || true)"
echo "${collections}"
count="$(grep -c . <<<"${collections}" || true)"
stopped_us="$(sed -n "s/.*the world stopped for \([0-9.]*\)\([a-zµ]*\) of it.*/\1 \2/p" <<<"${collections}" |
  awk '{ v = $1; if ($2 == "ns") v /= 1000; else if ($2 == "ms") v *= 1000; else if ($2 == "s") v *= 1000000; if (v > max) max = v } END { printf "%d", max }')"
echo "the longest the Sentry stopped the world to collect a save's garbage: ${stopped_us} us, over ${count} collections; the longest the container was held up after a save: ${worst_stall_us} us"

if (( kept > KEPT_LIMIT_MIB )); then
  echo "the Sentry keeps the memory of its saves: ${kept} MiB past its baseline, past ${KEPT_LIMIT_MIB} MiB" >&2
  exit 1
fi
if (( count < STEPS )); then
  echo "the Sentry collected the garbage of ${count} saves, not ${STEPS}, as its log says" >&2
  exit 1
fi
if (( stopped_us > STALL_LIMIT_MS * 1000 || worst_stall_us > STALL_LIMIT_MS * 1000 )); then
  echo "the world stopped for ${stopped_us} us, the container was held up for ${worst_stall_us} us, after a save: past ${STALL_LIMIT_MS} ms" >&2
  exit 1
fi
echo "a save's memory given back: ok"
