// Spike, not for main: the two workloads of the parent-level OOM experiment
// (bench/val/oom.py).
//
//   om grow MIB_PER_S          allocates and touches memory at that rate, never
//                              freeing it, until it is killed;
//   om churn HOLD_MIB MIB_PER_S holds HOLD_MIB MiB, and allocates, touches and
//                              frees memory at that rate beside, 4 MiB at a
//                              time, so that its sandbox keeps charging pages.
//
// Each prints "READY" once it runs, and "ALIVE <n>" every 100 ms.
//
// Build: gcc -O2 -static -o om om.c

#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

#define CHUNK (4UL << 20)

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

static uint8_t* touched(size_t bytes) {
  uint8_t* p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) return NULL;
  for (size_t off = 0; off < bytes; off += 4096) p[off] = (uint8_t)off | 1;
  return p;
}

int main(int argc, char** argv) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  if (argc < 3) {
    fprintf(stderr, "usage: %s grow MIB_PER_S | churn HOLD_MIB MIB_PER_S\n", argv[0]);
    return 2;
  }
  int grow = !strcmp(argv[1], "grow");
  double rate = atof(argv[grow ? 2 : 3]);
  if (!grow && !touched(strtoul(argv[2], NULL, 10) << 20)) return 1;
  printf("READY\n");
  double interval = CHUNK / 1048576.0 / rate * 1000, next = now_ms(), report = next;
  unsigned long n = 0;
  for (;;) {
    uint8_t* p = touched(CHUNK);
    if (!grow && p) munmap(p, CHUNK);
    n++;
    double t = now_ms();
    if (t >= report) {
      printf("ALIVE %lu\n", n);
      report += 100;
    }
    next += interval;
    if (next > t) {
      struct timespec ts = {(time_t)((next - t) / 1000), (long)(((next - t) - (time_t)((next - t) / 1000) * 1000) * 1e6)};
      nanosleep(&ts, NULL);
    } else {
      next = t;
    }
  }
}
