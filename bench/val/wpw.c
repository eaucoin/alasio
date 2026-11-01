// Spike, not for main: a workload for measuring first writes under dirty
// tracking (bench/val/arm.py). It holds MIB MiB of anonymous memory, filled
// with pseudo-random bytes (no zero pages), prints "READY <ms>", then serves
// commands read from stdin, one per line:
//
//   write PCT   writes one byte in each 4 KiB page of the first PCT % of the
//               buffer, once, and answers "WROTE <pages> <ms>";
//   ping X      answers "PONG X".
//
// With RATE_MIB_S > 0 a thread rewrites the buffer, page by page, at that rate
// (in 10 ms ticks, a second owed at most), as the saturating fixture does.
//
// Build: gcc -O2 -static -pthread -o wpw wpw.c

#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static uint8_t* buf;
static size_t npages;
static double rate_mib_s;
static volatile uint8_t gen = 1;

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

static void* rewriter(void* arg) {
  (void)arg;
  double owed = 0, last = now_ms();
  size_t next = 0;
  struct timespec tick = {0, 10 * 1000 * 1000};
  for (;;) {
    nanosleep(&tick, NULL);
    double now = now_ms();
    owed += rate_mib_s * 256 * (now - last) / 1000;  // pages
    if (owed > rate_mib_s * 256) owed = rate_mib_s * 256;
    last = now;
    for (; owed >= 1; owed -= 1) {
      buf[next * 4096 + 64] = gen;
      if (++next == npages) {
        next = 0;
        gen++;
      }
    }
  }
  return NULL;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s MIB [RATE_MIB_S]\n", argv[0]);
    return 2;
  }
  setvbuf(stdout, NULL, _IOLBF, 0);
  size_t mib = strtoul(argv[1], NULL, 10);
  rate_mib_s = argc > 2 ? atof(argv[2]) : 0;
  npages = mib * 256;
  buf = mmap(NULL, npages * 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (buf == MAP_FAILED) {
    perror("mmap");
    return 1;
  }
  double t0 = now_ms();
  uint64_t x = 0x9e3779b97f4a7c15ULL, *w = (uint64_t*)buf;
  for (size_t i = 0; i < npages * 4096 / 8; i++) {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    w[i] = x;
  }
  printf("READY %.0f\n", now_ms() - t0);
  if (rate_mib_s > 0) {
    pthread_t th;
    pthread_create(&th, NULL, rewriter, NULL);
  }
  char line[256];
  while (fgets(line, sizeof line, stdin)) {
    if (!strncmp(line, "write ", 6)) {
      double pct = atof(line + 6);
      size_t pages = (size_t)(npages * pct / 100);
      double s = now_ms();
      for (size_t p = 0; p < pages; p++) buf[p * 4096 + 128]++;
      printf("WROTE %zu %.3f\n", pages, now_ms() - s);
    } else if (!strncmp(line, "ping", 4)) {
      printf("PONG%s", line + 4);
    } else if (!strncmp(line, "quit", 4)) {
      break;
    }
  }
  return 0;
}
