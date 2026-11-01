// Spike, not for main: what reading the time costs a sandbox's application
// right after a restore (G-18, bench/val/g18.py). Prints "READY", then serves
// commands read from stdin:
//
//   probe MS    reads CLOCK_MONOTONIC in a loop for MS milliseconds and answers
//               "PROBE" and, for each 10 ms window, the nanoseconds per read:
//               a read the VDSO serves costs tens of nanoseconds, one that
//               falls back to a system call microseconds;
//   ping X      answers "PONG X".
//
// Build: gcc -O2 -static -o clk clk.c

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e9 + ts.tv_nsec;
}

int main(void) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  printf("READY\n");
  char line[256];
  static double per_read[1000];
  while (fgets(line, sizeof line, stdin)) {
    if (!strncmp(line, "probe", 5)) {
      int windows = atoi(line + 6) / 10;
      if (windows > 1000) windows = 1000;
      double start = now_ns();
      for (int w = 0; w < windows; w++) {
        double end = start + (w + 1) * 1e7, t0 = now_ns(), t;
        long n = 1;
        while ((t = now_ns()) < end) n++;
        per_read[w] = (t - t0) / n;
      }
      printf("PROBE");
      for (int w = 0; w < windows; w++) printf(" %.0f", per_read[w]);
      printf("\n");
    } else if (!strncmp(line, "ping", 4)) {
      printf("PONG%s", line + 4);
    }
  }
  return 0;
}
