// Spike, not for main: a workload for reclaiming a frozen sandbox's memory and
// faulting it back (bench/val/zswap.py). It holds MIB MiB of anonymous memory
// filled from the file CORPUS, cycled (or pseudo-random bytes where CORPUS is
// "random"), prints "READY <ms>", then serves commands read from stdin:
//
//   touch    reads one word of every 4 KiB page, answering "TOUCHED <ms> <sum>";
//   ptouch N the same in N threads, each over its own part, answering
//            "PTOUCHED <ms> <sum>";
//   ping X   answers "PONG X".
//
// Build: gcc -O2 -static -pthread -o zsw zsw.c

#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

struct part {
  uint8_t* from;
  size_t bytes;
  uint64_t sum;
};

static void* touch_part(void* arg) {
  struct part* p = arg;
  for (size_t off = 0; off < p->bytes; off += 4096) p->sum += *(volatile uint64_t*)(p->from + off);
  return NULL;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    fprintf(stderr, "usage: %s MIB CORPUS|random\n", argv[0]);
    return 2;
  }
  setvbuf(stdout, NULL, _IOLBF, 0);
  size_t bytes = strtoul(argv[1], NULL, 10) << 20;
  uint8_t* buf = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (buf == MAP_FAILED) {
    perror("mmap");
    return 1;
  }
  double t0 = now_ms();
  if (!strcmp(argv[2], "random")) {
    uint64_t x = 0x9e3779b97f4a7c15ULL, *w = (uint64_t*)buf;
    for (size_t i = 0; i < bytes / 8; i++) {
      x ^= x << 13;
      x ^= x >> 7;
      x ^= x << 17;
      w[i] = x;
    }
  } else {
    int fd = open(argv[2], O_RDONLY);
    if (fd < 0) {
      perror(argv[2]);
      return 1;
    }
    size_t at = 0;
    while (at < bytes) {
      ssize_t n = read(fd, buf + at, bytes - at);
      if (n <= 0) {
        if (lseek(fd, 0, SEEK_SET) < 0 || at == 0) return 1;
        continue;
      }
      at += n;
    }
    close(fd);
  }
  printf("READY %.0f\n", now_ms() - t0);
  char line[256];
  while (fgets(line, sizeof line, stdin)) {
    if (!strncmp(line, "touch", 5)) {
      double s = now_ms();
      uint64_t sum = 0;
      for (size_t off = 0; off < bytes; off += 4096) sum += *(volatile uint64_t*)(buf + off);
      printf("TOUCHED %.3f %llu\n", now_ms() - s, (unsigned long long)sum);
    } else if (!strncmp(line, "ptouch", 6)) {
      int n = atoi(line + 7);
      if (n < 1 || n > 64) n = 1;
      struct part parts[64];
      pthread_t threads[64];
      size_t each = bytes / n / 4096 * 4096;
      double s = now_ms();
      for (int i = 0; i < n; i++) {
        parts[i] = (struct part){.from = buf + i * each, .bytes = i == n - 1 ? bytes - i * each : each, .sum = 0};
        pthread_create(&threads[i], NULL, touch_part, &parts[i]);
      }
      uint64_t sum = 0;
      for (int i = 0; i < n; i++) {
        pthread_join(threads[i], NULL);
        sum += parts[i].sum;
      }
      printf("PTOUCHED %.3f %llu\n", now_ms() - s, (unsigned long long)sum);
    } else if (!strncmp(line, "ping", 4)) {
      printf("PONG%s", line + 4);
    } else if (!strncmp(line, "quit", 4)) {
      break;
    }
  }
  return 0;
}
