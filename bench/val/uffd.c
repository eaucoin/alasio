// Spike, not for main: what userfaultfd write protection costs the host (G-23,
// and the host's floor beside the fork's wp arming, X-ARM), on a memfd mapped
// shared, as the Sentry maps its memory file. Prints one JSON line per
// measurement:
//
//   uffd arm   MIB REPS   arming (UFFDIO_WRITEPROTECT over the whole mapping)
//                         after 0, 25 and 100 % of it was first written, in
//                         async mode (the kernel clears the bit on a write, as
//                         the fork's uffd source runs it) and in sync mode (a
//                         handler clears it); and arming only the 64 KiB units
//                         written, as a range list;
//   uffd fault MIB REPS   a synchronous first write's cost, a handler thread
//                         resolving each fault by unprotecting its 4 KiB page or
//                         64 KiB unit, with and without copying it first;
//   uffd efault           whether a kernel-mode write (read(2) into protected
//                         memory) fails with EFAULT under UFFD_USER_MODE_ONLY,
//                         and completes once the handler resolves it without.
//
// Run as root for the measurements; `efault` runs both as root and, by
// setuid to nobody, without privilege, as a Sentry runs.
//
// Build: gcc -O2 -pthread -o uffd uffd.c

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <signal.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#ifndef UFFD_USER_MODE_ONLY
#define UFFD_USER_MODE_ONLY 1
#endif
#ifndef UFFD_FEATURE_WP_ASYNC
#define UFFD_FEATURE_WP_ASYNC (1 << 15)
#endif

#define PAGE 4096UL
#define UNIT (64 * 1024UL)

static double now_s(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int cmp_double(const void* a, const void* b) {
  double x = *(const double*)a, y = *(const double*)b;
  return (x > y) - (x < y);
}

static double median(double* v, int n) {
  qsort(v, n, sizeof v[0], cmp_double);
  return v[n / 2];
}

// A userfaultfd with `features`, `flags` beside O_CLOEXEC; -errno where it
// could not be made or its API not agreed.
static int open_uffd(uint64_t features, int flags) {
  int fd = syscall(SYS_userfaultfd, O_CLOEXEC | flags);
  if (fd < 0) return -errno;
  struct uffdio_api api = {.api = UFFD_API, .features = features};
  if (ioctl(fd, UFFDIO_API, &api) < 0) {
    int e = errno;
    close(fd);
    return -e;
  }
  return fd;
}

static uint8_t* memfd_map(size_t bytes) {
  int fd = memfd_create("val-uffd", 0);
  if (fd < 0 || ftruncate(fd, bytes) < 0) {
    perror("memfd");
    exit(1);
  }
  uint8_t* p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (p == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  close(fd);
  for (size_t off = 0; off < bytes; off += PAGE) p[off] = 1;
  return p;
}

static void reg(int uffd, uint8_t* p, size_t bytes) {
  struct uffdio_register r = {.range = {.start = (uintptr_t)p, .len = bytes}, .mode = UFFDIO_REGISTER_MODE_WP};
  if (ioctl(uffd, UFFDIO_REGISTER, &r) < 0) {
    perror("UFFDIO_REGISTER");
    exit(1);
  }
}

static int protect(int uffd, uint8_t* p, size_t bytes, int on, int dontwake) {
  struct uffdio_writeprotect w = {.range = {.start = (uintptr_t)p, .len = bytes},
                                  .mode = (on ? UFFDIO_WRITEPROTECT_MODE_WP : 0) | (dontwake ? UFFDIO_WRITEPROTECT_MODE_DONTWAKE : 0)};
  return ioctl(uffd, UFFDIO_WRITEPROTECT, &w);
}

static void arm(size_t mib, int reps) {
  size_t bytes = mib << 20;
  const char* modes[] = {"async", "sync"};
  for (int m = 0; m < 2; m++) {
    int async = m == 0;
    uint64_t features = UFFD_FEATURE_WP_HUGETLBFS_SHMEM | UFFD_FEATURE_WP_UNPOPULATED | (async ? UFFD_FEATURE_WP_ASYNC : 0);
    int uffd = open_uffd(features, 0);
    if (uffd < 0) {
      printf("{\"test\":\"arm\",\"mode\":\"%s\",\"mib\":%zu,\"error\":\"%s\"}\n", modes[m], mib, strerror(-uffd));
      continue;
    }
    uint8_t* p = memfd_map(bytes);
    reg(uffd, p, bytes);
    double t0 = now_s();
    protect(uffd, p, bytes, 1, 0);
    double first = now_s() - t0;
    int fills[] = {0, 25, 100};
    for (int f = 0; f < 3; f++) {
      double whole[64], ranges[64], writes[64];
      size_t written = bytes / 100 * fills[f] / UNIT * UNIT;
      for (int r = 0; r < reps && r < 64; r++) {
        // First writes: in async mode by writing, the kernel clearing each page's bit;
        // in sync mode as a handler would leave them, unprotected by the ioctl.
        double w0 = now_s();
        if (async) {
          for (size_t off = 0; off < written; off += PAGE) p[off]++;
        } else if (written > 0) {
          protect(uffd, p, written, 0, 1);
          for (size_t off = 0; off < written; off += PAGE) p[off]++;
        }
        writes[r] = now_s() - w0;
        t0 = now_s();
        if (protect(uffd, p, bytes, 1, 0) < 0) perror("arm");
        whole[r] = now_s() - t0;
        // Again, then armed by the written 64 KiB units' ranges alone.
        if (async) {
          for (size_t off = 0; off < written; off += PAGE) p[off]++;
        } else if (written > 0) {
          protect(uffd, p, written, 0, 1);
        }
        t0 = now_s();
        for (size_t off = 0; off < written; off += UNIT) protect(uffd, p + off, UNIT, 1, 1);
        ranges[r] = now_s() - t0;
      }
      int n = reps < 64 ? reps : 64;
      double gib = bytes / (double)(1UL << 30);
      double wgib = written / (double)(1UL << 30);
      double units_s = median(ranges, n);
      printf("{\"test\":\"arm\",\"mode\":\"%s\",\"mib\":%zu,\"fill_pct\":%d,\"reps\":%d,\"first_arm_s\":%.6f,"
             "\"arm_whole_s\":%.6f,\"arm_whole_s_per_GiB_mapped\":%.6f,\"arm_units_s\":%.6f,\"arm_units_s_per_GiB_written\":%.6f,"
             "\"write_s\":%.6f}\n",
             modes[m], mib, fills[f], n, first, median(whole, n), median(whole, n) / gib, units_s, wgib > 0 ? units_s / wgib : 0.0,
             median(writes, n));
    }
    munmap(p, bytes);
    close(uffd);
  }
}

struct handler {
  int uffd;
  size_t unit;
  int copy;
  uint8_t* staging;
  volatile int stop;
  long faults;
};

static void* handle(void* arg) {
  struct handler* h = arg;
  struct pollfd pfd = {.fd = h->uffd, .events = POLLIN};
  while (!h->stop) {
    if (poll(&pfd, 1, 100) <= 0) continue;
    struct uffd_msg msg;
    if (read(h->uffd, &msg, sizeof msg) != sizeof msg) continue;
    if (msg.event != UFFD_EVENT_PAGEFAULT) continue;
    uintptr_t at = msg.arg.pagefault.address & ~(h->unit - 1);
    if (h->copy) memcpy(h->staging, (void*)at, h->unit);
    // Retried where the kernel asks (EAGAIN while the mm changes); said and given up on otherwise.
    int tries = 0;
    while (protect(h->uffd, (uint8_t*)at, h->unit, 0, 0) < 0) {
      if (errno != EAGAIN || ++tries > 1000) {
        fprintf(stderr, "unprotect of %#lx failed: %s\n", (unsigned long)at, strerror(errno));
        break;
      }
    }
    if (++h->faults % 65536 == 0) fprintf(stderr, "fault: %ld faults resolved\n", h->faults);
  }
  return NULL;
}

static void fault(size_t mib, int reps) {
  size_t bytes = mib << 20;
  // A fault no handler resolves would hang the writer forever: a minute per measurement at most.
  alarm(60 * reps * 4);
  size_t units[] = {PAGE, UNIT};
  for (int u = 0; u < 2; u++) {
    for (int copy = 0; copy < 2; copy++) {
      double per_page[64];
      long faults = 0;
      int n = reps < 64 ? reps : 64;
      int uffd = -1;
      for (int r = 0; r < n; r++) {
        uffd = open_uffd(UFFD_FEATURE_WP_HUGETLBFS_SHMEM | UFFD_FEATURE_WP_UNPOPULATED, 0);
        if (uffd < 0) break;
        uint8_t* p = memfd_map(bytes);
        reg(uffd, p, bytes);
        protect(uffd, p, bytes, 1, 0);
        struct handler h = {.uffd = uffd, .unit = units[u], .copy = copy, .staging = aligned_alloc(PAGE, UNIT)};
        fprintf(stderr, "fault: %zu MiB, unit %zu, copy %d, rep %d\n", mib, units[u], copy, r);
        pthread_t th;
        pthread_create(&th, NULL, handle, &h);
        double t0 = now_s();
        for (size_t off = 0; off < bytes; off += PAGE) p[off]++;
        per_page[r] = (now_s() - t0) / (bytes / PAGE);
        h.stop = 1;
        pthread_join(th, NULL);
        faults = h.faults;
        free(h.staging);
        munmap(p, bytes);
        close(uffd);
      }
      if (uffd < 0) {
        printf("{\"test\":\"fault\",\"error\":\"%s\"}\n", strerror(-uffd));
        return;
      }
      printf("{\"test\":\"fault\",\"mib\":%zu,\"unit\":%zu,\"copy\":%d,\"reps\":%d,\"faults\":%ld,\"per_page_us\":%.3f,\"per_fault_us\":%.3f}\n",
             mib, units[u], copy, n, faults, median(per_page, n) * 1e6, median(per_page, n) * 1e6 * (bytes / PAGE) / (faults ? faults : 1));
    }
  }
}

// read(2) of a file into a page of protected memory, with or without
// UFFD_USER_MODE_ONLY, a handler resolving faults: what it returned.
static void efault_one(int user_mode_only, const char* who) {
  int uffd = open_uffd(UFFD_FEATURE_WP_HUGETLBFS_SHMEM | UFFD_FEATURE_WP_UNPOPULATED, user_mode_only ? UFFD_USER_MODE_ONLY : 0);
  if (uffd < 0) {
    printf("{\"test\":\"efault\",\"as\":\"%s\",\"user_mode_only\":%d,\"uffd\":\"%s\"}\n", who, user_mode_only, strerror(-uffd));
    return;
  }
  uint8_t* p = memfd_map(UNIT);
  fprintf(stderr, "efault: as %s, user mode only %d\n", who, user_mode_only);
  reg(uffd, p, UNIT);
  protect(uffd, p, UNIT, 1, 0);
  struct handler h = {.uffd = uffd, .unit = PAGE, .copy = 0, .staging = NULL};
  pthread_t th;
  pthread_create(&th, NULL, handle, &h);
  int fd = open("/proc/self/stat", O_RDONLY);
  double t0 = now_s();
  ssize_t got = read(fd, p, 256);
  int e = errno;
  double took = now_s() - t0;
  // A user-mode write, beside, which the handler resolves either way.
  t0 = now_s();
  p[PAGE] = 7;
  double user_s = now_s() - t0;
  h.stop = 1;
  pthread_join(th, NULL);
  printf("{\"test\":\"efault\",\"as\":\"%s\",\"user_mode_only\":%d,\"read\":%zd,\"errno\":\"%s\",\"read_s\":%.6f,\"user_write_s\":%.6f,\"faults\":%ld}\n",
         who, user_mode_only, got, got < 0 ? strerror(e) : "", took, user_s, h.faults);
  close(fd);
  munmap(p, UNIT);
  close(uffd);
}

static void efault(void) {
  alarm(60);
  FILE* f = fopen("/proc/sys/vm/unprivileged_userfaultfd", "r");
  int unpriv = -1;
  if (f) {
    if (fscanf(f, "%d", &unpriv) != 1) unpriv = -1;
    fclose(f);
  }
  printf("{\"test\":\"sysctl\",\"unprivileged_userfaultfd\":%d}\n", unpriv);
  efault_one(1, "root");
  efault_one(0, "root");
  if (setgid(65534) == 0 && setuid(65534) == 0) {
    efault_one(1, "nobody");
    efault_one(0, "nobody");
  }
}

int main(int argc, char** argv) {
  setvbuf(stdout, NULL, _IOLBF, 0);
  if (argc >= 2 && !strcmp(argv[1], "efault")) {
    efault();
    return 0;
  }
  if (argc < 4) {
    fprintf(stderr, "usage: %s arm|fault MIB REPS | efault\n", argv[0]);
    return 2;
  }
  size_t mib = strtoul(argv[2], NULL, 10);
  int reps = atoi(argv[3]);
  if (!strcmp(argv[1], "arm")) arm(mib, reps);
  if (!strcmp(argv[1], "fault")) fault(mib, reps);
  return 0;
}
