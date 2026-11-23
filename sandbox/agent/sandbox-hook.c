// bayma's sandbox hook, without a Node process of its own.
//
//   bayma-sandbox-hook [save|restore|resume|stop] --state-dir PATH
//                      [--leave-running]
//
// It is `bayma sandbox-hook` (packages/core/src/mcp/sandbox-hook-client.ts),
// taking the same arguments and variables and saying the same to the server
// over the same socket, for a host to run in the sandbox in its place: a host
// that saves the sandbox whole runs the hook on the path of every save and
// restore, where a Node process's start, reading its binary and modules into
// a restored sandbox, takes a few hundred milliseconds, and this one's a few.
// What the server answers it prints, a save's what it caught, as JSON; what
// it could not do it says, in the Node hook's words, system errors as Node
// names them, and exits with 1.
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

// As sandbox-hook-client.ts names them.
#define MODE_VARIABLE "GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE"
#define STOP_REASON_VARIABLE "BAYMA_SANDBOX_STOP_REASON"
#define MAX_STOP_REASON_BYTES 256
#define LEAVE_RUNNING_OPTION "--leave-running"
#define STATE_DIR_OPTION "--state-dir"

static const char *const MODES[] = {"save", "restore", "resume", "stop"};

// Says what the hook could not do, and exits as the Node hook does.
static void fail(const char *format, ...)
    __attribute__((format(printf, 1, 2), noreturn));
static void fail(const char *format, ...) {
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fputc('\n', stderr);
  exit(1);
}

// Whether `value` is one of the modes.
static int is_mode(const char *value) {
  for (size_t i = 0; i < sizeof MODES / sizeof *MODES; i++)
    if (value && strcmp(value, MODES[i]) == 0) return 1;
  return 0;
}

// The name Node gives `error`, as ENOENT.
static const char *code_of(int error) {
  const char *name = strerrorname_np(error);
  return name ? name : "UNKNOWN";
}

// What Node says of `error` in a file system call's message, as libuv says it.
static const char *description_of(int error) {
  switch (error) {
    case ENOENT:
      return "no such file or directory";
    case EACCES:
      return "permission denied";
    case ENOTDIR:
      return "not a directory";
    case ELOOP:
      return "too many symbolic links encountered";
    case ENAMETOOLONG:
      return "name too long";
    case EPERM:
      return "operation not permitted";
    case EIO:
      return "i/o error";
    case ENOMEM:
      return "not enough memory";
    default: {
      // libuv's own words are strerror's, but for their first letter.
      static char said[256];
      snprintf(said, sizeof said, "%s", strerror(error));
      said[0] = (char)tolower((unsigned char)said[0]);
      return said;
    }
  }
}

// Fails as Node's fs.realpathSync does where `syscall` on `path` fails.
static void fs_failed(int error, const char *syscall, const char *path)
    __attribute__((noreturn));
static void fs_failed(int error, const char *syscall, const char *path) {
  fail("%s: %s, %s '%s'", code_of(error), description_of(error), syscall,
       path);
}

// Says that no server answers on the socket at `path`, as Node's net says
// `syscall` failed with `error`: a connect naming the path, a read or a
// write naming none.
static void unanswered(const char *path, const char *syscall, int error)
    __attribute__((noreturn));
static void unanswered(const char *path, const char *syscall, int error) {
  if (strcmp(syscall, "connect") == 0)
    fail("no bayma server answers on %s: %s %s %s", path, syscall,
         code_of(error), path);
  fail("no bayma server answers on %s: %s %s", path, syscall, code_of(error));
}

// `path`, absolute and normalized, as Node's path.resolve makes it from the
// working directory: without `.` or `..`, repeated or trailing slashes.
static char *resolve(const char *path) {
  char cwd[PATH_MAX];
  if (path[0] != '/' && !getcwd(cwd, sizeof cwd)) fail("%s", strerror(errno));
  size_t length = (path[0] == '/' ? 0 : strlen(cwd)) + strlen(path) + 2;
  char *joined = malloc(length), *out = malloc(length + 1);
  if (!joined || !out) fail("%s", strerror(ENOMEM));
  snprintf(joined, length, "%s/%s", path[0] == '/' ? "" : cwd, path);
  size_t used = 0;
  for (char *part = strtok(joined, "/"); part; part = strtok(NULL, "/")) {
    if (strcmp(part, ".") == 0) continue;
    if (strcmp(part, "..") == 0) {
      while (used > 0 && out[used - 1] != '/') used--;
      if (used > 0) used--;
      continue;
    }
    out[used++] = '/';
    memcpy(out + used, part, strlen(part));
    used += strlen(part);
  }
  if (used == 0) out[used++] = '/';
  out[used] = '\0';
  free(joined);
  return out;
}

// `path` with every symbolic link in it resolved, as Node's fs.realpathSync
// resolves it: each prefix of the path looked at in turn (lstat), and a link
// followed (stat, which fails on a link to nowhere or a loop of links) and
// read (readlink), and resolved against its directory, the rest of the path
// after it; what fails says the prefix it failed on.
static char *realpath_as_node(const char *path) {
  char *p = resolve(path);
  size_t pos = 1;
  while (pos < strlen(p)) {
    size_t end = pos;
    while (p[end] != '\0' && p[end] != '/') end++;
    char *base = strndup(p, end);
    struct stat st;
    if (lstat(base, &st) < 0) fs_failed(errno, "lstat", base);
    if (S_ISLNK(st.st_mode)) {
      if (stat(base, &st) < 0) fs_failed(errno, "stat", base);
      char target[PATH_MAX];
      ssize_t n = readlink(base, target, sizeof target - 1);
      if (n < 0) fs_failed(errno, "readlink", base);
      target[n] = '\0';
      // The link's target, against the link's directory, then what follows.
      size_t dir_length = strrchr(base, '/') - base;
      size_t length = dir_length + n + strlen(p + end) + 3;
      char *next = malloc(length);
      if (!next) fail("%s", strerror(ENOMEM));
      if (target[0] == '/')
        snprintf(next, length, "%s%s", target, p + end);
      else
        snprintf(next, length, "%.*s/%s%s", (int)dir_length, base, target,
                 p + end);
      free(p);
      p = resolve(next);
      free(next);
      pos = 1;
    } else {
      pos = p[end] == '/' ? end + 1 : end;
    }
    free(base);
  }
  return p;
}

// Writes the whole of `data` to the socket `fd` at `path`.
static void write_all(int fd, const char *data, size_t length,
                      const char *path) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);
    if (written < 0 && errno == EINTR) continue;
    if (written < 0) unanswered(path, "write", errno);
    data += written;
    length -= (size_t)written;
  }
}

int main(int argc, char **argv) {
  // What the server is told past its mode of a save that leaves the sandbox
  // running, where a restore or resume after one, run with its arguments,
  // is told nothing of it.
  int leave_running = 0;
  // The arguments but --leave-running, in their order.
  const char *options[argc];
  int count = 0;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], LEAVE_RUNNING_OPTION) == 0) {
      if (leave_running)
        fail("option " LEAVE_RUNNING_OPTION " may be provided only once");
      leave_running = 1;
    } else {
      options[count++] = argv[i];
    }
  }
  // The mode named first, where the options' count is odd.
  const char *named = count % 2 == 1 ? options[0] : NULL;
  const char *state_dir = NULL;
  for (int i = named ? 1 : 0; i < count; i += 2) {
    if (strcmp(options[i], STATE_DIR_OPTION) != 0)
      fail("unknown option %s", options[i]);
    if (state_dir)
      fail("option " STATE_DIR_OPTION " may be provided only once");
    if (i + 1 == count || strncmp(options[i + 1], "--", 2) == 0)
      fail("option " STATE_DIR_OPTION " requires one value");
    state_dir = options[i + 1];
  }
  if (!state_dir) fail("sandbox-hook needs " STATE_DIR_OPTION ", its server's");
  const char *mode = named ? named : getenv(MODE_VARIABLE);
  if (!is_mode(mode))
    fail("the sandbox hook's mode must be save, restore, resume, stop, not %s",
         mode ? mode : "none");

  // The request, the mode and what it is told, as formatSandboxHookRequest
  // makes it, and a newline.
  char request[16 + MAX_STOP_REASON_BYTES + 2];
  const char *told = NULL;
  if (strcmp(mode, "save") == 0 && leave_running) told = "leave-running";
  if (strcmp(mode, "restore") == 0) {
    const char *reason = getenv(STOP_REASON_VARIABLE);
    if (reason && *reason) {
      if (strpbrk(reason, "\r\n"))
        fail("the reason the sandbox stopped must be one line "
             "(" STOP_REASON_VARIABLE ")");
      if (strlen(reason) > MAX_STOP_REASON_BYTES)
        fail("the reason the sandbox stopped must be at most %d bytes "
             "(" STOP_REASON_VARIABLE ")",
             MAX_STOP_REASON_BYTES);
      told = reason;
    }
  }
  int request_length =
      told ? snprintf(request, sizeof request, "%s %s\n", mode, told)
           : snprintf(request, sizeof request, "%s\n", mode);

  // The server's state directory is where its lease resolved it to.
  char *resolved = realpath_as_node(state_dir);
  char path[PATH_MAX + 32];
  snprintf(path, sizeof path, "%s%ssandbox-hook.sock", resolved,
           strcmp(resolved, "/") == 0 ? "" : "/");
  free(resolved);
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  // A path too long for a Unix socket, as Node's net names it then.
  if (strlen(path) >= sizeof address.sun_path)
    fail("no bayma server answers on %s: connect EINVAL %s - Local "
         "(undefined:undefined)",
         path, path);
  strcpy(address.sun_path, path);

  // A server that closes the connection early is said as one that answered
  // nothing, not a signal's death.
  signal(SIGPIPE, SIG_IGN);
  int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) unanswered(path, "connect", errno);
  if (connect(fd, (struct sockaddr *)&address, sizeof address) < 0)
    unanswered(path, "connect", errno);
  write_all(fd, request, (size_t)request_length, path);

  // The answer, whole: the server ends the connection after it.
  size_t size = 4096, length = 0;
  char *answer = malloc(size);
  if (!answer) fail("%s", strerror(ENOMEM));
  for (;;) {
    if (length + 1 == size) {
      char *grown = realloc(answer, size *= 2);
      if (!grown) fail("%s", strerror(ENOMEM));
      answer = grown;
    }
    ssize_t got = read(fd, answer + length, size - 1 - length);
    if (got < 0 && errno == EINTR) continue;
    if (got < 0) unanswered(path, "read", errno);
    if (got == 0) break;
    length += (size_t)got;
  }
  answer[length] = '\0';
  close(fd);

  if (strcmp(answer, "ok\n") == 0) return 0;
  if (strncmp(answer, "ok ", 3) == 0 && length > 3 &&
      answer[length - 1] == '\n') {
    fputs(answer + 3, stdout);
    return fflush(stdout) == 0 ? 0 : 1;
  }
  // What it said, its trailing whitespace trimmed, as the Node hook says it.
  while (length > 0 && isspace((unsigned char)answer[length - 1]))
    answer[--length] = '\0';
  fail("the bayma server of %s could not %s: %s", state_dir, mode,
       length > 0 ? answer : "it closed the connection");
}
