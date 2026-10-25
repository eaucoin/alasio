/**
 * The lock of a volume one server at a time may run on. A pod deleted outright, as
 * `kubectl delete --force` deletes one and as Kubernetes deletes those of a node it holds
 * lost, is made anew by its StatefulSet at once, on the same claim, while its container may
 * still run: two servers on one volume, the second loading what the first goes on writing,
 * and each writing over what the other wrote, as SeaweedFS's volume server lost objects it
 * had acknowledged. So each server of alasio's on a volume holds a lock there that the next
 * waits for:
 *
 * - one started by oneAtATime, where the server takes none of its own: each Valkey
 *   (./valkey.ts), and the filer store's restore, which writes its Valkey's volume
 *   (./object-store.ts filerStoreRestore); the object store's masters and volume servers
 *   (./object-store.ts); and the storage controller's Postgres (./neon.ts), whose
 *   postmaster.pid is no lock here, as it tells a postmaster that runs by its PID, which is
 *   1 in each container, so a postmaster takes the other's for one it left itself;
 * - or its own: the pageserver and the safekeepers each claim a pid file in their workdir
 *   with flock (Neon's utils::pid_file) and start no further while another holds it, and the
 *   log's broker locks its log.dirs' .lock likewise.
 *
 * The filers keep nothing on a volume, and the filer store's backups are written by their
 * CronJob's Jobs one at a time (concurrencyPolicy Forbid) and read read-only, so neither
 * needs one.
 */

/**
 * The script a server is started by, its command its arguments: once no other server holds
 * `lock`, the lock of its volume, which it then holds as long as it runs, on a descriptor it
 * keeps across its exec, so the kernel lets it go as it ends however it ends; saying, while
 * it waits, that it waits for the `server` that holds it. Where the lock cannot be opened or
 * taken (flock failing, or killed as it waits), the script ends, and with it the container,
 * starting no server. The lock is on the volume itself, which one node alone mounts: that
 * holds of local-path and of ReadWriteOnce block storage, not of a shared file system such as
 * NFS, which two nodes may mount at once. A server waiting on it listens on nothing, so its
 * pod is not ready and its Service routes nothing to it.
 */
export const oneAtATime = (lock: string, server: string): string => [
  `exec 9>>${lock} || exit 1`,
  "if ! flock -n 9; then",
  `  echo "waiting for the ${server} that holds ${lock} to end" >&2`,
  `  flock 9 || { echo "could not take ${lock}" >&2; exit 1; }`,
  "fi",
  'exec "$0" "$@"',
].join("\n");
