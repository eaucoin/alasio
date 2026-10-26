# criu: CRIU on runc

CRIU v4.3 (2026-10-19) on runc v1.5.2 (2026-09-25), the latest releases on 2026-10-26. The
benchmark is DESIGN.md; this is how the `criu` system meets it, which knobs it sets, and why.

- `install.sh`: CRIU built from its `v4.3` tag (the project publishes no binaries), runc's
  release binary, Mountpoint for Amazon S3 1.24.0's package; then `criu check` and the two
  features the system needs (`mem_dirty_track`, `uffd-noncoop`).
- `run.sh <target> <profile>`: the job. It exports the instance image to a root filesystem,
  makes the network namespace and runs `harness.ts`.
- `harness.ts`: the runs, as DESIGN.md section 5 describes them. It writes CRIU's options for
  runc's requests to a configuration file, which each container names in its `org.criu.config`
  annotation; that file takes precedence over runc's own options (runc
  `libcontainer/criu_linux.go`). `tsconfig.json`, `package.json`: its typecheck.

## How a run goes

1. **Instance.** A runc container from common's OCI spec (`common/oci/config.ts`): alasio's
   session image, read-only, bayma as `agent`, `/attach` bound from the run's own directory,
   2 CPUs and 5 GiB, in a cgroup of its own under `/sys/fs/cgroup/criu-bench`. Its network
   namespace is made by `run.sh` and handed to runc by path. CRIU treats it as external, the way
   runc's checkpoint tests and Podman set one up. Its stdio are pipes, as a container's are where
   containerd's shim starts it: stdin held open by a `sleep`, stdout and stderr drained by a
   `cat` into the job's log. runc hands pipes to a restored container anew (CRIU's
   `--inherit-fd`, as runc's `checkpoint.bats` runs it). A FIFO or a runner file would be a file
   CRIU looks for in the container's mounts, and fails to find.
2. **save-live.** P.Haul-style pre-dump rounds (below):
   `runc checkpoint --pre-dump --image-path <stage>/live-preN --parent-path ../live-pre(N-1)`,
   then `runc checkpoint --leave-running --image-path <stage>/live --parent-path ../live-preK`.
   Each directory is uploaded to the bucket as soon as it is written, while the next round
   runs. A manifest, `chain-live.json`, holds the directories' order and is uploaded last. t1 is
   when it is in the bucket.
3. **save-stop.** The same rounds, built on the live save's image, the first round's parent
   being `../live`. Then `runc checkpoint` without `--leave-running`, which ends the container,
   and then `chain-stop.json`. This image is what every restore restores. Right after it, untimed,
   `/attach` is copied (`cp -a --reflink=auto`) and put back before each restore after the first.
4. **Restores, two variants.** Odd runs restore `lazy` first, even runs `eager` first. Each
   variant restores warm first, then cold.
   - `lazy`, warm: `criu lazy-pages --images-dir <stage>/stop --work-dir W`, then
     `runc restore --lazy-pages --image-path <stage>/stop --work-path W`. The two meet at
     `W/lazy-pages.socket`.
   - `lazy`, cold: the staged images deleted, `drop_caches`. Then, from t0: the bucket mounted by
     `mount-s3 --read-only --prefix <run>/` with Mountpoint's defaults otherwise (no cache), a
     directory of links to the mount's files (Mountpoint has no symlinks, and CRIU follows each
     image's `parent` link), then the daemon and the restore as warm. CRIU reads every image
     through the mount, so each page the daemon serves is read from the bucket when it is
     needed.
   - `eager`, warm: `runc restore --image-path <stage>/stop`. CRIU loads every page before the
     tasks run.
   - `eager`, cold: from t0, every object of the run's prefix downloaded with common's
     `s3.mjs download` (32 MiB ranged GETs, 4 at once), the `parent` links made from
     `chain-stop.json`, then the restore as warm.

   The probe starts at t0, beside the restore. `resident_ms`: for `lazy`, when the lazy-pages
   daemon has injected every page and ends (it exits once nothing is left, `criu/uffd.c`
   `handle_requests`); for `eager`, when `runc restore` returns. Helpers started for one restore
   alone run under GNU time, and their peak RSS is added to `memory_peak_bytes`: the lazy-pages
   daemon and mount-s3 (`lazy`), and the download (`eager` cold).
5. The saves are the same for both variants, so their rows go under `lazy`, the variant named
   first here. Restore rows go under each variant.

## Lazy restore from an object store

CRIU's lazy-pages daemon serves pages from one of two places: an image directory it reads with
ordinary file reads, or a page server, which is the dump itself (`dump --lazy-pages`, pages left
in the source's memory) or `criu page-server --lazy-pages` reading a local image directory
(`Documentation/criu.txt`, `page-server --lazy-pages`). Neither reads an object store. The
documented flow is a live migration between two hosts (https://criu.org/Lazy_migration). The
choices were:

- **A page server fed from the bucket.** CRIU's page server reads only local images, so this
  would mean writing one of our own that speaks CRIU's page-server protocol. That is not CRIU's
  documented use, and its performance would be ours, not CRIU's. Rejected.
- **Download, then restore, inside `first_exec_ms`.** DESIGN.md section 9's fallback for a
  system that cannot read an object store on demand. Measured as the `eager` variant.
- **The image directory on a FUSE mount of the bucket.** Stock `criu lazy-pages`, reading the
  image as a local directory, with the reads going to the bucket as they happen. This is the
  `lazy` variant, and the closest arrangement to "pages fetched on demand from the bucket":
  only the pages a fault or the daemon's background transfer asks for are read. The mount is
  Mountpoint for Amazon S3 (https://github.com/awslabs/mountpoint-s3), AWS's FUSE client for
  read-heavy workloads, with its defaults. Its reads are CRIU's: the daemon reads one range at a
  time, a page-fault range or a background chunk of 64 KiB growing to 4 MiB (`criu/uffd.c`
  `DEFAULT_XFER_LEN`, `MAX_XFER_LEN`), and Mountpoint's prefetcher widens sequential reads.

Both are CRIU's restore unchanged. Neither has a hot-page prefetch of the kind the Firecracker
variant replays: CRIU has none.

## Knobs

| knob | value | why | source |
|---|---|---|---|
| pre-copy | `runc checkpoint --pre-dump` rounds, then the dump with `--parent-path` | CRIU's iterative migration: "several pre-dump iterations may reduce the amount of data dumped on dump stage and thus lead to shorter freeze time" | https://criu.org/Iterative_migration; `man runc-checkpoint` (`--pre-dump`, `--parent-path`) |
| dirty tracking | soft-dirty (`--track-mem`), set by runc wherever `--parent-path` is given; CRIU reads it with `PAGEMAP_SCAN` where the kernel has it (6.7+) | the only tracker CRIU has | runc `libcontainer/criu_linux.go`; CRIU `Documentation/under-the-hood/memory-changes-tracking.md` |
| rounds | at most 8; stop after a round of at most 64 pages, or one more than 10 % larger than the round before (P.Haul's policy); **and** stop after a round less than 10 % smaller than the round before (`converge`) | P.Haul is CRIU's own live-migration driver, and its policy is CRIU's documented one. LXD's is a round cap and a goal: `migration.incremental.memory.iterations` 10, `.goal` 70 % of memory already copied, which a workload dirtying steadily never reaches. P.Haul alone keeps a workload that dirties steadily (`poller`, `saturating`) at 8 rounds of the same pages, which cost disk, bucket and save time without shrinking the final dump. `converge` ends them once they stop shrinking. **Tuned on the benchmark**: the P.Haul-only numbers are reported beside, as variants `lazy-phaul`/`eager-phaul` | https://github.com/checkpoint-restore/p.haul `phaul/iters.py`; https://documentation.ubuntu.com/lxd/en/latest/reference/instance_options/#migration-options |
| `pre-dump-mode` | `read` (`criu.conf`) | "The 'read' mode incurs reduced frozen time and reduced memory pressure as compared to 'splice' mode": pages are read with `process_vm_readv` after the tasks thaw, instead of being spliced into pipes while frozen. CRIU's default is `splice`, and its newer under-the-hood page says the opposite. **Measured both ways**: the `splice` numbers are reported beside, as variants `lazy-splice`/`eager-splice` | `Documentation/criu.txt` `--pre-dump-mode`; `Documentation/under-the-hood/optimizing-pre-dump-algorithm.md` |
| live dump | `--leave-running` | the save under load; the dump also resets dirty tracking (`mem.c` `task_reset_dirty_track`), so save-stop's rounds build on it | `man runc-checkpoint` |
| freezing | the cgroup v2 freezer, which runc asks CRIU for | runc's default with cgroup v2 and CRIU 3.14+ | runc `criu_linux.go` (`FreezeCgroup`) |
| TCP | `tcp-close` (`criu.conf`) | bayma promises that a restored session's connections are closed. With `tcp-close` established connections are neither dumped nor locked, and come back closed. `--tcp-established` would also need the peer to wait out the save | `Documentation/criu.txt` `--tcp-close`; https://criu.org/TCP_connection |
| image location | the runner's staging disk, then the bucket; each pre-dump uploaded while the next round runs | the documented flow copies each pre-dump's images between rounds (`rsync`). Uploading during the next round keeps the rounds short. The bucket stays off the freeze, as with alasio | https://criu.org/Iterative_migration |
| upload, eager download | common's `s3.mjs`: 32 MiB parts and ranged GETs, 4 at once | the same client and settings as every runner system, which are alasio's node agent's | DESIGN.md section 4 |
| lazy restore | `runc restore --lazy-pages` + `criu lazy-pages`, CRIU's default log level for the daemon | CRIU's post-copy restore; the daemon injects faulted pages first and the rest in the background, then exits | https://criu.org/Lazy_migration; `Documentation/criu.txt` `lazy-pages`; `man runc-restore` |
| cold lazy reads | Mountpoint 1.24.0, `--read-only --prefix --force-path-style`, defaults otherwise: 8 MiB read parts, 16 threads, no cache | AWS's documented defaults for reading S3 as files; a cache would make cold warm | https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md |
| network lock | `network-lock skip` (`criu.conf`) | CRIU drops the namespace's packets while a dump or a restore runs, so that a dumped connection sees no RST. With `tcp-close` no connection is kept, which is the case the man page names for `skip`. The first run without it showed the cost: the probe's SYN, dropped during the restore, waited out TCP's 1 s retransmission, about 1 s on every restore's `first_exec_ms` | `Documentation/criu.txt` `--network-lock` |
| file locks | `file-locks` (`criu.conf`) | bayma's SQLite state holds POSIX locks, which CRIU refuses to dump without it; "only safe to use this for enclosed containers where locks are not held by any processes outside of dumped process tree", which the instance is | `Documentation/criu.txt` `--file-locks`; `man runc-checkpoint` |
| `--compress` (new in 4.3) | off (default) | the fixtures' memory is random bytes by design (DESIGN.md section 3), so LZ4 has nothing to remove and only adds dump and restore CPU | `Documentation/criu.txt` `--compress` |
| `--image-io-mode` (new in 4.3) | `writeback` (default) | the upload reads each staged image back as soon as it is written; `direct` would take that read off the page cache | `Documentation/criu.txt` `--image-io-mode` |
| `--auto-dedup` | off (default) | it punches superseded pages out of the parent images on the staging disk only: the bucket's objects and the restores' reads stay the same | `Documentation/criu.txt` `--auto-dedup` |
| log level | runc's: `-v4` for every request runc makes (fixed in `criu_linux.go`) | runc's own setting, which its users get | runc `criu_linux.go` (`LogLevel: 4`) |
| cgroups | `--manage-cgroups-mode soft` (runc's default) | the restored tasks go into the new container's cgroup | `man runc-checkpoint` |
| io_uring | off on the runner: `sysctl kernel.io_uring_disabled=2` (`run.sh`) | CRIU 4.3 cannot dump an io_uring instance (`criu/proc_parse.c`: "Unknown shit 600 (anon_inode:[io_uring])", the first run's pre-dump of bayma's `node`). Node's libuv sets one up for every event loop to batch `epoll_ctl`, whatever `UV_USE_IO_URING` says (that variable governs only its SQPOLL ring), and falls back to plain `epoll_ctl` where the kernel refuses. gVisor offers no io_uring by default either (`runsc --iouring`, off), so bayma runs the same way under alasio and gvisor | libuv `src/unix/linux.c` (`uv__use_io_uring`, `uv__platform_loop_init`); Linux `Documentation/admin-guide/sysctl/kernel.rst` `io_uring_disabled` |

Not wired: bayma's save/restore hook. CRIU's documented hooks (`--action-script` `pre-dump`,
`post-resume`) run on the host, not in the sandbox, and bayma's `sandbox-hook` reads gVisor's
`GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE`. So bayma never learns of the save, and gives no notice
after a restore, which the probe accepts.

## Measured

- `pause_reported_ms`: CRIU's `frozen_time` of the final dump, from its `stats-dump`
  (`images/stats.proto`). Each pre-dump round's own frozen time is in `details.rounds`.
- `precopy_passes`, `precopy_bytes`: the pre-dump rounds and the pages they wrote
  (`pages_written`).
- `image_bytes`: the bytes of the directories this save wrote. `uploaded_bytes`: the bucket's
  bytes under the run's prefix after the save, less before.
- Pause check: once per job, `runc pause`, 1000 ms, `runc resume` on an `idle` fixture. The
  ticker must see 1000 ms +/- 20 ms.
