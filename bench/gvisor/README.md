# gvisor: upstream gVisor's checkpoint and restore

System 2 of the memory-snapshot benchmark (`~/snapshot-bench/DESIGN.md` on the box, section 1).
Upstream gVisor `release-20261019.0`, the latest release on 2026-10-26, installed from its
release tarball (`gvisor.tar.zstd`, checked against its SHA-512, as
[gvisor.dev's install guide](https://gvisor.dev/docs/user_guide/install/) does it). It runs as
raw `runsc` from an OCI bundle (`common/oci/config.ts`), the way Modal runs it in production:
`runsc`, not Docker, which cannot restore into a new container (the
[checkpoint guide](https://gvisor.dev/docs/user_guide/checkpoint_restore/), "Issues Preventing
Compatibility with Docker").

- `install.sh`: runsc and its `gvisor-bin/` sidecars, and rclone for the `lazy` variant.
- `run.sh <target> <profile>`: what ci.yml's bench job runs: install, then `harness.ts` as root.
- `harness.ts`: every case's runs (DESIGN.md section 5), one line per run and phase in
  `results.jsonl`.

## How a run goes

1. The instance: the session image's root filesystem (`docker export`), the runner systems'
   spec, 2 CPUs and 5 GiB through runsc's own cgroup (`/sys/fs/cgroup/gvbench/<id>`), a network
   namespace of its own with one veth (`10.88.0.2/30`, the same on every restore), and netstack
   (`--network=sandbox`, the default). The driver reaches bayma at `http://10.88.0.2:8765/mcp`.
2. `save-live`: `runsc checkpoint --leave-running` into a directory on the runner's disk, then
   that directory uploaded to `snapshots/gvisor/<profile>/<id>/live/`. It is done when the last
   object is in.
3. `save-stop`: `runsc checkpoint`, which ends the sandbox, done the same way, to `.../stop/`.
   `/attach` is then copied (`cp -a --reflink=auto`), untimed, and put back before each cold
   restore (DESIGN.md section 5).
4. `restore-warm`: `runsc restore` from the directory the save staged.
5. `restore-cold`, twice; odd runs do `fetch` first, even runs `lazy` first:
   - `fetch`: the image is downloaded whole from the bucket into a directory, then restored from
     it. The download is inside `first_exec_ms`.
   - `lazy`: the bucket is mounted with rclone, and the restore reads straight from the mount.
     Pages the sandbox touches are read at once and the rest in the background, while a reader
     preloads `pages.img` through the mount. The mount is inside `first_exec_ms`.

Every restore starts with the page cache dropped (`sync; echo 1 > /proc/sys/vm/drop_caches`,
DESIGN.md section 4). Cold restores also start with the node's copies removed. A restore fails
if its first exec has not answered, or its pages are not all resident, 300 s after t0.

`save-live`, `save-stop` and `restore-warm` use the same mechanism in both variants, so they
carry variant `fetch`, and `lazy` has only `restore-cold` rows. Variant `defaults` (local
profile; the job's variant `defaults`, or `all`) is the same run with everything as runsc and the
runner come: no checkpoint or restore flag but the hook, and the runner's host settings.

## What is measured, and how

- `pause_reported_ms`: runsc's boot log (`--debug-log`; `pkg/sentry/state/state.go`), from the
  Sentry's `Sandbox save started, pausing all tasks.` to its `Tasks resumed after save.` for a
  live save, or to `Save succeeded` for one that ends the sandbox. Upstream logs no "paused for"
  line.
- `unavailable_ms`: the fixture's ticker. Upstream has no pre-copy. `--leave-running` pauses
  every task for the whole write and resumes them after (`k.Pause()` until `SaveTo` returns,
  `state.go`), so `precopy_passes` and `precopy_bytes` are 0.
- `image_bytes`: the image directory's files (`checkpoint.img`, `pages_meta.img`, `pages.img`).
- `resident_ms`: until `runsc wait --restore` returns, which happens once async page loading is
  done (the checkpoint guide, "Background Restore"); `resident_how` is `signal`.
- `memory_peak_bytes`: the sandbox cgroup's `memory.peak`, plus the peak RSS (`VmHWM`, sampled
  every 50 ms) of the restore's own helpers: the download for `fetch`, and rclone and the preload
  reader for `lazy`. `memory_bytes`: the cgroup's `memory.current` after verify. The page cache
  that holds the downloaded image (`fetch`) or rclone's cache file (`lazy`) belongs to the runner,
  not the sandbox, so it is not counted. `details.helper_peak_bytes` gives the helpers alone.
- `details`: the checkpoint's own time and the upload's, the download's or the mount's, runsc
  restore's own time (`runsc_restore_ms`), the host settings in force, and the Sentry's lines on
  saving and loading memory (`Async page loading completed in ...`). Where a probe runs past
  120 s, `logs/<run>/<restore>.slow` records the sandbox's processes and TCP sockets.

## Every knob that matters

| knob | value | why | source |
|---|---|---|---|
| release | `release-20261019.0` | the latest release on 2026-10-26 | storage.googleapis.com/gvisor/releases/release |
| platform | `systrap` (default) | the runners are VMs; KVM is for bare metal | [production guide](https://gvisor.dev/docs/user_guide/production/), "Choosing a platform" |
| `--host-settings` | `adjust` (default `check`) | sets what runsc recommends: THP `shmem_enabled=advise`, `defrag=never`, `vm.max_map_count=4194304` | production guide, "Optimizing MM performance"; `runsc --help` |
| network | `--network=sandbox` (default), in a netns | checkpoint/restore supports it; host networking cannot save host sockets | checkpoint guide, "Networking" |
| `--compression` | `none` (default) | uses less CPU and is faster; kernel and memory restore in parallel; `--background` needs it | checkpoint guide, "Compression" |
| `--exclude-committed-zero-pages` | on (default off) | skips zero pages, at the cost of a scan | checkpoint guide, "Exclude Committed Zero Pages" |
| checkpoint `--direct` | off (default) | the pages go through the page cache, which the upload then reads; the guide advises O_DIRECT for an image not read again on the machine, and the upload reads it again | checkpoint guide, "Direct I/O"; "Tuning" below |
| `--leave-running` | live save only | the documented save that keeps the sandbox running | checkpoint guide |
| restore `--background` | on (default off) | the sandbox runs once its kernel state is loaded, touched pages first, the rest async; `lazy` needs it | checkpoint guide, "Background Restore"; Modal, [mem-snapshots](https://modal.com/blog/mem-snapshots) |
| restore `--direct` | warm only | the staged image is read from disk once, the case the guide names | checkpoint guide, "Direct I/O" |
| restore `--direct`, cold | off | `fetch`: the image was just downloaded and is in the page cache; `lazy`: FUSE | checkpoint guide, "Direct I/O" |
| `--save-restore-exec-argv` | bayma's `sandbox-hook`, as alasio wires it | upstream's save/restore hook, which bayma documents | `runsc checkpoint --help`; checkpoint guide, annotations table |
| `--save-restore-exec-timeout` | 10 min (default) | `alasio-start` bounds the hook's save to 30 s itself | `runsc checkpoint --help` |
| `allow-live-tcp-migration` | `true` (default) | changes nothing here: netstack keeps connections over a link with `CapabilitySaveRestore`, which loopback has, whatever the flag (`pkg/tcpip/transport/tcp/endpoint_state.go`), and the fixture's only connection is loopback; alasio sets `false` | `runsc/config/flags.go` |
| upload | multipart, 32 MiB parts, 4 at once (common's) | as alasio's node agent uploads, and as gVisor's own remote writer writes the pages file (`pagesFileMaxWriteBytes`, `pagesFileMaxWriteParallel`) | `common/store/s3.ts`; `runsc/checkpointgofer/gcs/gcs.go` |
| `fetch` download | 16 MiB ranges, 16 at once | on the restore's critical path: one connection per 85-90 MB/s wanted, in 8-16 MB ranges; gVisor's own remote reader reads 16 MiB ranges, up to 160 at once | [AWS S3 performance whitepaper](https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf); `gcs.go` (`pagesFileMaxReadBytes`, `pagesFileMaxReadParallel`) |
| `lazy` mount | rclone v1.75.2 `mount --read-only --vfs-cache-mode full` | full caching serves random reads from a sparse local file that downloads only what is read; rclone's own chunked reading beat the docs' suggested 16 streams of 4 MiB ("Tuning") | [rclone mount](https://rclone.org/commands/rclone_mount/), "VFS File Caching", "VFS Chunked Reading" |
| `lazy` preload | `dd if=pages.img of=/dev/null bs=16M`, started once the mount is up | Modal "aggressively preload[s] the entire pages file into page cache as early as we can" | Modal, [mem-snapshots](https://modal.com/blog/mem-snapshots) |

Upstream also has an on-demand remote reader, `runsc-checkpointgofer`, enabled by a
`gcs_opts.json` in `--image-path`. It speaks Google Cloud Storage alone
(`runsc/checkpointgofer/gcs`), and the guide does not document it. The benchmark's store is S3,
so neither variant uses it. `lazy` stands in for it on an S3 store, following Modal's FUSE
serving.

## Tuning

These knobs were chosen on the benchmark itself, one at a time, with every other knob at the
tuned value. Each cell is the median over the runs, in ms. The full matrix's `defaults` variant
gives everything-at-default numbers beside the tuned ones.

Checkpoint `--direct` (CI run 40402698453, local, 3 runs; the warm restores there ran before
the page cache was dropped, so only the saves are read from it):

| case | live pause, direct | live pause, buffered | stop save total, direct | stop save total, buffered |
|---|---|---|---|---|
| poller/1024 | 2553 | 367 | 8808 | 4871 |
| saturating/1024 | 2555 | 376 | 9505 | 4778 |
| saturating/3072 | 7778 | 6530 | 24487 | 22257 |

`--exclude-committed-zero-pages` off: poller/1024 stop pause 1140 against 371, and
saturating/1024 cold first exec 6060 against 4561. `--host-settings=check`: poller/1024 cold
first exec 4178 against 3766.

Warm restore `--direct`, and `--background` (CI run 40415419362, local, 3 runs, page cache
dropped before each restore; first exec / resident, and memory peak):

| case | warm, `--direct` | warm, buffered | warm, no `--background` |
|---|---|---|---|
| poller/1024 | 3108 / 3043, 1428 MiB | 3114 / 3048, 2539 MiB | 3104 / 3042, 1424 MiB |
| saturating/1024 | 3716 / 3057, 1443 MiB | 3720 / 3067, 2567 MiB | 3731 / 3069, 1451 MiB |
| saturating/3072 | 10061 / 8280, 3650 MiB | 8904 / 8126, 5120 MiB | 10044 / 8287, 3647 MiB |

`--direct` takes the same time and keeps the page cache, charged to the sandbox, out of its
peak. `--background` gains nothing here, because Bun and Python touch nearly every page before
their first exec answers (the Sentry logs tens of threads waiting on pages). It stays as
documented, and because `lazy` needs it.

rclone (CI run 40712306777, s3-standard, 2 runs; cold first exec / resident, and memory peak
with rclone's):

| case | rclone defaults | 16 streams of 4 MiB | `fetch`, for comparison |
|---|---|---|---|
| poller/1024 | 4939 / 7163, 2718 MiB | 5374 / 7479, 4293 MiB | 3892 / 3821, 1769 MiB |
| saturating/3072 | 18346 / 18308, 5288 MiB | 19352 / 19165, 6839 MiB | 17140 / 14753, 3930 MiB |

With `--max-buffer-memory 256M`, rclone stalled after 200-300 MiB on every restore. `lazy`
stays slower than `fetch`: these workloads need nearly the whole image before their first exec,
and rclone reads it more slowly than the parallel ranged download does.

## Known failures

In the first full matrix (CI runs 40725473764 and 40724539284; 855 lines), 15 lines failed, all of them Bun's
or bayma's. All of them ran on kernel 6.17.0-1022-azure, where every Bun loss in the benchmark so far happened,
and two of its three jobs on an AMD EPYC 9V74. The benchmark now pins its jobs to an EPYC 7763 on a 7.0 kernel, and
those results, kept by CPU model in `results/earlier-by-cpu` on the box, are supplementary:

- 4 restores (s3-standard, poller and saturating) lost the session's Bun runtime after the restore. bayma's
  server, the fixture's other processes and the driver's connections live on, but no exec answers, so the run
  fails at the 300 s deadline. alasio's gVisor fork shows the same failure; there bayma starts the runtime
  again, and the first exec finds the fixture gone.
- 2 runs' setups (s3-standard, poller) failed before any save, when the session's Bun runtime crashed (SIGSEGV;
  stdin closed). Their saves' 4 lines fail with it, and their 6 restore lines are "not reached".
- 1 lazy cold restore (local, poller/3072): bayma's restore hook ended with status 9 once the pages had loaded,
  and runsc killed the sandbox as its post-restore work failed.

Each hung restore's `logs/<run>/<restore>.slow` in the job's artifact records what the sandbox ran.
