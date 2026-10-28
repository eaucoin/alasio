# Firecracker in the memory-snapshot benchmark

System `firecracker` of the benchmark (`../../../snapshot-bench/DESIGN.md` on the box; DESIGN.md
below): microVM snapshots of Firecracker v1.17.0, saved to the bucket, restored by a userfaultfd
page-fault handler that serves guest memory from the bucket on demand, with the practices E2B's
runtime uses: zero blocks never fetched, a hot set prefetched in the background, a per-host chunk
cache. The handler and the save tool are `fcmem` (Go, `fcmem/`); `bench.ts` runs the cases,
`run.sh` is the CI job's entry, `guest/init.go` is the guest's PID 1, `e2b/main.go` runs E2B's own
handler as a reference.

Sources, read at these versions and cited below by path:

- Firecracker v1.17.0 (the latest release, 2026-09-10): `docs/snapshotting/snapshot-support.md`,
  `docs/snapshotting/handling-page-faults-on-snapshot-resume.md`, `docs/hugepages.md`,
  `src/vmm/src/persist.rs`, `src/vmm/src/vstate/vm.rs`, `src/vmm/src/vstate/memory.rs`,
  `src/firecracker/examples/uffd/`.
- E2B's runtime, github.com/e2b-dev/runtime at `f551e44a` (2026-10-26), the open-source
  orchestrator E2B's sandboxes pause and resume with: `packages/orchestrator/pkg/sandbox/`
  (`uffd/`, `uffd/userfaultfd/`, `uffd/prefetch/`, `block/`, `build/`, `sandbox.go`,
  `resume_prefetch.go`, `fc/`), `packages/shared/pkg/storage/` and
  `packages/shared/pkg/featureflags/flags.go` (the defaults of its flags, cited as `ff:<line>`).
- E2B's Firecracker fork, github.com/e2b-dev/firecracker at `431f1fc7`, released as
  e2b-dev/fc-versions `v1.14.1_431f1fc`, the release E2B's v1.14 line serves (ff:1070).

## How a run goes

DESIGN.md section 5, on a GitHub-hosted ubuntu-24.04 runner with `/dev/kvm`:

1. A microVM of 2 vCPUs and 5120 MiB on 2 MiB hugetlbfs pages boots the guest kernel and the
   session image `ghcr.io/eaucoin/alasio-agent:10.0.0` as its read-only root drive, with the
   run's `/attach` as a second drive. `bench-init` mounts what the runc systems' container has
   (`bench/common/oci/config.ts`): `/tmp` a tmpfs, so in guest memory as theirs is in the
   container's; loopback up; hostname, open-file limit, no new privileges. Then it runs alasio's
   pod command as `agent`. The driver reaches bayma over a tap device.
2. save-live: `PATCH /vm Paused`, `PUT /snapshot/create` (Diff) to the runner's disk,
   `PATCH /vm Resumed`, then `fcmem save` into the bucket while the guest runs.
3. save-stop: pause, Diff snapshot, the Firecracker process killed, `fcmem save` over save-live's
   snapshot: only the blocks that changed are stored. The `/attach` image is copied (holes kept)
   and each restore gets a fresh copy at the same path.
4. restore-warm and restore-cold of save-stop's snapshot, once per variant. `fcmem serve` starts
   at t0, reads the header and state file, and listens. A new Firecracker process loads the state
   file with `mem_backend: Uffd` and `resume_vm: true`. Then the driver's probe runs. Every
   restore starts after `sync` and `echo 1 > /proc/sys/vm/drop_caches`, so a warm one reads the
   node's copy from its disk (as alasio reads its node cache with O_DIRECT), whatever ran
   before it. Cold: the node's cache is removed first. Warm: the cache as the saves left it.

The variants' restores alternate in order by run. Each restore runs alone on the runner.

## Variants

| variant | what | where |
|---|---|---|
| `prefetch` | 2 MiB pages, the hot set fetched into the cache from t0 | every case, every profile; also carries the saves |
| `no-prefetch` | the same restores, nothing prefetched | every case, every profile |
| `4k` | Firecracker's default 4 KiB pages, with prefetch, its own saves | every case, `local` only |
| `fcmem-on-e2b-fork`, `fcmem-on-e2b-fork-no-prefetch` | fcmem behind E2B's Firecracker | poller/1024, `local` and `s3-express`, reference |
| `e2b-handler`, `e2b-handler-no-prefetch` | E2B's own handler behind E2B's Firecracker | poller/1024, `local` and `s3-express`, reference |

`prefetch` and `no-prefetch` restore the same save, whose lines are written once, under
`prefetch` (DESIGN.md section 6). The four reference variants restore one save of their own; its
lines are not reported, since that save is neither E2B's save path nor the system's.

## The knobs

Every knob that matters, with the value, why, and the source. None was tuned on the benchmark;
`4k` reports the one default this departs from, the page size, beside it.

| knob | value | why | source |
|---|---|---|---|
| Firecracker | v1.17.0, no jailer, as root | The latest release. Root because Firecracker creates the userfaultfd through `/dev/userfaultfd`, root-only on the runner (`vm.unprivileged_userfaultfd = 0`); the jailer's chroot and cgroups would change nothing measured. | snapshot-support.md; handling-page-faults... ("Creating a UFFD object") |
| guest kernel | 6.1.188, Firecracker's CI build (`firecracker-ci/20261023-dd7c24b0d165-0`) | The 6.1 series E2B's sandboxes run (`vmlinux-6.1.158`, ff:1053), at Firecracker's latest CI build of it. | ff:1053 |
| boot args | `console=ttyS0 reboot=k panic=1 pci=off quiet loglevel=1 random.trust_cpu=on` | E2B's kernel arguments (`pci=off`, `random.trust_cpu=on`, `quiet loglevel=1`, `panic=1`), and the console for the job's log. | `fc/kernel_args.go:103-160` |
| vCPUs, memory | 2, 5120 MiB | DESIGN.md section 2. | DESIGN.md |
| `huge_pages` | `2M` (hugetlbfs), 2600 pages reserved on the runner | E2B's default for sandboxes on Firecracker 1.7 and later. hugepages.md: 2M pages "can also help reduce the number of KVM_EXITS required to rebuild extended page tables post snapshot restore", and THP does nothing under UFFD. With 4 KiB pages (`4k`) a fault's page is 512 times smaller, so the guest takes up to 512 times the faults for the same memory. fcmem still installs a whole 2 MiB block per fault there, which keeps the fault count down, but every install maps 512 small pages. | `shared/fcversion/sandbox_features.go:52-58`, `fc/client.go:429-431`; hugepages.md |
| `track_dirty_pages` | false | E2B's (`fc/client.go:421-427`). KVM dirty tracking forces 4 KiB guest page tables, which "negates the performance benefits of using huge pages". | hugepages.md "Known Limitations" |
| `snapshot_type` | `Diff`, both saves | Without dirty tracking, Firecracker writes only resident pages (mincore), and a Diff of a booted VM is resumable. On the runner, a Full snapshot of the 5 GiB guest takes 9.3 s and a Diff 70 ms (probe run 40389779197). Swap is off (`bench/common/host.sh`), which mincore needs. | snapshot-support.md "Creating diff snapshots"; `vstate/vm.rs:767-800` |
| `sync_snapshot_files` | false | v1.17.0's documented faster option. The memory file stays in the page cache and is not `fsync`'d. alasio, by contrast, writes with `--direct`. The save's total still runs until the last object is in the bucket. | snapshot-support.md "Syncing snapshot files" |
| staging | the runner's disk, as the other systems stage | No RAM staging the others lack. | DESIGN.md |
| balloon, entropy device, MMDS, `clock_realtime` | none, none, none, false | E2B installs a balloon only for free-page reporting or hinting, both off by default (`fc/process.go:512-513`, ff:385, ff:845). The guest's own RNG is seeded by `random.trust_cpu` and VMGenID. Nothing in the guest reads MMDS. E2B sets the guest clock from its agent rather than through `clock_realtime`. | as cited |
| block | 2 MiB | The page, so a fault installs one block whole. E2B's header block is the page size, 2 MiB with hugepages. | `shared/pkg/storage/header/diff.go:10-12` |
| zero elision | holes of the Diff file, and blocks of zeros, map to no object | Never stored, never fetched; installed as zeros (a copy of zeros on hugetlbfs, `UFFDIO_ZEROPAGE` on 4 KiB pages). | `header/metadata.go:78,145-150`; `userfaultfd.go:1066-1082` |
| layering | a save-stop block whose SHA-256 equals save-live's maps to save-live's object | E2B maps unchanged blocks to the parent build. E2B finds them from its fork's write-protect dirty tracking; stock Firecracker has none without KVM's, so fcmem compares content. | `header/metadata.go:134-189` |
| upload | one packed object per snapshot, 10 MiB parts, 16 at once, streamed as the file is read | E2B's S3 part size and `gcloud-max-tasks` (16). Uses the AWS SDK's transfer manager, which supersedes the uploader E2B uses. | `shared/pkg/storage/storage_aws.go:31,300-306`; ff:596 |
| fetch | 4 MiB-aligned ranged GETs, one per chunk, no readahead; each 2 MiB block handed out as its bytes arrive | E2B's `MemoryChunkSize`, one GET per chunk, read a block at a time with waiters released as each is in. The first fcmem lacked the progressive release, and its cold verify of poller/1024 through S3 Express took 17.5 s against E2B's 12.7 s (run 40411904868). | `shared/pkg/storage/storage.go:46`; `block/streaming_chunk.go:279-350,367-381`; `block/fetch_session.go` |
| per-host cache | the memory files this node's saves wrote (where the guest's blocks are), their header and state; and every chunk fetched, a file each | E2B writes a snapshot once, into its template cache, and serves restores on that node from there. Stock Firecracker must write its memory file, so that file is the node's copy, and nothing is written twice. Fetched chunks are written off the fault's path. | `block/cache.go`; `orch/server/sandboxes.go:2195-2224` |
| dedup | a block's install is claimed once; a fault on a block being installed is dropped (its `UFFDIO_COPY` wakes every waiter); a chunk asked for while it is fetched waits for that fetch | E2B's install dedup (default on, ff:247) and fetch sessions. | `userfaultfd.go:608-637`; `block/streaming_chunk.go:127-155` |
| prefetch source | the blocks the save stored (changed since its parent), in address order | E2B's `last-cycle` source: "the pages this sandbox's last resume→pause cycle wrote". A session's snapshot is restored once, so there is no earlier restore to record a trace from. alasio's 3 s working set needs one too, and no system records a trace within the run (lead's ruling). | `resume_prefetch.go:83-143`; `sandbox.go:1629-1650` |
| prefetch mode | fetch-only, 16 workers, from t0, before Firecracker connects | E2B replays last-cycle fetch-only, "because prefaulting a multi-GiB diff would load UFFDIO_COPY onto the resume-critical path for no workload gain and would regress warm resumes". The first runs here installed the hot set with 8 workers and showed exactly that: saturating/3072 warm first exec 5.6 s with it against 3.2 s without (run 40395345325). | `sandbox.go:1636-1645,1700-1706`; ff:688 |
| handler failure | Firecracker is killed (its PID from `SO_PEERCRED`) | A guest whose handler is gone waits on its next fault forever. | handling-page-faults... "Caveats" |

## Why fcmem, and how it compares with E2B's handler

Firecracker's example handlers (`src/firecracker/examples/uffd/`) are test utilities. They read a
local file, and `on_demand_handler` copies a whole region on each fault. None fetches remotely,
caches or prefetches. E2B's handler is the maintained production one. It cannot run as is
against stock Firecracker, though. It installs every read fault with `UFFDIO_COPY_MODE_WP`
(`userfaultfd.go:1060-1063`, unconditionally), and the kernel refuses that on memory registered
for MISSING only, as stock Firecracker registers it (`persist.rs`, `guest_memory_from_uffd`).
E2B's fork registers `MISSING | WRITE_PROTECT` (`src/vmm/src/persist/mod.rs:556-559` at
`431f1fc7`). Its handshake also comes with E2B's storage, header and feature-flag stack. So fcmem
implements what E2B's handler does, as the table cites, in about 800 lines, tested against a
real userfaultfd (`fcmem_test.go`).

To show it does what E2B's does, the reference variants run E2B's own code, unmodified, behind
E2B's own Firecracker: its `uffd`, `uffd/prefetch` and `build` packages, wired as its
orchestrator's test utilities wire a build device (`nbd/testutils/template_rootfs.go`). They read
the same snapshot through E2B's header (`e2b-serve export` writes it, objects copied to E2B's
layout) and E2B's S3 chunker, with guest memory in a memfd handed over beside the userfaultfd,
as E2B runs it (`use-memfd`, on by default, ff:230). fcmem runs on the same Firecracker beside
it, without the memfd, which it does not use. E2B's cache lives as
long as its process (an orchestrator that keeps the snapshots it took). So for a warm restore
E2B's handler is started and has read every block before t0; for a cold one it starts at t0, as
fcmem always does. Its cache is a memory-mapped file, whose pages the page cache drop before t0
does not take while they are mapped, so its warm restores read from memory where fcmem's read
from disk; its RSS (`details.handler.max_rss_bytes`, about 1.6 GB for poller/1024) counts those
pages. E2B's prefetch is also fetch-only for this source.

## What is measured, and how

- `pause_reported_ms`: save-live, `PATCH Paused` sent to `PATCH Resumed` answered; save-stop,
  pause to the snapshot written (the instance then ends). `unavailable_ms` is the ticker's.
- `image_bytes`: what the save wrote to the bucket (memory object, state file, header);
  `uploaded_bytes`: the bucket's listing of the snapshot's prefix. `precopy_*`: 0, as
  Firecracker has no pre-copy. `details.staged_bytes`: the memory file on the runner's disk.
- A restore fails if its first exec and its verify have not answered 300 s after t0 (DESIGN.md
  section 5); the verify reads every page, so its end is every page resident.
- `resident_how: "touched"`: nothing loads blocks the guest does not touch (a fetch-only prefetch
  only warms the cache), so `resident_ms` is t0 to the verify's end.
- Memory: Firecracker and the handler each run in a cgroup of their own. A hugetlbfs page is
  charged to the cgroup of the process that faults it in, the handler's for what `UFFDIO_COPY`
  installs, so the guest's memory is both cgroups' `hugetlb.2MB.current` (sampled every 20 ms
  for the peak). `memory_peak_bytes` is Firecracker's `memory.peak` + that hugetlb peak + the
  handler's peak RSS; `memory_bytes` is Firecracker's `memory.current` + hugetlb + the handler's
  RSS after verify. On 4 KiB pages the guest's memory is Firecracker's own `memory.*`.
- The pause check (DESIGN.md section 3): a 1000 ms `PATCH /vm` pause showed 1002.5 ms on the
  guest's ticker (kvm-clock does not hide a pause).

## Results

In `results/` beside this README on the box: `results.jsonl` (every line) and `summary.md`.
