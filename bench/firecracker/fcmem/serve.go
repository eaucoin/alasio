package main

// fcmem serve: Firecracker's page-fault handler for a restore from the bucket, as
// docs/snapshotting/handling-page-faults-on-snapshot-resume.md (v1.17.0) describes one. It reads
// the snapshot's header and state file, listens on the socket Firecracker's `PUT /snapshot/load`
// names, takes the userfaultfd and the guest memory regions Firecracker sends, and from then on
// answers every fault on guest memory by installing the faulting block whole with UFFDIO_COPY:
// zeros for a block of zeros, without a fetch; otherwise its chunk, from the node's cache or with
// a ranged GET of the bucket.
//
// The serve loop reads the faults and hands each block to an install of its own; a block's
// install is claimed once, and a fault on a block being installed is dropped, as the install's
// UFFDIO_COPY wakes every thread waiting on it (E2B's install dedup,
// packages/orchestrator/pkg/sandbox/uffd/userfaultfd/userfaultfd.go). With --prefetch, the
// header's hot set, the blocks the last save stored, is fetched into the cache from the start,
// before Firecracker connects, by 16 workers, in address order, and the guest faults them in
// from there: E2B's "last-cycle" resume prefetch, which E2B replays fetch-only, since installing
// a diff of gigabytes would put UFFDIO_COPY on the resume's critical path for nothing
// (pkg/sandbox/sandbox.go, uffd/prefetch/prefetcher.go, its memory-prefetch-fetch-workers=16).
//
// If the handler fails, it kills Firecracker, whose guest would otherwise wait on the fault
// for ever (the document's Caveats).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	prefetchFetchWorkers = 16
	// How long Firecracker has to connect (E2B's handler waits 10 s, uffd/uffd.go).
	connectTimeout = 10 * time.Second
)

// region is Firecracker's GuestRegionUffdMapping: where a region of guest memory is mapped in
// Firecracker, how long it is, and where it starts in the memory file.
type region struct {
	BaseHostVirtAddr uint64 `json:"base_host_virt_addr"`
	Size             uint64 `json:"size"`
	Offset           uint64 `json:"offset"`
	PageSize         uint64 `json:"page_size"`
}

const (
	blockMissing uint32 = iota
	blockInstalling
	blockInstalled
)

type handler struct {
	start  time.Time
	peer   int
	header *header
	cache  *cache
	uffd   int
	// The guest address of each block, from the regions.
	addrs    []uintptr
	pageSize uint64
	state    []atomic.Uint32

	faults, installs, zeroInstalls, wakes                    atomic.Int64
	firstFaultNs, lastFaultNs, lastInstallNs, prefetchDoneNs atomic.Int64
}

type serveStats struct {
	PageSize        uint64 `json:"page_size"`
	Faults          int64  `json:"faults"`
	Installs        int64  `json:"installs"`
	ZeroInstalls    int64  `json:"zero_installs"`
	Wakes           int64  `json:"wakes"`
	HotBlocks       int    `json:"hot_blocks"`
	ChunksFromStore int64  `json:"chunks_from_store"`
	BlocksRead      int64  `json:"blocks_read"`
	BytesFetched    int64  `json:"bytes_fetched"`
	ReadyMs         int64  `json:"ready_ms"`
	FirstFaultMs    int64  `json:"first_fault_ms"`
	LastFaultMs     int64  `json:"last_fault_ms"`
	LastInstallMs   int64  `json:"last_install_ms"`
	PrefetchDoneMs  int64  `json:"prefetch_done_ms"`
	InstalledBlocks int    `json:"installed_blocks"`
	MaxRSSBytes     int64  `json:"max_rss_bytes"`
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	socket := fs.String("socket", "", "the socket Firecracker connects to")
	prefix := fs.String("prefix", "", "key prefix of the snapshots")
	id := fs.String("snapshot", "", "the snapshot restored")
	stateOut := fs.String("state-out", "", "where the state file is written for Firecracker")
	cacheDir := fs.String("cache", "", "the node's chunk cache")
	prefetch := fs.Bool("prefetch", false, "prefetch the header's hot set")
	statsOut := fs.String("stats", "", "where the handler's counters are written as it ends")
	fs.Parse(args)

	h := &handler{start: time.Now()}
	ctx := context.Background()
	s, err := newStore(ctx)
	if err != nil {
		return err
	}
	h.cache = newCache(*cacheDir, s)
	encoded, err := h.cache.object(ctx, *prefix+"/"+*id+"/header.json")
	if err != nil {
		return err
	}
	if h.header, err = decodeHeader(encoded); err != nil {
		return err
	}
	h.state = make([]atomic.Uint32, len(h.header.Blocks))

	// The hot set's chunks are fetched while the state file is read and Firecracker loads it.
	if *prefetch {
		go func() {
			h.fetchHot()
			h.prefetchDoneNs.Store(int64(time.Since(h.start)))
		}()
	}
	state, err := h.cache.object(ctx, h.header.State)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*stateOut, state, 0o600); err != nil {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: *socket, Net: "unix"})
	if err != nil {
		return err
	}
	readyMs := time.Since(h.start).Milliseconds()
	// The harness asks Firecracker to load the snapshot once this line is out.
	fmt.Println("ready")

	listener.SetDeadline(time.Now().Add(connectTimeout))
	conn, err := listener.AcceptUnix()
	if err != nil {
		return fmt.Errorf("Firecracker did not connect: %w", err)
	}
	listener.Close()
	regions, err := h.handshake(conn)
	if err != nil {
		return err
	}
	if err := h.mapBlocks(regions); err != nil {
		h.fatal(err)
	}

	// The handler ends when Firecracker does, or when it is told to.
	pidfd, err := unix.PidfdOpen(h.peer, 0)
	if err != nil {
		h.fatal(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	stopPipe := make([]int, 2)
	if err := unix.Pipe2(stopPipe, unix.O_CLOEXEC); err != nil {
		h.fatal(err)
	}
	go func() {
		<-stop
		unix.Write(stopPipe[1], []byte{0})
	}()
	if err := h.serveLoop(pidfd, stopPipe[0]); err != nil {
		h.fatal(err)
	}

	stats := serveStats{
		PageSize:        h.pageSize,
		Faults:          h.faults.Load(),
		Installs:        h.installs.Load(),
		ZeroInstalls:    h.zeroInstalls.Load(),
		Wakes:           h.wakes.Load(),
		HotBlocks:       len(h.header.Hot),
		ChunksFromStore: h.cache.fromStore.Load(),
		BlocksRead:      h.cache.blocksRead.Load(),
		BytesFetched:    h.cache.bytesFetched.Load(),
		ReadyMs:         readyMs,
		FirstFaultMs:    h.firstFaultNs.Load() / 1e6,
		LastFaultMs:     h.lastFaultNs.Load() / 1e6,
		LastInstallMs:   h.lastInstallNs.Load() / 1e6,
		PrefetchDoneMs:  h.prefetchDoneNs.Load() / 1e6,
	}
	for i := range h.state {
		if h.state[i].Load() == blockInstalled {
			stats.InstalledBlocks++
		}
	}
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) == nil {
		stats.MaxRSSBytes = usage.Maxrss * 1024
	}
	encodedStats, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return os.WriteFile(*statsOut, encodedStats, 0o644)
}

// fatal ends the handler and Firecracker with it: from the handshake on, a handler that is gone
// leaves the guest waiting on its next fault.
func (h *handler) fatal(err error) {
	fmt.Fprintln(os.Stderr, "fcmem:", err)
	if h.peer > 0 {
		unix.Kill(h.peer, unix.SIGKILL)
	}
	os.Exit(1)
}

// handshake takes Firecracker's message: the regions as JSON, the userfaultfd beside them, and
// Firecracker's PID from the socket.
func (h *handler) handshake(conn *net.UnixConn) ([]region, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *unix.Ucred
	var credErr error
	raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	if credErr != nil {
		return nil, credErr
	}
	h.peer = int(cred.Pid)
	conn.SetReadDeadline(time.Now().Add(connectTimeout))
	body := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(4))
	n, oobn, _, _, err := conn.ReadMsgUnix(body, oob)
	if err != nil {
		return nil, err
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil || len(msgs) != 1 {
		return nil, fmt.Errorf("no userfaultfd in Firecracker's message %q", body[:n])
	}
	fds, err := unix.ParseUnixRights(&msgs[0])
	if err != nil || len(fds) != 1 {
		return nil, fmt.Errorf("no userfaultfd in Firecracker's message %q", body[:n])
	}
	h.uffd = fds[0]
	var regions []region
	if err := json.Unmarshal(body[:n], &regions); err != nil {
		return nil, fmt.Errorf("Firecracker's regions %q: %w", body[:n], err)
	}
	return regions, nil
}

// mapBlocks finds each block's guest address from the regions, which tile the memory file.
func (h *handler) mapBlocks(regions []region) error {
	h.addrs = make([]uintptr, len(h.header.Blocks))
	var covered uint64
	for _, r := range regions {
		if h.pageSize == 0 {
			h.pageSize = r.PageSize
		}
		if r.PageSize != h.pageSize || blockSize%r.PageSize != 0 || r.Offset%blockSize != 0 || r.Size%blockSize != 0 {
			return fmt.Errorf("region %+v does not hold whole %d-byte blocks of one page size", r, blockSize)
		}
		for off := uint64(0); off < r.Size; off += blockSize {
			b := (r.Offset + off) / blockSize
			if b >= uint64(len(h.addrs)) {
				return fmt.Errorf("region %+v past the snapshot's %d bytes", r, h.header.MemSize)
			}
			h.addrs[b] = uintptr(r.BaseHostVirtAddr + off)
		}
		covered += r.Size
	}
	if covered != uint64(h.header.MemSize) {
		return fmt.Errorf("regions of %d bytes for a snapshot of %d", covered, h.header.MemSize)
	}
	return nil
}

// serveLoop answers the guest's faults until one of ends is readable.
func (h *handler) serveLoop(ends ...int) error {
	index := h.addressIndex()
	buf := make([]byte, 64*uffdMsgSize)
	fds := []unix.PollFd{{Fd: int32(h.uffd), Events: unix.POLLIN}}
	for _, end := range ends {
		fds = append(fds, unix.PollFd{Fd: int32(end), Events: unix.POLLIN})
	}
	for {
		if _, err := unix.Poll(fds, -1); err != nil {
			if err == unix.EINTR {
				continue
			}
			return err
		}
		for _, end := range fds[1:] {
			if end.Revents != 0 {
				return nil
			}
		}
		for {
			n, err := unix.Read(h.uffd, buf)
			if err == unix.EAGAIN {
				break
			}
			if err != nil {
				return fmt.Errorf("read userfaultfd: %w", err)
			}
			for _, msg := range parseMsgs(buf[:n]) {
				if msg.event != uffdEventPagefault {
					// Firecracker's guest has no balloon, so nothing but faults comes.
					return fmt.Errorf("userfaultfd event %#x", msg.event)
				}
				if h.faults.Add(1) == 1 {
					h.firstFaultNs.Store(int64(time.Since(h.start)))
				}
				h.lastFaultNs.Store(int64(time.Since(h.start)))
				b := index(msg.address)
				if b < 0 {
					return fmt.Errorf("fault at %#x outside guest memory", msg.address)
				}
				switch {
				case h.state[b].CompareAndSwap(blockMissing, blockInstalling):
					go func() {
						if err := h.install(b); err != nil {
							h.fatal(err)
						}
					}()
				case h.state[b].Load() == blockInstalled:
					// A fault read after its block's install, whose thread the install woke.
					h.wakes.Add(1)
					if err := wake(h.uffd, h.addrs[b], blockSize); err != nil {
						return err
					}
				}
			}
		}
	}
}

// addressIndex returns a lookup of the block holding an address, from the blocks' addresses,
// which run contiguously within each region.
func (h *handler) addressIndex() func(uint64) int {
	type run struct {
		start, end uint64
		first      int
	}
	var runs []run
	for b, addr := range h.addrs {
		a := uint64(addr)
		if len(runs) > 0 && runs[len(runs)-1].end == a {
			runs[len(runs)-1].end += blockSize
			continue
		}
		runs = append(runs, run{start: a, end: a + blockSize, first: b})
	}
	return func(addr uint64) int {
		for _, r := range runs {
			if addr >= r.start && addr < r.end {
				return r.first + int((addr-r.start)/blockSize)
			}
		}
		return -1
	}
}

// install puts block b into guest memory, once its install is claimed.
func (h *handler) install(b int) error {
	dst := h.addrs[b]
	src := h.header.Blocks[b]
	var err error
	if src.Object < 0 {
		h.zeroInstalls.Add(1)
		if h.pageSize == blockSize {
			// A hugetlbfs page cannot take the zero page: a copy of zeros.
			err = copyPages(h.uffd, dst, zeroBlock, uintptr(h.pageSize))
		} else {
			err = zeroPages(h.uffd, dst, blockSize, uintptr(h.pageSize))
		}
	} else {
		var data []byte
		var release func()
		if data, release, err = h.cache.block(h.header.Objects[src.Object], src.Offset, b); err == nil {
			err = copyPages(h.uffd, dst, data, uintptr(h.pageSize))
			release()
		}
	}
	if err != nil {
		return fmt.Errorf("install block %d at %#x: %w", b, dst, err)
	}
	h.state[b].Store(blockInstalled)
	h.installs.Add(1)
	h.lastInstallNs.Store(int64(time.Since(h.start)))
	return nil
}

var zeroBlock = make([]byte, blockSize)

// fetchHot brings the hot set's chunks into the cache, in order.
func (h *handler) fetchHot() {
	type job struct {
		obj   object
		chunk int64
	}
	jobs := make(chan job)
	go func() {
		defer close(jobs)
		var last *job
		for _, b := range h.header.Hot {
			src := h.header.Blocks[b]
			next := job{obj: h.header.Objects[src.Object], chunk: src.Offset / chunkSize}
			if last == nil || *last != next {
				jobs <- next
				last = &next
			}
		}
	}()
	var workers sync.WaitGroup
	for range prefetchFetchWorkers {
		workers.Go(func() {
			for j := range jobs {
				if err := h.cache.ensure(j.obj, j.chunk); err != nil {
					fmt.Fprintln(os.Stderr, "fcmem: prefetch:", err)
				}
			}
		})
	}
	workers.Wait()
}
