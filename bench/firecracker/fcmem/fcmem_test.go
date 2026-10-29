package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// memoryStore is a bucket in memory, which counts its ranged GETs.
type memoryStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	ranges  atomic.Int64
}

func newMemoryStore() *memoryStore { return &memoryStore{objects: map[string][]byte{}} }

func (s *memoryStore) get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, errors.New("no object " + key)
	}
	return bytes.Clone(data), nil
}

func (s *memoryStore) openRange(_ context.Context, key string, off, length int64) (io.ReadCloser, error) {
	s.ranges.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok || off+length > int64(len(data)) {
		return nil, errors.New("no range of " + key)
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(data[off : off+length]))), nil
}

func (s *memoryStore) put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = bytes.Clone(data)
	return nil
}

func (s *memoryStore) upload(ctx context.Context, key string, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	return s.put(ctx, key, data)
}

func randomBlock(t *testing.T) []byte {
	t.Helper()
	data := make([]byte, blockSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

// writeMemory writes a Diff snapshot's memory file of n blocks: the given ones written, the rest
// holes.
func writeMemory(t *testing.T, path string, n int, blocks map[int][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(int64(n) * blockSize); err != nil {
		t.Fatal(err)
	}
	for i, data := range blocks {
		if _, err := f.WriteAt(data, int64(i)*blockSize); err != nil {
			t.Fatal(err)
		}
	}
}

func savedHeader(t *testing.T, s *memoryStore, key string) *header {
	t.Helper()
	encoded, err := s.get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestSaveStoresChangedBlocksAndNoZeros(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := newMemoryStore()
	a, b, c := randomBlock(t), randomBlock(t), randomBlock(t)
	in := saveInput{prefix: "p", work: filepath.Join(dir, "work"), cacheDir: filepath.Join(dir, "cache"), statePath: filepath.Join(dir, "state")}
	if err := os.WriteFile(in.statePath, []byte("state"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Block 0 a hole, 1 written zeros, 2 and 3 random.
	live := in
	live.id, live.memPath = "live", filepath.Join(dir, "live.mem")
	writeMemory(t, live.memPath, 4, map[int][]byte{1: make([]byte, blockSize), 2: a, 3: b})
	report, err := saveSnapshot(ctx, s, live)
	if err != nil {
		t.Fatal(err)
	}
	if report.HoleBlocks != 1 || report.ZeroBlocks != 1 || report.StoredBlocks != 2 || report.ParentBlocks != 0 {
		t.Fatalf("live saved as %+v", report)
	}
	h := savedHeader(t, s, "p/live/header.json")
	if want := []block{{-1, 0}, {-1, 0}, {0, 0}, {0, blockSize}}; !slices.Equal(h.Blocks, want) || !slices.Equal(h.Hot, []int{2, 3}) {
		t.Fatalf("live's blocks %v, hot %v", h.Blocks, h.Hot)
	}
	if memory, _ := s.get(ctx, "p/live/memory"); !bytes.Equal(memory, append(bytes.Clone(a), b...)) {
		t.Fatal("live's memory object is not its two blocks in order")
	}

	// Over it: block 2 unchanged, 3 rewritten, 1 now holding data.
	stop := in
	stop.id, stop.parent, stop.memPath = "stop", "live", filepath.Join(dir, "stop.mem")
	writeMemory(t, stop.memPath, 4, map[int][]byte{1: c, 2: a, 3: randomBlock(t)})
	report, err = saveSnapshot(ctx, s, stop)
	if err != nil {
		t.Fatal(err)
	}
	if report.StoredBlocks != 2 || report.ParentBlocks != 1 || report.HoleBlocks != 1 {
		t.Fatalf("stop saved as %+v", report)
	}
	h = savedHeader(t, s, "p/stop/header.json")
	if want := []block{{-1, 0}, {0, 0}, {1, 0}, {0, blockSize}}; !slices.Equal(h.Blocks, want) || !slices.Equal(h.Hot, []int{1, 3}) {
		t.Fatalf("stop's blocks %v, hot %v", h.Blocks, h.Hot)
	}
	if h.Objects[1].Key != "p/live/memory" || h.Objects[1].Size != 2*blockSize || h.Objects[0].Size != 2*blockSize {
		t.Fatalf("stop's objects %+v", h.Objects)
	}
	// The node keeps both memory files, where the guest's blocks are.
	cache := newCache(in.cacheDir, s)
	got, release, err := cache.block(h.Objects[1], 0, 2)
	if err != nil || !bytes.Equal(got, a) {
		t.Fatalf("block 2 from the node's copy of live: %v", err)
	}
	release()
	if s.ranges.Load() != 0 {
		t.Fatalf("%d ranged GETs for blocks the node holds", s.ranges.Load())
	}
}

func TestCacheFetchesAChunkOnceAndKeepsIt(t *testing.T) {
	ctx := context.Background()
	s := newMemoryStore()
	data := append(randomBlock(t), randomBlock(t)...)
	obj := object{Key: "p/s/memory", Size: int64(len(data))}
	s.put(ctx, obj.Key, data)
	dir := t.TempDir()
	c := newCache(dir, s)

	// Both blocks of the one chunk at once: one GET.
	var wait sync.WaitGroup
	for b := range 2 {
		wait.Go(func() {
			got, release, err := c.block(obj, int64(b)*blockSize, b)
			if err != nil || !bytes.Equal(got, data[b*blockSize:(b+1)*blockSize]) {
				t.Errorf("block %d: %v", b, err)
			}
			release()
		})
	}
	wait.Wait()
	if s.ranges.Load() != 1 {
		t.Fatalf("%d ranged GETs for one chunk", s.ranges.Load())
	}
	// A later handler on the node reads it from the cache's file.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(c.chunkIn(obj.Key, 0)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the chunk's file was not written")
		}
		time.Sleep(10 * time.Millisecond)
	}
	later := newCache(dir, s)
	got, release, err := later.block(obj, blockSize, 1)
	if err != nil || !bytes.Equal(got, data[blockSize:]) {
		t.Fatalf("block 1 from the cache: %v", err)
	}
	release()
	if s.ranges.Load() != 1 {
		t.Fatalf("%d ranged GETs after the chunk was cached", s.ranges.Load())
	}
}

// streamStore is a bucket whose ranged GETs answer what is written to them, as it is.
type streamStore struct {
	*memoryStore
	body *io.PipeReader
}

func (s *streamStore) openRange(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return s.body, nil
}

func TestCacheHandsOutABlockBeforeItsChunkIsIn(t *testing.T) {
	body, bucket := io.Pipe()
	s := &streamStore{memoryStore: newMemoryStore(), body: body}
	c := newCache(t.TempDir(), s)
	obj := object{Key: "p/s/memory", Size: chunkSize}
	first, second := randomBlock(t), randomBlock(t)
	go bucket.Write(first)
	got, release, err := c.block(obj, 0, 0)
	if err != nil || !bytes.Equal(got, first) {
		t.Fatalf("the first block, with the second not yet sent: %v", err)
	}
	release()
	go bucket.Write(second)
	if got, _, err = c.block(obj, blockSize, 1); err != nil || !bytes.Equal(got, second) {
		t.Fatalf("the second block: %v", err)
	}
}

const (
	// _IOWR(0xAA, 0x3F, struct uffdio_api), _IOWR(0xAA, 0x00, struct uffdio_register).
	uffdioAPI      = 0xc018aa3f
	uffdioRegister = 0xc020aa00
	uffdAPI        = 0xaa
	uffdUserOnly   = 1
	registerMiss   = 1
	guestBlocks    = 3
	guestSocket    = "FCMEM_TEST_GUEST_SOCKET"
)

// TestGuest is the guest of TestServeInstallsEachFaultingBlock, run as a process of its own, as
// Firecracker is: a Go program cannot serve its own faults, as a collection stops the world for
// threads that wait on them. It registers anonymous memory with a userfaultfd, hands both to the
// handler as Firecracker does, reads every page from two threads, and prints each block's digest.
func TestGuest(t *testing.T) {
	socket := os.Getenv(guestSocket)
	if socket == "" {
		t.Skip("the guest of TestServeInstallsEachFaultingBlock")
	}
	fd, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|unix.O_NONBLOCK|uffdUserOnly, 0, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	api := [3]uint64{uffdAPI, 0, 0}
	if err := ioctl(int(fd), uffdioAPI, unsafe.Pointer(&api)); err != nil {
		t.Fatal(err)
	}
	mem, err := unix.Mmap(-1, 0, guestBlocks*blockSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		t.Fatal(err)
	}
	base := uint64(uintptr(unsafe.Pointer(&mem[0])))
	reg := [4]uint64{base, uint64(len(mem)), registerMiss, 0}
	if err := ioctl(int(fd), uffdioRegister, unsafe.Pointer(&reg)); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	regions, _ := json.Marshal([]region{{BaseHostVirtAddr: base, Size: uint64(len(mem)), PageSize: uint64(os.Getpagesize())}})
	if _, _, err := conn.WriteMsgUnix(regions, unix.UnixRights(int(fd)), nil); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			for off := 0; off < len(mem); off += os.Getpagesize() {
				_ = mem[off]
			}
		})
	}
	wait.Wait()
	for b := range guestBlocks {
		fmt.Printf("block %x\n", sha256.Sum256(mem[b*blockSize:(b+1)*blockSize]))
	}
}

func TestServeInstallsEachFaultingBlock(t *testing.T) {
	ctx := context.Background()
	if fd, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|uffdUserOnly, 0, 0); errno != 0 {
		t.Skipf("no userfaultfd here: %v", errno)
	} else {
		unix.Close(int(fd))
	}
	s := newMemoryStore()
	data := append(randomBlock(t), randomBlock(t)...)
	h := &header{
		MemSize: guestBlocks * blockSize, BlockSize: blockSize, ChunkSize: chunkSize,
		Objects: []object{{Key: "p/s/memory", Size: int64(len(data))}},
		Blocks:  []block{{-1, 0}, {0, blockSize}, {0, 0}},
	}
	s.put(ctx, h.Objects[0].Key, data)
	socket := filepath.Join(t.TempDir(), "uffd.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	guest := exec.Command(os.Args[0], "-test.run=^TestGuest$", "-test.v")
	guest.Env = append(os.Environ(), guestSocket+"="+socket)
	var said bytes.Buffer
	guest.Stdout, guest.Stderr = &said, &said
	if err := guest.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	handler := &handler{start: time.Now(), header: h, cache: newCache(t.TempDir(), s), state: make([]atomic.Uint32, guestBlocks)}
	regions, err := handler.handshake(conn)
	if err != nil {
		t.Fatal(err)
	}
	if handler.peer != guest.Process.Pid {
		t.Fatalf("the peer is %d, the guest %d", handler.peer, guest.Process.Pid)
	}
	if err := handler.mapBlocks(regions); err != nil {
		t.Fatal(err)
	}
	pidfd, err := unix.PidfdOpen(handler.peer, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The handler ends with its guest, as it does with Firecracker.
	if err := handler.serveLoop(pidfd); err != nil {
		t.Fatal(err)
	}
	if err := guest.Wait(); err != nil {
		t.Fatalf("the guest: %v\n%s", err, said.String())
	}
	zero := make([]byte, blockSize)
	for b, want := range [][]byte{zero, data[blockSize:], data[:blockSize]} {
		if line := fmt.Sprintf("block %x\n", sha256.Sum256(want)); !strings.Contains(said.String(), line) {
			t.Fatalf("the guest's block %d is not the header's:\n%s", b, said.String())
		}
	}
	// An install's counts follow the copy that woke the guest.
	for deadline := time.Now().Add(5 * time.Second); handler.installs.Load() < guestBlocks && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if handler.installs.Load() != guestBlocks || handler.zeroInstalls.Load() != 1 || s.ranges.Load() != 1 {
		t.Fatalf("%d installs, %d of zeros, %d ranged GETs", handler.installs.Load(), handler.zeroInstalls.Load(), s.ranges.Load())
	}
}

// A block whose pages are 4 KiB and one of them already there is installed whole around it.
func TestCopyPagesGoesPastAPageAlreadyThere(t *testing.T) {
	fd, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|unix.O_NONBLOCK|uffdUserOnly, 0, 0)
	if errno != 0 {
		t.Skipf("no userfaultfd here: %v", errno)
	}
	defer unix.Close(int(fd))
	api := [3]uint64{uffdAPI, 0, 0}
	if err := ioctl(int(fd), uffdioAPI, unsafe.Pointer(&api)); err != nil {
		t.Fatal(err)
	}
	mem, err := unix.Mmap(-1, 0, blockSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(mem)
	base := uintptr(unsafe.Pointer(&mem[0]))
	reg := [4]uint64{uint64(base), uint64(len(mem)), registerMiss, 0}
	if err := ioctl(int(fd), uffdioRegister, unsafe.Pointer(&reg)); err != nil {
		t.Fatal(err)
	}
	page := uintptr(os.Getpagesize())
	data := randomBlock(t)
	// The page in the middle, there first, with other bytes.
	if err := copyPages(int(fd), base+blockSize/2, bytes.Repeat([]byte{7}, int(page)), page); err != nil {
		t.Fatal(err)
	}
	if err := copyPages(int(fd), base, data, page); err != nil {
		t.Fatal(err)
	}
	// Every page is now in: reading them faults nothing, which in this process would wait for ever.
	want := bytes.Clone(data)
	copy(want[blockSize/2:], bytes.Repeat([]byte{7}, int(page)))
	if !bytes.Equal(mem, want) {
		t.Fatal("the block is not the copy around the page that was there")
	}
}
