package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

func (s *memoryStore) getRange(_ context.Context, key string, off int64, buf []byte) error {
	s.ranges.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok || off+int64(len(buf)) > int64(len(data)) {
		return errors.New("no range of " + key)
	}
	copy(buf, data[off:])
	return nil
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
	got, release, err := cache.block(ctx, h.Objects[1], 0, 2)
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
			got, release, err := c.block(ctx, obj, int64(b)*blockSize, b)
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
		if _, err := os.Stat(c.path(obj.Key, 0)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the chunk's file was not written")
		}
		time.Sleep(10 * time.Millisecond)
	}
	later := newCache(dir, s)
	got, release, err := later.block(ctx, obj, blockSize, 1)
	if err != nil || !bytes.Equal(got, data[blockSize:]) {
		t.Fatalf("block 1 from the cache: %v", err)
	}
	release()
	if s.ranges.Load() != 1 {
		t.Fatalf("%d ranged GETs after the chunk was cached", s.ranges.Load())
	}
}

const (
	// _IOWR(0xAA, 0x3F, struct uffdio_api), _IOWR(0xAA, 0x00, struct uffdio_register).
	uffdioAPI      = 0xc018aa3f
	uffdioRegister = 0xc020aa00
	uffdAPI        = 0xaa
	uffdUserOnly   = 1
	registerMiss   = 1
)

// registeredMemory maps n blocks of anonymous memory and registers them with a new
// userfaultfd, as Firecracker does guest memory; skips where the kernel gives none.
func registeredMemory(t *testing.T, n int) ([]byte, int) {
	t.Helper()
	fd, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|unix.O_NONBLOCK|uffdUserOnly, 0, 0)
	if errno != 0 {
		t.Skipf("no userfaultfd here: %v", errno)
	}
	t.Cleanup(func() { unix.Close(int(fd)) })
	api := [3]uint64{uffdAPI, 0, 0}
	if err := ioctl(int(fd), uffdioAPI, unsafe.Pointer(&api)); err != nil {
		t.Fatal(err)
	}
	mem, err := unix.Mmap(-1, 0, n*blockSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Munmap(mem) })
	reg := [4]uint64{uint64(uintptr(unsafe.Pointer(&mem[0]))), uint64(len(mem)), registerMiss, 0}
	if err := ioctl(int(fd), uffdioRegister, unsafe.Pointer(&reg)); err != nil {
		t.Fatal(err)
	}
	return mem, int(fd)
}

func TestServeInstallsEachFaultingBlock(t *testing.T) {
	ctx := context.Background()
	mem, fd := registeredMemory(t, 3)
	s := newMemoryStore()
	data := append(randomBlock(t), randomBlock(t)...)
	h := &header{
		MemSize: 3 * blockSize, BlockSize: blockSize, ChunkSize: chunkSize,
		Objects: []object{{Key: "p/s/memory", Size: int64(len(data))}},
		Blocks:  []block{{-1, 0}, {0, blockSize}, {0, 0}},
	}
	s.put(ctx, h.Objects[0].Key, data)
	encoded, _ := json.Marshal(h)
	if _, err := decodeHeader(encoded); err != nil {
		t.Fatal(err)
	}
	handler := &handler{start: time.Now(), header: h, cache: newCache(t.TempDir(), s), uffd: fd, state: make([]atomic.Uint32, 3)}
	base := uint64(uintptr(unsafe.Pointer(&mem[0])))
	if err := handler.mapBlocks([]region{{BaseHostVirtAddr: base, Size: uint64(len(mem)), PageSize: uint64(os.Getpagesize())}}); err != nil {
		t.Fatal(err)
	}
	stop := make([]int, 2)
	if err := unix.Pipe2(stop, unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- handler.serveLoop(ctx, stop[0]) }()

	// Every page read, from two threads, faults its block in. A thread waiting on a fault holds
	// its P, so there are Ps enough for the handler beside them.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			for off := 0; off < len(mem); off += os.Getpagesize() {
				_ = mem[off]
			}
		})
	}
	wait.Wait()
	if !isZero(mem[:blockSize]) || !bytes.Equal(mem[blockSize:2*blockSize], data[blockSize:]) || !bytes.Equal(mem[2*blockSize:], data[:blockSize]) {
		t.Fatal("guest memory is not the header's blocks")
	}
	unix.Write(stop[1], []byte{0})
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if handler.installs.Load() != 3 || handler.zeroInstalls.Load() != 1 || s.ranges.Load() != 1 {
		t.Fatalf("%d installs, %d of zeros, %d ranged GETs", handler.installs.Load(), handler.zeroInstalls.Load(), s.ranges.Load())
	}
}
