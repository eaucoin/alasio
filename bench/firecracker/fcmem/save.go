package main

// fcmem save: puts a snapshot Firecracker wrote to the node's disk into the bucket. The memory
// file is a Diff snapshot's, so the blocks the guest never touched are holes; of the rest, a
// block of zeros is stored as nothing, a block whose SHA-256 is its parent snapshot's is mapped
// to the parent's copy, and every other block is stored, packed in address order into the
// snapshot's memory object as it is read, which is uploaded in parts while the file is still
// being read. The memory file itself is then kept in the node's cache, as the node's copy of the
// snapshot, which a restore here reads blocks from as E2B's orchestrator does the snapshots it
// takes from its template cache: the copy Firecracker had to write is the only one written.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// How many blocks are read and hashed ahead of the one being uploaded.
const saveWindow = 32

type saveReport struct {
	MemoryBytes  int64 `json:"memory_bytes"`
	StateBytes   int64 `json:"state_bytes"`
	HeaderBytes  int64 `json:"header_bytes"`
	ImageBytes   int64 `json:"image_bytes"`
	StoredBlocks int   `json:"stored_blocks"`
	ParentBlocks int   `json:"parent_blocks"`
	ZeroBlocks   int   `json:"zero_blocks"`
	HoleBlocks   int   `json:"hole_blocks"`
	MemoryMs     int64 `json:"memory_ms"`
	TotalMs      int64 `json:"total_ms"`
}

// The snapshot a save puts into the bucket, and where the node keeps its own of it.
type saveInput struct {
	memPath, statePath string
	prefix, id, parent string
	work, cacheDir     string
}

func save(args []string) error {
	fs := flag.NewFlagSet("save", flag.ExitOnError)
	var in saveInput
	fs.StringVar(&in.memPath, "mem", "", "Firecracker's memory file")
	fs.StringVar(&in.statePath, "state", "", "Firecracker's state file")
	fs.StringVar(&in.prefix, "prefix", "", "key prefix of the snapshots")
	fs.StringVar(&in.id, "snapshot", "", "this snapshot's name")
	fs.StringVar(&in.parent, "parent", "", "the snapshot this one is saved over, taken on this node")
	fs.StringVar(&in.work, "work", "", "where this node keeps its snapshots' headers and block digests")
	fs.StringVar(&in.cacheDir, "cache", "", "the node's cache")
	fs.Parse(args)

	ctx := context.Background()
	s, err := newStore(ctx)
	if err != nil {
		return err
	}
	report, err := saveSnapshot(ctx, s, in)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func saveSnapshot(ctx context.Context, s objectStore, in saveInput) (saveReport, error) {
	start := time.Now()
	mem, err := os.Open(in.memPath)
	if err != nil {
		return saveReport{}, err
	}
	defer mem.Close()
	info, err := mem.Stat()
	if err != nil {
		return saveReport{}, err
	}
	if info.Size()%blockSize != 0 {
		return saveReport{}, fmt.Errorf("%s: %d bytes, not whole blocks", in.memPath, info.Size())
	}
	n := int(info.Size() / blockSize)
	present, err := blocksWithData(int(mem.Fd()), n)
	if err != nil {
		return saveReport{}, err
	}

	h := &header{MemSize: info.Size(), BlockSize: blockSize, ChunkSize: chunkSize, State: in.prefix + "/" + in.id + "/state"}
	h.Objects = []object{{Key: in.prefix + "/" + in.id + "/memory"}}
	h.Blocks = make([]block, n)
	var parentHeader *header
	var parentSums []byte
	if in.parent != "" {
		if parentHeader, parentSums, err = loadLocal(in.work, in.parent); err != nil {
			return saveReport{}, err
		}
		if len(parentHeader.Blocks) != n {
			return saveReport{}, fmt.Errorf("parent %s has %d blocks, this snapshot %d", in.parent, len(parentHeader.Blocks), n)
		}
		h.Objects = append(h.Objects, parentHeader.Objects...)
	}
	sums := make([]byte, n*sha256.Size)

	// Blocks are read and hashed by every core, a window ahead of the upload, and taken in
	// address order.
	type result struct {
		buf  []byte
		zero bool
		sum  [sha256.Size]byte
		err  error
	}
	slots := make([]chan result, n)
	pool := sync.Pool{New: func() any { return make([]byte, blockSize) }}
	window := make(chan struct{}, saveWindow)
	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for i := range n {
			if present[i] {
				window <- struct{}{}
				jobs <- i
			}
		}
	}()
	for i := range n {
		if present[i] {
			slots[i] = make(chan result, 1)
		}
	}
	for range runtime.GOMAXPROCS(0) {
		go func() {
			for i := range jobs {
				buf := pool.Get().([]byte)
				var r result
				if _, r.err = mem.ReadAt(buf, int64(i)*blockSize); r.err == nil {
					r.zero = isZero(buf)
					if !r.zero {
						r.sum = sha256.Sum256(buf)
					}
				}
				r.buf = buf
				slots[i] <- r
			}
		}()
	}

	body, stream := io.Pipe()
	uploaded := make(chan error, 1)
	go func() { uploaded <- s.upload(ctx, h.Objects[0].Key, body) }()

	report := saveReport{}
	var packed int64
	var streamErr error
	for i := range n {
		if !present[i] {
			h.Blocks[i] = block{Object: -1}
			report.HoleBlocks++
			continue
		}
		r := <-slots[i]
		if r.err != nil {
			streamErr = r.err
			break
		}
		copy(sums[i*sha256.Size:], r.sum[:])
		switch {
		case r.zero:
			h.Blocks[i] = block{Object: -1}
			report.ZeroBlocks++
		case parentHeader != nil && bytes.Equal(r.sum[:], parentSums[i*sha256.Size:(i+1)*sha256.Size]):
			b := parentHeader.Blocks[i]
			h.Blocks[i] = block{Object: b.Object + 1, Offset: b.Offset}
			report.ParentBlocks++
		default:
			if _, streamErr = stream.Write(r.buf); streamErr != nil {
				break
			}
			h.Blocks[i] = block{Object: 0, Offset: packed}
			h.Hot = append(h.Hot, i)
			packed += blockSize
			report.StoredBlocks++
		}
		pool.Put(r.buf)
		<-window
		if streamErr != nil {
			break
		}
	}
	if streamErr != nil {
		stream.CloseWithError(streamErr)
		<-uploaded
		return saveReport{}, streamErr
	}
	stream.Close()
	if err := <-uploaded; err != nil {
		return saveReport{}, err
	}
	report.MemoryMs = time.Since(start).Milliseconds()
	h.Objects[0].Size = packed

	state, err := os.ReadFile(in.statePath)
	if err != nil {
		return saveReport{}, err
	}
	if err := s.put(ctx, h.State, state); err != nil {
		return saveReport{}, err
	}
	headerKey := in.prefix + "/" + in.id + "/header.json"
	encoded, err := json.Marshal(h)
	if err != nil {
		return saveReport{}, err
	}
	if err := storeLocal(in.work, in.id, encoded, sums); err != nil {
		return saveReport{}, err
	}
	c := newCache(in.cacheDir, s, chunkSize)
	if err := c.keep(h.Objects[0].Key, in.memPath); err != nil {
		return saveReport{}, err
	}
	if err := c.keepWhole(h.State, state); err != nil {
		return saveReport{}, err
	}
	if err := c.keepWhole(headerKey, encoded); err != nil {
		return saveReport{}, err
	}
	if err := s.put(ctx, headerKey, encoded); err != nil {
		return saveReport{}, err
	}
	report.MemoryBytes, report.StateBytes, report.HeaderBytes = packed, int64(len(state)), int64(len(encoded))
	report.ImageBytes = packed + int64(len(state)) + int64(len(encoded))
	report.TotalMs = time.Since(start).Milliseconds()
	return report, nil
}

// blocksWithData says which blocks of the file hold any data, from its holes.
func blocksWithData(fd int, n int) ([]bool, error) {
	present := make([]bool, n)
	size := int64(n) * blockSize
	for off := int64(0); off < size; {
		data, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if err != nil {
			return nil, err
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if err != nil {
			return nil, err
		}
		for b := data / blockSize; b*blockSize < hole; b++ {
			present[b] = true
		}
		off = hole
	}
	return present, nil
}

func isZero(buf []byte) bool {
	for len(buf) > 0 {
		n := min(len(buf), len(zeros))
		if !bytes.Equal(buf[:n], zeros[:n]) {
			return false
		}
		buf = buf[n:]
	}
	return true
}

var zeros = make([]byte, 64<<10)

func storeLocal(work, id string, encoded, sums []byte) error {
	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(work, id+".header.json"), encoded, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(work, id+".sha256"), sums, 0o644)
}

func loadLocal(work, id string) (*header, []byte, error) {
	encoded, err := os.ReadFile(filepath.Join(work, id+".header.json"))
	if err != nil {
		return nil, nil, err
	}
	h, err := decodeHeader(encoded)
	if err != nil {
		return nil, nil, err
	}
	sums, err := os.ReadFile(filepath.Join(work, id+".sha256"))
	if err != nil {
		return nil, nil, err
	}
	return h, sums, nil
}
