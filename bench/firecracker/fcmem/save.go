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

func save(args []string) error {
	fs := flag.NewFlagSet("save", flag.ExitOnError)
	memPath := fs.String("mem", "", "Firecracker's memory file")
	statePath := fs.String("state", "", "Firecracker's state file")
	prefix := fs.String("prefix", "", "key prefix of the snapshots")
	id := fs.String("snapshot", "", "this snapshot's name")
	parent := fs.String("parent", "", "the snapshot this one is saved over, taken on this node")
	work := fs.String("work", "", "where this node keeps its snapshots' headers and block digests")
	cacheDir := fs.String("cache", "", "the node's chunk cache")
	fs.Parse(args)

	start := time.Now()
	ctx := context.Background()
	s, err := newStore(ctx)
	if err != nil {
		return err
	}
	mem, err := os.Open(*memPath)
	if err != nil {
		return err
	}
	defer mem.Close()
	info, err := mem.Stat()
	if err != nil {
		return err
	}
	if info.Size()%blockSize != 0 {
		return fmt.Errorf("%s: %d bytes, not whole blocks", *memPath, info.Size())
	}
	n := int(info.Size() / blockSize)
	present, err := blocksWithData(int(mem.Fd()), n)
	if err != nil {
		return err
	}

	h := &header{MemSize: info.Size(), BlockSize: blockSize, ChunkSize: chunkSize, State: *prefix + "/" + *id + "/state"}
	h.Objects = []object{{Key: *prefix + "/" + *id + "/memory"}}
	h.Blocks = make([]block, n)
	var parentHeader *header
	var parentSums []byte
	if *parent != "" {
		if parentHeader, parentSums, err = loadLocal(*work, *parent); err != nil {
			return err
		}
		if len(parentHeader.Blocks) != n {
			return fmt.Errorf("parent %s has %d blocks, this snapshot %d", *parent, len(parentHeader.Blocks), n)
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
		return streamErr
	}
	stream.Close()
	if err := <-uploaded; err != nil {
		return err
	}
	report.MemoryMs = time.Since(start).Milliseconds()
	h.Objects[0].Size = packed

	state, err := os.ReadFile(*statePath)
	if err != nil {
		return err
	}
	if err := s.put(ctx, h.State, state); err != nil {
		return err
	}
	headerKey := *prefix + "/" + *id + "/header.json"
	encoded, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if err := storeLocal(*work, *id, encoded, sums); err != nil {
		return err
	}
	c := newCache(*cacheDir, s)
	if err := c.keep(h.Objects[0].Key, *memPath); err != nil {
		return err
	}
	if err := c.keepWhole(h.State, state); err != nil {
		return err
	}
	if err := c.keepWhole(headerKey, encoded); err != nil {
		return err
	}
	if err := s.put(ctx, headerKey, encoded); err != nil {
		return err
	}
	report.MemoryBytes, report.StateBytes, report.HeaderBytes = packed, int64(len(state)), int64(len(encoded))
	report.ImageBytes = packed + int64(len(state)) + int64(len(encoded))
	report.TotalMs = time.Since(start).Milliseconds()
	return json.NewEncoder(os.Stdout).Encode(report)
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
