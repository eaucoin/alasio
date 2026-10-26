package main

// The node's chunk cache: every 4 MiB chunk of a memory object this node saved or fetched, a
// file of its own, <dir>/<object key with / as %>/<chunk index>. A file is written whole under a
// temporary name and renamed into place, so one that exists is complete, and the cache outlives
// the handler process: the next restore on this node reads what the last one fetched. E2B keeps
// its chunks in one sparse file per build with the bitmap of what is cached in memory
// (packages/orchestrator/pkg/sandbox/block/cache.go), which lives only as long as its
// orchestrator; a handler started per restore keeps the bitmap in the filesystem instead.
//
// A block is read from its chunk's file, alone, into a buffer of a pool. A chunk the cache does
// not hold is fetched whole with one ranged GET; one asked for while it is being fetched waits
// for that fetch rather than starting another (E2B's fetch sessions,
// block/streaming_chunk.go), and once fetched is kept in memory until its file is written.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type chunks struct {
	dir   string
	store *store

	mu sync.Mutex
	// The chunks being fetched, and those fetched whose file is being written.
	fetching map[string]*fetch
	writing  map[string][]byte

	buffers sync.Pool

	fromStore, fromCache atomic.Int64
	bytesFetched         atomic.Int64
}

type fetch struct {
	done chan struct{}
	data []byte
	err  error
}

func newChunks(dir string, s *store) *chunks {
	c := &chunks{dir: dir, store: s, fetching: map[string]*fetch{}, writing: map[string][]byte{}}
	c.buffers.New = func() any { return make([]byte, blockSize) }
	return c
}

func (c *chunks) path(key string, index int64) string {
	return filepath.Join(c.dir, strings.ReplaceAll(key, "/", "%"), strconv.FormatInt(index, 10))
}

// block returns the block at off in obj, and what to call once its bytes are no longer used.
func (c *chunks) block(ctx context.Context, obj object, off int64) ([]byte, func(), error) {
	index, inner := off/chunkSize, off%chunkSize
	name := c.path(obj.Key, index)
	if data, ok := c.held(name); ok {
		c.fromCache.Add(1)
		return slice(data, inner), func() {}, nil
	}
	file, err := os.Open(name)
	if err == nil {
		defer file.Close()
		buf := c.buffers.Get().([]byte)
		if _, err := file.ReadAt(buf, inner); err != nil {
			c.buffers.Put(buf)
			return nil, nil, fmt.Errorf("cached chunk %s: %w", name, err)
		}
		c.fromCache.Add(1)
		return buf, func() { c.buffers.Put(buf) }, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, err
	}
	data, err := c.fetch(ctx, obj, index, name)
	if err != nil {
		return nil, nil, err
	}
	return slice(data, inner), func() {}, nil
}

// ensure has the cache hold chunk index of obj, fetching it if it does not.
func (c *chunks) ensure(ctx context.Context, obj object, index int64) error {
	name := c.path(obj.Key, index)
	if _, ok := c.held(name); ok {
		return nil
	}
	if _, err := os.Stat(name); err == nil {
		return nil
	}
	_, err := c.fetch(ctx, obj, index, name)
	return err
}

func slice(data []byte, inner int64) []byte {
	return data[inner : inner+blockSize]
}

// held returns a chunk being written into the cache.
func (c *chunks) held(name string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.writing[name]
	return data, ok
}

func (c *chunks) fetch(ctx context.Context, obj object, index int64, name string) ([]byte, error) {
	c.mu.Lock()
	if f, ok := c.fetching[name]; ok {
		c.mu.Unlock()
		<-f.done
		return f.data, f.err
	}
	if data, ok := c.writing[name]; ok {
		c.mu.Unlock()
		return data, nil
	}
	f := &fetch{done: make(chan struct{})}
	c.fetching[name] = f
	c.mu.Unlock()

	f.data, f.err = c.get(ctx, obj, index)
	c.mu.Lock()
	delete(c.fetching, name)
	if f.err == nil {
		c.writing[name] = f.data
	}
	c.mu.Unlock()
	close(f.done)
	if f.err == nil {
		// Off the fault's path: the fault is answered from the fetched bytes as the file is written.
		go func() {
			if err := c.put(name, f.data); err != nil {
				fmt.Fprintln(os.Stderr, "fcmem: cache:", err)
			}
			c.mu.Lock()
			delete(c.writing, name)
			c.mu.Unlock()
		}()
	}
	return f.data, f.err
}

func (c *chunks) get(ctx context.Context, obj object, index int64) ([]byte, error) {
	off := index * chunkSize
	if off >= obj.Size {
		return nil, fmt.Errorf("chunk %d past the end of %s, %d bytes", index, obj.Key, obj.Size)
	}
	data := make([]byte, min(chunkSize, obj.Size-off))
	if err := c.store.getRange(ctx, obj.Key, off, data); err != nil {
		return nil, err
	}
	c.fromStore.Add(1)
	c.bytesFetched.Add(int64(len(data)))
	return data, nil
}

// put writes a chunk into the cache.
func (c *chunks) put(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(name), ".chunk-")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), name)
}
