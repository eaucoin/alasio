package main

// The node's cache, in a directory of its own, of three kinds of file, by object (its key with /
// as %):
//
//	<key>          the header and state file of a snapshot this node took, whole
//	<key>.mem      the memory file of a snapshot this node took, as Firecracker wrote it (save.go),
//	               whose blocks are where the guest's are
//	<key>/<chunk>  each 4 MiB chunk of a memory object this node fetched, written whole under a
//	               temporary name and renamed into place, so one that exists is complete
//
// It outlives the handler process: the next restore on this node reads what the last one
// fetched. E2B keeps a build's blocks in one sparse file per build with the bitmap of what is
// cached in memory (packages/orchestrator/pkg/sandbox/block/cache.go), which lives only as
// long as its orchestrator; a handler started per restore keeps that bitmap in the filesystem.
//
// A block is read alone, into a buffer of a pool. A chunk the cache does not hold is fetched
// whole with one ranged GET; one asked for while it is being fetched waits for that fetch rather
// than starting another (E2B's fetch sessions, block/streaming_chunk.go), and once fetched is
// kept in memory until its file is written.

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

type cache struct {
	dir   string
	store objectStore

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

func newCache(dir string, s objectStore) *cache {
	c := &cache{dir: dir, store: s, fetching: map[string]*fetch{}, writing: map[string][]byte{}}
	c.buffers.New = func() any { return make([]byte, blockSize) }
	return c
}

func (c *cache) path(key string, index int64) string {
	return filepath.Join(c.dir, strings.ReplaceAll(key, "/", "%"), strconv.FormatInt(index, 10))
}

func (c *cache) whole(key string) string {
	return filepath.Join(c.dir, strings.ReplaceAll(key, "/", "%"))
}

func (c *cache) taken(key string) string {
	return c.whole(key) + ".mem"
}

// keep moves the memory file of a snapshot this node took into the cache, as obj's copy.
func (c *cache) keep(key, memPath string) error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	return os.Rename(memPath, c.taken(key))
}

// keepWhole keeps a small object of a snapshot this node took.
func (c *cache) keepWhole(key string, data []byte) error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	return c.put(c.whole(key), data)
}

// object returns a small object from the cache, or from the bucket.
func (c *cache) object(ctx context.Context, key string) ([]byte, error) {
	if data, err := os.ReadFile(c.whole(key)); !errors.Is(err, fs.ErrNotExist) {
		return data, err
	}
	return c.store.get(ctx, key)
}

// block returns guest block b, stored at off in obj, and what to call once its bytes are no
// longer used.
func (c *cache) block(ctx context.Context, obj object, off int64, b int) ([]byte, func(), error) {
	if buf, err := c.read(c.taken(obj.Key), int64(b)*blockSize); !errors.Is(err, fs.ErrNotExist) {
		return buf, func() { c.buffers.Put(buf) }, err
	}
	index, inner := off/chunkSize, off%chunkSize
	name := c.path(obj.Key, index)
	if data, ok := c.held(name); ok {
		c.fromCache.Add(1)
		return slice(data, inner), func() {}, nil
	}
	if buf, err := c.read(name, inner); !errors.Is(err, fs.ErrNotExist) {
		return buf, func() { c.buffers.Put(buf) }, err
	}
	data, err := c.fetch(ctx, obj, index, name)
	if err != nil {
		return nil, nil, err
	}
	return slice(data, inner), func() {}, nil
}

// ensure has the cache hold chunk index of obj, fetching it if it does not.
func (c *cache) ensure(ctx context.Context, obj object, index int64) error {
	name := c.path(obj.Key, index)
	if _, ok := c.held(name); ok {
		return nil
	}
	for _, held := range []string{c.taken(obj.Key), name} {
		if _, err := os.Stat(held); err == nil {
			return nil
		}
	}
	_, err := c.fetch(ctx, obj, index, name)
	return err
}

// read reads the block at off in a file of the cache into a buffer of the pool.
func (c *cache) read(name string, off int64) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	buf := c.buffers.Get().([]byte)
	if _, err := file.ReadAt(buf, off); err != nil {
		c.buffers.Put(buf)
		return nil, fmt.Errorf("cached %s at %d: %w", name, off, err)
	}
	c.fromCache.Add(1)
	return buf, nil
}

func slice(data []byte, inner int64) []byte {
	return data[inner : inner+blockSize]
}

// held returns a chunk being written into the cache.
func (c *cache) held(name string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.writing[name]
	return data, ok
}

func (c *cache) fetch(ctx context.Context, obj object, index int64, name string) ([]byte, error) {
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

func (c *cache) get(ctx context.Context, obj object, index int64) ([]byte, error) {
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
func (c *cache) put(name string, data []byte) error {
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
