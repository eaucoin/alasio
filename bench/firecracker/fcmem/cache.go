package main

// The node's cache, in a directory of its own, of files named by object (its key with / as %):
//
//	<key>            the header and state file of a snapshot this node took, whole
//	<key>.mem        the memory file of a snapshot this node took, as Firecracker wrote it
//	                 (save.go), whose blocks are where the guest's are
//	<key>.data       a memory object this node fetched, sparse: each chunk fetched where it is
//	                 in the object
//	<key>.in/<chunk> a chunk of <key>.data wholly in, made once its last byte is written
//
// It outlives the handler process: the next restore on this node reads what the last one
// fetched. E2B keeps a build's blocks in one sparse file per build with the bitmap of what is
// cached in memory (packages/orchestrator/pkg/sandbox/block/cache.go), which lives only as
// long as its orchestrator; a handler started per restore keeps that bitmap in the filesystem.
//
// A block is read alone, into a buffer of a pool. A chunk the cache does not hold is fetched
// whole with one ranged GET, apart from whoever asked for it, a block at a time written into
// <key>.data, so what is fetched is in the page cache and not the handler's memory; each block is
// handed out as soon as it is written, rather than once the chunk is, and a chunk asked for while
// it is being fetched waits on that fetch rather than starting another (E2B's fetch sessions,
// block/streaming_chunk.go progressiveFetch and block/fetch_session.go).

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	// The chunks being fetched, and the data files open for them, by name.
	fetching map[string]*fetch
	data     map[string]*os.File

	buffers sync.Pool

	fromStore, blocksRead atomic.Int64
	bytesFetched          atomic.Int64
}

// fetch is a chunk being fetched: how many of its bytes are written, or why it failed.
type fetch struct {
	mu   sync.Mutex
	in   *sync.Cond
	have int64
	err  error
}

// wait returns once the chunk's first end bytes are written.
func (f *fetch) wait(end int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.have < end && f.err == nil {
		f.in.Wait()
	}
	if f.have >= end {
		return nil
	}
	return f.err
}

func (f *fetch) advance(have int64, err error) {
	f.mu.Lock()
	f.have, f.err = have, err
	f.mu.Unlock()
	f.in.Broadcast()
}

func newCache(dir string, s objectStore) *cache {
	c := &cache{dir: dir, store: s, fetching: map[string]*fetch{}, data: map[string]*os.File{}}
	c.buffers.New = func() any { return make([]byte, blockSize) }
	return c
}

func (c *cache) whole(key string) string {
	return filepath.Join(c.dir, strings.ReplaceAll(key, "/", "%"))
}

func (c *cache) taken(key string) string {
	return c.whole(key) + ".mem"
}

func (c *cache) fetched(key string) string {
	return c.whole(key) + ".data"
}

func (c *cache) chunkIn(key string, index int64) string {
	return filepath.Join(c.whole(key)+".in", strconv.FormatInt(index, 10))
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
func (c *cache) block(obj object, off int64, b int) ([]byte, func(), error) {
	if buf, err := c.read(c.taken(obj.Key), int64(b)*blockSize); !errors.Is(err, fs.ErrNotExist) {
		return buf, func() { c.buffers.Put(buf) }, err
	}
	index := off / chunkSize
	if f := c.fetch(obj, index); f != nil {
		if err := f.wait(off%chunkSize + blockSize); err != nil {
			return nil, nil, err
		}
	}
	buf, err := c.read(c.fetched(obj.Key), off)
	if err != nil {
		return nil, nil, err
	}
	return buf, func() { c.buffers.Put(buf) }, nil
}

// ensure has the cache hold chunk index of obj, fetching it if it does not.
func (c *cache) ensure(obj object, index int64) error {
	if _, err := os.Stat(c.taken(obj.Key)); err == nil {
		return nil
	}
	if f := c.fetch(obj, index); f != nil {
		return f.wait(min(chunkSize, obj.Size-index*chunkSize))
	}
	return nil
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
	c.blocksRead.Add(1)
	return buf, nil
}

// fetch returns the fetch of chunk index of obj, started if none is under way, or nil if the
// chunk is in. A fetch is one of its own, which goes on whoever stops waiting on it, as other
// faults may need its other blocks.
func (c *cache) fetch(obj object, index int64) *fetch {
	in := c.chunkIn(obj.Key, index)
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.fetching[in]; ok {
		return f
	}
	if _, err := os.Stat(in); err == nil {
		return nil
	}
	f := &fetch{}
	f.in = sync.NewCond(&f.mu)
	off := index * chunkSize
	if off >= obj.Size {
		f.err = fmt.Errorf("chunk %d past the end of %s, %d bytes", index, obj.Key, obj.Size)
		return f
	}
	file, err := c.dataFile(obj.Key)
	if err != nil {
		f.err = err
		return f
	}
	c.fetching[in] = f
	go func() {
		err := c.get(obj.Key, off, min(chunkSize, obj.Size-off), file, f)
		if err == nil {
			err = c.put(in, nil)
		}
		c.mu.Lock()
		delete(c.fetching, in)
		c.mu.Unlock()
		if err != nil {
			f.advance(f.have, err)
		}
	}()
	return f
}

// dataFile opens, once, the file a memory object's chunks are fetched into.
func (c *cache) dataFile(key string) (*os.File, error) {
	if file, ok := c.data[key]; ok {
		return file, nil
	}
	if err := os.MkdirAll(c.whole(key)+".in", 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(c.fetched(key), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	c.data[key] = file
	return file, nil
}

// get reads length bytes of key from off with one ranged GET, written to file a block at a
// time, each handed out as it is.
func (c *cache) get(key string, off, length int64, file *os.File, f *fetch) error {
	body, err := c.store.openRange(context.Background(), key, off, length)
	if err != nil {
		return err
	}
	defer body.Close()
	buf := c.buffers.Get().([]byte)
	defer c.buffers.Put(buf)
	for have := int64(0); have < length; {
		n, err := io.ReadFull(body, buf[:min(blockSize, length-have)])
		if err != nil {
			return fmt.Errorf("get %s at %d: %w", key, off+have, err)
		}
		if _, err := file.WriteAt(buf[:n], off+have); err != nil {
			return err
		}
		have += int64(n)
		c.bytesFetched.Add(int64(n))
		f.advance(have, nil)
	}
	c.fromStore.Add(1)
	return nil
}

// put writes a file of the cache whole: under a temporary name, renamed into place, so one that
// exists is complete.
func (c *cache) put(name string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(name), ".put-")
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
