package main

// The node's chunk cache: every 4 MiB chunk of a memory object this node saved or fetched, a
// file of its own, <dir>/<object key with / as %>/<chunk index>. A file is written whole under a
// temporary name and renamed into place, so one that exists is complete, and the cache outlives
// the handler process: the next restore on this node reads what the last one fetched. E2B keeps
// its chunks in one sparse file per build with the bitmap of what is cached in memory
// (packages/orchestrator/pkg/sandbox/block/cache.go), which lives only as long as its
// orchestrator; a handler started per restore keeps the bitmap in the filesystem instead.
//
// A chunk asked for while it is being read waits for that read rather than starting another
// (E2B's fetch sessions, block/streaming_chunk.go), and one fetched is kept in memory until its
// file is written.

import (
	"context"
	"fmt"
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

	mu       sync.Mutex
	inflight map[string]*fetch
	writing  map[string][]byte

	fromStore, fromCache atomic.Int64
	bytesFetched         atomic.Int64
}

type fetch struct {
	done chan struct{}
	data []byte
	err  error
}

func newChunks(dir string, s *store) *chunks {
	return &chunks{dir: dir, store: s, inflight: map[string]*fetch{}, writing: map[string][]byte{}}
}

func (c *chunks) path(key string, index int64) string {
	return filepath.Join(c.dir, strings.ReplaceAll(key, "/", "%"), strconv.FormatInt(index, 10))
}

// get returns chunk index of obj: from the cache, or from the bucket, kept in the cache.
func (c *chunks) get(ctx context.Context, obj object, index int64) ([]byte, error) {
	name := c.path(obj.Key, index)
	c.mu.Lock()
	if f, ok := c.inflight[name]; ok {
		c.mu.Unlock()
		<-f.done
		return f.data, f.err
	}
	if data, ok := c.writing[name]; ok {
		c.mu.Unlock()
		c.fromCache.Add(1)
		return data, nil
	}
	f := &fetch{done: make(chan struct{})}
	c.inflight[name] = f
	c.mu.Unlock()

	f.data, f.err = c.read(ctx, obj, index, name)
	c.mu.Lock()
	delete(c.inflight, name)
	c.mu.Unlock()
	close(f.done)
	return f.data, f.err
}

func (c *chunks) read(ctx context.Context, obj object, index int64, name string) ([]byte, error) {
	if data, err := os.ReadFile(name); err == nil {
		c.fromCache.Add(1)
		return data, nil
	}
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
	// Kept off the fault's path: the fault is answered from data while the file is written.
	c.mu.Lock()
	c.writing[name] = data
	c.mu.Unlock()
	go func() {
		if err := c.put(name, data); err != nil {
			fmt.Fprintln(os.Stderr, "fcmem: cache:", err)
		}
		c.mu.Lock()
		delete(c.writing, name)
		c.mu.Unlock()
	}()
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
