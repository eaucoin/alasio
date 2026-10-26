package main

// A snapshot in the bucket, under <prefix>/<snapshot>/:
//
//	memory       the guest memory blocks this save stored, packed in address order
//	state        Firecracker's microVM state file
//	header.json  the header, written last, which commits the snapshot
//
// The header maps every block of guest memory (Firecracker's memory file offsets, which follow
// its regions) to where its bytes are: an offset in this snapshot's memory object or in an
// earlier snapshot's, whose blocks this one has unchanged, or nowhere for a block of zeros. This
// is E2B's layered memory header (packages/shared/pkg/storage/header/mapping.go), whose zero
// blocks map to no build and whose unchanged blocks keep pointing at their parent's.

import (
	"encoding/json"
	"fmt"
)

const (
	// The header's block: the guest's page size on 2 MiB hugetlbfs pages, E2B's block size
	// for its sandboxes (header/diff.go), and so what a fault installs.
	blockSize = 2 << 20
	// The unit fetched from the bucket and kept in the node's cache: E2B's
	// storage.MemoryChunkSize (packages/shared/pkg/storage/storage.go:46).
	chunkSize = 4 << 20
)

type object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// block is where a block's bytes are: Object indexes the header's objects; -1 is zeros.
type block struct {
	Object int   `json:"o"`
	Offset int64 `json:"off"`
}

type header struct {
	MemSize   int64    `json:"mem_size"`
	BlockSize int64    `json:"block_size"`
	ChunkSize int64    `json:"chunk_size"`
	State     string   `json:"state"`
	Objects   []object `json:"objects"`
	Blocks    []block  `json:"blocks"`
	// The blocks this save stored that its parent did not have: the hot set a restore
	// prefetches, in address order (E2B's "last-cycle" resume prefetch source).
	Hot []int `json:"hot"`
}

func decodeHeader(data []byte) (*header, error) {
	var h header
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	if h.BlockSize != blockSize || h.ChunkSize != chunkSize || h.MemSize != int64(len(h.Blocks))*blockSize {
		return nil, fmt.Errorf("header of %d blocks of %d bytes, %d-byte chunks, for %d bytes", len(h.Blocks), h.BlockSize, h.ChunkSize, h.MemSize)
	}
	return &h, nil
}
