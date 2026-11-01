package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// TestSeekable checks that a seekable file decodes, as one stream, to its input, and that its
// seek table's entries add up to its frames and the input.
func TestSeekable(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "pages.img")
	data := bytes.Repeat([]byte("pages of a checkpoint, "), 50000)
	if err := os.WriteFile(in, data, 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "pages.img.zst")
	written, logical, _, err := writeSeekable(in, out, 64<<10, 1)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(file)) != written || logical != int64(len(data)) {
		t.Fatalf("wrote %d of %d bytes, file %d, data %d", written, logical, len(file), len(data))
	}
	dec, err := zstd.NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("decoded %d bytes unlike the input's %d", len(got), len(data))
	}
	footer := file[len(file)-9:]
	frames := int(binary.LittleEndian.Uint32(footer))
	if binary.LittleEndian.Uint32(footer[5:]) != 0x8F92EAB1 || footer[4] != 0 || frames != (len(data)+64<<10-1)/(64<<10) {
		t.Fatalf("footer %x", footer)
	}
	table := file[len(file)-9-8*frames-8:]
	var physical, size int
	for i := range frames {
		physical += int(binary.LittleEndian.Uint32(table[8+8*i:]))
		size += int(binary.LittleEndian.Uint32(table[12+8*i:]))
	}
	if physical+len(table) != len(file) || size != len(data) {
		t.Fatalf("table says %d bytes of frames and %d of data; file %d with a table of %d, data %d", physical, size, len(file), len(table), len(data))
	}
}
