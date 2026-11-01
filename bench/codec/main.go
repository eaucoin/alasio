// Command codec measures how the pages of a checkpoint image compress in independent frames,
// as M8 of the endpoint plan would store them (a zstd seekable file, or LZ4 blocks), with the
// zstd implementation the gVisor fork links (klauspost/compress, as its go.mod pins it): the
// ratio over every byte and over the bytes the image's extents refer to, the share of zero
// pages, compression and decompression throughput on one and two goroutines, and, given a
// working set, the frame amplification A_F of reading it (bytes of the frames it touches over
// its bytes). One JSON object per line on stdout; aggregates only, never page contents.
//
//	codec image --pages DIR/pages.img --inspect inspect.json [--ws ws.json] [--label L]
//	codec random --bytes N
//	codec seekable --in pages.img --out pages.img.zst [--frame 262144] [--level 1]
//
// seekable writes a pages file as zstd's seekable format (contrib/seekable_format): independent
// frames of --frame logical bytes, each with its content checksum, then the seek table in a
// skippable frame, entries without checksums; it prints its sizes and time.
//
// inspect.json is `runsc image inspect --json --extents` of the image; ws.json the same of the
// image saved after a restore of it, whose working set was recorded as the sandbox ran.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// frameSizes are the frame sizes M8 considers, 64 KiB to 2 MiB.
var frameSizes = []int{64 << 10, 256 << 10, 1 << 20, 2 << 20}

// sampleBytes bounds the bytes of a pages file measured: its first sampleBytes.
const sampleBytes = 512 << 20

// readBytes is the size of the loader's background reads, which a streaming decode serves.
const readBytes = 16 << 20

type inspected struct {
	Digest      string `json:"digest"`
	MemoryFiles []struct {
		CommittedBytes uint64      `json:"committed_bytes"`
		ExtentList     [][4]uint64 `json:"extent_list"`
	} `json:"memory_files"`
	WorkingSet *struct {
		Unit       uint64      `json:"unit"`
		Bytes      uint64      `json:"bytes"`
		ExtentList [][2]uint64 `json:"extent_list"`
	} `json:"working_set"`
}

func readInspect(path string) (*inspected, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var i inspected
	if err := json.Unmarshal(b, &i); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &i, nil
}

// codec compresses and decompresses frames.
type codec interface {
	name() string
	encode(dst, src []byte) []byte
	decode(dst, src []byte) ([]byte, error)
}

type zstdCodec struct {
	level int
	enc   *zstd.Encoder
	dec   *zstd.Decoder
}

func newZstd(level int) *zstdCodec {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true), zstd.WithZeroFrames(true))
	if err != nil {
		panic(err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		panic(err)
	}
	return &zstdCodec{level: level, enc: enc, dec: dec}
}

func (z *zstdCodec) name() string { return fmt.Sprintf("zstd-%d", z.level) }

func (z *zstdCodec) encode(dst, src []byte) []byte { return z.enc.EncodeAll(src, dst[:0]) }

func (z *zstdCodec) decode(dst, src []byte) ([]byte, error) { return z.dec.DecodeAll(src, dst[:0]) }

type lz4Codec struct{ c lz4.Compressor }

func (*lz4Codec) name() string { return "lz4" }

// encode prefixes each block with a byte that says whether it is compressed (0) or stored raw
// (1), as LZ4 leaves incompressible input to its caller.
func (l *lz4Codec) encode(dst, src []byte) []byte {
	dst = dst[:cap(dst)]
	if len(dst) < 1+lz4.CompressBlockBound(len(src)) {
		dst = make([]byte, 1+lz4.CompressBlockBound(len(src)))
	}
	n, err := l.c.CompressBlock(src, dst[1:])
	if err != nil {
		panic(err)
	}
	if n == 0 || n >= len(src) {
		dst[0] = 1
		return append(dst[:1], src...)
	}
	dst[0] = 0
	return dst[:1+n]
}

func (*lz4Codec) decode(dst, src []byte) ([]byte, error) {
	if src[0] == 1 {
		return append(dst[:0], src[1:]...), nil
	}
	n, err := lz4.UncompressBlock(src[1:], dst[:cap(dst)])
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

func codecs() []func() codec {
	return []func() codec{
		func() codec { return newZstd(1) },
		func() codec { return newZstd(3) },
		func() codec { return &lz4Codec{} },
	}
}

// result is one line of output.
type result struct {
	Label           string  `json:"label"`
	Digest          string  `json:"digest,omitempty"`
	Codec           string  `json:"codec"`
	FrameBytes      int     `json:"frame_bytes"`
	Over            string  `json:"over"`
	LogicalBytes    int     `json:"logical_bytes"`
	PhysicalBytes   int     `json:"physical_bytes"`
	Ratio           float64 `json:"ratio"`
	EncodeMBps1     float64 `json:"encode_mb_s_1"`
	EncodeMBps2     float64 `json:"encode_mb_s_2"`
	DecodeMBps1     float64 `json:"decode_mb_s_1"`
	DecodeMBps2     float64 `json:"decode_mb_s_2"`
	StreamMBps1     float64 `json:"stream_decode_mb_s_1,omitempty"`
	ZeroPageShare   float64 `json:"zero_page_share"`
	WorkingSetBytes uint64  `json:"working_set_bytes,omitempty"`
	FramesBytesWS   uint64  `json:"working_set_frame_bytes,omitempty"`
	AF              float64 `json:"a_f,omitempty"`
	CPU             string  `json:"cpu"`
	GOMAXPROCS      int     `json:"gomaxprocs"`
}

func frames(data []byte, size int) [][]byte {
	var out [][]byte
	for off := 0; off < len(data); off += size {
		out = append(out, data[off:min(off+size, len(data))])
	}
	return out
}

// encodeAll compresses fs with the codec make gives each of goroutines, the frames dealt out in
// turn; it returns the compressed frames and the logical bytes per second.
func encodeAll(fs [][]byte, mk func() codec, goroutines int) ([][]byte, float64) {
	out := make([][]byte, len(fs))
	cs := make([]codec, goroutines)
	for g := range cs {
		cs[g] = mk()
	}
	var total int
	for _, f := range fs {
		total += len(f)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g; i < len(fs); i += goroutines {
				out[i] = cs[g].encode(make([]byte, 0, len(fs[i])+1024), fs[i])
			}
		}()
	}
	wg.Wait()
	return out, float64(total) / time.Since(start).Seconds() / 1e6
}

// decodeAll decompresses cfs the same way, best of three passes, in logical bytes per second.
func decodeAll(cfs [][]byte, logical int, frameBytes int, mk func() codec, goroutines int) float64 {
	cs := make([]codec, goroutines)
	bufs := make([][]byte, goroutines)
	for g := range cs {
		cs[g] = mk()
		bufs[g] = make([]byte, 0, frameBytes)
	}
	best := 0.0
	for range 3 {
		var wg sync.WaitGroup
		start := time.Now()
		for g := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := g; i < len(cfs); i += goroutines {
					var err error
					if bufs[g], err = cs[g].decode(bufs[g], cfs[i]); err != nil {
						panic(err)
					}
				}
			}()
		}
		wg.Wait()
		best = max(best, float64(logical)/time.Since(start).Seconds()/1e6)
	}
	return best
}

// streamDecode decodes the zstd frames cfs as the gofer would serve a 16 MiB read of them: the
// frames of each read concatenated, decoded as one stream into a buffer; best of three.
func streamDecode(cfs [][]byte, logical, frameBytes int) float64 {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(uint64(max(frameBytes, zstd.MinWindowSize))))
	if err != nil {
		panic(err)
	}
	defer dec.Close()
	perRead := max(1, readBytes/frameBytes)
	var reads [][]byte
	for i := 0; i < len(cfs); i += perRead {
		reads = append(reads, bytes.Join(cfs[i:min(i+perRead, len(cfs))], nil))
	}
	sink := make([]byte, readBytes)
	best := 0.0
	for range 3 {
		start := time.Now()
		for _, r := range reads {
			if err := dec.Reset(bytes.NewReader(r)); err != nil {
				panic(err)
			}
			for {
				if _, err := io.ReadFull(dec, sink); err != nil {
					break
				}
			}
		}
		best = max(best, float64(logical)/time.Since(start).Seconds()/1e6)
	}
	return best
}

func zeroShare(data []byte) float64 {
	var zero, pages int
	page := make([]byte, 4096)
	for off := 0; off+4096 <= len(data); off += 4096 {
		pages++
		if bytes.Equal(data[off:off+4096], page) {
			zero++
		}
	}
	if pages == 0 {
		return 0
	}
	return float64(zero) / float64(pages)
}

func cpuModel() string {
	b, _ := os.ReadFile("/proc/cpuinfo")
	for _, line := range bytes.Split(b, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("model name")) {
			if i := bytes.IndexByte(line, ':'); i >= 0 {
				return string(bytes.TrimSpace(line[i+1:]))
			}
		}
	}
	return ""
}

func measure(label, digest, over string, data []byte, ws *wsFrames, emit func(result)) {
	zs := zeroShare(data)
	for _, mk := range codecs() {
		for _, size := range frameSizes {
			fs := frames(data, size)
			cfs, enc1 := encodeAll(fs, mk, 1)
			_, enc2 := encodeAll(fs, mk, 2)
			physical := 0
			for _, c := range cfs {
				physical += len(c)
			}
			r := result{
				Label: label, Digest: digest, Codec: mk().name(), FrameBytes: size, Over: over,
				LogicalBytes: len(data), PhysicalBytes: physical, Ratio: float64(len(data)) / float64(physical),
				EncodeMBps1: enc1, EncodeMBps2: enc2,
				DecodeMBps1: decodeAll(cfs, len(data), size, mk, 1), DecodeMBps2: decodeAll(cfs, len(data), size, mk, 2),
				ZeroPageShare: zs, CPU: cpuModel(), GOMAXPROCS: runtime.GOMAXPROCS(0),
			}
			if _, ok := mk().(*zstdCodec); ok {
				r.StreamMBps1 = streamDecode(cfs, len(data), size)
			}
			if ws != nil {
				r.WorkingSetBytes, r.FramesBytesWS = ws.bytes, ws.frameBytes(size)
				if ws.bytes != 0 {
					r.AF = float64(r.FramesBytesWS) / float64(ws.bytes)
				}
			}
			emit(r)
		}
	}
}

// wsFrames is a working set as the pages-file offsets of its pages.
type wsFrames struct {
	bytes uint64
	pages []uint64
}

// frameBytes returns the bytes of the frames of size bytes that the working set's pages are in.
func (w *wsFrames) frameBytes(size int) uint64 {
	seen := map[uint64]struct{}{}
	for _, p := range w.pages {
		seen[p/uint64(size)] = struct{}{}
	}
	return uint64(len(seen)) * uint64(size)
}

// workingSet maps the working set's MemoryFile ranges through the image's extents of layer 0
// to its pages file's offsets, a page at a time.
func workingSet(img, ws *inspected) *wsFrames {
	if ws == nil || ws.WorkingSet == nil || len(img.MemoryFiles) == 0 {
		return nil
	}
	ext := img.MemoryFiles[0].ExtentList
	sort.Slice(ext, func(i, j int) bool { return ext[i][0] < ext[j][0] })
	w := &wsFrames{}
	for _, fr := range ws.WorkingSet.ExtentList {
		for p := fr[0]; p < fr[1]; p += 4096 {
			i := sort.Search(len(ext), func(i int) bool { return ext[i][1] > p })
			if i == len(ext) || ext[i][0] > p || ext[i][2] != 0 {
				continue
			}
			w.bytes += 4096
			w.pages = append(w.pages, ext[i][3]+(p-ext[i][0]))
		}
	}
	return w
}

// referenced returns the bytes of the pages file that layer 0's extents refer to, in pages-file order.
func referenced(f *os.File, img *inspected, limit int) ([]byte, error) {
	var ext [][4]uint64
	for _, mf := range img.MemoryFiles {
		for _, e := range mf.ExtentList {
			if e[2] == 0 {
				ext = append(ext, e)
			}
		}
	}
	sort.Slice(ext, func(i, j int) bool { return ext[i][3] < ext[j][3] })
	var out []byte
	for _, e := range ext {
		n := int(e[1] - e[0])
		if len(out)+n > limit {
			n = limit - len(out)
		}
		buf := make([]byte, n)
		if _, err := f.ReadAt(buf, int64(e[3])); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: codec image|random ...")
		os.Exit(2)
	}
	enc := json.NewEncoder(os.Stdout)
	emit := func(r result) {
		if err := enc.Encode(r); err != nil {
			panic(err)
		}
	}
	switch os.Args[1] {
	case "random":
		fl := flag.NewFlagSet("random", flag.ExitOnError)
		n := fl.Int("bytes", 256<<20, "bytes of random data")
		fl.Parse(os.Args[2:])
		data := make([]byte, *n)
		r := rand.NewChaCha8([32]byte{1})
		r.Read(data)
		measure("random", "", "all", data, nil, emit)
	case "image":
		fl := flag.NewFlagSet("image", flag.ExitOnError)
		pages := fl.String("pages", "", "the image's pages.img")
		insp := fl.String("inspect", "", "runsc image inspect --json --extents of the image")
		wsPath := fl.String("ws", "", "the same of the image saved after a restore of it")
		label := fl.String("label", "", "a label for the lines")
		fl.Parse(os.Args[2:])
		img, err := readInspect(*insp)
		if err != nil {
			panic(err)
		}
		var ws *inspected
		if *wsPath != "" {
			if ws, err = readInspect(*wsPath); err != nil {
				panic(err)
			}
		}
		f, err := os.Open(*pages)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		all := make([]byte, sampleBytes)
		n, err := io.ReadFull(f, all)
		if err != nil && err != io.ErrUnexpectedEOF {
			panic(err)
		}
		all = all[:n]
		w := workingSet(img, ws)
		measure(*label, img.Digest, "all", all, w, emit)
		ref, err := referenced(f, img, sampleBytes)
		if err != nil {
			panic(err)
		}
		measure(*label, img.Digest, "referenced", ref, nil, emit)
	case "seekable":
		fl := flag.NewFlagSet("seekable", flag.ExitOnError)
		in := fl.String("in", "", "the pages file")
		out := fl.String("out", "", "the seekable zstd file to write")
		frame := fl.Int("frame", 256<<10, "logical bytes per frame")
		level := fl.Int("level", 1, "zstd level")
		fl.Parse(os.Args[2:])
		written, logical, took, err := writeSeekable(*in, *out, *frame, *level)
		if err != nil {
			panic(err)
		}
		if err := enc.Encode(map[string]any{"in": *in, "logical_bytes": logical, "physical_bytes": written, "ratio": float64(logical) / float64(written), "seconds": took.Seconds(), "frame_bytes": *frame, "level": *level}); err != nil {
			panic(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// writeSeekable writes the file in as a seekable zstd file out, as the seekable command says;
// it returns the bytes written and read, and the time it took.
func writeSeekable(in, out string, frameBytes, level int) (int64, int64, time.Duration, error) {
	start := time.Now()
	src, err := os.Open(in)
	if err != nil {
		return 0, 0, 0, err
	}
	defer src.Close()
	dst, err := os.Create(out)
	if err != nil {
		return 0, 0, 0, err
	}
	defer dst.Close()
	w := bufio.NewWriterSize(dst, 4<<20)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		return 0, 0, 0, err
	}
	var table []byte
	var written, logical int64
	frames := uint32(0)
	buf := make([]byte, frameBytes)
	var cbuf []byte
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			cbuf = enc.EncodeAll(buf[:n], cbuf[:0])
			if _, err := w.Write(cbuf); err != nil {
				return 0, 0, 0, err
			}
			table = binary.LittleEndian.AppendUint32(table, uint32(len(cbuf)))
			table = binary.LittleEndian.AppendUint32(table, uint32(n))
			written += int64(len(cbuf))
			logical += int64(n)
			frames++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, 0, 0, err
		}
	}
	var tail []byte
	tail = binary.LittleEndian.AppendUint32(tail, 0x184D2A5E)
	tail = binary.LittleEndian.AppendUint32(tail, uint32(len(table)+9))
	tail = append(tail, table...)
	tail = binary.LittleEndian.AppendUint32(tail, frames)
	tail = append(tail, 0)
	tail = binary.LittleEndian.AppendUint32(tail, 0x8F92EAB1)
	if _, err := w.Write(tail); err != nil {
		return 0, 0, 0, err
	}
	if err := w.Flush(); err != nil {
		return 0, 0, 0, err
	}
	return written + int64(len(tail)), logical, time.Since(start), dst.Sync()
}
