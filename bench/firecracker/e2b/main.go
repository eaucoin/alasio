// e2b-serve runs E2B's own restore path against a snapshot fcmem saved, as the reference the
// benchmark's handler is measured beside: E2B's userfaultfd handler (pkg/sandbox/uffd), its
// prefetcher (pkg/sandbox/uffd/prefetch), and the memory file it serves from, its build File
// over its diff store and chunker reading the bucket through its storage provider
// (pkg/sandbox/build, pkg/sandbox/block, packages/shared/pkg/storage), all unmodified, wired as
// the orchestrator's own test utilities wire a build device (pkg/sandbox/nbd/testutils
// TemplateRootfs). run.sh builds it inside E2B's repository at the commit it pins, as
// packages/orchestrator/cmd/bench-e2b-serve, so it builds with E2B's go.mod as E2B does.
//
//	e2b-serve export --prefix P --snapshot S
//	    writes the snapshot in E2B's layout beside fcmem's: each memory object copied to
//	    <build>/memfile, a build ID named after its key, and <build>/memfile.header, E2B's
//	    header of the same mapping, zero blocks mapped to no build
//	e2b-serve serve --prefix P --snapshot S --socket F --state-out F --cache D --stats F [--prefetch] [--warm]
//	    serves one restore; with --warm, every block is read into E2B's cache first, as an
//	    orchestrator holds a snapshot it took in its template cache
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/prefetch"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// fcmem's header (bench/firecracker/fcmem/header.go), as far as it is read here.
type fcmemHeader struct {
	MemSize   int64 `json:"mem_size"`
	BlockSize int64 `json:"block_size"`
	Objects   []struct {
		Key  string `json:"key"`
		Size int64  `json:"size"`
	} `json:"objects"`
	Blocks []struct {
		Object int   `json:"o"`
		Offset int64 `json:"off"`
	} `json:"blocks"`
	Hot []int `json:"hot"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: e2b-serve export|serve [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "export":
		err = export(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		err = fmt.Errorf("no command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2b-serve:", err)
		os.Exit(1)
	}
}

// buildOf names a build after the fcmem object holding its data.
func buildOf(key string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key))
}

func s3Client(ctx context.Context) (*s3.Client, string, error) {
	region := os.Getenv("S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	conf, err := config.LoadDefaultConfig(ctx,
		config.WithBaseEndpoint(os.Getenv("S3_ENDPOINT")),
		config.WithRegion(region),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		return nil, "", err
	}
	return s3.NewFromConfig(conf, func(o *s3.Options) { o.UsePathStyle = true }), os.Getenv("S3_BUCKET"), nil
}

func readHeader(ctx context.Context, client *s3.Client, bucket, prefix, snapshot string) (*fcmemHeader, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String(prefix + "/" + snapshot + "/header.json")})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	var h fcmemHeader
	return &h, json.NewDecoder(out.Body).Decode(&h)
}

func export(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix of the snapshots")
	snapshot := fs.String("snapshot", "", "the snapshot exported")
	fs.Parse(args)

	ctx := context.Background()
	client, bucket, err := s3Client(ctx)
	if err != nil {
		return err
	}
	h, err := readHeader(ctx, client, bucket, *prefix, *snapshot)
	if err != nil {
		return err
	}
	for _, obj := range h.Objects {
		if obj.Size == 0 {
			continue
		}
		dst := storage.Paths{BuildID: buildOf(obj.Key).String()}.Memfile()
		if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &bucket, Key: &dst, CopySource: aws.String(bucket + "/" + obj.Key)}); err != nil {
			return fmt.Errorf("copy %s to %s: %w", obj.Key, dst, err)
		}
	}
	// The mapping, a run of blocks per entry, zeros mapped to no build.
	var mappings []header.BuildMap
	for i, b := range h.Blocks {
		m := header.BuildMap{Offset: uint64(int64(i) * h.BlockSize), Length: uint64(h.BlockSize), BuildId: uuid.Nil}
		if b.Object >= 0 {
			m.BuildId, m.BuildStorageOffset = buildOf(h.Objects[b.Object].Key), uint64(b.Offset)
		}
		if n := len(mappings); n > 0 {
			last := &mappings[n-1]
			if last.BuildId == m.BuildId && (m.BuildId == uuid.Nil || last.BuildStorageOffset+last.Length == m.BuildStorageOffset) {
				last.Length += m.Length
				continue
			}
		}
		mappings = append(mappings, m)
	}
	self := buildOf(h.Objects[0].Key)
	eh, err := header.NewHeader(header.NewTemplateMetadata(self, uint64(h.BlockSize), uint64(h.MemSize)), mappings)
	if err != nil {
		return err
	}
	data, err := header.SerializeHeader(eh)
	if err != nil {
		return err
	}
	key := storage.Paths{BuildID: self.String()}.MemfileHeader()
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"build": self.String(), "mappings": len(mappings)})
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix of the snapshots")
	snapshot := fs.String("snapshot", "", "the snapshot restored")
	socket := fs.String("socket", "", "the socket Firecracker connects to")
	stateOut := fs.String("state-out", "", "where the state file is written for Firecracker")
	cacheDir := fs.String("cache", "", "E2B's diff cache")
	statsOut := fs.String("stats", "", "where the handler's counters are written as it ends")
	withPrefetch := fs.Bool("prefetch", false, "prefetch the hot set")
	warm := fs.Bool("warm", false, "read every block into the cache first")
	fs.Parse(args)

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, bucket, err := s3Client(ctx)
	if err != nil {
		return err
	}
	fh, err := readHeader(ctx, client, bucket, *prefix, *snapshot)
	if err != nil {
		return err
	}
	buildID := buildOf(fh.Objects[0].Key).String()
	state, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: aws.String(*prefix + "/" + *snapshot + "/state")})
	if err != nil {
		return err
	}
	defer state.Body.Close()
	stateFile, err := os.Create(*stateOut)
	if err != nil {
		return err
	}
	if _, err := stateFile.ReadFrom(state.Body); err != nil {
		stateFile.Close()
		return err
	}
	if err := stateFile.Close(); err != nil {
		return err
	}

	// As TemplateRootfs builds its device, for the memory file.
	persistence, err := storage.NewProvider(ctx, storage.Spec{
		Provider: storage.AWSStorageProvider, Bucket: bucket, Endpoint: os.Getenv("S3_ENDPOINT"), UsePathStyle: true, Region: os.Getenv("S3_REGION"),
	})
	if err != nil {
		return err
	}
	h, _, err := header.LoadHeader(ctx, persistence, storage.Paths{BuildID: buildID}.MemfileHeader())
	if err != nil {
		return err
	}
	flags, err := featureflags.NewClient("", "", "")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*cacheDir, 0o755); err != nil {
		return err
	}
	store, err := build.NewDiffStore(cfg.Config{}, flags, *cacheDir, 24*time.Hour, 24*time.Hour)
	if err != nil {
		return err
	}
	store.Start(ctx)
	defer store.Close()
	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	if err != nil {
		return err
	}
	device := testutils.NewBuildDevice(build.NewFile(h, store, build.Memfile, persistence, m), h, int64(h.Metadata.BlockSize))

	if *warm {
		buf := make([]byte, h.Metadata.BlockSize)
		for i, b := range fh.Blocks {
			if b.Object < 0 {
				continue
			}
			if _, err := device.ReadAt(ctx, buf, int64(i)*int64(h.Metadata.BlockSize)); err != nil {
				return fmt.Errorf("warm block %d: %w", i, err)
			}
		}
	}

	lg := logger.NewNopLogger()
	backend := uffd.New(device, *socket, lg)
	if *withPrefetch {
		// The orchestrator starts its prefetch before Firecracker loads the snapshot.
		indices := make([]uint64, len(fh.Hot))
		accessTypes := make([]metadata.AccessType, len(fh.Hot))
		for i, b := range fh.Hot {
			indices[i] = uint64(b)
			accessTypes[i] = metadata.AccessRead
		}
		mapping := &metadata.MemoryPrefetchMapping{Indices: indices, AccessTypes: accessTypes, BlockSize: int64(h.Metadata.BlockSize)}
		// The hot set is the last save's diff, which E2B replays fetch-only (pkg/sandbox/sandbox.go).
		prefetcher := prefetch.New(lg, device, backend, mapping, flags)
		prefetcher.Prefault = false
		go prefetcher.Start(ctx)
	}
	if err := backend.Start(ctx); err != nil {
		return err
	}
	readyMs := time.Since(start).Milliseconds()
	fmt.Println("ready")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	serveStats := backend.ServeStats()
	if err := backend.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "e2b-serve: stop:", err)
	}
	var usage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	stats := map[string]any{"ready_ms": readyMs, "serve": serveStats, "max_rss_bytes": usage.Maxrss * 1024, "build": buildID}
	encoded, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Clean(*statsOut), encoded, 0o644)
}
