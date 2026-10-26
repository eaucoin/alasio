package main

// The bucket, through the AWS SDK as E2B's runtime reaches S3 (packages/shared/pkg/storage/
// storage_aws.go): the store the environment names as for every runner system (S3_ENDPOINT,
// S3_BUCKET, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY; bench/common/store/s3.ts),
// path-style addressing, and multipart
// uploads of E2B's 10 MiB parts, 16 at once, through the SDK's transfer manager (which
// supersedes the uploader E2B uses).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	uploadPartSize    = 10 << 20
	uploadConcurrency = 16
)

// objectStore is what fcmem asks of the bucket.
type objectStore interface {
	get(ctx context.Context, key string) ([]byte, error)
	openRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error)
	put(ctx context.Context, key string, data []byte) error
	upload(ctx context.Context, key string, body io.Reader) error
}

type store struct {
	client *s3.Client
	bucket string
}

func newStore(ctx context.Context) (*store, error) {
	endpoint, bucket, region := os.Getenv("S3_ENDPOINT"), os.Getenv("S3_BUCKET"), os.Getenv("S3_REGION")
	if endpoint == "" || bucket == "" {
		return nil, fmt.Errorf("S3_ENDPOINT and S3_BUCKET name the store")
	}
	if region == "" {
		region = "us-east-1"
	}
	// Enough idle connections kept for every fetch and upload at once, so none is dialled again.
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = 64
	})
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithBaseEndpoint(endpoint),
		config.WithRegion(region),
		config.WithHTTPClient(httpClient),
		// The store checks what S3 requires and no more, as SeaweedFS does not take the
		// SDK's default trailing checksums.
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	return &store{client: client, bucket: bucket}, nil
}

func (s *store) get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// openRange reads length bytes of key from off.
func (s *store) openRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	rng := fmt.Sprintf("bytes=%d-%d", off, off+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key, Range: &rng})
	if err != nil {
		return nil, fmt.Errorf("get %s %s: %w", key, rng, err)
	}
	return out.Body, nil
}

func (s *store) put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// upload streams body into key, in parts uploaded concurrently as the body is read.
func (s *store) upload(ctx context.Context, key string, body io.Reader) error {
	uploader := transfermanager.New(s.client, func(o *transfermanager.Options) {
		o.PartSizeBytes = uploadPartSize
		o.Concurrency = uploadConcurrency
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	if _, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: &s.bucket, Key: &key, Body: body}); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}
