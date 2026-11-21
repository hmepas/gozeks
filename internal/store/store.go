// Package store wraps the S3 client with request counters and a small API.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hmepas/gozeks/internal/config"
)

// ErrNotFound is returned by Head/Get for a missing key.
var ErrNotFound = errors.New("not found")

// Counters count requests by billing class.
type Counters struct {
	Put, Get, Delete, BytesUp, BytesDown atomic.Int64
}

// Snapshot is a plain copy of Counters.
type Snapshot struct {
	Put       int64 `json:"put"`
	Get       int64 `json:"get"`
	Delete    int64 `json:"delete"`
	BytesUp   int64 `json:"bytes_up"`
	BytesDown int64 `json:"bytes_down"`
}

// Snapshot returns the current values.
func (c *Counters) Snapshot() Snapshot {
	return Snapshot{c.Put.Load(), c.Get.Load(), c.Delete.Load(), c.BytesUp.Load(), c.BytesDown.Load()}
}

// Add adds a snapshot (used to restore persisted totals).
func (c *Counters) Add(s Snapshot) {
	c.Put.Add(s.Put)
	c.Get.Add(s.Get)
	c.Delete.Add(s.Delete)
	c.BytesUp.Add(s.BytesUp)
	c.BytesDown.Add(s.BytesDown)
}

// Object is one LIST result.
type Object struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

// Store is an S3 bucket.
type Store struct {
	c        *s3.Client
	bucket   string
	Counters Counters
}

// New builds a client for any S3-compatible endpoint.
func New(cfg config.S3) (*Store, error) {
	ep := cfg.Endpoint
	if !strings.Contains(ep, "://") {
		ep = "https://" + ep
	}
	c := s3.New(s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: aws.String(ep),
		UsePathStyle: cfg.PathStyle,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		// Non-AWS endpoints often reject the CRC trailers the SDK adds by default.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           4,
		// Some providers answer 409 OperationAborted ("Try again.") when a request
		// collides with a concurrent write of the same key; not retryable by default.
		Retryer: retry.AddWithErrorCodes(retry.NewStandard(), "OperationAborted"),
	})
	return &Store{c: c, bucket: cfg.Bucket}, nil
}

// Put uploads body with optional user metadata and returns the ETag.
func (s *Store) Put(ctx context.Context, key string, body []byte, meta map[string]string) (string, error) {
	s.Counters.Put.Add(1)
	s.Counters.BytesUp.Add(int64(len(body)))
	out, err := s.c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     bytes.NewReader(body),
		Metadata: meta,
	})
	if err != nil {
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	return clean(out.ETag), nil
}

// Get downloads a whole object.
func (s *Store) Get(ctx context.Context, key string) ([]byte, string, map[string]string, error) {
	var buf bytes.Buffer
	etag, meta, err := s.GetTo(ctx, key, &buf)
	return buf.Bytes(), etag, meta, err
}

// GetTo streams an object into w.
func (s *Store) GetTo(ctx context.Context, key string, w io.Writer) (string, map[string]string, error) {
	s.Counters.Get.Add(1)
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return "", nil, ErrNotFound
		}
		return "", nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	n, err := io.Copy(w, out.Body)
	s.Counters.BytesDown.Add(n)
	if err != nil {
		return "", nil, fmt.Errorf("get %s: %w", key, err)
	}
	return clean(out.ETag), out.Metadata, nil
}

// Head returns the ETag and user metadata.
func (s *Store) Head(ctx context.Context, key string) (string, map[string]string, error) {
	s.Counters.Get.Add(1)
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return "", nil, ErrNotFound
		}
		return "", nil, fmt.Errorf("head %s: %w", key, err)
	}
	return clean(out.ETag), out.Metadata, nil
}

// Delete removes a key; deleting a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	s.Counters.Delete.Add(1)
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// List calls fn for every object under prefix with key > startAfter.
// Each page is one request.
func (s *Store) List(ctx context.Context, prefix, startAfter string, fn func(Object) error) error {
	in := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix}
	if startAfter != "" {
		in.StartAfter = &startAfter
	}
	p := s3.NewListObjectsV2Paginator(s.c, in)
	for p.HasMorePages() {
		s.Counters.Get.Add(1)
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			obj := Object{Key: aws.ToString(o.Key), ETag: clean(o.ETag), Size: aws.ToInt64(o.Size)}
			if o.LastModified != nil {
				obj.LastModified = *o.LastModified
			}
			if err := fn(obj); err != nil {
				return err
			}
		}
	}
	return nil
}

func clean(etag *string) string {
	return strings.Trim(aws.ToString(etag), `"`)
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return true
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return true
	}
	return false
}
