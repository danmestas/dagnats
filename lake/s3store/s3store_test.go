// Methodology: the store runs the conformance suite (conformance_test.go) twice: against an
// in-memory fake of the S3 API that also asserts every request is signed and every PUT is
// conditional, and, when LAKE_S3_TEST_ENDPOINT is set, against a real S3-compatible server
// (MinIO, Garage or R2; CI sets none, so it skips there).
// Store-specific tests cover retries on 503, the unsigned-request failure, and URL shapes.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/lake/writer"
)

// The lake writer's Store seam; the concrete type keeps List and Delete beyond it.
var _ writer.Store = (*Store)(nil)

func fakeConfig(url string) Config {
	return Config{
		Endpoint: url, Region: "us-east-1", Bucket: "lake-test", Prefix: "lake",
		AccessKey: "AKIAFAKE", SecretKey: "secret", PathStyle: true,
	}
}

func newFakeStore(t *testing.T) *Store {
	t.Helper()
	_, server := newFakeS3(t)
	t.Cleanup(server.Close)
	s, err := New(fakeConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformanceAgainstFake(t *testing.T) {
	runStoreSuite(t, newFakeStore)
}

// TestConformanceAgainstLiveServer needs LAKE_S3_TEST_ENDPOINT, LAKE_S3_TEST_ACCESS_KEY and
// LAKE_S3_TEST_SECRET_KEY; the bucket named by LAKE_S3_TEST_BUCKET must exist. Each subtest
// uses a fresh prefix so runs do not collide.
func TestConformanceAgainstLiveServer(t *testing.T) {
	endpoint := os.Getenv("LAKE_S3_TEST_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("LAKE_S3_TEST_REQUIRED") == "1" {
			t.Fatal("live S3 test required but LAKE_S3_TEST_ENDPOINT is not set")
		}
		t.Skip("LAKE_S3_TEST_ENDPOINT not set")
	}
	counter := 0
	runStoreSuite(t, func(t *testing.T) *Store {
		counter++
		s, err := New(Config{
			Endpoint:  endpoint,
			Region:    envOr("LAKE_S3_TEST_REGION", "us-east-1"),
			Bucket:    envOr("LAKE_S3_TEST_BUCKET", "lake-test"),
			Prefix:    fmt.Sprintf("conformance-%d-%d", os.Getpid(), counter),
			AccessKey: os.Getenv("LAKE_S3_TEST_ACCESS_KEY"),
			SecretKey: os.Getenv("LAKE_S3_TEST_SECRET_KEY"),
			PathStyle: os.Getenv("LAKE_S3_TEST_PATH_STYLE") != "0",
			// Garage ignores If-None-Match; set LAKE_S3_TEST_NO_CONDITIONAL_PUT=1 for it.
			NoConditionalPut: os.Getenv("LAKE_S3_TEST_NO_CONDITIONAL_PUT") == "1",
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// TestNoConditionalPutFallsBackToHead runs the suite against a fake that ignores
// If-None-Match, as Garage does, with the fallback on: write-once must still hold through HEAD.
func TestNoConditionalPutFallsBackToHead(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) *Store {
		fake, server := newFakeS3(t)
		fake.ignoreConditional = true
		t.Cleanup(server.Close)
		cfg := fakeConfig(server.URL)
		cfg.NoConditionalPut = true
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if fake.heads < fake.puts {
				t.Errorf("%d PUTs but only %d HEADs: fallback not checking first", fake.puts, fake.heads)
			}
		})
		return s
	})
}

func TestRetriesTransientFailures(t *testing.T) {
	fake, server := newFakeS3(t)
	defer server.Close()
	s, err := New(fakeConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fake.failNext = 2
	if err := s.Put(ctx, "chunks/aa/a", []byte("x")); err != nil {
		t.Fatalf("Put after two 503s: %v", err)
	}
	fake.failNext = retriesMax + 1
	if _, err := s.Get(ctx, "chunks/aa/a"); err == nil || !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("Get past the retry bound: %v", err)
	}
}

func TestUnsignedOrWrongKeyIsRefused(t *testing.T) {
	_, server := newFakeS3(t)
	defer server.Close()
	cfg := fakeConfig(server.URL)
	cfg.AccessKey = "AKIAOTHER"
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Put(context.Background(), "config", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("wrong access key: %v", err)
	}
	if errors.Is(err, ErrExists) {
		t.Fatal("403 reported as ErrExists")
	}
}

func TestURLShapes(t *testing.T) {
	cases := []struct {
		cfg  Config
		want string
	}{
		{Config{Endpoint: "http://127.0.0.1:3900", Bucket: "bkt", PathStyle: true},
			"http://127.0.0.1:3900/bkt/"},
		{Config{Endpoint: "https://acct.r2.cloudflarestorage.com", Bucket: "bkt"},
			"https://bkt.acct.r2.cloudflarestorage.com/"},
		{Config{Region: "eu-west-1", Bucket: "bkt"}, "https://bkt.s3.eu-west-1.amazonaws.com/"},
	}
	for _, c := range cases {
		if c.cfg.Region == "" {
			c.cfg.Region = "us-east-1"
		}
		u, err := baseURL(c.cfg)
		if err != nil || u.String() != c.want {
			t.Fatalf("baseURL(%+v) = %v, %v; want %s", c.cfg, u, err, c.want)
		}
	}
	for _, bad := range []Config{
		{Bucket: "bkt"},
		{Bucket: "bkt", AccessKey: "a"},
		{Bucket: "Bad_Bucket", AccessKey: "a", SecretKey: "s"},
		{Bucket: "bkt", AccessKey: "a", SecretKey: "s", Endpoint: "ftp://x"},
		{Bucket: "bkt", AccessKey: "a", SecretKey: "s", Prefix: "../up"},
	} {
		if _, err := New(bad); err == nil {
			t.Fatalf("New accepted %+v", bad)
		}
	}
	s, err := New(Config{
		Endpoint: "http://h", Bucket: "bkt", Prefix: "/p/q/", AccessKey: "a",
		SecretKey: "s", PathStyle: true, HTTPClient: &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.objectURL("chunks/ab/c").String(); got != "http://h/bkt/p/q/chunks/ab/c" {
		t.Fatalf("objectURL %q", got)
	}
}
