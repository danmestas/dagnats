// Tests for the read lock and the s3:// data dir.
// Methodology: real DuckDB over real Parquet files; the S3 case runs
// against a minimal in-process S3 endpoint (ListObjectsV2, HEAD and ranged
// GET) so the httpfs path is exercised end to end without a cloud bucket.
package main

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Queries read the configured data dir and nothing else: files outside
// it, a ".." escape, another database and writes are all refused, and
// the lock cannot be lifted from a query.
func TestOpenDB_LocksReadsToTheDataDir(t *testing.T) {
	dir := t.TempDir()
	createTestTracesMultiple(t, dir, 5)
	outside := filepath.Join(t.TempDir(), "secret.csv")
	if err := os.WriteFile(outside, []byte("x\n1\n"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	db, err := OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, "SELECT * FROM traces", 100)
	if err != nil || len(rows) != 5 {
		t.Fatalf("view read: %d rows, err %v; want 5", len(rows), err)
	}
	for _, query := range []string{
		"SELECT * FROM read_csv('" + outside + "')",
		"SELECT * FROM read_parquet('" + dir + "/../**/*.parquet')",
		"SELECT * FROM read_text('/etc/hosts')",
		"COPY (SELECT 1) TO '" + filepath.Join(dir, "written.csv") + "'",
		"SET enable_external_access = true",
		"ATTACH '" + filepath.Join(t.TempDir(), "other.db") + "'",
	} {
		if _, err := db.Query(ctx, query, 10); err == nil {
			t.Errorf("%q was allowed", query)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "written.csv")); err == nil {
		t.Error("a query wrote a file into the data dir")
	}
}

func TestOpenDB_ReadsAnS3DataDir(t *testing.T) {
	local := t.TempDir()
	createTestTracesMultiple(t, local, 7)
	part, err := os.ReadFile(filepath.Join(local, "traces", "test.parquet"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	store := fakeS3{
		"bucket": {
			"telemetry/traces/dt=2026-10-30/part-1.parquet": part,
			"elsewhere/private.parquet":                     part,
		},
	}
	db, err := OpenDB("s3://bucket/telemetry", WithS3Endpoint(newFakeS3Server(t, store)))
	if err != nil {
		t.Fatalf("OpenDB over s3: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, "SELECT TraceId, dt FROM traces", 100)
	if err != nil || len(rows) != 7 {
		t.Fatalf("s3 view read: %d rows, err %v; want 7", len(rows), err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM logs", 10); err != nil {
		t.Errorf("a signal with no objects should be an empty view: %v", err)
	}
	escape := "SELECT * FROM read_parquet('s3://bucket/elsewhere/*.parquet')"
	if _, err := db.Query(ctx, escape, 10); err == nil {
		t.Error("a query read outside the configured s3 prefix")
	}
}

func TestParseS3Endpoint(t *testing.T) {
	cases := []struct {
		in, host string
		ssl      bool
	}{
		{"http://127.0.0.1:3900", "127.0.0.1:3900", false},
		{"https://acct.r2.cloudflarestorage.com", "acct.r2.cloudflarestorage.com", true},
		{"acct.r2.cloudflarestorage.com", "acct.r2.cloudflarestorage.com", true},
		{"https://s3.example.com/", "s3.example.com", true},
	}
	for _, c := range cases {
		host, ssl := parseS3Endpoint(c.in)
		if host != c.host || ssl != c.ssl {
			t.Errorf("parseS3Endpoint(%q) = %q, %v; want %q, %v", c.in, host, ssl, c.host, c.ssl)
		}
	}
}

// newFakeS3Server serves store for the test and sets the credentials an endpoint needs; it
// returns the endpoint URL.
func newFakeS3Server(t *testing.T, store fakeS3) string {
	t.Helper()
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_REGION", "auto")
	return server.URL
}

// fakeS3 serves path-style buckets from memory: ListObjectsV2, HEAD and
// GET with a single byte range, which is all httpfs uses to glob and
// read Parquet.
type fakeS3 map[string]map[string][]byte

func (s fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	objects, ok := s[bucket]
	if !ok {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}
	if key == "" && r.URL.Query().Get("list-type") == "2" {
		s.list(w, objects, r.URL.Query().Get("prefix"))
		return
	}
	body, ok := objects[key]
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", `"fixture"`)
	w.Header().Set("Last-Modified", "Thu, 29 Oct 2026 00:00:00 GMT")
	http.ServeContent(w, r, key, time.Time{}, strings.NewReader(string(body)))
}

func (fakeS3) list(w http.ResponseWriter, objects map[string][]byte, prefix string) {
	type content struct {
		Key  string `xml:"Key"`
		Size int    `xml:"Size"`
	}
	result := struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Contents    []content `xml:"Contents"`
		IsTruncated bool      `xml:"IsTruncated"`
	}{}
	for key, body := range objects {
		if strings.HasPrefix(key, prefix) {
			result.Contents = append(result.Contents, content{Key: key, Size: len(body)})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
