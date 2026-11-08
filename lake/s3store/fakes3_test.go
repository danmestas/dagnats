package s3store

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeS3 is the slice of the S3 API the store uses, in memory: path-style PUT with
// If-None-Match, GET, HEAD and ListObjectsV2 with continuation tokens. It checks that every
// request carries a SigV4 Authorization header for the expected access key, so an unsigned
// request fails the way a real store would.
type fakeS3 struct {
	t         *testing.T
	accessKey string
	mu        sync.Mutex
	objects   map[string][]byte // bucket/key
	failNext  int               // number of 503s to answer before serving
	// ignoreConditional makes PUT overwrite regardless of If-None-Match, like Garage.
	ignoreConditional bool
	puts              int
	heads             int
}

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server) {
	f := &fakeS3{t: t, accessKey: "AKIAFAKE", objects: map[string][]byte{}}
	return f, httptest.NewServer(f)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential="+f.accessKey+"/") ||
		r.Header.Get("x-amz-date") == "" || r.Header.Get("x-amz-content-sha256") == "" {
		http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		http.Error(w, "<Error><Code>SlowDown</Code></Error>", http.StatusServiceUnavailable)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket == "" {
		http.Error(w, "no bucket", http.StatusBadRequest)
		return
	}
	switch {
	case r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2":
		f.list(w, r, bucket)
	case r.Method == http.MethodPut:
		f.put(w, r, bucket+"/"+key)
	case r.Method == http.MethodGet:
		f.get(w, bucket+"/"+key)
	case r.Method == http.MethodDelete:
		delete(f.objects, bucket+"/"+key)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodHead:
		f.heads++
		if _, ok := f.objects[bucket+"/"+key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unsupported", http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, full string) {
	f.puts++
	if r.Header.Get("If-None-Match") != "*" && !f.ignoreConditional {
		f.t.Errorf("PUT %s without If-None-Match: *", full)
	}
	if _, exists := f.objects[full]; exists && !f.ignoreConditional {
		http.Error(w, "<Error><Code>PreconditionFailed</Code></Error>", http.StatusPreconditionFailed)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.ContentLength != int64(len(data)) {
		f.t.Errorf("PUT %s: Content-Length %d, body %d", full, r.ContentLength, len(data))
	}
	f.objects[full] = data
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, full string) {
	data, ok := f.objects[full]
	if !ok {
		http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

type fakeListing struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

// list pages at max-keys, using the last key as the continuation token.
func (f *fakeS3) list(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	after := q.Get("continuation-token")
	// Like S3, start-after applies to the first request; a continuation token supersedes it.
	startAfter := q.Get("start-after")
	if after != "" {
		startAfter = ""
	}
	maxKeys, _ := strconv.Atoi(q.Get("max-keys"))
	if maxKeys < 1 || maxKeys > 1000 {
		maxKeys = 1000
	}
	var keys []string
	for full := range f.objects {
		b, k, _ := strings.Cut(full, "/")
		if b == bucket && strings.HasPrefix(k, prefix) && k > after && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out fakeListing
	for i, k := range keys {
		if i == maxKeys {
			out.IsTruncated = true
			out.NextContinuationToken = keys[i-1]
			break
		}
		out.Contents = append(out.Contents, struct {
			Key string `xml:"Key"`
		}{k})
	}
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(out); err != nil {
		f.t.Error(err)
	}
}
