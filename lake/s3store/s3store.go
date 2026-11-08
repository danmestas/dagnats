// Package s3store is an object store for the lake on S3-compatible storage: Cloudflare R2,
// Garage, Tigris, MinIO and S3 itself. It is the lake writer's Store (writer.Store) over a
// bucket, and also lists and deletes, which other lake tools need. Put is write-once through a
// conditional PUT (If-None-Match: *), which those stores honour atomically, so two writers of
// one name leave exactly one object and no HEAD-then-PUT race. Errors match fs.ErrExist and
// fs.ErrNotExist through errors.Is. The client is net/http plus SigV4; there is no SDK.
package s3store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrExists is returned by Put when the name is taken. It matches fs.ErrExist.
var ErrExists = fmt.Errorf("object exists: %w", fs.ErrExist)

// ErrNotFound is returned by Get when the name is absent. It matches fs.ErrNotExist.
var ErrNotFound = fmt.Errorf("object not found: %w", fs.ErrNotExist)

// ObjectSizeMaxDefault is the object ceiling when Config.ObjectSizeMax is zero. It is lode's
// bound for a sealed snapshot chunk.
const ObjectSizeMaxDefault = 128 << 20

// Config names a bucket and how to reach it. Endpoint is the service URL, for example
// https://<account>.r2.cloudflarestorage.com or http://127.0.0.1:3900 for Garage; empty means
// AWS S3 in Region. PathStyle puts the bucket in the path (Garage, MinIO) rather than the host.
type Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	PathStyle bool
	// NoConditionalPut is for stores that ignore If-None-Match on PUT (Garage through v2.1
	// does). Put then checks with HEAD first, which is write-once in practice but not under
	// two writers racing on one name. For content-addressed objects the loser's bytes equal
	// the winner's, so the race is harmless there; Init and prune locks are where it matters.
	NoConditionalPut bool
	// ObjectSizeMax bounds one object, on Put and on Get, which holds the whole object in
	// memory. Zero means ObjectSizeMaxDefault (128 MiB). The lake writer's parts stay under
	// writer.PartBytesMax (64 MiB) and its commit records are small, so the default leaves room;
	// a value below PartBytesMax makes Put fail on a large part.
	ObjectSizeMax int
	// HTTPClient defaults to one with a bounded timeout.
	HTTPClient *http.Client
	// now is overridable for signing tests.
	now func() time.Time
}

// Store is a bucket prefix. Create it with New.
type Store struct {
	cfg     Config
	sizeMax int
	base    *url.URL
	client  *http.Client
	creds   credentials
	now     func() time.Time
	retries int
}

// Bounds: one listing page is S3's maximum; pages per List are derived from the caller's limit.
const (
	listPageMax = 1000
	// listFromLimitMax bounds one ListFrom call, so a "next N" never pages without end.
	listFromLimitMax = 10 * listPageMax
	requestTimeout   = 60 * time.Second
	retriesMax       = 3
)

// New validates cfg and returns a Store. It makes no request; the first operation does.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("s3store: bucket, access key and secret key are required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if !validBucketName(cfg.Bucket) {
		return nil, fmt.Errorf("s3store: bad bucket name %q", cfg.Bucket)
	}
	if cfg.ObjectSizeMax < 0 {
		return nil, fmt.Errorf("s3store: negative ObjectSizeMax %d", cfg.ObjectSizeMax)
	}
	if cfg.ObjectSizeMax == 0 {
		cfg.ObjectSizeMax = ObjectSizeMaxDefault
	}
	prefix := strings.Trim(cfg.Prefix, "/")
	if prefix != "" {
		if err := validName(prefix); err != nil {
			return nil, err
		}
		prefix += "/"
	}
	cfg.Prefix = prefix
	base, err := baseURL(cfg)
	if err != nil {
		return nil, err
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	return &Store{
		cfg:     cfg,
		sizeMax: cfg.ObjectSizeMax,
		base:    base,
		client:  client,
		creds:   credentials{accessKey: cfg.AccessKey, secretKey: cfg.SecretKey, region: cfg.Region},
		now:     now,
		retries: retriesMax,
	}, nil
}

// baseURL is the bucket root: https://bucket.host/ or https://host/bucket/ in path style.
func baseURL(cfg Config) (*url.URL, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + cfg.Region + ".amazonaws.com"
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("s3store: bad endpoint %q", cfg.Endpoint)
	}
	if cfg.PathStyle {
		u.Path = "/" + cfg.Bucket + "/"
	} else {
		u.Host = cfg.Bucket + "." + u.Host
		u.Path = "/"
	}
	u.RawQuery = ""
	return u, nil
}

// validName is the name rule: relative, slash-separated, no empty or dot segments.
func validName(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return fmt.Errorf("s3store: bad object name %q", name)
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("s3store: bad object name %q", name)
		}
	}
	return nil
}

func validBucketName(b string) bool {
	if len(b) < 3 || len(b) > 63 {
		return false
	}
	for _, c := range b {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

func (s *Store) objectURL(name string) *url.URL {
	u := *s.base
	u.Path = s.base.Path + s.cfg.Prefix + name
	return &u
}

// do signs and sends one request, retrying transient failures a bounded number of times. The
// caller owns the response body.
func (s *Store) do(ctx context.Context, method string, u *url.URL, body []byte,
	headers http.Header,
) (*http.Response, error) {
	payloadHash := emptyPayload
	if body != nil {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}
	var lastErr error
	for attempt := 0; attempt <= s.retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("s3store: %w", err)
		}
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		if body != nil {
			req.ContentLength = int64(len(body))
		}
		sign(req, s.creds, payloadHash, s.now())
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode == http.StatusInternalServerError ||
			resp.StatusCode == http.StatusServiceUnavailable {
			lastErr = fmt.Errorf("s3store: %s %s: %s", method, u.Path, resp.Status)
			drain(resp)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("s3store: after %d attempts: %w", s.retries+1, lastErr)
}

func drain(resp *http.Response) {
	// Reading the rest lets the connection be reused; the content is not needed.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
}

// unexpected turns a non-2xx response into an error carrying the store's message.
func unexpected(method string, u *url.URL, resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("s3store: %s %s: %s: %s", method, u.Path, resp.Status,
		strings.TrimSpace(string(msg)))
}

// Put writes data under name once; an existing object yields ErrExists.
func (s *Store) Put(ctx context.Context, name string, data []byte) error {
	if err := validName(name); err != nil {
		return err
	}
	if len(data) > s.sizeMax {
		return fmt.Errorf("s3store: object %q is %d bytes, over %d", name, len(data),
			s.sizeMax)
	}
	if data == nil {
		data = []byte{}
	}
	u := s.objectURL(name)
	headers := http.Header{"If-None-Match": {"*"}, "Content-Type": {"application/octet-stream"}}
	if s.cfg.NoConditionalPut {
		has, err := s.Has(ctx, name)
		if err != nil {
			return err
		}
		if has {
			return ErrExists
		}
		headers.Del("If-None-Match")
	}
	resp, err := s.do(ctx, http.MethodPut, u, data, headers)
	if err != nil {
		return err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusPreconditionFailed, http.StatusConflict:
		// 412 is the conditional PUT refusing; some stores answer 409 under concurrent writers.
		return ErrExists
	default:
		return unexpected(http.MethodPut, u, resp)
	}
}

// Get returns the object under name, or ErrNotFound.
func (s *Store) Get(ctx context.Context, name string) ([]byte, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	u := s.objectURL(name)
	resp, err := s.do(ctx, http.MethodGet, u, nil, nil)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(io.LimitReader(resp.Body, int64(s.sizeMax)+1))
		if err != nil {
			return nil, fmt.Errorf("s3store: read %s: %w", name, err)
		}
		if len(data) > s.sizeMax {
			return nil, fmt.Errorf("s3store: object %q over %d bytes", name, s.sizeMax)
		}
		return data, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, unexpected(http.MethodGet, u, resp)
	}
}

// Has reports whether an object exists under name, with a HEAD.
func (s *Store) Has(ctx context.Context, name string) (bool, error) {
	if err := validName(name); err != nil {
		return false, err
	}
	u := s.objectURL(name)
	resp, err := s.do(ctx, http.MethodHead, u, nil, nil)
	if err != nil {
		return false, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, unexpected(http.MethodHead, u, resp)
	}
}

// Delete removes the object under name. S3 answers 204 whether or not it existed.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	u := s.objectURL(name)
	resp, err := s.do(ctx, http.MethodDelete, u, nil, nil)
	if err != nil {
		return err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return unexpected(http.MethodDelete, u, resp)
	}
}

type listResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

// List returns every object name under prefix via ListObjectsV2, following continuation
// tokens internally. More than limit names is an error, never a silent truncation.
func (s *Store) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("s3store: list limit %d", limit)
	}
	if prefix != "" {
		if err := validName(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, err
		}
	}
	var names []string
	token := ""
	pagesMax := limit/listPageMax + 2
	for page := 0; page < pagesMax; page++ {
		result, err := s.listPage(ctx, s.cfg.Prefix+prefix, "", token, listPageMax)
		if err != nil {
			return nil, err
		}
		for _, c := range result.Contents {
			if !strings.HasPrefix(c.Key, s.cfg.Prefix) {
				return nil, fmt.Errorf("s3store: listed key %q outside prefix %q", c.Key, s.cfg.Prefix)
			}
			names = append(names, strings.TrimPrefix(c.Key, s.cfg.Prefix))
			if len(names) > limit {
				return nil, fmt.Errorf("s3store: more than %d objects under %q", limit, prefix)
			}
		}
		if !result.IsTruncated {
			return names, nil
		}
		if result.NextContinuationToken == "" {
			return nil, errors.New("s3store: truncated listing without a continuation token")
		}
		token = result.NextContinuationToken
	}
	return nil, fmt.Errorf("s3store: more than %d objects under %q", limit, prefix)
}

// ListFrom returns up to limit object names under prefix that sort after startAfter, in key
// order, and whether more exist beyond them. It is for a reader walking an ever-growing prefix:
// ask for the first keys after the last one seen, and resume from the last name returned while
// more is true. Truncation is explicit in more, never silent. startAfter is a name relative to
// the store's prefix and need not exist ("" starts at the beginning); limit is at most
// listFromLimitMax. List is unchanged: it errors, rather than truncates, past its
// limit.
func (s *Store) ListFrom(
	ctx context.Context, prefix, startAfter string, limit int,
) (names []string, more bool, err error) {
	if limit < 1 || limit > listFromLimitMax {
		return nil, false, fmt.Errorf("s3store: list limit %d: want 1..%d", limit,
			listFromLimitMax)
	}
	if prefix != "" {
		if err := validName(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, false, err
		}
	}
	if startAfter != "" {
		if err := validName(startAfter); err != nil {
			return nil, false, err
		}
	}
	token := ""
	// One extra key tells "exactly limit" from "more"; each page is capped at what is still due.
	for page := 0; page < limit/listPageMax+2; page++ {
		want := min(listPageMax, limit+1-len(names))
		result, err := s.listPage(ctx, s.cfg.Prefix+prefix, s.cfg.Prefix+startAfter, token, want)
		if err != nil {
			return nil, false, err
		}
		for _, c := range result.Contents {
			if !strings.HasPrefix(c.Key, s.cfg.Prefix) {
				return nil, false, fmt.Errorf("s3store: listed key %q outside prefix %q",
					c.Key, s.cfg.Prefix)
			}
			names = append(names, strings.TrimPrefix(c.Key, s.cfg.Prefix))
			if len(names) > limit {
				return names[:limit], true, nil
			}
		}
		if !result.IsTruncated {
			return names, false, nil
		}
		if result.NextContinuationToken == "" {
			return nil, false, errors.New("s3store: truncated listing without a continuation token")
		}
		token = result.NextContinuationToken
	}
	return nil, false, fmt.Errorf("s3store: listing after %q did not converge", startAfter)
}

func (s *Store) listPage(
	ctx context.Context, fullPrefix, startAfter, token string, maxKeys int,
) (listResult, error) {
	u := *s.base
	q := url.Values{"list-type": {"2"}, "max-keys": {strconv.Itoa(maxKeys)}}
	if fullPrefix != "" {
		q.Set("prefix", fullPrefix)
	}
	if token != "" {
		q.Set("continuation-token", token)
	} else if startAfter != "" {
		q.Set("start-after", startAfter)
	}
	u.RawQuery = q.Encode()
	resp, err := s.do(ctx, http.MethodGet, &u, nil, nil)
	if err != nil {
		return listResult{}, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return listResult{}, unexpected("LIST", &u, resp)
	}
	var result listResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result); err != nil {
		return listResult{}, fmt.Errorf("s3store: decode listing: %w", err)
	}
	return result, nil
}
