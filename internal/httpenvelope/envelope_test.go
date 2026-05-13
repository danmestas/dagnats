// httpenvelope/envelope_test.go
//
// Methodology: pure unit tests with httptest-built requests. Each test
// covers one envelope field: method, path, query, headers, body. Body
// path delegates to BoundedBody so we keep coverage focused on the lift
// rather than re-testing the bound. Two assertions per test.
package httpenvelope

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildEnvelopeLiftsMethodPathQuery(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet, "/orders?status=open&limit=5", nil,
	)

	env, err := BuildEnvelope(req, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.Method != http.MethodGet {
		t.Fatalf("Method = %q, want GET", env.Method)
	}
	if env.Path != "/orders" {
		t.Fatalf("Path = %q, want /orders", env.Path)
	}
	if env.Query["status"] != "open" || env.Query["limit"] != "5" {
		t.Fatalf("Query = %v, want status=open limit=5", env.Query)
	}
}

func TestBuildEnvelopePreservesHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api", nil)
	req.Header.Set("X-Custom", "abc")
	req.Header.Set("Authorization", "Bearer xyz")

	env, err := BuildEnvelope(req, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.Headers["X-Custom"] != "abc" {
		t.Fatalf("X-Custom = %q, want abc", env.Headers["X-Custom"])
	}
	// Q1 resolution: headers (incl. Authorization) NOT stripped.
	if env.Headers["Authorization"] != "Bearer xyz" {
		t.Fatalf(
			"Authorization = %q, want Bearer xyz",
			env.Headers["Authorization"],
		)
	}
}

func TestBuildEnvelopeBodyBounded(t *testing.T) {
	big := bytes.Repeat([]byte("x"), 200)
	req := httptest.NewRequest(
		http.MethodPost, "/api", bytes.NewReader(big),
	)

	_, err := BuildEnvelope(req, 100)
	if err == nil {
		t.Fatal("expected ErrBodyTooLarge, got nil")
	}
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("err = %v, want ErrBodyTooLarge", err)
	}
}

func TestBuildEnvelopeWithBody(t *testing.T) {
	payload := []byte(`{"hello":"world"}`)
	req := httptest.NewRequest(
		http.MethodPost, "/api", bytes.NewReader(payload),
	)

	env, err := BuildEnvelope(req, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(env.Body, payload) {
		t.Fatalf("Body = %q, want %q", env.Body, payload)
	}
	if env.Method != http.MethodPost {
		t.Fatalf("Method = %q, want POST", env.Method)
	}
}

func TestBuildEnvelopeEmptyBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api", nil)

	env, err := BuildEnvelope(req, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(env.Body) != 0 {
		t.Fatalf("Body len = %d, want 0", len(env.Body))
	}
	if env.Method != http.MethodPost {
		t.Fatalf("Method = %q, want POST", env.Method)
	}
}

func TestBuildEnvelopeJSONRoundTrip(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost, "/api?k=v",
		strings.NewReader(`{"a":1}`),
	)
	req.Header.Set("X-Trace-Id", "trace-1")

	env, err := BuildEnvelope(req, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Headers["X-Trace-Id"] != "trace-1" {
		t.Fatalf("header lost in round-trip: %v", got.Headers)
	}
	if got.Query["k"] != "v" {
		t.Fatalf("query lost in round-trip: %v", got.Query)
	}
}

func TestBuildEnvelopeNilRequestPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil request")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recover() = %T, want string", r)
		}
		if !strings.Contains(msg, "request") {
			t.Fatalf("panic %q must mention request", msg)
		}
	}()
	_, _ = BuildEnvelope(nil, 1024)
}
