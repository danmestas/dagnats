// auth_test.go
// Tests for bearer token authentication middleware.
// Methodology: real NATS, httptest, verify 401 on bad token.
package bridge

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/natsutil"
)

func TestAuthRejectsWithoutToken(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	b.token = "secret-token"
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// No Authorization header
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(`{"task_types":["echo"]}`),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthAcceptsCorrectToken(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	b.token = "secret-token"
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// With correct Authorization header
	body := `{"task_types":["echo"],"timeout_ms":100}`
	req, err := http.NewRequest(
		"POST", ts.URL+"/v1/tasks/poll",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthAllowsWhenNoTokenConfigured(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	b := NewBridge(nc)
	// token is empty — dev mode
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	body := `{"task_types":["echo"],"timeout_ms":100}`
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
