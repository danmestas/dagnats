// event_bus_sse_test.go covers the integration between the in-process
// event bus and the DLQ SSE stream.
//
// Methodology:
//   - httptest.Server hosts the console handler.
//   - A second goroutine reads the SSE stream until it sees the
//     remove patch for the configured row id.
//   - Each test creates its own console.Mount; nothing is shared.
//   - Minimum 2 assertions per test.
package console

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

func TestSSEDLQ_busRemoveEventPatchesRowOut(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{{
		DeadLetter: api.DeadLetter{
			Sequence: 73, Subject: "dead.task.x",
			RunID: "r1", Error: "boom",
		},
	}}
	cfg := makeBusEnabledConfig(t, fake)
	srv := httptest.NewServer(Mount(cfg))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, srv.URL+"/console/sse/dlq", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sse: %v", err)
	}
	defer resp.Body.Close()

	// Publish a row.remove event for seq 73.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cfg.bus.publish(busEventDLQRemove("73"))
	}()
	// Read SSE until we see the remove patch (selector "#dlq-row-73"
	// + mode=remove) or context times out.
	saw := waitForSSEFragment(t, resp.Body, "dlq-row-73", "remove",
		2500*time.Millisecond)
	if !saw {
		t.Fatalf("did not observe DLQ row-remove patch within deadline")
	}
}

// makeBusEnabledConfig returns a Config wired to the fake DS with
// the event bus attached and basic auth + read-only disabled.
func makeBusEnabledConfig(
	t *testing.T, fake *fakeDataSource,
) Config {
	t.Helper()
	cfg := Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	}
	AttachBus(&cfg)
	return cfg
}

// waitForSSEFragment scans the SSE body until it sees both wantSelector
// and wantMode (both as substrings) within deadline.
func waitForSSEFragment(
	t *testing.T, body io.Reader,
	wantSelector, wantMode string,
	deadline time.Duration,
) bool {
	t.Helper()
	done := time.After(deadline)
	buf := make([]byte, 4096)
	var seen string
	for {
		select {
		case <-done:
			t.Logf("SSE accumulated body:\n%s", seen)
			return false
		default:
		}
		// Set per-read deadline by reading in chunks; if no data
		// arrives in 200ms we re-check the outer deadline.
		n, err := body.Read(buf)
		if n > 0 {
			seen += string(buf[:n])
			if strings.Contains(seen, wantSelector) &&
				strings.Contains(seen, wantMode) {
				return true
			}
		}
		if err != nil {
			return false
		}
	}
}
