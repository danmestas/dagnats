// dlq_softdiscard_test.go covers the soft-discard-with-undo flow.
//
// Methodology:
//   - The console Mount is exercised end-to-end through httptest so
//     the middleware + dispatcher + tombstone store are all in play.
//   - A controllable on-expire callback records permanent removals so
//     tests can assert "sweep ran" without standing up time.Sleep.
//   - Each test creates its own console.Mount; nothing is shared.
//   - Minimum 2 assertions per test: state-machine outcome + boundary
//     (token mismatch, window closed, etc.).
package console

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/console/events"
)

// softDiscardHarness builds a console handler with the soft-discard
// flow enabled. The harness exposes the underlying fake + the
// permanent-removal log so tests can assert sweep behaviour.
type softDiscardHarness struct {
	fake    *fakeDataSource
	handler http.Handler
	cfg     *Config
	expired []uint64
	mu      sync.Mutex
}

func (h *softDiscardHarness) record(seq uint64) {
	h.mu.Lock()
	h.expired = append(h.expired, seq)
	h.mu.Unlock()
}

func (h *softDiscardHarness) expiredCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.expired)
}

func newSoftDiscardHarness(t *testing.T) *softDiscardHarness {
	t.Helper()
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{{
		DeadLetter: api.DeadLetter{
			Sequence: 501, Subject: "dead.task.x", RunID: "r1",
			Error: "boom",
		},
	}}
	cfg := Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	}
	h := &softDiscardHarness{fake: fake, cfg: &cfg}
	EnableSoftDiscard(&cfg, 200*time.Millisecond, h.record)
	h.handler = Mount(cfg)
	return h
}

func TestSoftDiscard_returnsTokenAndKeepsEntry(t *testing.T) {
	h := newSoftDiscardHarness(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/discard", nil)
	h.handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"undoToken"`) {
		t.Fatalf("response missing undoToken: %s", body)
	}
	if !strings.Contains(body, `"undoHref":"/console/dlq/501/undo-discard"`) {
		t.Fatalf("response missing undoHref: %s", body)
	}
	// JetStream entry must NOT be removed yet; soft-discard defers
	// permanent removal until the sweeper runs.
	if len(h.fake.discardCalls) != 0 {
		t.Fatalf("DiscardDeadLetter called %d times; want 0 during soft window",
			len(h.fake.discardCalls))
	}
}

func TestSoftDiscard_undoWithCorrectTokenRestores(t *testing.T) {
	h := newSoftDiscardHarness(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/discard", nil)
	h.handler.ServeHTTP(rr, req)
	token := extractToken(t, rr.Body.String())
	// Now undo within window.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/undo-discard", nil)
	req2.Header.Set("X-Undo-Token", token)
	h.handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("undo status = %d, want 200; body=%s",
			rr2.Code, rr2.Body.String())
	}
	if !strings.Contains(rr2.Body.String(), "Discard cancelled") {
		t.Fatalf("undo body missing cancellation toast: %s", rr2.Body.String())
	}
}

func TestSoftDiscard_undoWithBadTokenReturns410(t *testing.T) {
	h := newSoftDiscardHarness(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/discard", nil)
	h.handler.ServeHTTP(rr, req)
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/undo-discard", nil)
	req2.Header.Set("X-Undo-Token", "bogus-token")
	h.handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusGone {
		t.Fatalf("undo status with bad token = %d, want 410", rr2.Code)
	}
	if !strings.Contains(rr2.Body.String(), "undo window closed") {
		t.Fatalf("undo body missing closed-window message")
	}
}

func TestSoftDiscard_undoMissingTokenHeaderRejected(t *testing.T) {
	h := newSoftDiscardHarness(t)
	rr := httptest.NewRecorder()
	rr2 := httptest.NewRecorder()
	h.handler.ServeHTTP(rr, httptest.NewRequest(
		http.MethodPost, "/console/dlq/501/discard", nil))
	h.handler.ServeHTTP(rr2, httptest.NewRequest(
		http.MethodPost, "/console/dlq/501/undo-discard", nil))
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for missing token", rr2.Code)
	}
}

func TestSoftDiscard_sweepRemovesAfterWindow(t *testing.T) {
	h := newSoftDiscardHarness(t)
	rr := httptest.NewRecorder()
	h.handler.ServeHTTP(rr, httptest.NewRequest(
		http.MethodPost, "/console/dlq/501/discard", nil))
	// Wait past window then sweep manually.
	time.Sleep(250 * time.Millisecond)
	h.cfg.tomb.SweepOnce()
	if h.expiredCount() != 1 {
		t.Fatalf("expired count = %d, want 1", h.expiredCount())
	}
}

// TestSoftDiscard_sweepPublishesRowRemove pins the behaviour that was
// missing: when a tombstone's window closes, the permanent removal must
// also publish a row.remove on the bus so every open /console/sse/dlq
// stream patches #dlq-row-<seq> out. Without it the entry vanishes
// server-side but lingers in the operator's list until a manual refresh
// — the "after discard nothing deletes" report.
func TestSoftDiscard_sweepPublishesRowRemove(t *testing.T) {
	h := newSoftDiscardHarness(t)
	// Subscribe before the sweep so the emitted event can't be missed.
	ch, cancel := h.cfg.bus.subscribe(events.TopicDLQ)
	defer cancel()
	rr := httptest.NewRecorder()
	h.handler.ServeHTTP(rr, httptest.NewRequest(
		http.MethodPost, "/console/dlq/501/discard", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("discard status = %d, want 200", rr.Code)
	}
	time.Sleep(250 * time.Millisecond) // past the 200ms harness window
	h.cfg.tomb.SweepOnce()
	select {
	case evt := <-ch:
		if evt.Op != events.OpRowRemove {
			t.Fatalf("event op = %v, want OpRowRemove", evt.Op)
		}
		if evt.Key != "501" {
			t.Fatalf("event key = %q, want \"501\"", evt.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("sweep published no row.remove; SSE clients keep the row")
	}
}

func extractToken(t *testing.T, body string) string {
	t.Helper()
	var resp struct {
		Toast struct {
			UndoToken string `json:"undoToken"`
		} `json:"toast"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal body: %v; body=%s", err, body)
	}
	if resp.Toast.UndoToken == "" {
		t.Fatalf("token empty in body: %s", body)
	}
	return resp.Toast.UndoToken
}
