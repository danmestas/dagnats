// Methodology: Pure unit tests of the nats-server log adapter. A
// capturing slog handler records every forwarded record, so the tests
// assert level mapping, the component attribute and the formatted
// message, and that Debug and Trace never reach slog. One integration
// check starts a real embedded server to confirm the adapter is
// installed and the server's own log calls reach it.

package server

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// captureHandler is a slog.Handler that keeps every record it sees.
// Handlers derived through WithAttrs share the parent's record store.
type captureHandler struct {
	store *captureStore
	attrs []slog.Attr
}

type captureStore struct {
	mu      sync.Mutex
	records []slog.Record
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{store: &captureStore{}}
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	clone := r.Clone()
	clone.AddAttrs(h.attrs...)
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.records = append(h.store.records, clone)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	joined := append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &captureHandler{store: h.store, attrs: joined}
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) snapshot() []slog.Record {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	return append([]slog.Record(nil), h.store.records...)
}

func recordAttr(r slog.Record, key string) string {
	var value string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value = a.Value.String()
			return false
		}
		return true
	})
	return value
}

func newCapturedAdapter() (*natsLogAdapter, *captureHandler) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	return newNATSLogAdapter(func() *slog.Logger { return logger }), handler
}

func TestNATSLogAdapter_ForwardsWarnAndError(t *testing.T) {
	adapter, handler := newCapturedAdapter()
	adapter.Warnf("route %s slow", "a")
	adapter.Errorf("JetStream out of %s resources, will be DISABLED", "file")
	adapter.Fatalf("listen on %d failed", 4222)

	records := handler.snapshot()
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	want := []struct {
		level   slog.Level
		message string
	}{
		{slog.LevelWarn, "route a slow"},
		{slog.LevelError, "JetStream out of file resources, will be DISABLED"},
		{slog.LevelError, "listen on 4222 failed"},
	}
	for i, w := range want {
		if records[i].Level != w.level || records[i].Message != w.message {
			t.Errorf("record %d = %v %q, want %v %q", i,
				records[i].Level, records[i].Message, w.level, w.message)
		}
		if got := recordAttr(records[i], "component"); got != "nats-server" {
			t.Errorf("record %d component = %q, want nats-server", i, got)
		}
	}
	if recordAttr(records[2], "fatal") != "true" {
		t.Errorf("Fatalf record lacks fatal=true")
	}
	if recordAttr(records[1], "fatal") != "" {
		t.Errorf("Errorf record carries a fatal attribute")
	}
}

func TestNATSLogAdapter_DropsDebugAndTrace(t *testing.T) {
	adapter, handler := newCapturedAdapter()
	adapter.Debugf("debug %d", 1)
	adapter.Tracef("trace %d", 2)
	if got := len(handler.snapshot()); got != 0 {
		t.Fatalf("debug/trace forwarded %d records, want 0", got)
	}
	adapter.Warnf("after")
	if got := len(handler.snapshot()); got != 1 {
		t.Fatalf("warn after debug/trace: %d records, want 1", got)
	}
}

// Startup notices are nats-server's banner (version, ids, the JetStream
// ASCII art); once ready, notices are route and leader changes.
func TestNATSLogAdapter_NoticeIsDebugUntilReady(t *testing.T) {
	adapter, handler := newCapturedAdapter()
	adapter.Noticef("Starting nats-server")
	adapter.markReady()
	adapter.Noticef("JetStream cluster new metadata leader: %s", "n1")

	records := handler.snapshot()
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].Level != slog.LevelDebug {
		t.Errorf("startup notice level = %v, want DEBUG", records[0].Level)
	}
	if records[1].Level != slog.LevelInfo ||
		records[1].Message != "JetStream cluster new metadata leader: n1" {
		t.Errorf("ready notice = %v %q, want INFO leader line",
			records[1].Level, records[1].Message)
	}
}

func TestTryStartNATS_InstallsAdapter(t *testing.T) {
	handler := newCaptureHandler()
	previous := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ns, err := tryStartNATS(&natsserver.Options{
		Port: -1, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer ns.Shutdown()

	adapter, ok := ns.Logger().(*natsLogAdapter)
	if !ok {
		t.Fatalf("logger = %T, want *natsLogAdapter", ns.Logger())
	}
	if !adapter.ready.Load() {
		t.Errorf("adapter not marked ready after start")
	}
	ns.Errorf("storage failure on %s", "file")
	ns.Debugf("debug line")
	var forwarded, debug int
	for _, record := range handler.snapshot() {
		switch record.Message {
		case "storage failure on file":
			forwarded++
		case "debug line":
			debug++
		}
	}
	if forwarded != 1 || debug != 0 {
		t.Errorf("error forwarded %d times, debug %d times; want 1 and 0",
			forwarded, debug)
	}
}
