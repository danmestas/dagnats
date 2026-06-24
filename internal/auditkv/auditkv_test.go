// Methodology: pure unit tests for the audit leaf package. KeyFor format is
// asserted positively (sortable RFC3339-nanos prefix + hex suffix) and
// negatively (zero time rejected). Emit is best-effort: a nil KV must warn
// and return nil rather than panic, so an audit gap never blocks an action.
package auditkv

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestKeyForFormat(t *testing.T) {
	ts := time.Date(2026, 6, 24, 12, 0, 0, 123456789, time.UTC)

	key, err := KeyFor(ts)
	if err != nil {
		t.Fatalf("KeyFor: %v", err)
	}
	prefix := ts.Format("20060102T150405.000000000Z07")
	if !strings.HasPrefix(key, prefix+"-") {
		t.Fatalf("key %q must start with sortable prefix %q", key, prefix)
	}
	// Negative space: a zero timestamp is rejected.
	if _, err := KeyFor(time.Time{}); err == nil {
		t.Fatal("KeyFor must reject zero timestamp")
	}
}

func TestKeyForSorts(t *testing.T) {
	t1 := time.Date(2026, 6, 24, 12, 0, 0, 1, time.UTC)
	t2 := time.Date(2026, 6, 24, 12, 0, 0, 2, time.UTC)

	k1, _ := KeyFor(t1)
	k2, _ := KeyFor(t2)
	if !(k1 < k2) {
		t.Fatalf("earlier key %q must sort before later key %q", k1, k2)
	}
}

func TestEmitNilKVIsBestEffort(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	evt := AuditEvent{Action: "grant.control_plane", Outcome: "success"}

	// nil KV must not panic and must return nil — an audit gap never
	// blocks the operator action that triggered it.
	if err := Emit(context.Background(), nil, logger, evt); err != nil {
		t.Fatalf("Emit with nil kv must return nil, got %v", err)
	}
}
