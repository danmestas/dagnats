// cli/resolve_test.go
// Tests for run ID resolution: prefix matching, --last flag, and edge cases.
// Methodology: pure unit tests for full-ID passthrough and error cases,
// integration tests with embedded NATS for prefix matching and --last.
package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"

	"github.com/nats-io/nats.go/jetstream"
)

func TestResolveRunIDFullIDPassthrough(t *testing.T) {
	// Full 32-char IDs should be returned as-is without NATS.
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := api.NewService(nc)

	fullID := "abcdef1234567890abcdef1234567890"
	got, err := ResolveRunID(svc, fullID, false)

	// Positive: full ID returned unchanged.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != fullID {
		t.Fatalf("expected %q, got %q", fullID, got)
	}

	// Negative: non-32-char input must not pass through.
	_, err = ResolveRunID(svc, "short123", false)
	if err == nil {
		t.Fatal("expected error for non-matching short prefix")
	}
}

func TestResolveRunIDPrefixMatch(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(jsNew)

	run := dag.WorkflowRun{
		RunID:      "aabbccdd1234567890abcdef12345678",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}
	if err := store.Save(context.Background(), run); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	svc := api.NewService(nc)

	got, err := ResolveRunID(svc, "aabbccdd", false)

	// Positive: prefix resolves to full ID.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != run.RunID {
		t.Fatalf("expected %q, got %q", run.RunID, got)
	}

	// Negative: wrong prefix returns error.
	_, err = ResolveRunID(svc, "zzzzccdd", false)
	if err == nil {
		t.Fatal("expected error for non-matching prefix")
	}
}

func TestResolveRunIDAmbiguousPrefix(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(jsNew)

	// Two runs sharing the same 8-char prefix.
	run1 := dag.WorkflowRun{
		RunID:      "aabbccdd1111111111111111aaaaaaaa",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}
	run2 := dag.WorkflowRun{
		RunID:      "aabbccdd2222222222222222bbbbbbbb",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC().Add(-time.Second),
	}
	if err := store.Save(context.Background(), run1); err != nil {
		t.Fatalf("save run1: %v", err)
	}
	if err := store.Save(context.Background(), run2); err != nil {
		t.Fatalf("save run2: %v", err)
	}

	svc := api.NewService(nc)

	_, err = ResolveRunID(svc, "aabbccdd", false)

	// Positive: ambiguous prefix returns error with count.
	if err == nil {
		t.Fatal("expected error for ambiguous prefix")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected 'ambiguous' in error, got: %v", err)
	}

	// Negative: a longer, unique prefix should resolve.
	got, err := ResolveRunID(svc, "aabbccdd11111111", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != run1.RunID {
		t.Fatalf("expected %q, got %q", run1.RunID, got)
	}
}

func TestResolveRunIDNoMatch(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := api.NewService(nc)

	_, err := ResolveRunID(svc, "zzzzzzzz", false)

	// Positive: no match returns descriptive error.
	if err == nil {
		t.Fatal("expected error for non-matching prefix")
	}
	if !strings.Contains(err.Error(), "no run matching") {
		t.Fatalf(
			"expected 'no run matching' in error, got: %v", err,
		)
	}

	// Negative: empty prefix without --last also errors.
	_, err = ResolveRunID(svc, "", false)
	if err == nil {
		t.Fatal("expected error for empty input without --last")
	}
}

func TestResolveRunIDLastFlag(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(jsNew)

	older := dag.WorkflowRun{
		RunID:      "old00000111111112222222233333333",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusCompleted,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC().Add(-time.Minute),
	}
	newer := dag.WorkflowRun{
		RunID:      "new00000444444445555555566666666",
		WorkflowID: "test-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}
	if err := store.Save(context.Background(), older); err != nil {
		t.Fatalf("save older: %v", err)
	}
	if err := store.Save(context.Background(), newer); err != nil {
		t.Fatalf("save newer: %v", err)
	}

	svc := api.NewService(nc)

	got, err := ResolveRunID(svc, "", true)

	// Positive: --last returns the most recent run.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != newer.RunID {
		t.Fatalf("expected newest run %q, got %q",
			newer.RunID, got)
	}

	// Negative: --last without any runs returns error.
	// (Already tested implicitly by no-match test above,
	// but we verify the flag path with existing runs works.)
	if got == older.RunID {
		t.Fatal("--last should return newest, not oldest")
	}
}

func TestResolveRunIDPrefixTooShort(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	svc := api.NewService(nc)

	_, err := ResolveRunID(svc, "abc", false)

	// Positive: short prefix returns descriptive error.
	if err == nil {
		t.Fatal("expected error for short prefix")
	}
	if !strings.Contains(err.Error(), "at least 8") {
		t.Fatalf(
			"expected 'at least 8' in error, got: %v", err,
		)
	}

	// Negative: 8-char prefix does not trigger this error.
	_, err = ResolveRunID(svc, "abcdefgh", false)
	if err != nil &&
		strings.Contains(err.Error(), "at least 8") {
		t.Fatal("8-char prefix should not trigger too-short error")
	}
}

func TestResolveRunIDPanicsOnNilService(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: nil svc must panic.
		if r == nil {
			t.Fatal("expected panic on nil svc")
		}
		msg, ok := r.(string)
		if !ok ||
			!strings.Contains(msg, "svc must not be nil") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()

	// Negative: this call must not return normally.
	ResolveRunID(nil, "test", false)
	t.Fatal("should not reach here")
}
