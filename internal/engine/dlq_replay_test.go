// internal/engine/dlq_replay_test.go
// Methodology: real embedded NATS via dagnatstest.Harness. Drive a
// workflow whose step is failed non-retriably so the engine writes a
// DLQ entry, then call ReplayDeadLetter and assert the replayed
// task delivery's body bytes equal the original task delivery's body
// bytes byte-for-byte. Separately, publish a synthetic legacy DLQ
// entry (no Body field) and assert replay returns the typed
// body-missing error.
//
// Issue: https://github.com/danmestas/dagnats/issues/200
package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/protocol"
)

// TestDLQReplay_PreservesOriginalBody drives a non-trivial typed
// input into the DLQ, replays it, and asserts the replayed message
// body equals the original task body bytes byte-for-byte. The DLQ
// entry must carry the original body for this to be possible.
func TestDLQReplay_PreservesOriginalBody(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)
	// Worker is not Start()ed: this test drives the engine via
	// synthetic step.failed events on the history stream, so the
	// engine's DLQ-publish path fires without any task delivery.
	dlq := dagnatstest.NewDLQFixture(h)

	originalInput := []byte(
		`{"endpoint":"https://example/x","classification":"FDC"}`,
	)
	seq := dlq.PublishAndExhaustToDLQ(t, originalInput)

	// Subscribe BEFORE replay so we catch the republished delivery.
	awaiter := dlq.AwaitReplay(t, 5*time.Second)

	ctx, cancel := context.WithTimeout(
		t.Context(), 10*time.Second,
	)
	defer cancel()
	if err := dlq.Replay(ctx, seq); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	got := <-awaiter
	if len(got) == 0 {
		t.Fatalf("replayed body must not be empty (the #200 bug)")
	}

	var payload protocol.TaskPayload
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatalf("replayed body must be a valid TaskPayload, got: %s",
			string(got))
	}
	if !bytes.Equal(payload.Input, originalInput) {
		t.Fatalf("replayed input must equal original byte-for-byte;\n"+
			" got:  %q\n want: %q", payload.Input, originalInput)
	}
}

// TestDLQReplay_LegacyEntryReturnsBodyMissingError asserts that
// replay of a pre-fix DLQ entry (no Body field) returns the typed
// ErrDLQBodyMissing error so operators don't silently re-publish
// stub data.
func TestDLQReplay_LegacyEntryReturnsBodyMissingError(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)
	dlq := dagnatstest.NewDLQFixture(h)

	seq := dlq.PublishLegacyEntry(t)

	ctx, cancel := context.WithTimeout(
		t.Context(), 10*time.Second,
	)
	defer cancel()
	err := dlq.Replay(ctx, seq)
	if err == nil {
		t.Fatalf("replay against legacy entry must error; got nil")
	}
	if !dlq.IsBodyMissingError(err) {
		t.Fatalf(
			"error must be the typed body-missing error;\n got %T: %v",
			err, err,
		)
	}
}
