// dagnatstest/dlq_fixture.go
// DLQFixture clusters DLQ-related test helpers off a shared Harness.
// Construct via NewDLQFixture(h). Helpers stay under 70 lines (TigerStyle).
//
// Concern boundary: the Harness owns embedded NATS lifecycle; the fixture
// owns DLQ-stream observation and DLQ-publish drivers. The fixture never
// mutates the Harness beyond reading its NATS connection.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DLQFixture holds DLQ-focused test helpers bound to a Harness.
// Use NewDLQFixture(h) to construct.
type DLQFixture struct {
	h *Harness
}

// NewDLQFixture binds a DLQFixture to an existing Harness. Panics if h is nil.
func NewDLQFixture(h *Harness) *DLQFixture {
	if h == nil {
		panic("NewDLQFixture: h must not be nil")
	}
	if h.NC == nil {
		panic("NewDLQFixture: h.NC must not be nil")
	}
	return &DLQFixture{h: h}
}

// Count returns the current number of entries on the DEAD_LETTERS stream.
// Reads stream info directly — no consumer needed.
func (f *DLQFixture) Count(ctx context.Context) (int, error) {
	if ctx == nil {
		panic("Count: ctx must not be nil")
	}
	if f.h == nil {
		panic("Count: fixture not bound")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return 0, err
	}
	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return int(info.State.Msgs), nil
}

// PublishDLQWithMsgID publishes a synthetic DLQ entry on subject `dead.<task>`
// with the given Nats-Msg-Id header for dedup testing. Returns the publish
// error so callers can assert success/failure.
//
// Used by tests that exercise the dedup contract directly without driving
// the full workflow-failure path.
func (f *DLQFixture) PublishDLQWithMsgID(
	ctx context.Context, subject string, msgID string, body []byte,
) error {
	if ctx == nil {
		panic("PublishDLQWithMsgID: ctx must not be nil")
	}
	if subject == "" {
		panic("PublishDLQWithMsgID: subject must not be empty")
	}
	if msgID == "" {
		panic("PublishDLQWithMsgID: msgID must not be empty")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    body,
		Header: nats.Header{
			"Nats-Msg-Id": {msgID},
		},
	}
	_, err = js.PublishMsg(ctx, msg)
	return err
}

// PublishDLQNoMsgID publishes a synthetic DLQ entry on `subject` with no
// Nats-Msg-Id header — mirrors the pre-fix production code path in
// engine.RecoveryManager.PublishDeadLetter. Used to demonstrate that
// without dedup, repeated publishes for the same logical event all land.
func (f *DLQFixture) PublishDLQNoMsgID(
	ctx context.Context, subject string, body []byte,
) error {
	if ctx == nil {
		panic("PublishDLQNoMsgID: ctx must not be nil")
	}
	if subject == "" {
		panic("PublishDLQNoMsgID: subject must not be empty")
	}
	js, err := jetstream.New(f.h.NC)
	if err != nil {
		return err
	}
	_, err = js.Publish(ctx, subject, body)
	return err
}

// WaitForCount polls Count() until it returns target or timeout.
// Bounded wait per CLAUDE.md testing rules — fails the test on timeout.
// Returns the final observed count.
func (f *DLQFixture) WaitForCount(
	t *testing.T, target int, timeout time.Duration,
) int {
	if t == nil {
		panic("WaitForCount: t must not be nil")
	}
	if timeout <= 0 {
		panic("WaitForCount: timeout must be positive")
	}
	t.Helper()
	ctx := t.Context()
	deadline := time.After(timeout)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	last := -1
	for {
		got, err := f.Count(ctx)
		if err == nil {
			last = got
			if got == target {
				return got
			}
		}
		select {
		case <-deadline:
			t.Fatalf(
				"WaitForCount: DLQ count %d != target %d "+
					"after %s", last, target, timeout,
			)
			return last
		case <-ticker.C:
		}
	}
}
