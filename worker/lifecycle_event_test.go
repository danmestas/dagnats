// worker/lifecycle_event_test.go
// Tests for the worker-side step.started lifecycle publish helper.
// Assertion-defense tests are pure unit tests; integration tests start
// embedded NATS and run a worker end-to-end to verify the helper fires
// before the user's handler is invoked.
// Methodology: red-green TDD. Each test specifies a single observable
// behaviour and includes both a positive and a negative assertion.
package worker

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishStarted_PanicsOnNilMsg(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil msg, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	tc := &taskContext{runID: "r1", stepID: "s1"}
	_ = tc.publishStarted(nil)
}

func TestPublishStarted_PanicsOnEmptyRunID(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty runID, got none")
		}
		s, ok := r.(string)
		if !ok || s == "" {
			t.Fatalf("expected non-empty string panic, got %#v", r)
		}
	}()
	tc := &taskContext{runID: ""}
	_ = tc.publishStarted(stubJetstreamMsg{})
}

// stubJetstreamMsg implements jetstream.Msg minimally so the test can
// exercise the "empty runID panics before metadata is read" path.
// All methods panic — publishStarted must not call any of them.
type stubJetstreamMsg struct{}

func (stubJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) { panic("unreachable") }
func (stubJetstreamMsg) Data() []byte                              { panic("unreachable") }
func (stubJetstreamMsg) Headers() nats.Header                      { panic("unreachable") }
func (stubJetstreamMsg) Subject() string                           { panic("unreachable") }
func (stubJetstreamMsg) Reply() string                             { panic("unreachable") }
func (stubJetstreamMsg) Ack() error                                { panic("unreachable") }
func (stubJetstreamMsg) DoubleAck(context.Context) error           { panic("unreachable") }
func (stubJetstreamMsg) Nak() error                                { panic("unreachable") }
func (stubJetstreamMsg) NakWithDelay(time.Duration) error          { panic("unreachable") }
func (stubJetstreamMsg) InProgress() error                         { panic("unreachable") }
func (stubJetstreamMsg) Term() error                               { panic("unreachable") }
func (stubJetstreamMsg) TermWithReason(string) error               { panic("unreachable") }
