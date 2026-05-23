// internal/engine/approval_gate.go
// ApprovalGate owns the approval-step lifecycle: token generation,
// KV storage, timeout scheduling, and cleanup. Extracted from
// Orchestrator to reduce its surface area. No behavioral change.
package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

// ApprovalGate manages approval token lifecycle: generation,
// storage, timeout scheduling, grant/reject handling, and
// cleanup. Talks to NATS KV and JetStream but delegates
// snapshot persistence back to the Orchestrator via callbacks.
//
// Callback protocol: ApprovalGate modifies run.Steps state
// in-place (e.g. marking a step Running or Completed), then
// calls saveFn so the caller can persist the snapshot. Methods
// like HandleGranted additionally accept loadFn to reload
// current state and enqueueFn/completeFn to advance the DAG.
// The ordering contract is: load → modify run.Steps → save →
// enqueue/complete.
type ApprovalGate struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	tp         *natsutil.TracingPublisher
	sleepTimer *SleepTimer
	tracer     trace.Tracer
}

// NewApprovalGate creates an ApprovalGate with the given
// dependencies. All parameters are required. tp injects W3C
// trace context on every published approval event (#334).
func NewApprovalGate(
	nc *nats.Conn,
	js jetstream.JetStream,
	tp *natsutil.TracingPublisher,
	sleepTimer *SleepTimer,
	tracer trace.Tracer,
) *ApprovalGate {
	if nc == nil {
		panic("NewApprovalGate: nc must not be nil")
	}
	if js == nil {
		panic("NewApprovalGate: js must not be nil")
	}
	if tp == nil {
		panic("NewApprovalGate: tp must not be nil")
	}
	if sleepTimer == nil {
		panic(
			"NewApprovalGate: sleepTimer must not be nil",
		)
	}
	if tracer == nil {
		panic("NewApprovalGate: tracer must not be nil")
	}
	return &ApprovalGate{
		nc:         nc,
		js:         js,
		tp:         tp,
		sleepTimer: sleepTimer,
		tracer:     tracer,
	}
}

// generateApprovalToken returns a 64-character hex string from
// 32 crypto-random bytes. Panics if OS entropy is unavailable.
func generateApprovalToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(b), nil
}
