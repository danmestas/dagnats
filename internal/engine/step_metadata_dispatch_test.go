// engine/step_metadata_dispatch_test.go
// Regression test for #732: a step's Metadata must reach the worker in
// the task payload on EVERY dispatch. #450 copied StepDef.Metadata into
// TaskPayload.Metadata in collectReadyMessages; #652 replaced that path
// and the copy was lost, so no worker (NATS or bridge) received it and a
// jobs: workflow's "ci.job" (#728) never arrived.
//
// Methodology: plays a grouped worker exactly like the #721 test. It
// claims each attempt from the grouped subject, decodes the payload,
// checks its metadata, and fails the attempt, so the first dispatch AND
// the timer-driven retries (which rebuild the payload from a
// TimerMessage) are all covered. Negative space: a step with no metadata
// dispatches with none. Each test gets its own embedded server; waits
// are bounded.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

const dispatchMetadataJob = `{"steps":[{"run":"go test ./..."}]}`

func TestStepMetadataReachesEveryDispatch(t *testing.T) {
	payloads := collectGroupedDispatches(t, "meta-dispatch-1",
		map[string]string{"ci.job": dispatchMetadataJob})
	// MaxAttempts 2: the first dispatch plus two timer-driven retries.
	if len(payloads) != 3 {
		t.Fatalf("got %d dispatches, want 3", len(payloads))
	}
	for i, payload := range payloads {
		if got := payload.Metadata["ci.job"]; got != dispatchMetadataJob {
			t.Fatalf("dispatch %d: Metadata[ci.job] = %q, want %s",
				i+1, got, dispatchMetadataJob)
		}
	}
}

func TestStepWithoutMetadataDispatchesNone(t *testing.T) {
	payloads := collectGroupedDispatches(t, "meta-dispatch-2", nil)
	if len(payloads) == 0 {
		t.Fatal("the step was never dispatched")
	}
	for i, payload := range payloads {
		if payload.Metadata != nil {
			t.Fatalf("dispatch %d carries metadata %v, want none",
				i+1, payload.Metadata)
		}
	}
}

// collectGroupedDispatches runs one grouped step with the given
// metadata, fails every attempt, and returns each dispatched payload.
func collectGroupedDispatches(
	t *testing.T, runID string, metadata map[string]string,
) []protocol.TaskPayload {
	t.Helper()
	if runID == "" {
		t.Fatal("collectGroupedDispatches: runID must not be empty")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	orch := startGroupedRetryWorkflow(t, nc, js, runID, metadata)
	t.Cleanup(orch.Stop)
	grouped, err := js.PullSubscribe(
		"task.ml-training.=gpu.*", "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe grouped subject: %v", err)
	}
	const dispatchesMax = 10
	var payloads []protocol.TaskPayload
	for len(payloads) < dispatchesMax {
		msgs, err := grouped.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil || len(msgs) == 0 {
			break
		}
		var payload protocol.TaskPayload
		if err := json.Unmarshal(msgs[0].Data, &payload); err != nil {
			t.Fatalf("decode dispatch %d: %v", len(payloads)+1, err)
		}
		if err := msgs[0].Ack(); err != nil {
			t.Fatalf("ack dispatch %d: %v", len(payloads)+1, err)
		}
		payloads = append(payloads, payload)
		publishAttemptFailure(t, js, runID, len(payloads))
	}
	if len(payloads) >= dispatchesMax {
		t.Fatalf("still dispatching after %d attempts", dispatchesMax)
	}
	return payloads
}
