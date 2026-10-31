// bridge/journal_restart_limit_test.go
// Methodology: real embedded NATS and one Bridge, driving a non-Go
// worker that claims a journal step and dies before making progress
// (the task is nak'd, as AckWait lapsing would). ADR-028: after the
// restart limit's worth of claims with no progress the next claim
// fails the step with restart_limit instead of delivering it again;
// a journal record between claims resets the count. The Go SDK's
// side of the rule is pinned in worker/ and is not touched here.
package bridge

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
)

// crashClaim polls the step, claims its journal with a read and then
// "dies": the delivery is nak'd without any progress record.
func crashClaim(t *testing.T, b *Bridge, taskID string) {
	t.Helper()
	msg, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatalf("bridge lost the claim on %s", taskID)
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak: %v", err)
	}
}

func TestBridgeRestartLimitFailsStepAfterLimitClaims(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-rl", "step-rl"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	for i := 0; i < worker.RestartLimitDefault; i++ {
		taskID := pollOne(t, ts)
		if status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`); status != 200 {
			t.Fatalf("claim %d: %d %s", i+1, status, body)
		}
		crashClaim(t, b, taskID)
	}
	taskID := pollOne(t, ts)
	status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	if status != http.StatusGone || !strings.Contains(body, `"restart_limit"`) {
		t.Fatalf("claim past the limit = %d %s, want 410 restart_limit", status, body)
	}
	if n := countHistory(t, rig.nc, runID, protocol.EventStepFailed); n != 1 {
		t.Fatalf("step.failed events = %d, want 1", n)
	}
	if _, held := b.ackMap.Load(taskID); held {
		t.Fatal("failed task is still held in the ack map")
	}
	if tasks := postPoll(t, ts, "echo", 1, 500); len(tasks) != 0 {
		t.Fatalf("failed step was delivered again: %d tasks", len(tasks))
	}
}

func TestBridgeRestartLimitHonorsCustomLimit(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	b.SetRestartLimit(1)
	rig.publish(t, protocol.TaskPayload{RunID: "run-rr", StepID: "step-rr", DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	crashClaim(t, b, taskID)
	taskID = pollOne(t, ts)
	status, body := resolveBody(t, ts, taskID, `{"action":"journal.append","type":"entry","data":1}`)
	if status != http.StatusGone || !strings.Contains(body, `"restart_limit"`) {
		t.Fatalf("limit 1: second claim = %d %s, want 410 restart_limit", status, body)
	}
	if n := countHistory(t, rig.nc, "run-rr", protocol.EventStepFailed); n != 1 {
		t.Fatalf("step.failed events = %d, want 1", n)
	}
}

func TestBridgeRestartLimitProgressResetsCount(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID = "run-rp"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: "step-rp", DispatchNonce: "n1"})
	// Twice the limit of deliveries, each one appending an entry
	// before it dies: every claim sees zero restarts.
	for i := 0; i < 2*worker.RestartLimitDefault; i++ {
		taskID := pollOne(t, ts)
		status, body := resolveBody(t, ts, taskID,
			`{"action":"journal.append","type":"entry","data":{"i":1}}`)
		if status != 200 {
			t.Fatalf("delivery %d: %d %s, want 200", i+1, status, body)
		}
		crashClaim(t, b, taskID)
	}
	if n := countHistory(t, rig.nc, runID, protocol.EventStepFailed); n != 0 {
		t.Fatalf("step.failed events = %d, want 0 while progressing", n)
	}
}

func TestBridgeSetRestartLimitRejectsOutOfRange(t *testing.T) {
	rig := newJournalRig(t)
	b, _ := rig.front(t)
	for _, n := range []int{0, -1, 1001} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("SetRestartLimit(%d) did not panic", n)
				}
			}()
			b.SetRestartLimit(n)
		}()
	}
	b.SetRestartLimit(7)
	if b.restartLimit.Load() != 7 {
		t.Fatalf("restartLimit = %d, want 7", b.restartLimit.Load())
	}
}

func TestRestartLimitFromEnv(t *testing.T) {
	cases := []struct {
		name, value string
		want        int
		warns       bool
	}{
		{"unset", "", worker.RestartLimitDefault, false},
		{"zero", "0", worker.RestartLimitDefault, true},
		{"too large", "1001", worker.RestartLimitDefault, true},
		{"not a number", "abc", worker.RestartLimitDefault, true},
		{"valid", "9", 9, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(restartLimitEnv, tc.value)
			var logged bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
			defer slog.SetDefault(prev)
			if got := restartLimitFromEnv(); got != tc.want {
				t.Fatalf("limit = %d, want %d", got, tc.want)
			}
			if warned := strings.Contains(logged.String(), "ignored"); warned != tc.warns {
				t.Fatalf("warned = %v, want %v (log %q)", warned, tc.warns, logged.String())
			}
		})
	}
}
