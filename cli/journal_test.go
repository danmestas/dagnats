// cli/journal_test.go
// `dagnats journal check`: a clean run exits 0 and prints OK with the
// skipped invariants named; a forged record from a fenced claim exits
// 1 and names the violation; an unknown run exits 1 with an error.
//
// Methodology: embedded NATS with the journal provisioned, the run
// snapshot written to workflow_runs the way the engine stores it, and
// journals written through the journal package. No engine. Bounded
// contexts throughout.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func journalCheckFixture(t *testing.T, runID string, steps map[string]dag.StepState) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("workflow_runs: %v", err)
	}
	data, err := json.Marshal(dag.WorkflowRun{RunID: runID, Steps: steps})
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}
	if _, err := kv.Put(ctx, "run."+runID, data); err != nil {
		t.Fatalf("put run: %v", err)
	}
	return nc
}

func TestJournalCheck_CleanRunExitsZero(t *testing.T) {
	nc := journalCheckFixture(t, "run-ok", map[string]dag.StepState{
		"s": {Status: dag.StepStatusCompleted, Attempts: 1},
	})
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j, err := journal.Open(ctx, js, natsutil.NewTracingPublisher(nc, js), journal.Task{
		RunID: "run-ok", StepID: "s", Attempt: 1, DispatchNonce: "n", Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := j.Commit(ctx, journal.Commit{Action: journal.ActionComplete}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var out bytes.Buffer
	if code := checkJournal(ctx, nc, "run-ok", false, &out); code != 0 {
		t.Fatalf("exit code = %d, want 0; output:\n%s", code, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "OK") || !strings.Contains(text, "s: 2 records, 1 commits, ok") {
		t.Errorf("output = %q, want OK and the step line", text)
	}
	if !strings.Contains(text, "submissions bucket absent") {
		t.Errorf("output = %q, want invariant 5 reported as skipped", text)
	}
	out.Reset()
	if code := checkJournal(ctx, nc, "run-ok", true, &out); code != 0 {
		t.Fatalf("json exit code = %d, want 0", code)
	}
	var report journal.Report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.RunID != "run-ok" {
		t.Errorf("json output = %q (%v), want a Report for run-ok", out.String(), err)
	}
}

func TestJournalCheck_ViolationExitsOne(t *testing.T) {
	nc := journalCheckFixture(t, "run-bad", map[string]dag.StepState{
		"s": {Status: dag.StepStatusCompleted, Attempts: 2},
	})
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	pub := natsutil.NewTracingPublisher(nc, js)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: "run-bad", StepID: "s", Attempt: 1, DispatchNonce: "n1", Delivered: 1,
	}); err != nil {
		t.Fatalf("Open stalled: %v", err)
	}
	successor, err := journal.Open(ctx, js, pub, journal.Task{
		RunID: "run-bad", StepID: "s", Attempt: 2, DispatchNonce: "n2", Delivered: 2,
	})
	if err != nil {
		t.Fatalf("Open successor: %v", err)
	}
	if _, err := successor.Commit(ctx, journal.Commit{Action: journal.ActionComplete}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// A record the stalled worker could only write by bypassing the
	// fence: forged with the sequence it would have needed.
	forged := &nats.Msg{
		Subject: natsutil.JournalSubject("run-bad", "s"),
		Data:    []byte(`"forged"`),
		Header: nats.Header{
			"Nats-Expected-Last-Subject-Sequence": {"3"},
			journal.HeaderType:                    {journal.TypeEntry},
			journal.HeaderIteration:               {"0"},
			journal.HeaderAttempt:                 {"1"},
			journal.HeaderIndex:                   {"1"},
			journal.HeaderDelivered:               {"1"},
			journal.HeaderNonce:                   {"n1"},
		},
	}
	if _, err := js.PublishMsg(ctx, forged); err != nil {
		t.Fatalf("forge: %v", err)
	}
	var out bytes.Buffer
	if code := checkJournal(ctx, nc, "run-bad", false, &out); code != 1 {
		t.Fatalf("exit code = %d, want 1; output:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "fenced") || !strings.Contains(out.String(), "FAIL") {
		t.Errorf("output = %q, want the fencing violation and FAIL", out.String())
	}
}

func TestJournalCheck_UnknownRunExitsOne(t *testing.T) {
	nc := journalCheckFixture(t, "run-other", map[string]dag.StepState{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if code := checkJournal(ctx, nc, "run-missing", false, &out); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if out.Len() != 0 {
		t.Errorf("output = %q, want nothing on stdout for a failed audit", out.String())
	}
}
