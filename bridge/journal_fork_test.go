// bridge/journal_fork_test.go
// A forked step over the bridge (#810): a bridge worker's journal.view
// and load_checkpoint for a forked step answer what the Go SDK's Open
// would, so a non-Go client replays a fork exactly as a Go worker
// does: the view holds the parent's records of the forked iteration up
// to the fork point, restamped as iteration 0, and the state is the
// parent's committed state before that iteration.
//
// Methodology: real embedded NATS and a Bridge, as in journal_test.go.
// The parent journal is written through the journal package, the fork
// record by journal.WriteFork, then the forked step's task is polled
// and resolved over HTTP. Responses are decoded by their wire keys.
// Positive space: the parent's model response and tool result before
// the point are in the view, the parent's committed state is the
// state. Negative space: the parent's claims, its records after the
// point and the fork record itself are not in the view.
package bridge

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

// forkParent writes the parent's iteration 0 (committed with state
// {"turn":0}) and iteration 1 up to a model response and a tool
// result, then one record past the fork point. It returns the fork
// point: the tool result's sequence.
func forkParent(t *testing.T, rig *journalRig, runID, stepID string) uint64 {
	t.Helper()
	ctx := context.Background()
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	open := func(iteration int) *journal.Journal {
		j, err := journal.Open(ctx, rig.js, pub, journal.Task{
			RunID: runID, StepID: stepID, Iteration: iteration, Attempt: 1,
			DispatchNonce: "p", Delivered: 1,
		})
		if err != nil {
			t.Fatalf("open parent iteration %d: %v", iteration, err)
		}
		return j
	}
	first := open(0)
	if _, err := first.Commit(ctx, journal.Commit{
		Action: journal.ActionContinue, State: []byte(`{"turn":0}`),
	}); err != nil {
		t.Fatalf("commit parent iteration 0: %v", err)
	}
	second := open(1)
	appendRecord := func(typ, data string) uint64 {
		seq, err := second.Append(ctx, typ, []byte(data))
		if err != nil {
			t.Fatalf("append %s: %v", typ, err)
		}
		return seq
	}
	appendRecord(journal.TypeModelResponse, `{"call":0,"output":"m"}`)
	appendRecord(journal.TypeToolIntent, `{"name":"search","call":0,"key":"k","replay":"safe"}`)
	point := appendRecord(journal.TypeToolResult, `{"call":0,"key":"k","output":"r"}`)
	appendRecord(journal.TypeModelResponse, `{"call":1,"output":"after"}`)
	return point
}

func TestBridgeForkedStepViewsTheParentUpToTheForkPoint(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const parent, child, stepID = "run-fparent", "run-fchild", "step-fork"
	point := forkParent(t, rig, parent, stepID)
	ctx := context.Background()
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	if _, err := journal.WriteFork(ctx, rig.js, pub, child, journal.Fork{
		ParentRunID: parent, ParentStepID: stepID, ParentSeq: point,
	}); err != nil {
		t.Fatalf("WriteFork: %v", err)
	}
	rig.publish(t, protocol.TaskPayload{RunID: child, StepID: stepID, Attempt: 1, DispatchNonce: "c"})
	polled := postPoll(t, ts, "echo", 1, 3000)
	if len(polled) != 1 {
		t.Fatalf("poll = %+v, want the forked task", polled)
	}
	view := resolveView(t, ts, polled[0].TaskID)
	var types []string
	for _, r := range view.Records {
		types = append(types, r.Type)
		if r.Iteration != 0 || r.Seq > point {
			t.Fatalf("view record %+v: want iteration 0 and at or before seq %d", r, point)
		}
	}
	want := []string{journal.TypeModelResponse, journal.TypeToolIntent, journal.TypeToolResult}
	if len(types) != len(want) {
		t.Fatalf("view types = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("view types = %v, want %v", types, want)
		}
	}
	status, body := resolveBody(t, ts, polled[0].TaskID, `{"action":"load_checkpoint"}`)
	var state map[string]json.RawMessage
	if status != 200 || json.Unmarshal([]byte(body), &state) != nil {
		t.Fatalf("load_checkpoint: %d %s", status, body)
	}
	if string(state["source"]) != `"journal"` || string(state["state"]) != `{"turn":0}` {
		t.Fatalf("load_checkpoint = %s, want the parent's committed state from the journal", body)
	}
}
