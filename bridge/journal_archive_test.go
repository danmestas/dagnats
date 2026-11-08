// bridge/journal_archive_test.go
// Endless JetStream for a bridge worker: journal.view and journal.read
// replay a step whose journal was partly purged from AGENT_JOURNAL, with
// the purged records read from the archive, exactly as they would
// without the purge; with no archive they fail instead of replaying a
// journal with records missing.
//
// Methodology: real embedded NATS and a Bridge, as in
// journal_view_test.go. A crashed holder's claim, intent and entry are
// written, snapshotted below the entry and purged there; the fake
// archive of internal/archivetest serves the snapshot. Responses are
// decoded by their wire keys.
package bridge

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/archivetest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

// purgedBridgeStep leaves iteration 0 of the step with a crashed claim,
// an intent and an entry, purges below the entry, publishes the task
// and polls it; it returns the task id and a function that starts the
// archive.
func purgedBridgeStep(
	t *testing.T, runID string,
) (*httptest.Server, string, func() *archivetest.Responder) {
	t.Helper()
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	ctx := context.Background()
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	crashed, err := journal.Open(ctx, rig.js, pub, journal.Task{
		RunID: runID, StepID: "s", Iteration: 0, Attempt: 1, DispatchNonce: "n1",
		Delivered: 1, StreamSeq: 999,
	})
	if err != nil {
		t.Fatalf("open crashed holder: %v", err)
	}
	intent := `{"name":"send","call":0,"key":"k","replay":"unsafe","tool_call_id":"tc-1"}`
	if _, err := crashed.Append(ctx, journal.TypeToolIntent, []byte(intent)); err != nil {
		t.Fatalf("append intent: %v", err)
	}
	cut, err := crashed.Append(ctx, journal.TypeEntry, []byte(`{"e":1}`))
	if err != nil {
		t.Fatalf("append entry: %v", err)
	}
	before := archivetest.Snapshot(t, rig.js, natsutil.AgentJournalStream, cut)
	epoch, _ := archivetest.PurgeBelow(t, rig.js, natsutil.AgentJournalStream, cut)
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: "s", Attempt: 1,
		DispatchNonce: "n1"})
	items := postPoll(t, ts, "echo", 1, 3000)
	if len(items) != 1 {
		t.Fatalf("poll = %+v, want one task", items)
	}
	return ts, items[0].TaskID, func() *archivetest.Responder {
		return archivetest.Serve(t, rig.nc, "", natsutil.AgentJournalStream, epoch, before)
	}
}

func TestBridgeJournalViewReplaysPurgedRecordsFromArchive(t *testing.T) {
	ts, taskID, serve := purgedBridgeStep(t, "run-jarchive")
	fake := serve()
	view := resolveView(t, ts, taskID)
	if len(view.Records) != 3 || view.Records[0].Type != journal.TypeClaim ||
		view.Records[1].Type != journal.TypeToolIntent || view.Records[2].Type != journal.TypeEntry {
		t.Fatalf("view records = %+v, want claim, intent, entry", view.Records)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(view.Records[1].Data, &body); err != nil ||
		string(body["tool_call_id"]) != `"tc-1"` {
		t.Fatalf("archived intent data = %s, want tool_call_id preserved", view.Records[1].Data)
	}
	if fake.Requests.Load() == 0 {
		t.Error("the purged records were not read from the archive")
	}
	status, raw := resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	var read journalReadResponse
	if status != 200 || json.Unmarshal([]byte(raw), &read) != nil || len(read.Records) != 4 {
		t.Fatalf("journal.read: %d %s, want the 3 records and this claim", status, raw)
	}
}

func TestBridgeJournalViewFailsClosedWithoutArchive(t *testing.T) {
	ts, taskID, _ := purgedBridgeStep(t, "run-jarchive-none")
	status, body := resolveBody(t, ts, taskID, `{"action":"journal.view"}`)
	if status == 200 || !strings.Contains(body, "run-jarchive-none") {
		t.Fatalf("journal.view with no archive = %d %s, want a failure naming the run",
			status, body)
	}
	if status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`); status == 200 {
		t.Fatalf("journal.read with no archive = 200 %s, want a failure", body)
	}
}
