// journal/replay_fixture_test.go
// The replay fixtures in testdata/replay (#849): language-neutral
// cases of the ADR-026 replay rules that a client in another language
// runs to prove it decides what the Go SDK decides. This test is what
// keeps them honest: each fixture is replayed through the real Open,
// Tool and Model, and what Go did must equal what the fixture says.
//
// Methodology: one embedded server per fixture. The fixture's records
// are forged onto the step's subject with the identity they carry, so
// they land at the sequences they name; Open then claims for the
// fixture's task, and its replay view (Records) must equal the records
// exactly, which is what journal.view serves a bridge client. The
// fixture's calls are issued in order; a tool or model function runs
// only when the rules say so, and returns the output the fixture
// scripts. Observed per call: whether the function ran, the result
// handed back, and every record appended after the claim. Positive
// space: outcomes and appended records equal the fixture's; negative
// space: a function the fixture says is reused never runs, and a
// mismatch appends nothing. `go test ./journal -run ReplayFixtures
// -update` rewrites expect and appended from the Go behaviour; review
// that diff like code.
package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

var updateFixtures = flag.Bool("update", false, "rewrite replay fixtures from the Go behaviour")

// fixturesMax bounds the fixture directory a test run reads.
const fixturesMax = 64

// replayFixture is one file in testdata/replay. Records, Task and
// Calls are the input; Expect and Appended are what the rules yield.
type replayFixture struct {
	Description string           `json:"description"`
	Task        fixtureTask      `json:"task"`
	Records     []fixtureJournal `json:"records"`
	Calls       []fixtureCall    `json:"calls"`
	Expect      []fixtureResult  `json:"expect"`
	Appended    []fixtureRecord  `json:"appended"`
}

// fixtureTask is the delivery the replay runs as: the claim identity.
type fixtureTask struct {
	RunID         string `json:"run_id"`
	StepID        string `json:"step_id"`
	Iteration     int    `json:"iteration"`
	Attempt       int    `json:"attempt"`
	DispatchNonce string `json:"dispatch_nonce"`
	Delivered     int    `json:"delivered"`
	StreamSeq     uint64 `json:"stream_seq"`
}

// fixtureJournal is a journal record as journal.view serves it, less
// the timestamp, which replay never reads.
type fixtureJournal struct {
	Seq           uint64          `json:"seq"`
	Type          string          `json:"type"`
	Iteration     int             `json:"iteration"`
	Attempt       int             `json:"attempt"`
	DispatchNonce string          `json:"dispatch_nonce,omitempty"`
	Delivered     int             `json:"delivered"`
	StreamSeq     uint64          `json:"stream_seq,omitempty"`
	Index         int             `json:"index"`
	Data          json.RawMessage `json:"data,omitempty"`
}

// fixtureCall is one call the handler issues. Kind is "tool" or
// "model". Output and Error are what the function returns if it runs.
type fixtureCall struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name,omitempty"`
	Input  json.RawMessage `json:"input,omitempty"`
	Replay Replay          `json:"replay,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// fixtureResult is the outcome of one call: "ran", "reused",
// "interrupted" or "mismatch" for a tool; "requested" or "reused" for
// a model. Result is the tool result handed back, Output the model's.
type fixtureResult struct {
	Outcome string          `json:"outcome"`
	Result  *ToolResult     `json:"result,omitempty"`
	Output  json.RawMessage `json:"output,omitempty"`
}

// fixtureRecord is a record appended after the claim.
type fixtureRecord struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func TestReplayFixtures(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "replay", "*.json"))
	if err != nil {
		t.Fatalf("glob fixtures: %v", err)
	}
	if len(paths) == 0 || len(paths) > fixturesMax {
		t.Fatalf("found %d replay fixtures, want 1..%d", len(paths), fixturesMax)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			testReplayFixture(t, path)
		})
	}
}

func testReplayFixture(t *testing.T, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture replayFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if fixture.Description == "" || len(fixture.Calls) == 0 {
		t.Fatal("fixture needs a description and at least one call")
	}
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	forgeFixtureRecords(t, c, fixture)
	j := open(t, c, Task(fixture.Task))
	assertSameRecords(t, fixture.Records, j.Records())
	expect, appended := runFixtureCalls(t, c, j, fixture)
	if *updateFixtures {
		fixture.Expect, fixture.Appended = expect, appended
		writeFixture(t, path, fixture)
		return
	}
	assertSameJSON(t, "expect", fixture.Expect, expect)
	assertSameJSON(t, "appended", fixture.Appended, appended)
}

// forgeFixtureRecords publishes the fixture's records in order, each
// with its own claim identity, and checks it landed at its sequence.
func forgeFixtureRecords(t *testing.T, c client, fixture replayFixture) {
	t.Helper()
	for i, r := range fixture.Records {
		if r.Seq != uint64(i+1) || r.Iteration != fixture.Task.Iteration {
			t.Fatalf("record %d: seq %d iteration %d, want seq %d on the task's iteration",
				i, r.Seq, r.Iteration, i+1)
		}
		tk := Task{
			RunID: fixture.Task.RunID, StepID: fixture.Task.StepID, Iteration: r.Iteration,
			Attempt: r.Attempt, DispatchNonce: r.DispatchNonce, Delivered: r.Delivered,
			StreamSeq: r.StreamSeq,
		}
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		ack, err := c.js.PublishMsg(ctx, recordMessage(tk, r.Type, r.Index, r.Seq-1, r.Data))
		cancel()
		if err != nil {
			t.Fatalf("forge record %d: %v", r.Seq, err)
		}
		if ack.Sequence != r.Seq {
			t.Fatalf("record landed at %d, want %d", ack.Sequence, r.Seq)
		}
	}
}

// runFixtureCalls issues the fixture's calls on j and returns what
// happened to each, and the records appended after the claim. A
// mismatch ends the sequence: the handler is not replaying.
func runFixtureCalls(
	t *testing.T, c client, j *Journal, fixture replayFixture,
) ([]fixtureResult, []fixtureRecord) {
	t.Helper()
	claimSeq := j.Seq()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	results := []fixtureResult{}
	for _, call := range fixture.Calls {
		result := runFixtureCall(ctx, t, j, call)
		results = append(results, result)
		if result.Outcome == "mismatch" {
			break
		}
	}
	appended := []fixtureRecord{}
	records, err := Read(ctx, c.js, fixture.Task.RunID, fixture.Task.StepID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for _, r := range records {
		if r.Seq > claimSeq {
			appended = append(appended, fixtureRecord{Type: r.Type, Data: r.Data})
		}
	}
	return results, appended
}

// runFixtureCall issues one call and classifies its outcome.
func runFixtureCall(ctx context.Context, t *testing.T, j *Journal, call fixtureCall) fixtureResult {
	t.Helper()
	ran := false
	if call.Kind == "model" {
		output, err := j.Model(ctx, func(context.Context) ([]byte, error) {
			ran = true
			return call.Output, nil
		})
		if err != nil {
			t.Fatalf("Model: %v", err)
		}
		if ran {
			return fixtureResult{Outcome: "requested", Output: output}
		}
		return fixtureResult{Outcome: "reused", Output: output}
	}
	if call.Kind != "tool" {
		t.Fatalf("call kind %q, want tool or model", call.Kind)
	}
	result, err := j.Tool(ctx, ToolCall{Name: call.Name, Input: call.Input}, call.Replay,
		func(context.Context, ToolCall) ([]byte, error) {
			ran = true
			if call.Error != "" {
				return call.Output, errors.New(call.Error)
			}
			return call.Output, nil
		})
	switch {
	case errors.Is(err, ErrReplayMismatch):
		return fixtureResult{Outcome: "mismatch"}
	case err != nil:
		t.Fatalf("Tool %s: %v", call.Name, err)
	case ran:
		return fixtureResult{Outcome: "ran", Result: &result}
	case result.Interrupted:
		return fixtureResult{Outcome: "interrupted", Result: &result}
	}
	return fixtureResult{Outcome: "reused", Result: &result}
}

// assertSameRecords checks the replay view is the fixture's records:
// same order, identity and body.
func assertSameRecords(t *testing.T, want []fixtureJournal, got []Record) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("replay view has %d records, fixture %d", len(got), len(want))
	}
	for i, g := range got {
		assertSameJSON(t, "record", want[i], fixtureJournal{
			Seq: g.Seq, Type: g.Type, Iteration: g.Iteration, Attempt: g.Attempt,
			DispatchNonce: g.DispatchNonce, Delivered: g.Delivered, StreamSeq: g.StreamSeq,
			Index: g.Index, Data: g.Data,
		})
	}
}

// assertSameJSON compares want and got as canonical JSON.
func assertSameJSON(t *testing.T, what string, want, got any) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal %s: %v", what, err)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal %s: %v", what, err)
	}
	if canonicalJSON(wantJSON) != canonicalJSON(gotJSON) {
		t.Fatalf("%s:\n got  %s\n want %s", what, canonicalJSON(gotJSON), canonicalJSON(wantJSON))
	}
}

func writeFixture(t *testing.T, path string, fixture replayFixture) {
	t.Helper()
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(fixture); err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}
