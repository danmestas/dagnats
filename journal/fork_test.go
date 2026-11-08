// journal/fork_test.go
// Forks (#810): a forked step's journal starts with a fork record that
// points at a parent journal and a sequence in it, and Open treats the
// parent's records up to that sequence as the forked step's history.
// Model responses and tool results recorded before the fork point are
// replayed, not requested or run again; calls after it run afresh;
// an unsafe call the parent left unfinished at the fork point is
// answered as interrupted, never rerun; the parent journal is not
// written. A fork of a fork follows the chain. WriteFork refuses a
// parent step without a journal, a sequence that is not a record of
// the parent step, a commit or fork record as the point, and a child
// journal that already holds records.
//
// Methodology: one embedded server per case, through the public
// WriteFork/Open/Tool/Model surface. The parent is written by real
// claims; tool functions and the scripted model count their calls, so
// a replayed call is told from a re-executed one. Every wait is
// bounded; every case has a positive and a negative assertion.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

// forkClient starts an embedded server with the streams provisioned.
func forkClient(t *testing.T) client {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	return newClient(t, nc)
}

// countingTool returns a tool function that counts its runs and
// answers output.
func countingTool(runs *atomic.Int32, output string) ToolFunc {
	return func(context.Context, ToolCall) ([]byte, error) {
		runs.Add(1)
		return []byte(output), nil
	}
}

// parentRun writes the parent journal: iteration 0 asks the model,
// runs "search" (safe), asks again, runs "send" (unsafe) and commits
// continue with state "s0"; iteration 1 asks the model and completes.
// It returns the sequence of each landmark record.
func parentRun(t *testing.T, c client, runID string) map[string]uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	seqs := map[string]uint64{}
	model := NewScriptedModel([]byte(`"m1"`), []byte(`"m2"`), []byte(`"m3"`))
	var runs atomic.Int32
	j := open(t, c, task(runID, 0, 1, 1))
	steps := []struct {
		name string
		do   func() error
	}{
		{"m1", func() error { _, err := j.Model(ctx, model.Respond); return err }},
		{"search", func() error {
			_, err := j.Tool(ctx, ToolCall{Name: "search"}, Safe, countingTool(&runs, `"r1"`))
			return err
		}},
		{"m2", func() error { _, err := j.Model(ctx, model.Respond); return err }},
		{"send", func() error {
			_, err := j.Tool(ctx, ToolCall{Name: "send"}, Unsafe, countingTool(&runs, `"sent"`))
			return err
		}},
	}
	for _, s := range steps {
		if err := s.do(); err != nil {
			t.Fatalf("parent %s: %v", s.name, err)
		}
		seqs[s.name] = j.Seq()
	}
	if _, err := j.Commit(ctx, Commit{Action: ActionContinue, State: []byte(`"s0"`)}); err != nil {
		t.Fatalf("parent commit 0: %v", err)
	}
	seqs["commit0"] = j.Seq()
	next := open(t, c, task(runID, 1, 1, 1))
	seqs["claim1"] = next.Seq()
	if _, err := next.Model(ctx, model.Respond); err != nil {
		t.Fatalf("parent m3: %v", err)
	}
	seqs["m3"] = next.Seq()
	if _, err := next.Commit(ctx, Commit{Action: ActionComplete}); err != nil {
		t.Fatalf("parent commit 1: %v", err)
	}
	return seqs
}

func writeFork(t *testing.T, c client, child, parent string, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	if _, err := WriteFork(ctx, c.js, c.pub, child, Fork{
		ParentRunID: parent, ParentStepID: "s", ParentSeq: seq,
	}); err != nil {
		t.Fatalf("WriteFork %s at %d: %v", parent, seq, err)
	}
}

func TestFork_ReplaysParentUpToTheSequenceThenDiverges(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	before := len(readRecords(t, c, "parent"))
	writeFork(t, c, "child", "parent", seqs["search"])
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	j := open(t, c, task("child", 0, 1, 1))
	model := NewScriptedModel([]byte(`"x2"`))
	var searches, sends atomic.Int32
	first, err := j.Model(ctx, model.Respond)
	if err != nil || string(first) != `"m1"` {
		t.Fatalf("first model call = %s, %v; want the parent's m1", first, err)
	}
	result, err := j.Tool(ctx, ToolCall{Name: "search"}, Safe, countingTool(&searches, `"new"`))
	if err != nil || string(result.Output) != `"r1"` {
		t.Fatalf("search = %s, %v; want the parent's r1", result.Output, err)
	}
	second, err := j.Model(ctx, model.Respond)
	if err != nil || string(second) != `"x2"` {
		t.Fatalf("second model call = %s, %v; want a fresh x2", second, err)
	}
	if _, err := j.Tool(ctx, ToolCall{Name: "send"}, Unsafe, countingTool(&sends, `"s"`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if searches.Load() != 0 || model.Requests() != 1 || sends.Load() != 1 {
		t.Fatalf("searches %d, model requests %d, sends %d; want 0, 1, 1",
			searches.Load(), model.Requests(), sends.Load())
	}
	if after := len(readRecords(t, c, "parent")); after != before {
		t.Fatalf("parent journal grew from %d to %d records", before, after)
	}
	if state := j.State(); state != nil {
		t.Fatalf("fork inside iteration 0 has state %s, want none", state)
	}
}

func TestFork_LaterIterationStartsFromTheParentsCommittedState(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	writeFork(t, c, "child", "parent", seqs["m3"])
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	j := open(t, c, task("child", 0, 1, 1))
	if string(j.State()) != `"s0"` {
		t.Fatalf("state = %s, want the parent's s0", j.State())
	}
	model := NewScriptedModel([]byte(`"fresh"`))
	got, err := j.Model(ctx, model.Respond)
	if err != nil || string(got) != `"m3"` || model.Requests() != 0 {
		t.Fatalf("model = %s, %v, %d requests; want the parent's m3 replayed",
			got, err, model.Requests())
	}
	got, err = j.Model(ctx, model.Respond)
	if err != nil || string(got) != `"fresh"` {
		t.Fatalf("model after the fork point = %s, %v; want fresh", got, err)
	}
}

func TestFork_AtAClaimInheritsNoClaims(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	writeFork(t, c, "child", "parent", seqs["claim1"])
	j := open(t, c, task("child", 0, 1, 1))
	if j.Restarts() != 0 || len(j.Records()) != 0 {
		t.Fatalf("restarts %d, records %+v; want the parent's claim left out", j.Restarts(), j.Records())
	}
	if string(j.State()) != `"s0"` {
		t.Fatalf("state = %s, want s0", j.State())
	}
}

func TestFork_InARetriedAttemptRerunsTheSupersededAttemptsSafeCalls(t *testing.T) {
	c := forkClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	var parentRuns atomic.Int32
	first := open(t, c, task("parent", 0, 1, 1))
	if _, err := first.Tool(ctx, ToolCall{Name: "search"}, Safe,
		countingTool(&parentRuns, `"old"`)); err != nil {
		t.Fatalf("attempt 1 search: %v", err)
	}
	retry := open(t, c, task("parent", 0, 2, 1))
	if _, err := retry.Model(ctx, NewScriptedModel([]byte(`"m"`)).Respond); err != nil {
		t.Fatalf("attempt 2 model: %v", err)
	}
	writeFork(t, c, "child", "parent", retry.Seq())
	j := open(t, c, task("child", 0, 1, 1))
	model := NewScriptedModel([]byte(`"fresh"`))
	if got, err := j.Model(ctx, model.Respond); err != nil || string(got) != `"m"` {
		t.Fatalf("model = %s, %v; want attempt 2's m replayed", got, err)
	}
	var runs atomic.Int32
	result, err := j.Tool(ctx, ToolCall{Name: "search"}, Safe, countingTool(&runs, `"new"`))
	if err != nil || string(result.Output) != `"new"` || runs.Load() != 1 {
		t.Fatalf("search = %s, %v after %d runs; want attempt 1's result not reused",
			result.Output, err, runs.Load())
	}
}

func TestFork_UnfinishedUnsafeCallAtTheForkPointIsInterrupted(t *testing.T) {
	c := forkClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	parent := open(t, c, task("parent", 0, 1, 1))
	blocked := startBlockedTool(t, parent, "send", Unsafe, false)
	intentSeq := readRecords(t, c, "parent")[1].Seq
	writeFork(t, c, "child", "parent", intentSeq)
	j := open(t, c, task("child", 0, 1, 1))
	var sends atomic.Int32
	result, err := j.Tool(ctx, ToolCall{Name: "send"}, Unsafe, countingTool(&sends, `"again"`))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !result.Interrupted || sends.Load() != 0 {
		t.Fatalf("result %+v after %d runs; want interrupted without a run", result, sends.Load())
	}
	if result.Key != "parent.s.0.0" {
		t.Fatalf("key = %q, want the parent call's key", result.Key)
	}
	close(blocked.release)
	<-blocked.done
}

func TestFork_OfAForkFollowsTheChain(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	writeFork(t, c, "child", "parent", seqs["search"])
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	child := open(t, c, task("child", 0, 1, 1))
	// The child's first call replays the parent's m1; only its second
	// reaches the model.
	childModel := NewScriptedModel([]byte(`"c2"`))
	if _, err := child.Model(ctx, childModel.Respond); err != nil {
		t.Fatalf("child m1: %v", err)
	}
	var runs atomic.Int32
	_, err := child.Tool(ctx, ToolCall{Name: "search"}, Safe, countingTool(&runs, `"x"`))
	if err != nil {
		t.Fatalf("child search: %v", err)
	}
	if _, err := child.Model(ctx, childModel.Respond); err != nil {
		t.Fatalf("child m2: %v", err)
	}
	writeFork(t, c, "grandchild", "child", child.Seq())
	g := open(t, c, task("grandchild", 0, 1, 1))
	model := NewScriptedModel([]byte(`"g"`))
	var got []string
	for range 3 {
		out, err := g.Model(ctx, model.Respond)
		if err != nil {
			t.Fatalf("grandchild model: %v", err)
		}
		got = append(got, string(out))
	}
	want := []string{`"m1"`, `"c2"`, `"g"`}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("grandchild model calls = %v, want %v", got, want)
		}
	}
	if model.Requests() != 1 || runs.Load() != 0 {
		t.Fatalf("requests %d, child search runs %d; want 1 and 0", model.Requests(), runs.Load())
	}
}

func TestWriteFork_RejectsInvalidForkPoints(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	other := parentRun(t, c, "other")
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	cases := []struct {
		name string
		step string
		seq  uint64
		want error
	}{
		{"no journal", "nope", 1, ErrForkNoJournal},
		{"past the tail", "s", 1 << 40, ErrForkPoint},
		{"zero", "s", 0, ErrForkPoint},
		{"another subject", "s", other["m1"], ErrForkPoint},
		{"a commit", "s", seqs["commit0"], ErrForkPoint},
	}
	for i, tc := range cases {
		fork := Fork{ParentRunID: "parent", ParentStepID: tc.step, ParentSeq: tc.seq}
		_, err := WriteFork(ctx, c.js, c.pub, "child-"+strconv.Itoa(i), fork)
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := countRecords(t, c, "child-0"); n != 0 {
		t.Fatalf("a refused fork wrote %d records", n)
	}
	writeFork(t, c, "child", "parent", seqs["m1"])
	_, err := WriteFork(ctx, c.js, c.pub, "child", Fork{
		ParentRunID: "parent", ParentStepID: "s", ParentSeq: seqs["m2"],
	})
	if !errors.Is(err, ErrForkExists) {
		t.Fatalf("second fork into one journal: err = %v, want ErrForkExists", err)
	}
}

func TestFork_RecordShape(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	writeFork(t, c, "child", "parent", seqs["m2"])
	records := readRecords(t, c, "child")
	if len(records) != 1 || records[0].Type != TypeFork {
		t.Fatalf("child journal = %+v, want one fork record", records)
	}
	var body map[string]any
	if err := json.Unmarshal(records[0].Data, &body); err != nil {
		t.Fatalf("decode fork: %v", err)
	}
	want := map[string]any{
		"parent_run_id": "parent", "parent_step_id": "s",
		"parent_subject": "journal.parent.s", "parent_seq": float64(seqs["m2"]),
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("fork body[%q] = %v, want %v (body %s)", key, body[key], value, records[0].Data)
		}
	}
	if len(body) != len(want) {
		t.Fatalf("fork body has extra fields: %s", records[0].Data)
	}
}

func TestWithinReadBound_InheritedRecordsCountTowardTheBound(t *testing.T) {
	base := forkBase{records: make([]Record, recordsMax)}
	if err := withinReadBound(base, nil); err != nil {
		t.Fatalf("a full base alone: %v", err)
	}
	if err := withinReadBound(base, make([]Record, 1)); err == nil {
		t.Fatalf("a full base plus one record passed the bound")
	}
}

func TestCheckStep_ForkBaseAnswersTheParentsIntent(t *testing.T) {
	base := []Record{rec(1, TypeToolIntent, 0, 1, `{"call":0}`)}
	own := []Record{
		rec(10, TypeClaim, 0, 1, `{}`),
		rec(11, TypeToolResult, 0, 1, `{"call":0,"interrupted":true}`),
		commitRec(12, 0, 1, ActionComplete),
	}
	state := dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	history := []protocol.Event{started(0, 1)}
	if v := checkStepFrom("s", state, base, own, history).Violations; len(v) != 0 {
		t.Fatalf("forked step flagged: %v", v)
	}
	v := checkStep("s", state, own, history).Violations
	if !hasViolation(v, "without a tool.intent") {
		t.Fatalf("violations without the base = %v, want an unintended result", v)
	}
}
