// journal/replay_test.go
// Tool replay and model-response reuse (ADR-026, #805): the full
// replay matrix (recorded Safe/Unsafe x current Safe/Unsafe x result
// present/absent); an unsafe tool's side effect happens at most once
// across every crash point, including a redelivery of the same task
// and a stalled worker that resumes after its takeover; recorded
// model responses are reused, not re-requested; a tool's idempotency
// key is stable across deliveries and attempts.
//
// Methodology: one embedded server per case, through the public
// Open/Tool/Model surface, modelling dispatch as the engine does it:
// the redelivery of a task keeps its nonce, attempt (from iteration 1)
// and iteration and differs only in delivery count. A crash inside a
// tool is a claim whose tool function blocks on a channel while the
// redelivery takes over; releasing it afterwards is the stalled worker
// resuming. The side-effect counter lives outside the journal. Every
// wait is bounded; every case has a positive and a negative assertion.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const replayTimeout = 10 * time.Second

// forEachDispatchSingle runs fn per dispatch shape on one embedded
// server, with iteration 0 committed when the shape starts at 1.
func forEachDispatchSingle(
	t *testing.T, fn func(t *testing.T, c client, runID string, d dispatch),
) {
	t.Helper()
	for _, d := range dispatches() {
		t.Run(d.name, func(t *testing.T) {
			_, nc := natsutil.StartTestServer(t)
			if err := natsutil.SetupAll(nc); err != nil {
				t.Fatalf("SetupAll: %v", err)
			}
			c := newClient(t, nc)
			runID := natsutil.SubjectToken(strings.ReplaceAll(t.Name(), "/", "-"))
			if d.iteration == 1 {
				ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
				defer cancel()
				prior := open(t, c, task(runID, 0, 1, 1))
				if _, err := prior.Commit(ctx, Commit{
					Action: ActionContinue, State: []byte("s0"),
				}); err != nil {
					t.Fatalf("commit iteration 0: %v", err)
				}
			}
			fn(t, c, runID, d)
		})
	}
}

// blockedTool is a tool call in flight on a claim that will be taken
// over: fn blocks on release, and done reports Tool's return.
type blockedTool struct {
	release chan struct{}
	done    chan error
	effects *atomic.Int32
}

// startBlockedTool calls Tool on j in a goroutine. The tool appends a
// partial entry, then either blocks before its side effect
// (effectBeforeBlock false) or after it.
func startBlockedTool(
	t *testing.T, j *Journal, replay Replay, effectBeforeBlock bool,
) *blockedTool {
	t.Helper()
	b := &blockedTool{
		release: make(chan struct{}), done: make(chan error, 1), effects: &atomic.Int32{},
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
		defer cancel()
		_, err := j.Tool(ctx, ToolCall{Name: "send"}, replay,
			func(ctx context.Context, call ToolCall) ([]byte, error) {
				if _, err := j.Append(ctx, TypeEntry, []byte(`"half"`)); err != nil {
					return nil, err
				}
				if effectBeforeBlock {
					b.effects.Add(1)
				}
				<-b.release
				if !effectBeforeBlock {
					b.effects.Add(1)
				}
				return []byte(`"sent"`), nil
			})
		b.done <- err
	}()
	// The intent and the partial entry must land before the takeover.
	waitRecords(t, j, 3)
	return b
}

// waitRecords polls until the step's journal holds at least n records.
func waitRecords(t *testing.T, j *Journal, n int) {
	t.Helper()
	deadline := time.Now().Add(replayTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
		records, err := Read(ctx, j.js, j.task.RunID, j.task.StepID)
		cancel()
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(records) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("journal never reached %d records", n)
}

// resume releases the stalled claim and returns Tool's error.
func (b *blockedTool) resume(t *testing.T) error {
	t.Helper()
	close(b.release)
	select {
	case err := <-b.done:
		return err
	case <-time.After(replayTimeout):
		t.Fatal("stalled tool never returned")
		return nil
	}
}

func readRecords(t *testing.T, c client, runID string) []Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	records, err := Read(ctx, c.js, runID, "s")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return records
}

func countType(records []Record, typ string) int {
	n := 0
	for _, r := range records {
		if r.Type == typ {
			n++
		}
	}
	return n
}

type matrixCase struct {
	recorded, current Replay
	resultPresent     bool
}

func matrix() []matrixCase {
	var cases []matrixCase
	for _, recorded := range []Replay{Safe, Unsafe} {
		for _, current := range []Replay{Safe, Unsafe} {
			for _, present := range []bool{true, false} {
				cases = append(cases, matrixCase{recorded, current, present})
			}
		}
	}
	return cases
}

func (m matrixCase) String() string {
	result := "absent"
	if m.resultPresent {
		result = "present"
	}
	return fmt.Sprintf("recorded-%s/current-%s/result-%s", m.recorded, m.current, result)
}

func TestReplay_Matrix(t *testing.T) {
	for _, m := range matrix() {
		t.Run(m.String(), func(t *testing.T) {
			forEachDispatchSingle(t, func(t *testing.T, c client, runID string, d dispatch) {
				testReplayCase(t, c, runID, d, m)
			})
		})
	}
}

// testReplayCase: the first delivery records the intent (and the
// result when present); the redelivery calls the same tool under the
// current declaration. A present result is reused in every case; an
// absent one is rerun only when both declarations are Safe, else the
// redelivery sees an interrupted result carrying the partial output.
func testReplayCase(t *testing.T, c client, runID string, d dispatch, m matrixCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	first := open(t, c, task(runID, d.iteration, d.first, 1))
	var stalled *blockedTool
	effects := &atomic.Int32{}
	if m.resultPresent {
		res, err := first.Tool(ctx, ToolCall{Name: "send"}, m.recorded,
			func(context.Context, ToolCall) ([]byte, error) {
				effects.Add(1)
				return []byte(`"sent"`), nil
			})
		if err != nil || string(res.Output) != `"sent"` {
			t.Fatalf("first Tool = %+v, %v", res, err)
		}
	} else {
		stalled = startBlockedTool(t, first, m.recorded, false)
		effects = stalled.effects
	}
	before := len(readRecords(t, c, runID))
	second := open(t, c, task(runID, d.iteration, d.redelivered, 2))
	res, err := second.Tool(ctx, ToolCall{Name: "send"}, m.current,
		func(context.Context, ToolCall) ([]byte, error) {
			effects.Add(1)
			return []byte(`"sent-again"`), nil
		})
	if err != nil {
		t.Fatalf("redelivery Tool: %v", err)
	}
	after := readRecords(t, c, runID)
	switch {
	case m.resultPresent:
		if res.Interrupted || string(res.Output) != `"sent"` || effects.Load() != 1 {
			t.Errorf("result = %+v, effects = %d; want the recorded result reused", res, effects.Load())
		}
		if len(after) != before+1 {
			t.Errorf("records %d -> %d; a reused result appends only the claim", before, len(after))
		}
	case m.recorded == Safe && m.current == Safe:
		if res.Interrupted || string(res.Output) != `"sent-again"` || effects.Load() != 1 {
			t.Errorf("result = %+v, effects = %d; want the safe tool rerun", res, effects.Load())
		}
	default:
		if !res.Interrupted || res.Error == "" || effects.Load() != 0 {
			t.Errorf("result = %+v, effects = %d; want interrupted, tool not rerun", res, effects.Load())
		}
		if string(res.Partial) != `["half"]` {
			t.Errorf("partial = %s, want the entry the stalled tool wrote", res.Partial)
		}
	}
	if countType(after, TypeToolIntent) != 1 || countType(after, TypeToolResult) != 1 {
		t.Errorf("records = %+v; want one intent and one result for the call", after)
	}
	if stalled != nil {
		if err := stalled.resume(t); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled tool err = %v, want ErrFenced", err)
		}
		if n := effects.Load(); m.current != Safe && n > 1 {
			t.Errorf("effects = %d after the stalled worker resumed, want at most 1", n)
		}
	}
}

// crashPoint is where the first delivery dies.
type crashPoint int

const (
	crashBeforeIntent crashPoint = iota
	crashAfterIntent
	crashAfterToolBeforeResult
	crashAfterCommitBeforeEvent
)

func (p crashPoint) String() string {
	return [...]string{
		"before-intent", "after-intent", "after-tool-before-result", "after-commit-before-event",
	}[p]
}

func TestReplay_UnsafeSideEffectAtMostOnceAcrossCrashPoints(t *testing.T) {
	for _, p := range []crashPoint{
		crashBeforeIntent, crashAfterIntent, crashAfterToolBeforeResult, crashAfterCommitBeforeEvent,
	} {
		t.Run(p.String(), func(t *testing.T) {
			forEachDispatchSingle(t, func(t *testing.T, c client, runID string, d dispatch) {
				testUnsafeCrashPoint(t, c, runID, d, p)
			})
		})
	}
}

// testUnsafeCrashPoint: the first delivery crashes at p; the
// redelivery (same nonce, attempt from iteration 1, higher delivery
// count) runs the handler again. The unsafe tool's effect counter,
// kept outside the journal, never exceeds 1, and the stalled worker,
// once it resumes, is fenced before it can add to it.
func testUnsafeCrashPoint(t *testing.T, c client, runID string, d dispatch, p crashPoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	effects := &atomic.Int32{}
	run := func(j *Journal) (ToolResult, error) {
		return j.Tool(ctx, ToolCall{Name: "charge"}, Unsafe,
			func(context.Context, ToolCall) ([]byte, error) {
				effects.Add(1)
				return []byte(`"charged"`), nil
			})
	}
	first := open(t, c, task(runID, d.iteration, d.first, 1))
	var stalled *blockedTool
	switch p {
	case crashBeforeIntent:
	case crashAfterIntent:
		stalled = startBlockedTool(t, first, Unsafe, false)
		effects = stalled.effects
	case crashAfterToolBeforeResult:
		stalled = startBlockedTool(t, first, Unsafe, true)
		effects = stalled.effects
	case crashAfterCommitBeforeEvent:
		if _, err := run(first); err != nil {
			t.Fatalf("first Tool: %v", err)
		}
		if _, err := first.Commit(ctx, Commit{Action: ActionComplete}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	second := open(t, c, task(runID, d.iteration, d.redelivered, 2))
	if _, ok := second.Committed(); ok {
		if p != crashAfterCommitBeforeEvent || d.iteration == 0 {
			t.Fatalf("redelivery found a commit at %s on iteration %d", p, d.iteration)
		}
	} else {
		res, err := run(second)
		if err != nil {
			t.Fatalf("redelivery Tool: %v", err)
		}
		settled := p == crashBeforeIntent || (p == crashAfterCommitBeforeEvent && d.iteration == 0)
		if settled == res.Interrupted {
			t.Errorf("redelivery result = %+v at %s; interrupted should be %v", res, p, !settled)
		}
	}
	if stalled != nil {
		if err := stalled.resume(t); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled tool err = %v, want ErrFenced", err)
		}
		// The resumed worker tries another call: the fence holds.
		if _, err := run(first); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled worker's next Tool err = %v, want ErrFenced", err)
		}
	}
	if n := effects.Load(); n > 1 {
		t.Errorf("unsafe tool ran %d times across crash point %s, want at most 1", n, p)
	}
	if n := effects.Load(); n == 0 && p != crashAfterIntent {
		t.Errorf("unsafe tool never ran at crash point %s", p)
	}
	if v := checkToolPairs(readRecords(t, c, runID)); len(v) != 0 {
		t.Errorf("checker violations: %v", v)
	}
}

func TestReplay_ModelResponseReusedNotReRequested(t *testing.T) {
	forEachDispatchSingle(t, func(t *testing.T, c client, runID string, d dispatch) {
		ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
		defer cancel()
		model := NewScriptedModel([]byte(`"a"`), []byte(`"b"`))
		first := open(t, c, task(runID, d.iteration, d.first, 1))
		out, err := first.Model(ctx, model.Respond)
		if err != nil || string(out) != `"a"` {
			t.Fatalf("first Model = %s, %v", out, err)
		}
		second := open(t, c, task(runID, d.iteration, d.redelivered, 2))
		out, err = second.Model(ctx, model.Respond)
		if err != nil || string(out) != `"a"` || model.Requests() != 1 {
			t.Errorf("redelivery Model = %s, %v after %d requests; want \"a\" reused",
				out, err, model.Requests())
		}
		out, err = second.Model(ctx, model.Respond)
		if err != nil || string(out) != `"b"` || model.Requests() != 2 {
			t.Errorf("second Model = %s, %v after %d requests; want \"b\" requested",
				out, err, model.Requests())
		}
		if _, err := first.Model(ctx, model.Respond); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled Model err = %v, want ErrFenced", err)
		}
		if model.Requests() != 2 {
			t.Errorf("requests = %d, want 2: a fenced claim requests nothing", model.Requests())
		}
	})
}

func TestReplay_IdempotencyKeyIsStableAcrossDeliveriesAndAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	var keys []string
	record := func(_ context.Context, call ToolCall) ([]byte, error) {
		keys = append(keys, call.Key)
		return nil, errors.New("boom")
	}
	first := open(t, c, task("run-key", 1, 1, 1))
	post := ToolCall{Name: "post", Input: json.RawMessage(`{"n":1}`)}
	res, err := first.Tool(ctx, post, Safe, record)
	if err != nil || res.Error != "boom" || res.Interrupted {
		t.Fatalf("Tool = %+v, %v; want the tool's error recorded, not interrupted", res, err)
	}
	if _, err := first.Commit(ctx, Commit{Action: ActionFail, Error: "boom"}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The retry is attempt 2 of the same iteration: it resumes from
	// the journal, so call 0 is reused and call 1 is new.
	retry := open(t, c, task("run-key", 1, 2, 3))
	res, err = retry.Tool(ctx, ToolCall{Name: "post"}, Safe, record)
	if err != nil || res.Error != "boom" || len(keys) != 1 {
		t.Errorf("retry Tool = %+v, %v after %d runs; want the recorded error reused",
			res, err, len(keys))
	}
	if _, err := retry.Tool(ctx, ToolCall{Name: "post"}, Safe, record); err != nil {
		t.Fatalf("retry second Tool: %v", err)
	}
	if len(keys) != 2 || keys[0] != "run-key.s.1.0" || keys[1] != "run-key.s.1.1" {
		t.Errorf("keys = %v, want run-key.s.1.0 then run-key.s.1.1", keys)
	}
	records := readRecords(t, c, "run-key")
	if countType(records, TypeToolIntent) != 2 || countType(records, TypeToolResult) != 2 {
		t.Errorf("records = %+v; want two calls, each with one intent and one result", records)
	}
}
