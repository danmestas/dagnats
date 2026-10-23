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
// the redelivery of a task keeps its nonce, attempt (stamped by the
// engine on every dispatch, ADR-026) and iteration and differs only in
// delivery count. A crash inside a
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
	"strconv"
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
	// starts counts invocations of the tool function, effects the
	// side effects it completed.
	starts, effects *atomic.Int32
}

// startBlockedTool calls Tool for name on j in a goroutine. The tool
// appends a partial entry, then either blocks before its side effect
// (effectBeforeBlock false) or after it. It returns once the journal
// holds recordsAfter records: the intent and the entry have landed.
func startBlockedTool(
	t *testing.T, j *Journal, name string, replay Replay, effectBeforeBlock bool,
	recordsAfter int,
) *blockedTool {
	t.Helper()
	b := &blockedTool{
		release: make(chan struct{}), done: make(chan error, 1),
		starts: &atomic.Int32{}, effects: &atomic.Int32{},
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
		defer cancel()
		_, err := j.Tool(ctx, ToolCall{Name: name}, replay,
			func(ctx context.Context, call ToolCall) ([]byte, error) {
				b.starts.Add(1)
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
	waitRecords(t, j, recordsAfter)
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
		stalled = startBlockedTool(t, first, "send", m.recorded, false, 3)
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

// unsafeCounters is the side-effect accounting kept outside the
// journal: how often the SDK started the tool and how many effects
// completed.
type unsafeCounters struct{ starts, effects *atomic.Int32 }

// testUnsafeCrashPoint: the first delivery crashes at p; the
// redelivery (same nonce and stamped attempt, higher delivery count)
// runs the handler again, or finds its own commit when the crash was
// after it. The guarantee under test is the
// documented one: the SDK starts an unsafe call at most once per
// position, so a successor never starts it again and is told it was
// interrupted, with the call's key; a holder stalled between its
// intent and its effect may still complete that effect after the
// takeover, which is why effects never exceed starts, and starts
// never exceed 1.
func testUnsafeCrashPoint(t *testing.T, c client, runID string, d dispatch, p crashPoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	counters := unsafeCounters{starts: &atomic.Int32{}, effects: &atomic.Int32{}}
	run := func(j *Journal) (ToolResult, error) {
		return j.Tool(ctx, ToolCall{Name: "charge"}, Unsafe,
			func(context.Context, ToolCall) ([]byte, error) {
				counters.starts.Add(1)
				counters.effects.Add(1)
				return []byte(`"charged"`), nil
			})
	}
	first := open(t, c, task(runID, d.iteration, d.first, 1))
	var stalled *blockedTool
	switch p {
	case crashBeforeIntent:
	case crashAfterIntent, crashAfterToolBeforeResult:
		stalled = startBlockedTool(t, first, "charge", Unsafe, p == crashAfterToolBeforeResult, 3)
		counters = unsafeCounters{starts: stalled.starts, effects: stalled.effects}
	case crashAfterCommitBeforeEvent:
		if _, err := run(first); err != nil {
			t.Fatalf("first Tool: %v", err)
		}
		if _, err := first.Commit(ctx, Commit{Action: ActionComplete}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	second := open(t, c, task(runID, d.iteration, d.redelivered, 2))
	if _, ok := second.Committed(); !ok {
		res, err := run(second)
		if err != nil {
			t.Fatalf("redelivery Tool: %v", err)
		}
		if res.Key != runID+".s."+strconv.Itoa(d.iteration)+".0" {
			t.Errorf("result key = %q, want the call's idempotency key", res.Key)
		}
		settled := p == crashBeforeIntent || p == crashAfterCommitBeforeEvent
		if settled == res.Interrupted {
			t.Errorf("redelivery result = %+v at %s; interrupted should be %v", res, p, !settled)
		}
	} else if p != crashAfterCommitBeforeEvent {
		t.Fatalf("redelivery found a commit at %s on iteration %d", p, d.iteration)
	}
	if stalled != nil {
		if err := stalled.resume(t); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled tool err = %v, want ErrFenced", err)
		}
		if _, err := run(first); !errors.Is(err, ErrFenced) {
			t.Errorf("stalled worker's next Tool err = %v, want ErrFenced", err)
		}
	}
	starts, effects := counters.starts.Load(), counters.effects.Load()
	if starts != 1 || effects > starts {
		t.Errorf("unsafe tool started %d times with %d effects at %s; want exactly one start",
			starts, effects, p)
	}
	if v := checkToolPairs(readRecords(t, c, runID), false); len(v) != 0 {
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
	// The retry is attempt 2 of the same iteration: a safe call runs
	// afresh under the same key, and the next position has its own.
	retry := open(t, c, task("run-key", 1, 2, 3))
	res, err = retry.Tool(ctx, post, Safe, record)
	if err != nil || res.Error != "boom" || res.Key != "run-key.s.1.0" {
		t.Errorf("retry Tool = %+v, %v; want a fresh run under the same key", res, err)
	}
	if _, err := retry.Tool(ctx, ToolCall{Name: "post"}, Safe, record); err != nil {
		t.Fatalf("retry second Tool: %v", err)
	}
	want := []string{"run-key.s.1.0", "run-key.s.1.0", "run-key.s.1.1"}
	if strings.Join(keys, " ") != strings.Join(want, " ") {
		t.Errorf("keys = %v, want %v", keys, want)
	}
	records := readRecords(t, c, "run-key")
	if countType(records, TypeToolIntent) != 3 || countType(records, TypeToolResult) != 3 {
		t.Errorf("records = %+v; want three runs, each with one intent and one result", records)
	}
}

// TestReplay_NewAttemptRerunsSafeCallsAndModelButKeepsUnsafeOutcomes:
// attempt 1's worker dies inside an unsafe call and the engine times
// the attempt out; attempt 2 of the same iteration is the engine
// saying "do it again": safe calls and model requests run afresh,
// while an unsafe call's recorded outcome stands and the unsafe call
// left unfinished is reported interrupted. The dead worker is its own
// handle, blocked in its tool, and is fenced when it resumes.
func TestReplay_NewAttemptRerunsSafeCallsAndModelButKeepsUnsafeOutcomes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	safeRuns, unsafeRuns := &atomic.Int32{}, &atomic.Int32{}
	safe := func(context.Context, ToolCall) ([]byte, error) {
		return []byte(strconv.Itoa(int(safeRuns.Add(1)))), nil
	}
	unsafe := func(context.Context, ToolCall) ([]byte, error) {
		return []byte(strconv.Itoa(int(unsafeRuns.Add(1)))), nil
	}
	model := NewScriptedModel([]byte(`"m1"`), []byte(`"m2"`))
	first := open(t, c, task("run-attempts", 1, 1, 1))
	if _, err := first.Tool(ctx, ToolCall{Name: "read"}, Safe, safe); err != nil {
		t.Fatalf("safe Tool: %v", err)
	}
	if _, err := first.Tool(ctx, ToolCall{Name: "charge"}, Unsafe, unsafe); err != nil {
		t.Fatalf("unsafe Tool: %v", err)
	}
	if _, err := first.Model(ctx, model.Respond); err != nil {
		t.Fatalf("Model: %v", err)
	}
	stalled := startBlockedTool(t, first, "send", Unsafe, false, 6)
	retry := open(t, c, task("run-attempts", 1, 2, 2))
	res, err := retry.Tool(ctx, ToolCall{Name: "read"}, Safe, safe)
	if err != nil || string(res.Output) != "2" {
		t.Errorf("safe Tool on attempt 2 = %+v, %v; want a fresh run", res, err)
	}
	res, err = retry.Tool(ctx, ToolCall{Name: "charge"}, Unsafe, unsafe)
	if err != nil || string(res.Output) != "1" || unsafeRuns.Load() != 1 {
		t.Errorf("unsafe Tool on attempt 2 = %+v, %v after %d runs; want the recorded outcome",
			res, err, unsafeRuns.Load())
	}
	if out, err := retry.Model(ctx, model.Respond); err != nil || string(out) != `"m2"` {
		t.Errorf("Model on attempt 2 = %s, %v; want a fresh request", out, err)
	}
	res, err = retry.Tool(ctx, ToolCall{Name: "send"}, Unsafe, unsafe)
	if err != nil || !res.Interrupted || res.Key != "run-attempts.s.1.2" {
		t.Errorf("unfinished unsafe Tool on attempt 2 = %+v, %v; want interrupted with its key",
			res, err)
	}
	if err := stalled.resume(t); !errors.Is(err, ErrFenced) {
		t.Errorf("stalled tool err = %v, want ErrFenced", err)
	}
	if v := checkToolPairs(readRecords(t, c, "run-attempts"), false); len(v) != 0 {
		t.Errorf("checker violations: %v", v)
	}
}

func TestReplay_MismatchedCallOnRecoveryIsAnError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	runs := &atomic.Int32{}
	tool := func(context.Context, ToolCall) ([]byte, error) {
		runs.Add(1)
		return []byte(`"ok"`), nil
	}
	first := open(t, c, task("run-mismatch", 1, 1, 1))
	call := ToolCall{Name: "read", Input: json.RawMessage(`{"path": "a"}`)}
	if _, err := first.Tool(ctx, call, Safe, tool); err != nil {
		t.Fatalf("Tool: %v", err)
	}
	second := open(t, c, task("run-mismatch", 1, 1, 2))
	for _, wrong := range []ToolCall{
		{Name: "write", Input: call.Input},
		{Name: "read", Input: json.RawMessage(`{"path":"b"}`)},
	} {
		if _, err := second.Tool(ctx, wrong, Safe, tool); !errors.Is(err, ErrReplayMismatch) {
			t.Errorf("Tool(%+v) err = %v, want ErrReplayMismatch", wrong, err)
		}
	}
	// The same call, differently formatted, is the same call.
	same := ToolCall{Name: "read", Input: json.RawMessage(`{"path":"a"}`)}
	res, err := second.Tool(ctx, same, Safe, tool)
	if err != nil || string(res.Output) != `"ok"` || runs.Load() != 1 {
		t.Errorf("matching Tool = %+v, %v after %d runs; want the recorded result",
			res, err, runs.Load())
	}
}

func TestReplay_FailedModelRequestKeepsItsPosition(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	first := open(t, c, task("run-model-pos", 1, 1, 1))
	failing := func(context.Context) ([]byte, error) { return nil, errors.New("rate limited") }
	if _, err := first.Model(ctx, failing); err == nil {
		t.Fatal("failing request returned no error")
	}
	model := NewScriptedModel([]byte(`"a"`), []byte(`"b"`))
	if out, err := first.Model(ctx, model.Respond); err != nil || string(out) != `"a"` {
		t.Fatalf("Model after a failure = %s, %v", out, err)
	}
	second := open(t, c, task("run-model-pos", 1, 1, 2))
	out, err := second.Model(ctx, model.Respond)
	if err != nil || string(out) != `"a"` || model.Requests() != 1 {
		t.Errorf("redelivery Model = %s, %v after %d requests; want position 0 reused", out, err,
			model.Requests())
	}
	var response ModelResponse
	records := readRecords(t, c, "run-model-pos")
	for _, r := range records {
		if r.Type == TypeModelResponse {
			if err := json.Unmarshal(r.Data, &response); err != nil {
				t.Fatalf("decode: %v", err)
			}
		}
	}
	if countType(records, TypeModelResponse) != 1 || response.Call != 0 {
		t.Errorf("model records = %+v; want one response at position 0", records)
	}
}

// TestReplay_NewAttemptHonoursPriorUnsafeCallByContentNotPosition: a
// retry (after a timeout, attempt 1's worker dead inside its last
// call) that issues the same unsafe call at another position is
// answered from the prior attempt's intent, with its key, so external
// deduplication keeps working; a different call at that position is
// a fresh one.
func TestReplay_NewAttemptHonoursPriorUnsafeCallByContentNotPosition(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	runs := &atomic.Int32{}
	tool := func(_ context.Context, call ToolCall) ([]byte, error) {
		runs.Add(1)
		return []byte(`"` + call.Key + `"`), nil
	}
	charge := ToolCall{Name: "charge", Input: json.RawMessage(`{"amount": 5}`)}
	first := open(t, c, task("run-content", 1, 1, 1))
	if _, err := first.Tool(ctx, ToolCall{Name: "read"}, Safe, tool); err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, err := first.Tool(ctx, charge, Unsafe, tool); err != nil {
		t.Fatalf("charge: %v", err)
	}
	stalled := startBlockedTool(t, first, "send", Unsafe, false, 6)
	retry := open(t, c, task("run-content", 1, 2, 2))
	// The retry sends first, then charges another amount (a fresh
	// call: same name, other input), then the same amount (answered
	// from attempt 1 under its key), then reads (fresh, position 1).
	res, err := retry.Tool(ctx, ToolCall{Name: "send"}, Unsafe, tool)
	if err != nil || !res.Interrupted || res.Key != "run-content.s.1.2" {
		t.Errorf("send on attempt 2 = %+v, %v; want interrupted under the prior key", res, err)
	}
	other := ToolCall{Name: "charge", Input: json.RawMessage(`{"amount": 7}`)}
	res, err = retry.Tool(ctx, other, Unsafe, tool)
	if err != nil || res.Interrupted || res.Key != "run-content.s.1.0" || runs.Load() != 3 {
		t.Errorf("other charge on attempt 2 = %+v, %v after %d runs; want a fresh unsafe call",
			res, err, runs.Load())
	}
	res, err = retry.Tool(ctx, charge, Unsafe, tool)
	if err != nil || string(res.Output) != `"run-content.s.1.1"` || res.Key != "run-content.s.1.1" {
		t.Errorf("charge on attempt 2 = %+v, %v; want the prior result under its key", res, err)
	}
	if n := runs.Load(); n != 3 {
		t.Errorf("tool ran %d times after the honoured call, want 3: nothing unsafe reran", n)
	}
	res, err = retry.Tool(ctx, ToolCall{Name: "read"}, Safe, tool)
	if err != nil || res.Key != "run-content.s.1.1" || runs.Load() != 4 {
		t.Errorf("read on attempt 2 = %+v, %v after %d runs; want a fresh run at position 1",
			res, err, runs.Load())
	}
	// A redelivery of attempt 2 walks the same calls and reuses what
	// attempt 2 recorded, including the honoured answers.
	again := open(t, c, task("run-content", 1, 2, 3))
	res, err = again.Tool(ctx, ToolCall{Name: "send"}, Unsafe, tool)
	if err != nil || !res.Interrupted || res.Key != "run-content.s.1.2" {
		t.Errorf("send on the redelivery = %+v, %v; want the interrupted answer again", res, err)
	}
	res, err = again.Tool(ctx, other, Unsafe, tool)
	if err != nil || res.Key != "run-content.s.1.0" || runs.Load() != 4 {
		t.Errorf("other charge on the redelivery = %+v, %v after %d runs; want reuse",
			res, err, runs.Load())
	}
	res, err = again.Tool(ctx, charge, Unsafe, tool)
	if err != nil || res.Key != "run-content.s.1.1" || runs.Load() != 4 {
		t.Errorf("charge on the redelivery = %+v, %v after %d runs; want reuse", res, err, runs.Load())
	}
	if err := stalled.resume(t); !errors.Is(err, ErrFenced) {
		t.Errorf("stalled tool err = %v, want ErrFenced", err)
	}
	if v := checkToolPairs(readRecords(t, c, "run-content"), false); len(v) != 0 {
		t.Errorf("checker violations: %v", v)
	}
}

// TestReplay_Iteration0RetryRerunsSafeCalls: with the attempt stamped
// on the first dispatch (ADR-026, finding 1), a retry of iteration 0
// is a new attempt like any other: safe calls run afresh and prior
// unsafe outcomes are honoured, instead of the retry replaying the
// failed attempt's results as a redelivery would.
func TestReplay_Iteration0RetryRerunsSafeCalls(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	safeRuns, unsafeRuns := &atomic.Int32{}, &atomic.Int32{}
	safe := func(context.Context, ToolCall) ([]byte, error) {
		return []byte(strconv.Itoa(int(safeRuns.Add(1)))), nil
	}
	unsafe := func(context.Context, ToolCall) ([]byte, error) {
		return []byte(strconv.Itoa(int(unsafeRuns.Add(1)))), nil
	}
	first := open(t, c, task("run-attempt0", 0, 1, 1))
	if _, err := first.Tool(ctx, ToolCall{Name: "read"}, Safe, safe); err != nil {
		t.Fatalf("safe Tool: %v", err)
	}
	if _, err := first.Tool(ctx, ToolCall{Name: "charge"}, Unsafe, unsafe); err != nil {
		t.Fatalf("unsafe Tool: %v", err)
	}
	retry := open(t, c, task("run-attempt0", 0, 2, 1))
	res, err := retry.Tool(ctx, ToolCall{Name: "read"}, Safe, safe)
	if err != nil || string(res.Output) != "2" || safeRuns.Load() != 2 {
		t.Errorf("safe Tool on attempt 2 of iteration 0 = %+v, %v after %d runs; want a fresh run",
			res, err, safeRuns.Load())
	}
	res, err = retry.Tool(ctx, ToolCall{Name: "charge"}, Unsafe, unsafe)
	if err != nil || string(res.Output) != "1" || unsafeRuns.Load() != 1 {
		t.Errorf("unsafe Tool on attempt 2 of iteration 0 = %+v, %v after %d runs; want the recorded outcome",
			res, err, unsafeRuns.Load())
	}
}

func TestReplay_NonJSONToolOutputIsANamedError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), replayTimeout)
	defer cancel()
	j := open(t, c, task("run-not-json", 1, 1, 1))
	text := func(context.Context, ToolCall) ([]byte, error) {
		return []byte("total 0\n"), nil
	}
	_, err := j.Tool(ctx, ToolCall{Name: "bash"}, Safe, text)
	if err == nil || !strings.Contains(err.Error(), `tool "bash" returned output that is not JSON`) {
		t.Fatalf("Tool err = %v, want it to name the tool and say the output is not JSON", err)
	}
	encoded := func(context.Context, ToolCall) ([]byte, error) {
		return json.Marshal("total 0\n")
	}
	res, err := j.Tool(ctx, ToolCall{Name: "bash"}, Safe, encoded)
	if err != nil || string(res.Output) != `"total 0\n"` {
		t.Errorf("Tool with JSON output = %+v, %v; want the encoded text", res, err)
	}
}
