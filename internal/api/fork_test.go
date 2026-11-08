// internal/api/fork_test.go
// Forked runs (#810): POST /runs/{run}/fork and api.runs.fork start a
// new run of the parent's workflow whose forked step replays the
// parent's journal up to the sequence and then runs on; steps before
// the forked step are reused from the parent, not executed again; the
// parent run and journal are left as they were; the checker passes on
// the forked run. Unknown runs and steps, steps without a journal and
// sequences that are not fork points are refused before anything is
// written.
//
// Methodology: one embedded server per test with a real orchestrator,
// workers and the REST handler. Workflow a -> s -> c, where s is an
// agent step that asks a model, runs a safe "search" tool and asks
// again. Handlers, the model and the tool count their calls, so a
// replayed call is told from a re-executed one. Every wait is bounded;
// each test has positive and negative assertions.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const forkWait = 20 * time.Second

// forkFixture is a finished parent run of fork-wf and its counters.
type forkFixture struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	svc      *Service
	server   *httptest.Server
	parent   string
	calls    map[string]*atomic.Int32
	model    *atomic.Int32
	searches *atomic.Int32
}

func newForkFixture(t *testing.T) *forkFixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	f := &forkFixture{
		nc: nc, js: js, svc: NewService(nc),
		calls: map[string]*atomic.Int32{"a": {}, "s": {}, "c": {}},
		model: &atomic.Int32{}, searches: &atomic.Int32{},
	}
	f.server = httptest.NewServer(NewRESTHandler(f.svc))
	t.Cleanup(f.server.Close)
	f.startWorker(t)
	wb := dag.NewWorkflow("fork-wf")
	wb.Task("a", "fork-a")
	wb.Task("s", "fork-s")
	wb.DependsOn("a")
	wb.Task("c", "fork-c")
	wb.DependsOn("s")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	if err := f.svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	f.parent, err = f.svc.StartRun(ctx, "fork-wf", []byte(`{"topic":"x"}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	f.waitCompleted(t, f.parent)
	return f
}

func (f *forkFixture) startWorker(t *testing.T) {
	t.Helper()
	w := worker.NewWorker(f.nc)
	plain := func(id string) worker.HandlerFunc {
		return func(tc worker.TaskContext) error {
			f.calls[id].Add(1)
			return tc.Complete([]byte(`"` + id + `-out"`))
		}
	}
	w.Handle("fork-a", plain("a"))
	w.Handle("fork-c", plain("c"))
	w.Handle("fork-s", f.agentStep)
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// agentStep asks the model, searches, asks again and completes with
// both answers.
func (f *forkFixture) agentStep(tc worker.TaskContext) error {
	f.calls["s"].Add(1)
	j, err := tc.Journal()
	if err != nil {
		return err
	}
	ctx := tc.Context()
	respond := func(context.Context) ([]byte, error) {
		return []byte(strconv.Quote("resp-" + strconv.Itoa(int(f.model.Add(1))))), nil
	}
	first, err := j.Model(ctx, respond)
	if err != nil {
		return err
	}
	_, err = j.Tool(ctx, journal.ToolCall{Name: "search"}, journal.Safe,
		func(context.Context, journal.ToolCall) ([]byte, error) {
			f.searches.Add(1)
			return []byte(`"found"`), nil
		})
	if err != nil {
		return err
	}
	second, err := j.Model(ctx, respond)
	if err != nil {
		return err
	}
	return tc.Complete([]byte(fmt.Sprintf("[%s,%s]", first, second)))
}

func (f *forkFixture) waitCompleted(t *testing.T, runID string) dag.WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(forkWait)
	for time.Now().Before(deadline) {
		run, err := f.svc.GetRun(context.Background(), runID)
		if err == nil && run.Status == dag.RunStatusCompleted {
			return run
		}
		if err == nil && run.Status.IsTerminal() {
			t.Fatalf("run %s ended %s", runID, run.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not complete within %s", runID, forkWait)
	return dag.WorkflowRun{}
}

// recordSeq is the sequence of the parent's first record of typ.
func (f *forkFixture) recordSeq(t *testing.T, runID, typ string) (uint64, int) {
	t.Helper()
	records, err := journal.Read(context.Background(), f.js, runID, "s")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for _, r := range records {
		if r.Type == typ {
			return r.Seq, len(records)
		}
	}
	t.Fatalf("no %s record in %s", typ, runID)
	return 0, 0
}

func (f *forkFixture) postFork(t *testing.T, runID, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(f.server.URL+"/runs/"+runID+"/fork", "application/json",
		bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST fork: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		out = nil
	}
	return resp.StatusCode, out
}

func TestForkRun_ReplaysTheParentAndReusesEarlierSteps(t *testing.T) {
	f := newForkFixture(t)
	seq, parentRecords := f.recordSeq(t, f.parent, journal.TypeToolResult)
	status, body := f.postFork(t, f.parent, fmt.Sprintf(`{"step":"s","seq":%d}`, seq))
	if status != http.StatusCreated {
		t.Fatalf("fork status = %d, body %v; want 201", status, body)
	}
	child, _ := body["run_id"].(string)
	if child == "" || child == f.parent {
		t.Fatalf("fork run_id = %q", child)
	}
	run := f.waitCompleted(t, child)
	if got := string(run.Steps["s"].Output); got != `["resp-1","resp-3"]` {
		t.Fatalf("forked s output = %s; want the parent's first answer, then a fresh one", got)
	}
	if f.calls["a"].Load() != 1 || f.calls["s"].Load() != 2 || f.calls["c"].Load() != 2 {
		t.Fatalf("handler calls a=%d s=%d c=%d; want a reused (1), s and c run again (2)",
			f.calls["a"].Load(), f.calls["s"].Load(), f.calls["c"].Load())
	}
	if f.model.Load() != 3 || f.searches.Load() != 1 {
		t.Fatalf("model requests %d, searches %d; want 3 and 1", f.model.Load(), f.searches.Load())
	}
	if string(run.Steps["a"].Output) != `"a-out"` || run.ForkedFrom == nil ||
		run.ForkedFrom.RunID != f.parent || run.ForkedFrom.JournalSeq != seq {
		t.Fatalf("forked run a=%s forked_from=%+v", run.Steps["a"].Output, run.ForkedFrom)
	}
	if string(run.Input) != `{"topic":"x"}` {
		t.Fatalf("forked input = %s, want the parent's", run.Input)
	}
	if _, after := f.recordSeq(t, f.parent, journal.TypeToolResult); after != parentRecords {
		t.Fatalf("parent journal went from %d to %d records", parentRecords, after)
	}
	report, err := journal.Check(context.Background(), f.nc, child)
	if err != nil || !report.OK() {
		t.Fatalf("journal check on the fork: %+v, %v", report, err)
	}
}

func TestForkRun_RefusesInvalidRequests(t *testing.T) {
	f := newForkFixture(t)
	commitSeq, _ := f.recordSeq(t, f.parent, journal.TypeCommit)
	cases := []struct {
		name, run, body string
		want            int
	}{
		{"unknown run", "nope", `{"step":"s","seq":1}`, http.StatusNotFound},
		{"unknown step", f.parent, `{"step":"zz","seq":1}`, http.StatusNotFound},
		{"step without a journal", f.parent, `{"step":"a","seq":1}`, http.StatusBadRequest},
		{"seq out of range", f.parent, `{"step":"s","seq":99999999}`, http.StatusBadRequest},
		{"seq on a commit", f.parent,
			fmt.Sprintf(`{"step":"s","seq":%d}`, commitSeq), http.StatusBadRequest},
		{"no seq", f.parent, `{"step":"s"}`, http.StatusBadRequest},
		{"bad json", f.parent, `{`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		if status, body := f.postFork(t, tc.run, tc.body); status != tc.want {
			t.Errorf("%s: status %d (%v), want %d", tc.name, status, body, tc.want)
		}
	}
	resp, err := http.Get(f.server.URL + "/runs/" + f.parent + "/fork")
	if err != nil {
		t.Fatalf("GET fork: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET fork status = %d, want 405", resp.StatusCode)
	}
	runs, err := f.svc.ListRuns(context.Background(), RunsFilter{}, 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("%d runs after refused forks, want only the parent", len(runs.Runs))
	}
}

func TestForkRun_OverTheNATSAPI(t *testing.T) {
	f := newForkFixture(t)
	natsAPI := NewNATSAPI(f.svc, f.nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	seq, _ := f.recordSeq(t, f.parent, journal.TypeToolResult)
	request := func(body string) map[string]any {
		reply, err := f.nc.Request("api.runs.fork", []byte(body), 5*time.Second)
		if err != nil {
			t.Fatalf("api.runs.fork: %v", err)
		}
		var out map[string]any
		if err := json.Unmarshal(reply.Data, &out); err != nil {
			t.Fatalf("decode reply %s: %v", reply.Data, err)
		}
		return out
	}
	out := request(fmt.Sprintf(`{"run_id":%q,"step":"s","seq":%d}`, f.parent, seq))
	child, _ := out["run_id"].(string)
	if child == "" || out["error"] != nil {
		t.Fatalf("fork reply = %v, want a run id", out)
	}
	f.waitCompleted(t, child)
	refused := request(fmt.Sprintf(`{"run_id":%q,"step":"a","seq":%d}`, f.parent, seq))
	if refused["error"] == nil || refused["run_id"] != nil {
		t.Fatalf("fork of a step without a journal = %v, want an error", refused)
	}
}

func TestForkRunErrors_AreTyped(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &ForkError{NotFound: true, Msg: "step zz"})
	var forkErr *ForkError
	if !errors.As(err, &forkErr) || !forkErr.NotFound {
		t.Fatalf("errors.As(%v) = %+v", err, forkErr)
	}
	if (&ForkError{Msg: "bad"}).Error() != "fork: bad" {
		t.Fatalf("message = %q", (&ForkError{Msg: "bad"}).Error())
	}
}
