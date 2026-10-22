// e2e/durability/harness_test.go
// Shared fixture for the crash-point matrix, the crash-loop test and
// the chaos soak (#809).
//
// Methodology: one embedded NATS server (or an in-process three-node
// cluster) with one or more real orchestrators sharing the
// WORKFLOW_HISTORY durable (fixtureOptions), and, per agent, a one-step agent-loop
// workflow on its own task type with two real Workers and a short
// AckWait. A Worker handles its deliveries one at a time, so a stalled
// handler holds that worker's prefetched deliveries past their
// AckWait: a task type per agent keeps the matrix's parallel cells
// from stalling each other, while the soak shares one agent across
// its runs on purpose. The scripted agent is a deterministic model
// script of safe and unsafe tool calls ending in a final answer. Tools
// count their starts and effects in a KV bucket outside the journal,
// so the audit can prove an unsafe tool ran at most once per position.
// Crashes are injected through internal/chaos; the hook routes by run
// so parallel runs do not interfere. After every run the engine's
// snapshot, the journal audit and the counters are checked against a
// crash-free baseline. Every wait is bounded.
package durability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/chaos"
	enginepkg "github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	stepID = "s"
	// ackWait is short so a stalled delivery is redelivered quickly;
	// stallFor is the stall that outlives it.
	ackWait  = 500 * time.Millisecond
	stallFor = 3 * ackWait
	// runTimeout bounds one run end to end, including redeliveries
	// after kills and the engine's redelivery backoff.
	runTimeout = 40 * time.Second
	// effectsBucket is where tools count starts and effects.
	effectsBucket = "durability_effects"
	// counterRetryMax bounds the CAS loop of a counter bump.
	counterRetryMax = 50
	// historyReadMax bounds one history read.
	historyReadMax = 10_000
	// engineBackoff is the engines' history redelivery schedule: a
	// killed dispatch is retried within it instead of the 5s default.
	engineBackoff    = 200 * time.Millisecond
	engineBackoffLen = 8
	// workersPerAgent is how many Workers consume an agent's task type.
	workersPerAgent = 2
	// toolCharge is the unsafe tool, toolLookup the safe one.
	toolCharge = "charge"
	toolLookup = "lookup"
)

// turn is one scripted model response: a tool call or the final answer.
type turn struct {
	Tool  string          `json:"tool,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	Final string          `json:"final,omitempty"`
}

// script is the agent's model script by iteration: a safe call, an
// unsafe call, a safe call, then the answer. Iteration 1 holds the
// unsafe call, so that is where the matrix crashes.
var script = []turn{
	{Tool: toolLookup, Input: json.RawMessage(`{"q":"a"}`)},
	{Tool: toolCharge, Input: json.RawMessage(`{"amount":1}`)},
	{Tool: toolLookup, Input: json.RawMessage(`{"q":"b"}`)},
	{Final: "done"},
}

// unsafeIteration is the iteration whose tool call is unsafe;
// safeIteration is the safe call after it.
const (
	unsafeIteration = 1
	safeIteration   = 2
)

// answer is what the agent completes with: every tool outcome in
// order, then the final text.
type answer struct {
	Calls []string `json:"calls"`
	Final string   `json:"final"`
}

// fixture is one server, two engines, the API service and the
// counters; agents are started on it.
type fixture struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	svc      *api.Service
	effects  jetstream.KeyValue
	cluster  bool
	mu       sync.Mutex
	requests map[string]int
	agents   int
}

// agent is one workflow def on its own task type with its workers.
type agent struct {
	f        *fixture
	def      dag.WorkflowDef
	taskType string
}

// fixtureOptions is what newFixture starts: a standalone server or an
// in-process three-node cluster, and how many orchestrators share the
// history durable. Every test runs two, as a cluster does: the engine
// handling a worker's event may then load the step before the other
// engine has applied the same delivery's step.started, which is the
// ordering findings 2 and 3 of ADR-026 ("How durability is verified")
// needed.
type fixtureOptions struct {
	cluster bool
	engines int
}

// newFixture starts the server, the engines and the service.
func newFixture(t *testing.T, opts fixtureOptions) *fixture {
	t.Helper()
	if opts.engines < 1 || opts.engines > 4 {
		panic("newFixture: engines must be 1..4")
	}
	cluster := opts.cluster
	var nc *nats.Conn
	if cluster {
		nc = dagnatstest.StartTestCluster(t, 3)
	} else {
		_, nc = natsutil.StartTestServer(t)
	}
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	effects, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: effectsBucket, History: 1,
	})
	if err != nil {
		t.Fatalf("effects bucket: %v", err)
	}
	f := &fixture{
		nc: nc, js: js, effects: effects, cluster: cluster,
		svc: harness.NewTestService(t, nc), requests: map[string]int{},
	}
	f.startEngines(t, opts.engines)
	return f
}

// startEngines starts n orchestrators on the shared history durable.
func (f *fixture) startEngines(t *testing.T, n int) {
	t.Helper()
	if f.nc == nil {
		panic("startEngines: fixture has no connection")
	}
	if n < 1 {
		panic("startEngines: n must be positive")
	}
	schedule := make([]time.Duration, engineBackoffLen)
	for i := range schedule {
		schedule[i] = engineBackoff
	}
	for i := 0; i < n; i++ {
		orch := enginepkg.NewOrchestrator(f.nc, enginepkg.WithHistoryRedeliverBackoff(schedule))
		orch.Start()
		t.Cleanup(orch.Stop)
	}
}

// newAgent registers a one-step agent-loop workflow on a fresh task
// type, with a retry policy so a restart_limit failure is retried as
// ADR-028 describes, and starts its workers.
func (f *fixture) newAgent(t *testing.T) *agent {
	t.Helper()
	f.mu.Lock()
	f.agents++
	n := f.agents
	f.mu.Unlock()
	a := &agent{f: f, taskType: fmt.Sprintf("durability-agent-%d", n)}
	a.def = dag.WorkflowDef{
		Name: harness.UniqueName(t, "durability"), Version: "1",
		Steps: []dag.StepDef{{
			ID: stepID, Task: a.taskType, Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 10}),
			Retry: &dag.RetryPolicy{
				MaxAttempts: 3, Strategy: dag.RetryFixed,
				InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond,
			},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.svc.RegisterWorkflow(ctx, a.def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	for i := 0; i < workersPerAgent; i++ {
		w := worker.NewWorker(f.nc)
		w.Handle(a.taskType, f.handle, worker.WithAckWait(ackWait))
		w.Start()
		t.Cleanup(w.Stop)
	}
	return a
}

// handle is the scripted agent: one model turn per iteration.
func (f *fixture) handle(tc worker.TaskContext) error {
	j, err := tc.Journal()
	if err != nil {
		return err
	}
	iteration := j.Task().Iteration
	if iteration >= len(script) {
		return fmt.Errorf("iteration %d past the script", iteration)
	}
	response, err := j.Model(tc.Context(), f.model(tc.RunID(), iteration))
	if err != nil {
		return err
	}
	var next turn
	if err := json.Unmarshal(response, &next); err != nil {
		return fmt.Errorf("decode model response: %w", err)
	}
	var state answer
	if saved := j.State(); len(saved) > 0 {
		if err := json.Unmarshal(saved, &state); err != nil {
			return fmt.Errorf("decode state: %w", err)
		}
	}
	if next.Final != "" {
		state.Final = next.Final
		output, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return tc.Complete(output)
	}
	result, err := j.Tool(tc.Context(), journal.ToolCall{Name: next.Tool, Input: next.Input},
		replayOf(next.Tool), f.tool)
	if err != nil {
		return err
	}
	state.Calls = append(state.Calls, outcome(result))
	saved, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := tc.Checkpoint(saved); err != nil {
		return err
	}
	return tc.Continue(nil)
}

// model is the ModelFunc for one iteration: the scripted turn, and a
// count of requests per run so the audit can see re-requests.
func (f *fixture) model(runID string, iteration int) journal.ModelFunc {
	if iteration < 0 || iteration >= len(script) {
		panic("model: iteration out of script")
	}
	if runID == "" {
		panic("model: runID must not be empty")
	}
	return func(context.Context) ([]byte, error) {
		f.mu.Lock()
		f.requests[runID]++
		f.mu.Unlock()
		return json.Marshal(script[iteration])
	}
}

// replayOf is the tool's replay declaration.
func replayOf(tool string) journal.Replay {
	switch tool {
	case toolCharge:
		return journal.Unsafe
	case toolLookup:
		return journal.Safe
	}
	panic("replayOf: unknown tool " + tool)
}

// outcome renders a tool result for the answer: the output, or the
// interruption with its key.
func outcome(result journal.ToolResult) string {
	if result.Key == "" {
		panic("outcome: result must carry its key")
	}
	if result.Interrupted {
		return "interrupted:" + callSuffix(result.Key)
	}
	return strings.Trim(string(result.Output), `"`)
}

// callSuffix is the call key without its run ID, {step}.{iteration}.{call},
// so answers compare across runs.
func callSuffix(key string) string {
	dot := strings.Index(key, ".")
	if dot < 0 {
		panic("callSuffix: malformed call key " + key)
	}
	if key[dot+1:] == "" {
		panic("callSuffix: empty call key suffix")
	}
	return key[dot+1:]
}

// tool counts a start, performs the effect (counts it), and returns a
// deterministic output for the call's key.
func (f *fixture) tool(ctx context.Context, call journal.ToolCall) ([]byte, error) {
	if call.Key == "" {
		panic("tool: call must carry its key")
	}
	if call.Name != toolCharge && call.Name != toolLookup {
		panic("tool: unknown tool " + call.Name)
	}
	if err := f.bump(ctx, "start."+call.Key); err != nil {
		return nil, err
	}
	if err := f.bump(ctx, "effect."+call.Key); err != nil {
		return nil, err
	}
	return json.Marshal(call.Name + "d:" + callSuffix(call.Key))
}

// bump increments the counter at key with a bounded CAS loop.
func (f *fixture) bump(ctx context.Context, key string) error {
	if key == "" {
		panic("bump: key must not be empty")
	}
	if f.effects == nil {
		panic("bump: effects bucket must be bound")
	}
	for try := 0; try < counterRetryMax; try++ {
		entry, err := f.effects.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err := f.effects.Create(ctx, key, []byte("1")); err == nil {
				return nil
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("counter %s: %w", key, err)
		}
		var count int
		if _, err := fmt.Sscan(string(entry.Value()), &count); err != nil {
			return fmt.Errorf("counter %s: %w", key, err)
		}
		next := []byte(fmt.Sprint(count + 1))
		if _, err := f.effects.Update(ctx, key, next, entry.Revision()); err == nil {
			return nil
		}
	}
	return fmt.Errorf("counter %s: %d CAS retries", key, counterRetryMax)
}

// counters reads every counter for runID: name -> count.
func (f *fixture) counters(t *testing.T, runID string) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys, err := f.effects.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		t.Fatalf("counter keys: %v", err)
	}
	counts := map[string]int{}
	for _, key := range keys {
		if !strings.Contains(key, "."+runID+".") {
			continue
		}
		entry, err := f.effects.Get(ctx, key)
		if err != nil {
			t.Fatalf("counter %s: %v", key, err)
		}
		var count int
		if _, err := fmt.Sscan(string(entry.Value()), &count); err != nil {
			t.Fatalf("counter %s: %v", key, err)
		}
		counts[key] = count
	}
	return counts
}

// newRunID is a fresh run ID, as the API assigns one.
func newRunID() string {
	id := runid.New()
	if id == "" {
		panic("newRunID: empty id")
	}
	if len(id) < 8 {
		panic("newRunID: id too short")
	}
	return id
}

// startRun starts one run of the agent without a crash and returns
// its ID.
func (a *agent) startRun(t *testing.T) string {
	t.Helper()
	runID := newRunID()
	a.publishStart(t, runID)
	return runID
}

// startRunRouted registers hook for a fresh run ID with r and then
// starts the run, so the hook is in place before the engine's first
// dispatch reaches a point. It publishes workflow.started the way the
// API does (a run ID is assigned at publish time, which is why the
// service's StartRun cannot be used here).
func (a *agent) startRunRouted(t *testing.T, r *router, hook chaos.Hook) string {
	t.Helper()
	if hook == nil || r == nil {
		panic("startRunRouted: router and hook are required")
	}
	runID := newRunID()
	r.add(runID, hook)
	a.publishStart(t, runID)
	return runID
}

// publishStart publishes workflow.started for runID with the agent's
// def, as api.Service.StartRun does with no input and no labels.
func (a *agent) publishStart(t *testing.T, runID string) {
	t.Helper()
	if runID == "" {
		panic("publishStart: runID must not be empty")
	}
	defData, err := json.Marshal(a.def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	payload, err := json.Marshal(struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
	}{WorkflowDef: defData})
	if err != nil {
		t.Fatalf("marshal start payload: %v", err)
	}
	evt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, runID, payload)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.f.js.PublishMsg(ctx, &nats.Msg{
		Subject: evt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}); err != nil {
		t.Fatalf("publish start: %v", err)
	}
}

// waitCompleted waits for the run to complete and returns its answer,
// failing the test when it does not.
func (f *fixture) waitCompleted(t *testing.T, runID string) answer {
	t.Helper()
	got, ok := f.waitCompletedOK(runID)
	if !ok {
		f.dumpRun(t, runID)
		t.Fatalf("run %s did not complete within %s", runID, runTimeout)
	}
	return got
}

// waitCompletedOK polls the run until it completes with a decodable
// answer or runTimeout passes.
func (f *fixture) waitCompletedOK(runID string) (answer, bool) {
	if runID == "" {
		panic("waitCompletedOK: runID must not be empty")
	}
	if f.svc == nil {
		panic("waitCompletedOK: service must be bound")
	}
	deadline := time.Now().Add(runTimeout)
	for time.Now().Before(deadline) {
		run, err := f.svc.GetRun(context.Background(), runID)
		if err == nil && run.Status == dag.RunStatusCompleted {
			var got answer
			step := run.Steps[stepID]
			if step.Status == dag.StepStatusCompleted &&
				json.Unmarshal(step.Output, &got) == nil {
				return got, true
			}
			return answer{}, false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return answer{}, false
}

// audit runs the journal checker and the counter invariant for runID
// and fails the test on a violation.
func (f *fixture) audit(t *testing.T, runID string) {
	t.Helper()
	if !f.auditOK(t, runID, "run "+runID) {
		f.dumpRun(t, runID)
	}
}

// auditOK runs the journal checker and the counter invariant for
// runID: the unsafe tool started and took effect at most once per key.
// Violations are reported with label and make it return false.
func (f *fixture) auditOK(t *testing.T, runID, label string) bool {
	t.Helper()
	if runID == "" || label == "" {
		panic("auditOK: runID and label are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	report, err := journal.Check(ctx, f.nc, runID)
	if err != nil {
		t.Errorf("%s: journal.Check: %v", label, err)
		return false
	}
	ok := true
	if !report.OK() {
		t.Errorf("%s: journal.Check violations: %s", label, violations(report))
		ok = false
	}
	counts := f.counters(t, runID)
	if len(counts) == 0 {
		t.Errorf("%s: no side-effect counters", label)
		ok = false
	}
	for key, count := range counts {
		if !strings.HasPrefix(key, "start.") {
			continue
		}
		callKey := strings.TrimPrefix(key, "start.")
		if effect := counts["effect."+callKey]; effect > count {
			t.Errorf("%s: %s: %d effects for %d starts", label, callKey, effect, count)
			ok = false
		}
		if unsafeKey(callKey) && count > 1 {
			t.Errorf("%s: unsafe tool %s started %d times, want at most once", label, callKey, count)
			ok = false
		}
	}
	return ok
}

// unsafeKey reports whether the call key belongs to the unsafe
// iteration: {run}.{step}.{iteration}.{call}.
func unsafeKey(callKey string) bool {
	parts := strings.Split(callKey, ".")
	if len(parts) < 4 {
		panic("unsafeKey: malformed call key " + callKey)
	}
	if parts[len(parts)-3] != stepID {
		panic("unsafeKey: call key is not this step's: " + callKey)
	}
	return parts[len(parts)-2] == fmt.Sprint(unsafeIteration)
}

// violations renders the report's violations.
func violations(report journal.Report) string {
	var b strings.Builder
	for _, step := range report.Steps {
		for _, v := range step.Violations {
			fmt.Fprintf(&b, "\n  %s: %s", step.StepID, v)
		}
	}
	return b.String()
}

// interruptingPoints are the points where a crash leaves the running
// tool's intent without a result: a successor answers an unsafe call
// there as interrupted (ADR-026), and must rerun a safe one.
var interruptingPoints = map[chaos.Point]bool{
	chaos.WorkerAfterToolIntent: true, chaos.WorkerInsideTool: true,
}

// expectedAnswer is the baseline answer after crashes at points on
// iterations: exact, except that the unsafe call is interrupted when a
// crash hit an interrupting point on its iteration. A safe call is
// never interrupted, whatever the crash.
func expectedAnswer(baseline answer, crashed map[chaos.Point]int) answer {
	if len(baseline.Calls) == 0 {
		panic("expectedAnswer: baseline must have calls")
	}
	if crashed == nil {
		panic("expectedAnswer: crashed must not be nil")
	}
	want := answer{Final: baseline.Final, Calls: append([]string(nil), baseline.Calls...)}
	for p, iteration := range crashed {
		if interruptingPoints[p] && iteration == unsafeIteration {
			call := want.Calls[unsafeIteration]
			want.Calls[unsafeIteration] = "interrupted:" + call[strings.Index(call, ":")+1:]
		}
	}
	return want
}

// sameAnswer reports whether got equals want exactly.
func sameAnswer(want, got answer) bool {
	if len(want.Calls) == 0 {
		panic("sameAnswer: want must have calls")
	}
	if want.Final != got.Final || len(want.Calls) != len(got.Calls) {
		return false
	}
	for i := range want.Calls {
		if want.Calls[i] != got.Calls[i] {
			return false
		}
	}
	return true
}

// crashHook crashes one site of its run, once (the router already
// routes by run): a kill returns
// chaos.ErrKilled, a stall blocks for stallFor (or until the holder's
// context ends) and proceeds.
type crashHook struct {
	point     chaos.Point
	kill      bool
	iteration int
	fired     sync.Once
	count     atomic.Int32
}

// fire runs the hook for a site, once.
func (h *crashHook) fire(ctx context.Context, p chaos.Point, site chaos.Site) error {
	if site.RunID == "" {
		panic("crashHook: site must name a run")
	}
	if p != h.point || !h.targets(site) {
		return nil
	}
	err := error(nil)
	h.fired.Do(func() {
		h.count.Add(1)
		if h.kill {
			err = chaos.ErrKilled
			return
		}
		select {
		case <-time.After(stallFor):
		case <-ctx.Done():
		}
	})
	return err
}

// targets is which site the hook aims at: its iteration for worker
// points, any dispatch for engine points; fired once, so the first
// handling that reaches the point is the one crashed. It is the first
// handling, not delivery 1: a worker stalled by another run holds
// prefetched deliveries past their AckWait, so a run's first handled
// delivery can already be its second.
func (h *crashHook) targets(site chaos.Site) bool {
	if h.point <= 0 {
		panic("crashHook: point must be set")
	}
	if h.point >= chaos.EngineEnqueueSaved {
		return true
	}
	return site.Iteration == h.iteration
}

// router dispatches to the hook registered for a site's run, so
// parallel runs each see only their own crash.
type router struct {
	mu    sync.Mutex
	hooks map[string]chaos.Hook
}

// install registers the router as the process hook.
func (r *router) install(t *testing.T) {
	t.Helper()
	if r.hooks == nil {
		r.hooks = map[string]chaos.Hook{}
	}
	chaos.Install(r.route)
	t.Cleanup(func() { chaos.Install(nil) })
}

// add registers a hook for runID.
func (r *router) add(runID string, hook chaos.Hook) {
	if runID == "" || hook == nil {
		panic("router.add: runID and hook are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.hooks[runID]; dup {
		panic("router.add: hook already registered for " + runID)
	}
	r.hooks[runID] = hook
}

// remove drops runID's hook once its run is audited.
func (r *router) remove(runID string) {
	if runID == "" {
		panic("router.remove: runID must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.hooks[runID]; !ok {
		panic("router.remove: no hook for " + runID)
	}
	delete(r.hooks, runID)
}

// route is the chaos.Hook.
func (r *router) route(ctx context.Context, p chaos.Point, site chaos.Site) error {
	if site.RunID == "" {
		panic("router.route: site must name a run")
	}
	r.mu.Lock()
	hook := r.hooks[site.RunID]
	r.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, p, site)
}

// dumpRun logs the run's step state, journal records and history
// events, for a run that failed an invariant.
func (f *fixture) dumpRun(t *testing.T, runID string) {
	t.Helper()
	if runID == "" {
		panic("dumpRun: runID must not be empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := f.svc.GetRun(ctx, runID)
	if err != nil {
		t.Logf("dump %s: GetRun: %v", runID, err)
	} else {
		t.Logf("dump %s: run %s step %+v", runID, run.Status, run.Steps[stepID])
	}
	records, err := journal.Read(ctx, f.js, runID, stepID)
	if err != nil {
		t.Logf("dump %s: journal.Read: %v", runID, err)
	}
	for _, rec := range records {
		t.Logf("dump %s: journal %d %s it=%d att=%d del=%d seq=%d nonce=%s %s", runID,
			rec.Seq, rec.Type, rec.Iteration, rec.Attempt, rec.Delivered, rec.StreamSeq,
			rec.DispatchNonce, rec.Data)
	}
	for _, line := range historyLines(t, f.nc, runID) {
		t.Logf("dump %s: history %s", runID, line)
	}
}

// historyLines reads the run's history and renders each event's type
// and identity.
func historyLines(t *testing.T, nc *nats.Conn, runID string) []string {
	t.Helper()
	if runID == "" {
		panic("historyLines: runID must not be empty")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll(), nats.OrderedConsumer())
	if err != nil {
		t.Fatalf("subscribe history: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Logf("unsubscribe: %v", err)
		}
	}()
	var lines []string
	for i := 0; i < historyReadMax; i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			break
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		if err != nil {
			t.Fatalf("decode history event: %v", err)
		}
		lines = append(lines, fmt.Sprintf("%s step=%s it=%s att=%d nonce=%s jseq=%d %s",
			evt.Type, evt.StepID, iterationOf(evt), evt.AttemptNumber, evt.DispatchNonce,
			evt.JournalSeq, evt.Payload))
	}
	return lines
}

// iterationOf renders an event's iteration, "-" when it carries none.
func iterationOf(evt protocol.Event) string {
	if evt.RunID == "" {
		panic("iterationOf: event must name a run")
	}
	if evt.Iteration == nil {
		return "-"
	}
	return fmt.Sprint(*evt.Iteration)
}
