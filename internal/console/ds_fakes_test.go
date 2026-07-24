// ds_fakes_test.go holds one in-memory fake per domain port from
// ds_ports.go (#576, follow-up to #564). Before this split there was a
// single 41-method fakeDataSource; a test that cared about one page
// still dragged in the whole surface, and there was nowhere to reuse
// just the DLQ or just the trigger behaviour.
//
// Methodology:
//   - One fake per port, each implementing exactly that port and
//     nothing else. The `var _ Port = (*fakePort)(nil)` assertions
//     below are the compile-time proof each fake stands alone.
//   - Two seed structs carry the state genuinely shared BETWEEN ports,
//     by pointer, so the projections cannot drift:
//   - fakeEntities  — workflow defs, runs, trigger defs. The run,
//     trigger, workflow and cross-entity search ports all project
//     it, exactly as production projects one set of KV buckets.
//   - fakeDeployment — the config snapshot + service registrations.
//     Ops (ConfigSnapshot) and WorkerDirectory (worker/service row
//     projections) both fold it, mirroring the single `workers` KV
//     read the production adapter shares.
//   - fakeDataSource (pages_test.go) composes all ten by embedding, so
//     the ~70 existing call sites keep writing `fake.runs = ...` and
//     `Data: newFakeDS()` unchanged — Go promotes the seed fields at
//     depth 1, which also resolves them unambiguously against the
//     depth-2 copies reachable through each port fake.
//   - deepUnimplemented lets a NEW narrow test embed a port fake
//     alongside the panic-stub base: the port fake's methods sit one
//     level shallower, so they win promotion while every port the test
//     did not opt into still panics.
package console

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
)

// Compile-time proof that each fake satisfies exactly one port on its
// own — the whole point of the split. If a port grows a method, the
// matching fake fails to build here rather than at 70 call sites.
var (
	_ RunStore         = (*fakeRunStore)(nil)
	_ TriggerStore     = (*fakeTriggerStore)(nil)
	_ DLQStore         = (*fakeDLQStore)(nil)
	_ AuditLog         = (*fakeAuditLog)(nil)
	_ AgentRuntimeView = (*fakeAgentRuntimeView)(nil)
	_ OpsInventory     = (*fakeOpsInventory)(nil)
	_ WorkerDirectory  = (*fakeWorkerDirectory)(nil)
	_ AdmissionView    = (*fakeAdmissionView)(nil)
	_ SearchIndex      = (*fakeSearchIndex)(nil)
)

// deepUnimplemented buries unimplementedDataSource one embedding level
// deeper. A narrow fake embeds this ALONGSIDE the port fakes it wants:
// the port fake's methods promote from depth 1 and the panic stubs from
// depth 2, so Go picks the real behaviour and any port the fake did not
// opt into still fails loudly. Embedding unimplementedDataSource
// directly would tie for depth and promote neither.
type deepUnimplemented struct{ unimplementedDataSource }

// fakeEntities is the seed the workflow, run, trigger and search ports
// all project. Held by pointer so a trigger created through
// fakeTriggerStore is visible to the search index, matching production
// where both read the same bucket.
type fakeEntities struct {
	workflows []dag.WorkflowDef
	runs      []dag.WorkflowRun
	triggers  []trigger.TriggerDef
}

// fakeDeployment is the seed the ops and worker-directory ports share:
// the deployment self-portrait plus the service registrations. Held by
// pointer for the same reason — the /config page and the /workers page
// must not disagree about which workers are registered.
type fakeDeployment struct {
	configSnap    ConfigSnapshot
	configSnapErr error

	// services backs both the task-type description cross-reference
	// (#335) and the services roster page. serviceRowsErr forces the
	// read-error path so tests can assert the omit-on-error nav-count
	// contract.
	services       []worker.ServiceDef
	serviceRowsErr error
}

// fakeWorkflowDefs implements the two workflow-definition reads that sit
// on the DataSource union directly (see ds_ports.go: both are straight
// delegations with no projection to hide, so they have no named port).
type fakeWorkflowDefs struct {
	*fakeEntities
}

func (f *fakeWorkflowDefs) ListWorkflows(
	_ context.Context,
) ([]dag.WorkflowDef, error) {
	return append([]dag.WorkflowDef{}, f.workflows...), nil
}

func (f *fakeWorkflowDefs) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if name == "" {
		panic("fakeWorkflowDefs.GetWorkflow: empty name")
	}
	for _, d := range f.workflows {
		if d.Name == name {
			return d, nil
		}
	}
	return dag.WorkflowDef{}, errNotFound("workflow", name)
}

// signalCall captures one SendSignal invocation so tests can assert the
// (runID, name, data) the handler passed through.
type signalCall struct {
	RunID string
	Name  string
	Data  []byte
}

// startRunCall captures one StartRun invocation. Tests assert against
// the (Workflow, Input) pair to confirm the handler delegated correctly.
type startRunCall struct {
	Workflow string
	Input    []byte
}

// fakeRunStore implements RunStore over the shared entity seed plus the
// per-run event / trace / stream state only run pages care about.
type fakeRunStore struct {
	*fakeEntities

	events     map[string][]api.RunEvent
	runUpdates chan RunUpdate
	runHistory map[string]chan HistoryEvent
	// gotRunsLiveOnly records the liveOnly argument the last WatchRuns
	// call received, so SSE-handler tests can assert page>1 suppresses
	// the historical KV replay.
	gotRunsLiveOnly bool

	// Run-trace tab backing data. Tests assign runTrace directly to
	// drive the Trace tab; runTraceErr forces the read-error path.
	runTrace    []TraceRow
	runTraceErr error

	// #329 (R8 inline Run button): StartRun observability. Tests
	// either let startRunID default to the empty string (the handler
	// will still 200 with an empty payload echo) or assign a stable
	// id so they can assert against the response body.
	startRunID    string
	startRunErr   error
	startRunCalls []startRunCall

	// Run Signal / Cancel observability. cancelRunCalls records each
	// CancelRun(runID); signalCalls records each SendSignal invocation.
	// The *Err seams force the failure path.
	cancelRunCalls []string
	cancelRunErr   error
	signalCalls    []signalCall
	signalErr      error
}

func (f *fakeRunStore) ListRuns(
	_ context.Context, filter string,
) ([]dag.WorkflowRun, error) {
	if filter == "" {
		return append([]dag.WorkflowRun{}, f.runs...), nil
	}
	out := make([]dag.WorkflowRun, 0, len(f.runs))
	for _, r := range f.runs {
		if r.WorkflowID == filter {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeRunStore) GetRun(
	_ context.Context, runID string,
) (dag.WorkflowRun, error) {
	if runID == "" {
		panic("fakeRunStore.GetRun: empty runID")
	}
	for _, r := range f.runs {
		if r.RunID == runID {
			return r, nil
		}
	}
	return dag.WorkflowRun{}, errNotFound("run", runID)
}

func (f *fakeRunStore) ListRunEvents(
	_ context.Context, runID string, _ bool,
) ([]api.RunEvent, error) {
	if runID == "" {
		panic("fakeRunStore.ListRunEvents: empty runID")
	}
	return append([]api.RunEvent{}, f.events[runID]...), nil
}

func (f *fakeRunStore) GetRunTrace(
	_ context.Context, runID string,
) ([]TraceRow, error) {
	if runID == "" {
		panic("fakeRunStore.GetRunTrace: empty runID")
	}
	if f.runTraceErr != nil {
		return nil, f.runTraceErr
	}
	return append([]TraceRow{}, f.runTrace...), nil
}

// StartRun records the call and returns the seeded id / error. Tests
// that want a non-empty run id assign startRunID; the default empty
// string still exercises the response shape and audit emission.
func (f *fakeRunStore) StartRun(
	_ context.Context, workflowName string, input []byte,
) (string, error) {
	if workflowName == "" {
		panic("fakeRunStore.StartRun: workflowName is empty")
	}
	f.startRunCalls = append(f.startRunCalls,
		startRunCall{Workflow: workflowName, Input: input})
	if f.startRunErr != nil {
		return "", f.startRunErr
	}
	return f.startRunID, nil
}

// CancelRun records the call and returns the seeded error. Tests that
// exercise the success path leave cancelRunErr nil.
func (f *fakeRunStore) CancelRun(
	_ context.Context, runID string,
) error {
	if runID == "" {
		panic("fakeRunStore.CancelRun: empty runID")
	}
	f.cancelRunCalls = append(f.cancelRunCalls, runID)
	return f.cancelRunErr
}

// SendSignal records the call and returns the seeded error. The data is
// copied so a caller reusing the buffer can't mutate the recorded value.
func (f *fakeRunStore) SendSignal(
	_ context.Context, runID, name string, data []byte,
) error {
	if runID == "" {
		panic("fakeRunStore.SendSignal: empty runID")
	}
	if name == "" {
		panic("fakeRunStore.SendSignal: empty name")
	}
	f.signalCalls = append(f.signalCalls, signalCall{
		RunID: runID, Name: name, Data: append([]byte{}, data...),
	})
	return f.signalErr
}

// WatchRuns and WatchRunHistory cover the streaming surface. The default
// fake returns a static, never-firing channel; tests that exercise the
// SSE handlers (streams_test.go) supply their own runUpdates /
// runHistory channel and drive it directly.
func (f *fakeRunStore) WatchRuns(
	ctx context.Context, liveOnly bool,
) (<-chan RunUpdate, error) {
	if ctx == nil {
		panic("fakeRunStore.WatchRuns: ctx is nil")
	}
	f.gotRunsLiveOnly = liveOnly
	if f.runUpdates != nil {
		return f.runUpdates, nil
	}
	return closeOnDone[RunUpdate](ctx), nil
}

func (f *fakeRunStore) WatchRunHistory(
	ctx context.Context, runID string, _ uint64,
) (<-chan HistoryEvent, error) {
	if ctx == nil {
		panic("fakeRunStore.WatchRunHistory: ctx is nil")
	}
	if runID == "" {
		panic("fakeRunStore.WatchRunHistory: runID is empty")
	}
	if ch, ok := f.runHistory[runID]; ok {
		return ch, nil
	}
	return closeOnDone[HistoryEvent](ctx), nil
}

// closeOnDone returns an empty stream that closes when ctx is cancelled
// — the honest default for every watch a test does not drive itself.
// Shared by the run / trigger / DLQ watches so the ctx-cleanup contract
// is implemented once.
func closeOnDone[T any](ctx context.Context) <-chan T {
	if ctx == nil {
		panic("closeOnDone: ctx is nil")
	}
	ch := make(chan T)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

// triggerSetCall captures one SetTriggerEnabled invocation so tests can
// assert against the call pattern.
type triggerSetCall struct {
	ID      string
	Enabled bool
}

// updateTriggerCall captures one UpdateTrigger invocation so tests can
// assert the (id, updates) pair the handler delegated.
type updateTriggerCall struct {
	ID      string
	Updates api.TriggerUpdates
}

// fakeTriggerStore implements TriggerStore over the shared entity seed.
// Each *Calls slice records one invocation; the *Err seams force the
// failure path. On success the mutations rewrite the seed so subsequent
// ListTriggers (and the search index) reflect the change.
type fakeTriggerStore struct {
	*fakeEntities

	triggerFires   map[string][]TriggerFireRow
	triggerUpdates chan TriggerUpdate

	triggerSetCalls []triggerSetCall
	triggerSetErr   error

	createTriggerCalls []trigger.TriggerDef
	createTriggerErr   error
	listTriggerCalls   int
	updateTriggerCalls []updateTriggerCall
	updateTriggerErr   error
	deleteTriggerCalls []string
	deleteTriggerErr   error

	// #352 (fire-now button): manual fire observability.
	// fireTriggerRunID is the stable id the fake returns on success;
	// fireTriggerErr lets tests force the error path; fireTriggerCalls
	// captures each invocation so tests can assert the id the handler
	// passed through.
	fireTriggerRunID string
	fireTriggerErr   error
	fireTriggerCalls []string
}

func (f *fakeTriggerStore) ListTriggers(
	_ context.Context,
) ([]trigger.TriggerDef, error) {
	f.listTriggerCalls++
	return append([]trigger.TriggerDef{}, f.triggers...), nil
}

func (f *fakeTriggerStore) SetTriggerEnabled(
	_ context.Context, triggerID string, enabled bool,
) error {
	if triggerID == "" {
		panic("fakeTriggerStore.SetTriggerEnabled: empty triggerID")
	}
	f.triggerSetCalls = append(f.triggerSetCalls,
		triggerSetCall{ID: triggerID, Enabled: enabled})
	if f.triggerSetErr != nil {
		return f.triggerSetErr
	}
	for i := range f.triggers {
		if f.triggers[i].ID == triggerID {
			f.triggers[i].Enabled = enabled
			return nil
		}
	}
	return errNotFound("trigger", triggerID)
}

func (f *fakeTriggerStore) CreateTrigger(
	_ context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		panic("fakeTriggerStore.CreateTrigger: empty def.ID")
	}
	if def.WorkflowID == "" {
		panic("fakeTriggerStore.CreateTrigger: empty def.WorkflowID")
	}
	f.createTriggerCalls = append(f.createTriggerCalls, def)
	if f.createTriggerErr != nil {
		return f.createTriggerErr
	}
	f.triggers = append(f.triggers, def)
	return nil
}

func (f *fakeTriggerStore) UpdateTrigger(
	_ context.Context, triggerID string, updates api.TriggerUpdates,
) error {
	if triggerID == "" {
		panic("fakeTriggerStore.UpdateTrigger: empty triggerID")
	}
	f.updateTriggerCalls = append(f.updateTriggerCalls,
		updateTriggerCall{ID: triggerID, Updates: updates})
	if f.updateTriggerErr != nil {
		return f.updateTriggerErr
	}
	for i := range f.triggers {
		if f.triggers[i].ID == triggerID {
			return nil
		}
	}
	return errNotFound("trigger", triggerID)
}

func (f *fakeTriggerStore) DeleteTrigger(
	_ context.Context, triggerID string,
) error {
	if triggerID == "" {
		panic("fakeTriggerStore.DeleteTrigger: empty triggerID")
	}
	f.deleteTriggerCalls = append(f.deleteTriggerCalls, triggerID)
	if f.deleteTriggerErr != nil {
		return f.deleteTriggerErr
	}
	for i := range f.triggers {
		if f.triggers[i].ID == triggerID {
			f.triggers = append(f.triggers[:i], f.triggers[i+1:]...)
			return nil
		}
	}
	return errNotFound("trigger", triggerID)
}

// FireTrigger records the manual fire call and returns the seeded id /
// error. Tests that exercise the success path assign fireTriggerRunID;
// tests that exercise kind / disabled / transport errors assign
// fireTriggerErr.
func (f *fakeTriggerStore) FireTrigger(
	_ context.Context, triggerID string,
) (string, error) {
	if triggerID == "" {
		panic("fakeTriggerStore.FireTrigger: empty triggerID")
	}
	f.fireTriggerCalls = append(f.fireTriggerCalls, triggerID)
	if f.fireTriggerErr != nil {
		return "", f.fireTriggerErr
	}
	return f.fireTriggerRunID, nil
}

func (f *fakeTriggerStore) ListTriggerFires(
	_ context.Context, triggerID string, limit int,
) ([]TriggerFireRow, error) {
	if triggerID == "" {
		panic("fakeTriggerStore.ListTriggerFires: empty triggerID")
	}
	if limit <= 0 {
		panic("fakeTriggerStore.ListTriggerFires: limit must be positive")
	}
	rows := f.triggerFires[triggerID]
	out := make([]TriggerFireRow, 0, len(rows))
	for i := range rows {
		out = append(out, rows[i])
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeTriggerStore) WatchTriggers(
	ctx context.Context,
) (<-chan TriggerUpdate, error) {
	if ctx == nil {
		panic("fakeTriggerStore.WatchTriggers: ctx is nil")
	}
	if f.triggerUpdates != nil {
		return f.triggerUpdates, nil
	}
	return closeOnDone[TriggerUpdate](ctx), nil
}

// fakeDLQStore implements DLQStore. deadLetters is the list projection;
// the *Calls slices record mutations and the *Err seams force failures.
type fakeDLQStore struct {
	deadLetters  []api.DeadLetterView
	replayCalls  []uint64
	discardCalls []uint64
	replayErr    error
	discardErr   error
	dlqUpdates   chan DLQUpdate
	watchDLQErr  error
}

func (f *fakeDLQStore) ListDeadLetters(
	_ context.Context, limit int,
) ([]api.DeadLetterView, error) {
	if limit <= 0 {
		panic("fakeDLQStore.ListDeadLetters: limit must be positive")
	}
	out := append([]api.DeadLetterView{}, f.deadLetters...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeDLQStore) ReplayDeadLetter(
	_ context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("fakeDLQStore.ReplayDeadLetter: seq must be positive")
	}
	f.replayCalls = append(f.replayCalls, seq)
	return f.replayErr
}

func (f *fakeDLQStore) DiscardDeadLetter(
	_ context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("fakeDLQStore.DiscardDeadLetter: seq must be positive")
	}
	f.discardCalls = append(f.discardCalls, seq)
	if f.discardErr != nil {
		return f.discardErr
	}
	for i := range f.deadLetters {
		if f.deadLetters[i].Sequence == seq {
			f.deadLetters = append(
				f.deadLetters[:i], f.deadLetters[i+1:]...)
			break
		}
	}
	return nil
}

func (f *fakeDLQStore) WatchDLQ(
	ctx context.Context,
) (<-chan DLQUpdate, error) {
	if ctx == nil {
		panic("fakeDLQStore.WatchDLQ: ctx is nil")
	}
	if f.watchDLQErr != nil {
		return nil, f.watchDLQErr
	}
	if f.dlqUpdates != nil {
		return f.dlqUpdates, nil
	}
	return closeOnDone[DLQUpdate](ctx), nil
}

// fakeAuditLog implements AuditLog. EmitAuditEvent prepends so
// auditEvents[0] is always the most recent write — the ordering every
// mutation test asserts against.
type fakeAuditLog struct {
	auditEvents []AuditEvent
}

func (f *fakeAuditLog) ListAuditEvents(
	_ context.Context, limit int,
) ([]AuditEvent, error) {
	if limit <= 0 {
		panic("fakeAuditLog.ListAuditEvents: limit must be positive")
	}
	out := append([]AuditEvent{}, f.auditEvents...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeAuditLog) EmitAuditEvent(
	_ context.Context, evt AuditEvent,
) error {
	f.auditEvents = append([]AuditEvent{evt}, f.auditEvents...)
	return nil
}

// fakeAgentRuntimeView implements AgentRuntimeView (#379). agentRuntimes
// backs the list page; agentRuntimeByRoot backs the single-root SSE
// re-projection; agentRuntimesErr forces the list read-error path.
type fakeAgentRuntimeView struct {
	agentRuntimes      []AgentRuntimeRow
	agentRuntimesErr   error
	agentRuntimeByRoot map[string]AgentRuntimeRow
}

func (f *fakeAgentRuntimeView) ListAgentRuntimes(
	ctx context.Context, limit int,
) ([]AgentRuntimeRow, error) {
	if ctx == nil {
		panic("fakeAgentRuntimeView.ListAgentRuntimes: ctx is nil")
	}
	if limit <= 0 {
		panic("fakeAgentRuntimeView.ListAgentRuntimes: limit must be positive")
	}
	if f.agentRuntimesErr != nil {
		return nil, f.agentRuntimesErr
	}
	out := append([]AgentRuntimeRow{}, f.agentRuntimes...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeAgentRuntimeView) AgentRuntime(
	ctx context.Context, root string,
) (AgentRuntimeRow, bool, error) {
	if ctx == nil {
		panic("fakeAgentRuntimeView.AgentRuntime: ctx is nil")
	}
	if root == "" {
		return AgentRuntimeRow{}, false, nil
	}
	row, ok := f.agentRuntimeByRoot[root]
	return row, ok, nil
}

// fakeOpsInventory implements OpsInventory over the shared deployment
// seed plus the KV inspector / consumer / server / connection tables the
// ops pages read.
type fakeOpsInventory struct {
	*fakeDeployment

	// KV inspector backing data. kvBuckets is the side-nav inventory;
	// kvEntries is keyed by bucket/key so GetKVEntry can hand back
	// deterministic bytes.
	kvBuckets []KVBucketInfo
	kvKeys    map[string][]string
	kvEntries map[string][]byte

	// Consumers page backing data. Tests assign ConsumerRows directly
	// so the page renders without a JetStream consumer existing.
	consumers []ConsumerRow

	// Server page backing data. Tests assign a ServerHealth (and an
	// optional error seam) directly so the page renders without a live
	// nats.Conn or JetStream account.
	serverHealth    ServerHealth
	serverHealthErr error

	// Connections page backing data. Tests assign ConnRows directly so
	// the page renders without a live embedded server's Connz().
	connections []ConnRow
}

func (f *fakeOpsInventory) ListKVBuckets(
	_ context.Context,
) ([]KVBucketInfo, error) {
	return append([]KVBucketInfo{}, f.kvBuckets...), nil
}

func (f *fakeOpsInventory) ListKVKeys(
	_ context.Context, bucket, _ string, limit int,
) ([]string, string, error) {
	if bucket == "" {
		panic("fakeOpsInventory.ListKVKeys: bucket is empty")
	}
	if limit <= 0 {
		panic("fakeOpsInventory.ListKVKeys: limit must be positive")
	}
	keys := append([]string{}, f.kvKeys[bucket]...)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, "", nil
}

func (f *fakeOpsInventory) GetKVEntry(
	_ context.Context, bucket, key string,
) (KVEntryView, error) {
	if bucket == "" {
		panic("fakeOpsInventory.GetKVEntry: bucket is empty")
	}
	if key == "" {
		panic("fakeOpsInventory.GetKVEntry: key is empty")
	}
	val, ok := f.kvEntries[bucket+"/"+key]
	if !ok {
		return KVEntryView{}, ErrKVNotFound
	}
	return KVEntryView{
		Bucket: bucket, Key: key, Value: val, Revision: 1,
		IsJSON: looksLikeJSON(val),
	}, nil
}

// ConfigSnapshot is the test seam for the /console/config page (#312).
// The default zero value renders the empty-state shell; tests assign
// configSnap directly to drive richer scenarios.
func (f *fakeOpsInventory) ConfigSnapshot(
	_ context.Context,
) (ConfigSnapshot, error) {
	if f.configSnapErr != nil {
		return ConfigSnapshot{}, f.configSnapErr
	}
	return f.configSnap, nil
}

func (f *fakeOpsInventory) ListConsumers(
	_ context.Context,
) ([]ConsumerRow, error) {
	return append([]ConsumerRow{}, f.consumers...), nil
}

func (f *fakeOpsInventory) ServerHealth(
	_ context.Context,
) (ServerHealth, error) {
	return f.serverHealth, f.serverHealthErr
}

func (f *fakeOpsInventory) ListConnections(
	_ context.Context,
) ([]ConnRow, error) {
	return append([]ConnRow{}, f.connections...), nil
}

// fakeWorkerDirectory implements WorkerDirectory. Every method mirrors
// the production adapter by folding the SAME deployment seed the ops
// ConfigSnapshot returns — configSnap.Workers is the one worker
// registry, exactly as production reads one `workers` KV bucket.
type fakeWorkerDirectory struct {
	*fakeDeployment

	// Optional override for the rows AggregateTaskTypes returns
	// (#328). Default behaviour derives rows from configSnap.Workers so
	// most tests need no extra setup; tests that want a curated row set
	// assign taskTypeRows directly.
	taskTypeRows    []TaskTypeRow
	taskTypeRowsErr error
}

// AggregateTaskTypes derives task-type rows from the seeded worker
// registrations and then cross-references the services list (#335) for
// Description metadata, matching the production adapter.
func (f *fakeWorkerDirectory) AggregateTaskTypes(
	_ context.Context,
) ([]TaskTypeRow, error) {
	if f.taskTypeRowsErr != nil {
		return nil, f.taskTypeRowsErr
	}
	if f.taskTypeRows != nil {
		return append([]TaskTypeRow{}, f.taskTypeRows...), nil
	}
	rows := aggregateTaskTypesFromWorkers(f.configSnap.Workers)
	return attachServiceDescriptions(rows, f.services), nil
}

// ListWorkerRows projects the seeded registrations into render rows.
// now is wall-clock so liveness classification matches the production
// staleness window the tests assert against.
func (f *fakeWorkerDirectory) ListWorkerRows(
	_ context.Context,
) ([]WorkerStatusRow, error) {
	return workerRowsFromRegistrations(f.configSnap.Workers, time.Now()), nil
}

// ListServiceRows projects the seeded services slice through the
// production serviceRowsFromDefs. serviceRowsErr forces the error path
// so tests can assert the omit-on-error nav-count contract.
func (f *fakeWorkerDirectory) ListServiceRows(
	_ context.Context,
) ([]ServiceRow, error) {
	if f.serviceRowsErr != nil {
		return nil, f.serviceRowsErr
	}
	return serviceRowsFromDefs(f.services), nil
}

// WorkerDetail projects the same registrations the list page reads,
// matching by id, with now fixed to wall-clock so liveness
// classification matches the list page.
func (f *fakeWorkerDirectory) WorkerDetail(
	_ context.Context, id string,
) (WorkerDetail, error) {
	return workerDetailFromRegistrations(
		f.configSnap.Workers, id, time.Now()), nil
}

// fakeAdmissionView implements AdmissionView. Tests assign admission
// directly so the concurrency page renders without reading the engine
// KV gates.
type fakeAdmissionView struct {
	admission AdmissionState
}

func (f *fakeAdmissionView) AdmissionState(
	_ context.Context,
) (AdmissionState, error) {
	return f.admission, nil
}

// fakeSearchIndex implements SearchIndex. Search is a projection over
// the shared entity seed — cross-entity by definition — so it reads the
// same workflows / runs / triggers the other ports serve rather than
// keeping a second copy that could disagree with them.
type fakeSearchIndex struct {
	*fakeEntities

	// sparklineSeries is keyed by "kind/id" so a test can pre-seed
	// deterministic hourly counts without the metrics aggregator.
	sparklineSeries map[string][]float64
}

// Search mirrors the production adapter's contract over the seed. The
// rules stay identical (substring for workflows + triggers; prefix ≥4
// chars for runs) so unit tests exercise the same shape the real
// service hands the palette.
func (f *fakeSearchIndex) Search(
	_ context.Context, query string, limit int,
) ([]SearchHit, error) {
	if limit <= 0 {
		panic("fakeSearchIndex.Search: limit must be positive")
	}
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return nil, nil
	}
	hits := make([]SearchHit, 0, limit)
	for i := 0; i < len(f.workflows) && len(hits) < limit; i++ {
		wf := f.workflows[i]
		if !strings.Contains(strings.ToLower(wf.Name), q) {
			continue
		}
		hits = append(hits, SearchHit{
			Kind: "workflow", ID: wf.Name, Label: wf.Name,
			Subtitle: strconv.Itoa(len(wf.Steps)) + " steps",
			Href:     "/console/workflows/" + wf.Name,
		})
	}
	hits = f.appendRunHits(hits, q, limit)
	for i := 0; i < len(f.triggers) && len(hits) < limit; i++ {
		tr := f.triggers[i]
		if !strings.Contains(strings.ToLower(tr.ID), q) {
			continue
		}
		kind, _ := triggerKindAndTarget(tr)
		hits = append(hits, SearchHit{
			Kind: "trigger", ID: tr.ID, Label: tr.ID,
			Subtitle: kind,
			Href:     "/console/triggers/" + tr.ID,
		})
	}
	return hits, nil
}

// appendRunHits adds run-id prefix matches, enforcing the same
// cardinality guard as production: run ids are uuids, so a query
// shorter than runIDSearchMinChars would scan the world.
func (f *fakeSearchIndex) appendRunHits(
	hits []SearchHit, q string, limit int,
) []SearchHit {
	if limit <= 0 {
		panic("fakeSearchIndex.appendRunHits: limit must be positive")
	}
	if len(q) < runIDSearchMinChars {
		return hits
	}
	for i := 0; i < len(f.runs) && len(hits) < limit; i++ {
		run := f.runs[i]
		if !strings.HasPrefix(strings.ToLower(run.RunID), q) {
			continue
		}
		label := run.RunID
		if len(label) > 12 {
			label = label[:12] + "…"
		}
		hits = append(hits, SearchHit{
			Kind: "run", ID: run.RunID, Label: label,
			Subtitle: run.WorkflowID,
			Href:     "/console/runs/" + run.RunID,
		})
	}
	return hits
}

func (f *fakeSearchIndex) SparklineData(
	_ context.Context, kind, id string, hours int,
) ([]float64, error) {
	if kind == "" {
		panic("fakeSearchIndex.SparklineData: kind is empty")
	}
	if id == "" {
		panic("fakeSearchIndex.SparklineData: id is empty")
	}
	if hours <= 0 {
		panic("fakeSearchIndex.SparklineData: hours must be positive")
	}
	src, ok := f.sparklineSeries[kind+"/"+id]
	if !ok || len(src) == 0 {
		return nil, nil
	}
	out := make([]float64, hours)
	// Copy the trailing window so the newest bucket lands at index
	// len-1, matching the production bucketHourly contract.
	copyFrom := len(src) - hours
	if copyFrom < 0 {
		copyFrom = 0
	}
	src = src[copyFrom:]
	for i := 0; i < len(src) && i < hours; i++ {
		out[i] = src[i]
	}
	return out, nil
}

// seedSparklineHourly populates sparklineSeries with hours-many points
// for the (kind, id) tuple. Each bucket gets value i+1 so tests can
// assert ordering and non-zeroness in one shot. The time argument is
// unused — the fake stores by slot index, not wall-clock — but we keep
// the parameter so the call site reads like the production usage.
func (f *fakeSearchIndex) seedSparklineHourly(
	kind, id string, _ time.Time, hours int,
) {
	if kind == "" {
		panic("seedSparklineHourly: kind is empty")
	}
	if id == "" {
		panic("seedSparklineHourly: id is empty")
	}
	if hours <= 0 {
		panic("seedSparklineHourly: hours must be positive")
	}
	buckets := make([]float64, hours)
	for i := 0; i < hours; i++ {
		buckets[i] = float64(i + 1)
	}
	f.sparklineSeries[kind+"/"+id] = buckets
}
