// pages_test.go exercises the /console/workflows{,...} and
// /console/runs{,...} pages without standing up NATS.
//
// Methodology:
//   - A fakeDataSource implements console.DataSource over in-memory
//     slices/maps. Pages call the same DataSource the production
//     server passes in, so the rendering / filtering / pagination
//     logic gets full coverage without touching a JetStream bucket.
//   - Each test creates its own console.Mount with the fake; tests
//     never share state.
//   - Assertions look for stable substrings the templates emit so
//     they survive cosmetic tweaks; structural facts (number of
//     rows, presence of validator warnings, pagination clamping)
//     are checked separately from cosmetic ones.
//   - TestNoExternalURLs gets every new page added — the
//     local-first-asset policy must hold across the IA.
package console

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
)

// fakeDataSource is an in-memory DataSource that gives tests full
// control over what the console renders. Mutation helpers (addX)
// keep test setup verbose but transparent.
type fakeDataSource struct {
	workflows    []dag.WorkflowDef
	runs         []dag.WorkflowRun
	events       map[string][]api.RunEvent
	triggers     []trigger.TriggerDef
	runUpdates   chan RunUpdate
	runHistory   map[string]chan HistoryEvent
	deadLetters  []api.DeadLetterView
	auditEvents  []AuditEvent
	replayCalls  []uint64
	discardCalls []uint64
	replayErr    error
	discardErr   error

	// PR 5 additions: trigger toggle + recent firings + watch streams.
	triggerFires    map[string][]TriggerFireRow
	triggerSetCalls []triggerSetCall
	triggerSetErr   error
	triggerUpdates  chan TriggerUpdate
	dlqUpdates      chan DLQUpdate

	// Trigger CRUD observability (Add / Edit / Delete). Each slice
	// records one invocation; the *Err seams force the failure path.
	// On success the methods mutate f.triggers so subsequent
	// ListTriggers reflects the change.
	createTriggerCalls []trigger.TriggerDef
	createTriggerErr   error
	listTriggerCalls   int
	updateTriggerCalls []updateTriggerCall
	updateTriggerErr   error
	deleteTriggerCalls []string
	deleteTriggerErr   error

	// PR 5b additions: KV inspector backing data.
	kvBuckets []KVBucketInfo
	kvKeys    map[string][]string
	kvEntries map[string][]byte

	// #329 (R8 inline Run button): StartRun observability. Tests
	// either let startRunID default to the empty string (the handler
	// will still 200 with an empty payload echo) or assign a stable
	// id so they can assert against the response body.
	startRunID    string
	startRunErr   error
	startRunCalls []startRunCall

	// #352 (FireTrigger fire-now button): manual fire observability.
	// fireTriggerRunID is the stable id the fake returns on success;
	// fireTriggerErr lets tests force the error path. fireTriggerCalls
	// captures each invocation so tests can assert the (id) the handler
	// passed through.
	fireTriggerRunID string
	fireTriggerErr   error
	fireTriggerCalls []string

	// #312 (config page): test seam for the ConfigSnapshot
	// surface. Tests assign these directly to drive the page.
	configSnap    ConfigSnapshot
	configSnapErr error

	// #328 (task-types page): optional override for the rows the
	// fake returns from AggregateTaskTypes. Default behaviour derives
	// rows from configSnap.Workers so most tests need no extra setup;
	// tests that want a curated row set assign taskTypeRows directly.
	taskTypeRows    []TaskTypeRow
	taskTypeRowsErr error

	// #335 (services cross-reference): pre-seeded ServiceDef list. The
	// fake AggregateTaskTypes mirrors the production adapter — calls
	// attachServiceDescriptions(rows, services) after the fold — so a
	// test that wants tooltip rendering just sets this. The services
	// roster page (ListServiceRows) projects this same slice via the
	// production serviceRowsFromDefs. serviceRowsErr forces the read-
	// error path (omit-on-error nav-count contract).
	services       []worker.ServiceDef
	serviceRowsErr error

	// T13 (Phase 2): sparkline backing data. sparklineSeries is keyed
	// by "kind/id" so the test can pre-seed deterministic hourly counts
	// without going through the metrics aggregator.
	sparklineSeries map[string][]float64

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

	// Concurrency page backing data. Tests assign an AdmissionState
	// directly so the page renders without reading the engine KV gates.
	admission AdmissionState

	// Run-trace tab backing data. Tests assign runTrace directly to
	// drive the Trace tab; runTraceErr forces the read-error path.
	runTrace    []TraceRow
	runTraceErr error

	// Run Signal / Cancel observability. cancelRunCalls records each
	// CancelRun(runID); signalCalls records each SendSignal invocation.
	// The *Err seams force the failure path.
	cancelRunCalls []string
	cancelRunErr   error
	signalCalls    []signalCall
	signalErr      error

	// #379 (agent runtimes): pre-seeded provenance rows. agentRuntimes
	// backs the list page; agentRuntimeByRoot backs the single-root SSE
	// re-projection. agentRuntimesErr forces the list read-error path.
	agentRuntimes      []AgentRuntimeRow
	agentRuntimesErr   error
	agentRuntimeByRoot map[string]AgentRuntimeRow
}

// signalCall captures one SendSignal invocation so tests can assert the
// (runID, name, data) the handler passed through.
type signalCall struct {
	RunID string
	Name  string
	Data  []byte
}

// triggerSetCall captures one SetTriggerEnabled invocation so tests can
// assert against the call pattern.
type triggerSetCall struct {
	ID      string
	Enabled bool
}

// startRunCall captures one StartRun invocation. Tests assert against
// the (Workflow, Input) pair to confirm the handler delegated correctly.
type startRunCall struct {
	Workflow string
	Input    []byte
}

func newFakeDS() *fakeDataSource {
	return &fakeDataSource{
		events:          make(map[string][]api.RunEvent),
		runHistory:      make(map[string]chan HistoryEvent),
		triggerFires:    make(map[string][]TriggerFireRow),
		kvKeys:          make(map[string][]string),
		kvEntries:       make(map[string][]byte),
		sparklineSeries: make(map[string][]float64),
	}
}

// seedSparklineHourly populates sparklineSeries with hours-many points
// for the (kind, id) tuple. Each bucket gets value i+1 so tests can
// assert ordering and non-zeroness in one shot. now is unused — the
// fake stores by slot index, not wall-clock — but we keep the
// parameter so the call site reads like the production usage.
func (f *fakeDataSource) seedSparklineHourly(
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

func (f *fakeDataSource) ListWorkflows(
	_ context.Context,
) ([]dag.WorkflowDef, error) {
	return append([]dag.WorkflowDef{}, f.workflows...), nil
}

func (f *fakeDataSource) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if name == "" {
		panic("fakeDataSource.GetWorkflow: empty name")
	}
	for _, d := range f.workflows {
		if d.Name == name {
			return d, nil
		}
	}
	return dag.WorkflowDef{}, errNotFound("workflow", name)
}

func (f *fakeDataSource) ListRuns(
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

func (f *fakeDataSource) GetRun(
	_ context.Context, runID string,
) (dag.WorkflowRun, error) {
	if runID == "" {
		panic("fakeDataSource.GetRun: empty runID")
	}
	for _, r := range f.runs {
		if r.RunID == runID {
			return r, nil
		}
	}
	return dag.WorkflowRun{}, errNotFound("run", runID)
}

func (f *fakeDataSource) ListRunEvents(
	_ context.Context, runID string, _ bool,
) ([]api.RunEvent, error) {
	if runID == "" {
		panic("fakeDataSource.ListRunEvents: empty runID")
	}
	return append([]api.RunEvent{}, f.events[runID]...), nil
}

func (f *fakeDataSource) ListTriggers(
	_ context.Context,
) ([]trigger.TriggerDef, error) {
	f.listTriggerCalls++
	return append([]trigger.TriggerDef{}, f.triggers...), nil
}

func (f *fakeDataSource) GetRunTrace(
	_ context.Context, runID string,
) ([]TraceRow, error) {
	if runID == "" {
		panic("fakeDataSource.GetRunTrace: empty runID")
	}
	if f.runTraceErr != nil {
		return nil, f.runTraceErr
	}
	return append([]TraceRow{}, f.runTrace...), nil
}

// WatchRuns and WatchRunHistory let the fake satisfy the streaming
// surface PR 3 added. The default fake returns a static, never-firing
// channel; tests that exercise the SSE handlers (streams_test.go)
// supply a fake.runUpdates / fake.runHistory channel they own and
// drive directly.
func (f *fakeDataSource) WatchRuns(
	ctx context.Context,
) (<-chan RunUpdate, error) {
	if ctx == nil {
		panic("fakeDataSource.WatchRuns: ctx is nil")
	}
	if f.runUpdates != nil {
		return f.runUpdates, nil
	}
	ch := make(chan RunUpdate)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeDataSource) ListDeadLetters(
	_ context.Context, limit int,
) ([]api.DeadLetterView, error) {
	if limit <= 0 {
		panic("fakeDataSource.ListDeadLetters: limit must be positive")
	}
	out := append([]api.DeadLetterView{}, f.deadLetters...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeDataSource) ReplayDeadLetter(
	_ context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("fakeDataSource.ReplayDeadLetter: seq must be positive")
	}
	f.replayCalls = append(f.replayCalls, seq)
	return f.replayErr
}

func (f *fakeDataSource) DiscardDeadLetter(
	_ context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("fakeDataSource.DiscardDeadLetter: seq must be positive")
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

func (f *fakeDataSource) ListAuditEvents(
	_ context.Context, limit int,
) ([]AuditEvent, error) {
	if limit <= 0 {
		panic("fakeDataSource.ListAuditEvents: limit must be positive")
	}
	out := append([]AuditEvent{}, f.auditEvents...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeDataSource) EmitAuditEvent(
	_ context.Context, evt AuditEvent,
) error {
	f.auditEvents = append([]AuditEvent{evt}, f.auditEvents...)
	return nil
}

func (f *fakeDataSource) ListAgentRuntimes(
	ctx context.Context, limit int,
) ([]AgentRuntimeRow, error) {
	if ctx == nil {
		panic("fakeDataSource.ListAgentRuntimes: ctx is nil")
	}
	if limit <= 0 {
		panic("fakeDataSource.ListAgentRuntimes: limit must be positive")
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

func (f *fakeDataSource) AgentRuntime(
	ctx context.Context, root string,
) (AgentRuntimeRow, bool, error) {
	if ctx == nil {
		panic("fakeDataSource.AgentRuntime: ctx is nil")
	}
	if root == "" {
		return AgentRuntimeRow{}, false, nil
	}
	row, ok := f.agentRuntimeByRoot[root]
	return row, ok, nil
}

// StartRun records the call and returns the seeded id / error. Tests
// that want a non-empty run id assign startRunID; the default empty
// string still exercises the response shape and audit emission.
func (f *fakeDataSource) StartRun(
	_ context.Context, workflowName string, input []byte,
) (string, error) {
	if workflowName == "" {
		panic("fakeDataSource.StartRun: workflowName is empty")
	}
	f.startRunCalls = append(f.startRunCalls,
		startRunCall{Workflow: workflowName, Input: input})
	if f.startRunErr != nil {
		return "", f.startRunErr
	}
	return f.startRunID, nil
}

func (f *fakeDataSource) SetTriggerEnabled(
	_ context.Context, triggerID string, enabled bool,
) error {
	if triggerID == "" {
		panic("fakeDataSource.SetTriggerEnabled: empty triggerID")
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

// updateTriggerCall captures one UpdateTrigger invocation so tests can
// assert the (id, updates) pair the handler delegated.
type updateTriggerCall struct {
	ID      string
	Updates api.TriggerUpdates
}

func (f *fakeDataSource) CreateTrigger(
	_ context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		panic("fakeDataSource.CreateTrigger: empty def.ID")
	}
	if def.WorkflowID == "" {
		panic("fakeDataSource.CreateTrigger: empty def.WorkflowID")
	}
	f.createTriggerCalls = append(f.createTriggerCalls, def)
	if f.createTriggerErr != nil {
		return f.createTriggerErr
	}
	f.triggers = append(f.triggers, def)
	return nil
}

func (f *fakeDataSource) UpdateTrigger(
	_ context.Context, triggerID string, updates api.TriggerUpdates,
) error {
	if triggerID == "" {
		panic("fakeDataSource.UpdateTrigger: empty triggerID")
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

func (f *fakeDataSource) DeleteTrigger(
	_ context.Context, triggerID string,
) error {
	if triggerID == "" {
		panic("fakeDataSource.DeleteTrigger: empty triggerID")
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

// FireTrigger records the manual fire call and returns the seeded id
// / error. Tests that exercise the success path assign fireTriggerRunID;
// tests that exercise kind / disabled / transport errors assign
// fireTriggerErr.
func (f *fakeDataSource) FireTrigger(
	_ context.Context, triggerID string,
) (string, error) {
	if triggerID == "" {
		panic("fakeDataSource.FireTrigger: empty triggerID")
	}
	f.fireTriggerCalls = append(f.fireTriggerCalls, triggerID)
	if f.fireTriggerErr != nil {
		return "", f.fireTriggerErr
	}
	return f.fireTriggerRunID, nil
}

// CancelRun records the call and returns the seeded error. Tests that
// exercise the success path leave cancelRunErr nil.
func (f *fakeDataSource) CancelRun(
	_ context.Context, runID string,
) error {
	if runID == "" {
		panic("fakeDataSource.CancelRun: empty runID")
	}
	f.cancelRunCalls = append(f.cancelRunCalls, runID)
	return f.cancelRunErr
}

// SendSignal records the call and returns the seeded error. The data is
// copied so a caller reusing the buffer can't mutate the recorded value.
func (f *fakeDataSource) SendSignal(
	_ context.Context, runID, name string, data []byte,
) error {
	if runID == "" {
		panic("fakeDataSource.SendSignal: empty runID")
	}
	if name == "" {
		panic("fakeDataSource.SendSignal: empty name")
	}
	f.signalCalls = append(f.signalCalls, signalCall{
		RunID: runID, Name: name, Data: append([]byte{}, data...),
	})
	return f.signalErr
}

func (f *fakeDataSource) ListTriggerFires(
	_ context.Context, triggerID string, limit int,
) ([]TriggerFireRow, error) {
	if triggerID == "" {
		panic("fakeDataSource.ListTriggerFires: empty triggerID")
	}
	if limit <= 0 {
		panic("fakeDataSource.ListTriggerFires: limit must be positive")
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

func (f *fakeDataSource) WatchTriggers(
	ctx context.Context,
) (<-chan TriggerUpdate, error) {
	if ctx == nil {
		panic("fakeDataSource.WatchTriggers: ctx is nil")
	}
	if f.triggerUpdates != nil {
		return f.triggerUpdates, nil
	}
	ch := make(chan TriggerUpdate)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeDataSource) WatchDLQ(
	ctx context.Context,
) (<-chan DLQUpdate, error) {
	if ctx == nil {
		panic("fakeDataSource.WatchDLQ: ctx is nil")
	}
	if f.dlqUpdates != nil {
		return f.dlqUpdates, nil
	}
	ch := make(chan DLQUpdate)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeDataSource) WatchRunHistory(
	ctx context.Context, runID string, _ uint64,
) (<-chan HistoryEvent, error) {
	if ctx == nil {
		panic("fakeDataSource.WatchRunHistory: ctx is nil")
	}
	if runID == "" {
		panic("fakeDataSource.WatchRunHistory: runID is empty")
	}
	if ch, ok := f.runHistory[runID]; ok {
		return ch, nil
	}
	ch := make(chan HistoryEvent)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// PR 5b: KV inspector data the fake exposes. kvBuckets is the side-nav
// inventory; kvEntries is keyed by bucket/key so GetKVEntry can hand
// back deterministic bytes.
func (f *fakeDataSource) ListKVBuckets(
	_ context.Context,
) ([]KVBucketInfo, error) {
	return append([]KVBucketInfo{}, f.kvBuckets...), nil
}

func (f *fakeDataSource) ListConsumers(
	_ context.Context,
) ([]ConsumerRow, error) {
	return append([]ConsumerRow{}, f.consumers...), nil
}

func (f *fakeDataSource) ServerHealth(
	_ context.Context,
) (ServerHealth, error) {
	return f.serverHealth, f.serverHealthErr
}

func (f *fakeDataSource) ListConnections(
	_ context.Context,
) ([]ConnRow, error) {
	return append([]ConnRow{}, f.connections...), nil
}

func (f *fakeDataSource) AdmissionState(
	_ context.Context,
) (AdmissionState, error) {
	return f.admission, nil
}

func (f *fakeDataSource) ListKVKeys(
	_ context.Context, bucket, _ string, limit int,
) ([]string, string, error) {
	if bucket == "" {
		panic("fakeDataSource.ListKVKeys: bucket is empty")
	}
	if limit <= 0 {
		panic("fakeDataSource.ListKVKeys: limit must be positive")
	}
	keys := append([]string{}, f.kvKeys[bucket]...)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, "", nil
}

func (f *fakeDataSource) SparklineData(
	_ context.Context, kind, id string, hours int,
) ([]float64, error) {
	if kind == "" {
		panic("fakeDataSource.SparklineData: kind is empty")
	}
	if id == "" {
		panic("fakeDataSource.SparklineData: id is empty")
	}
	if hours <= 0 {
		panic("fakeDataSource.SparklineData: hours must be positive")
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

func (f *fakeDataSource) GetKVEntry(
	_ context.Context, bucket, key string,
) (KVEntryView, error) {
	if bucket == "" {
		panic("fakeDataSource.GetKVEntry: bucket is empty")
	}
	if key == "" {
		panic("fakeDataSource.GetKVEntry: key is empty")
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

// ConfigSnapshot is the test seam for the /console/config page
// (#312). The default zero value renders the empty-state shell;
// tests assign configSnap directly to drive richer scenarios.
func (f *fakeDataSource) ConfigSnapshot(
	_ context.Context,
) (ConfigSnapshot, error) {
	if f.configSnapErr != nil {
		return ConfigSnapshot{}, f.configSnapErr
	}
	return f.configSnap, nil
}

// AggregateTaskTypes is the test seam for the /console/task-types
// page (#328). Default behaviour mirrors the production adapter:
// derive task-type rows from the worker registrations the test
// pre-seeded on configSnap.Workers and then cross-reference the
// services list (#335) for Description metadata. Tests that want to
// override the derivation (e.g. inject pre-computed rows) assign
// taskTypeRows or taskTypeRowsErr directly.
func (f *fakeDataSource) AggregateTaskTypes(
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

// ListWorkerRows is the test seam for the /console/workers page.
// Mirrors the production adapter: project the worker registrations the
// test pre-seeded on configSnap.Workers into render rows. now is fixed
// to time.Now() so liveness classification matches wall-clock tests.
func (f *fakeDataSource) ListWorkerRows(
	_ context.Context,
) ([]WorkerStatusRow, error) {
	return workerRowsFromRegistrations(f.configSnap.Workers, time.Now()), nil
}

// ListServiceRows is the test seam for the /console/services page.
// Mirrors the production adapter: project the pre-seeded services slice
// through serviceRowsFromDefs. serviceRowsErr forces the error path so
// tests can assert the omit-on-error nav-count contract.
func (f *fakeDataSource) ListServiceRows(
	_ context.Context,
) ([]ServiceRow, error) {
	if f.serviceRowsErr != nil {
		return nil, f.serviceRowsErr
	}
	return serviceRowsFromDefs(f.services), nil
}

// WorkerDetail is the test seam for /console/workers/{id}. Mirrors the
// production adapter: project the same configSnap.Workers the list page
// reads, matching by id, with now fixed to wall-clock so liveness
// classification matches the list page.
func (f *fakeDataSource) WorkerDetail(
	_ context.Context, id string,
) (WorkerDetail, error) {
	return workerDetailFromRegistrations(f.configSnap.Workers, id, time.Now()), nil
}

// Search mirrors the production adapter's contract over the fake's
// in-memory slices. We keep the rules identical (substring for
// workflows + triggers; prefix ≥4 chars for runs) so unit tests
// exercise the same shape the real service hands the palette.
func (f *fakeDataSource) Search(
	_ context.Context, query string, limit int,
) ([]SearchHit, error) {
	if limit <= 0 {
		panic("fakeDataSource.Search: limit must be positive")
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
	if len(q) >= runIDSearchMinChars {
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
	}
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

type stringError string

func (e stringError) Error() string { return string(e) }

func errNotFound(kind, name string) error {
	return stringError(kind + " " + name + " not found")
}

// testLogWriter routes slog output to t.Log so failures surface
// the handler's diagnostic line in the verbose test output. With
// io.Discard a 500 was opaque; this lets us debug template /
// render failures inline.
type tlw struct{ t *testing.T }

func (w tlw) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func testLogWriter(t *testing.T) io.Writer {
	if t == nil {
		panic("testLogWriter: t is nil")
	}
	return tlw{t: t}
}

// mountWithFake builds a console handler wired against fake data.
// Mirrors the PR 1 helper but injects the DataSource.
func mountWithFake(t *testing.T, fake *fakeDataSource) http.Handler {
	t.Helper()
	if fake == nil {
		t.Fatalf("mountWithFake: fake is nil")
	}
	return Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	})
}

func sampleWorkflow(name string) dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    name,
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "first", Task: "echo", Timeout: time.Minute},
			{ID: "second", Task: "echo", Timeout: time.Minute, DependsOn: []string{"first"}},
		},
	}
}

// withSteps clones a workflow's step set with custom states applied.
// Used to assemble a run snapshot with known step status patterns.
func runWithSteps(
	id, workflow string, status dag.RunStatus,
	stepStates map[string]dag.StepState, created time.Time,
) dag.WorkflowRun {
	steps := make(map[string]dag.StepState, len(stepStates))
	for k, v := range stepStates {
		steps[k] = v
	}
	return dag.WorkflowRun{
		RunID:      id,
		WorkflowID: workflow,
		Status:     status,
		Steps:      steps,
		CreatedAt:  created,
	}
}

func TestWorkflowsList_rendersExpectedColumns(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		sampleWorkflow("alpha"),
		sampleWorkflow("beta"),
		sampleWorkflow("gamma"),
	}
	fake.runs = []dag.WorkflowRun{
		{
			RunID: "run-1", WorkflowID: "alpha", Status: dag.RunStatusCompleted,
			CreatedAt: time.Now().Add(-time.Hour),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/workflows", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"<title>Workflows", "alpha", "beta", "gamma",
		"<th>Steps</th>", "<th>Triggers</th>", "<th>Last run</th>",
		`href="/console/workflows/alpha"`,
		`id="workflows-tbody"`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("workflows page missing %q", sub)
		}
	}
}

func TestWorkflowDetail_rendersDefinitionAndWarnings(t *testing.T) {
	fake := newFakeDS()
	// Build a workflow that triggers the missing_respond warning.
	wf := dag.WorkflowDef{
		Name:    "needs-respond",
		Version: "v1",
		Steps:   []dag.StepDef{{ID: "only", Task: "echo", Timeout: time.Minute}},
	}
	fake.workflows = []dag.WorkflowDef{wf}
	fake.triggers = []trigger.TriggerDef{{
		ID: "trg", WorkflowID: "needs-respond",
		HTTP: &trigger.HTTPConfig{Method: "POST", Path: "/hooks/test"},
	}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/workflows/needs-respond", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "needs-respond") {
		t.Errorf("workflow detail missing name")
	}
	if !strings.Contains(body, "missing_respond") {
		t.Errorf("workflow detail missing validator warning kind")
	}
	// html/template escapes double quotes in &lt;pre&gt;&lt;code&gt; blocks
	// so the literal " becomes &#34;. Either form is acceptable as long
	// as the JSON content is present.
	hasEscaped := strings.Contains(body, `&#34;name&#34;: &#34;needs-respond&#34;`)
	hasRaw := strings.Contains(body, `"name": "needs-respond"`)
	if !hasEscaped && !hasRaw {
		t.Errorf("workflow detail missing definition JSON (escaped or raw)")
	}
	if !strings.Contains(body, "POST /hooks/test") {
		t.Errorf("workflow detail missing trigger target")
	}
}

func TestWorkflowDetail_buildsStepDAG(t *testing.T) {
	ds := newFakeDS()
	ds.workflows = []dag.WorkflowDef{{
		Name:    "pipe",
		Version: "v2",
		Steps: []dag.StepDef{
			{ID: "fetch", Task: "fetch", Type: dag.StepTypeNormal},
			{ID: "think", Task: "think", Type: dag.StepTypeAgent,
				DependsOn: []string{"fetch"}},
			{ID: "fan", Task: "fan", Type: dag.StepTypeMap,
				DependsOn: []string{"fetch", "think"}},
		},
	}}
	view := buildWorkflowDetail(context.Background(), ds, "pipe")

	if view.StepCount != 3 {
		t.Fatalf("StepCount = %d, want 3", view.StepCount)
	}
	if len(view.Steps) != 3 {
		t.Fatalf("Steps len = %d, want 3", len(view.Steps))
	}
	first := view.Steps[0]
	if first.Num != 1 || first.Name != "fetch" {
		t.Errorf("step[0] = {Num:%d Name:%q}, want {1 fetch}", first.Num, first.Name)
	}
	if first.TypeLabel != "normal" {
		t.Errorf("step[0].TypeLabel = %q, want normal", first.TypeLabel)
	}
	if !first.IsEntry {
		t.Errorf("step[0].IsEntry = false, want true (no deps)")
	}
	second := view.Steps[1]
	if second.Num != 2 || second.TypeLabel != "agent" {
		t.Errorf("step[1] = {Num:%d TypeLabel:%q}, want {2 agent}",
			second.Num, second.TypeLabel)
	}
	if second.IsEntry {
		t.Errorf("step[1].IsEntry = true, want false (has deps)")
	}
	if len(second.DependsOn) != 1 || second.DependsOn[0] != "fetch" {
		t.Errorf("step[1].DependsOn = %v, want [fetch]", second.DependsOn)
	}
	third := view.Steps[2]
	if third.TypeLabel != "map" {
		t.Errorf("step[2].TypeLabel = %q, want map", third.TypeLabel)
	}
	if len(third.DependsOn) != 2 {
		t.Errorf("step[2].DependsOn = %v, want 2 deps", third.DependsOn)
	}
}

func TestWorkflowDetail_rendersStepDAGAndDemotesJSON(t *testing.T) {
	ds := newFakeDS()
	ds.workflows = []dag.WorkflowDef{{
		Name:    "pipe",
		Version: "v2",
		Steps: []dag.StepDef{
			{ID: "fetch", Task: "fetch", Type: dag.StepTypeNormal},
			{ID: "think", Task: "think", Type: dag.StepTypeAgent,
				DependsOn: []string{"fetch"}},
		},
	}}
	h := mountWithFake(t, ds)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/workflows/pipe", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"console-steps", "console-step", "2 steps", "depends_on",
		"console-step-type", "fetch", "think", "entry",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("step DAG body missing %q", want)
		}
	}
	// Raw JSON must be demoted into a collapsible <details>, not the
	// primary card. The JSON content (escaped or raw) must still be present.
	if !strings.Contains(body, "<details") {
		t.Errorf("raw definition JSON not wrapped in <details>")
	}
	detailsIdx := strings.Index(body, "<details")
	jsonRawIdx := strings.Index(body, `"name": "pipe"`)
	jsonEscIdx := strings.Index(body, `&#34;name&#34;: &#34;pipe&#34;`)
	jsonIdx := jsonRawIdx
	if jsonIdx < 0 {
		jsonIdx = jsonEscIdx
	}
	if jsonIdx < 0 {
		t.Fatalf("definition JSON (escaped or raw) absent from page")
	}
	if jsonIdx < detailsIdx {
		t.Errorf("definition JSON appears before <details>; not demoted")
	}
}

func TestRunsList_filtersByStatus(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	fake.runs = []dag.WorkflowRun{
		{
			RunID: "run-1", WorkflowID: "alpha", Status: dag.RunStatusCompleted,
			CreatedAt: now.Add(-3 * time.Minute),
		},
		{
			RunID: "run-2", WorkflowID: "alpha", Status: dag.RunStatusFailed,
			CreatedAt: now.Add(-2 * time.Minute),
		},
		{
			RunID: "run-3", WorkflowID: "alpha", Status: dag.RunStatusRunning,
			CreatedAt: now.Add(-1 * time.Minute),
		},
		{
			RunID: "run-4", WorkflowID: "alpha", Status: dag.RunStatusFailed,
			CreatedAt: now,
		},
		{
			RunID: "run-5", WorkflowID: "alpha", Status: dag.RunStatusPending,
			CreatedAt: now.Add(-time.Hour),
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/runs?status=failed", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "run-2") {
		t.Errorf("failed filter missing run-2")
	}
	if !strings.Contains(body, "run-4") {
		t.Errorf("failed filter missing run-4")
	}
	if strings.Contains(body, "run-1") || strings.Contains(body, "run-3") {
		t.Errorf("failed filter leaked non-failed rows")
	}
}

func TestRunDetail_rendersEventTimelineAndStepGrid(t *testing.T) {
	// Mockup reshape: the run detail page is a tabs container with
	// Events default-active (eager) and the step list living under the
	// Timeline panel. We assert the structural anchors that survive the
	// restructure plus the per-step error message that surfaces on the
	// Timeline tab.
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	run := runWithSteps("run-x", "alpha", dag.RunStatusFailed,
		map[string]dag.StepState{
			"first":  {Status: dag.StepStatusCompleted, Attempts: 1},
			"second": {Status: dag.StepStatusFailed, Attempts: 3, Error: "boom"},
		},
		now.Add(-time.Minute),
	)
	fake.runs = []dag.WorkflowRun{run}
	fake.events["run-x"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-x", Timestamp: now.Add(-2 * time.Minute)},
		{Type: "step.completed", RunID: "run-x", StepID: "first",
			Timestamp: now.Add(-90 * time.Second), Data: `{"out":1}`},
		{Type: "step.failed", RunID: "run-x", StepID: "second",
			Timestamp: now.Add(-30 * time.Second), Data: `{"error":"boom"}`},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/runs/run-x", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`id="panel-events"`,
		`id="panel-timeline"`,
		"boom",
		`href="/console/workflows/alpha"`,
		"first", "second",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("run detail missing %q", sub)
		}
	}
}

// TestRunDetail_eventTimelineRowsUnique pins the close-out fix for the
// end-of-arc bug where every row appeared twice — once from the
// server-rendered tbody and once from the SSE replay. The SSE URL
// must carry ?from=<MaxEventSeq> so live updates resume past the
// rendered prefix.
//
// Phase 2: events now live behind a lazy-loaded tab. We assert the
// SSE URL still carries ?from=<seq> on the initial page (so when the
// operator opens the events tab the prefix is correct), and we hit
// the events-tab fragment endpoint to verify each event id appears
// exactly once inside the fragment HTML.
func TestRunDetail_eventTimelineRowsUnique(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	run := runWithSteps("run-dup", "alpha", dag.RunStatusCompleted,
		map[string]dag.StepState{
			"first":  {Status: dag.StepStatusCompleted, Attempts: 1},
			"second": {Status: dag.StepStatusCompleted, Attempts: 1},
		},
		now.Add(-time.Minute),
	)
	fake.runs = []dag.WorkflowRun{run}
	fake.events["run-dup"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-dup",
			Timestamp: now.Add(-2 * time.Minute), Seq: 11},
		{Type: "step.queued", RunID: "run-dup", StepID: "first",
			Timestamp: now.Add(-110 * time.Second), Seq: 12},
		{Type: "step.completed", RunID: "run-dup", StepID: "first",
			Timestamp: now.Add(-100 * time.Second), Seq: 13},
		{Type: "step.completed", RunID: "run-dup", StepID: "second",
			Timestamp: now.Add(-30 * time.Second), Seq: 14},
		{Type: "workflow.completed", RunID: "run-dup",
			Timestamp: now.Add(-25 * time.Second), Seq: 15},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-dup", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// SSE must resume past the rendered prefix using ?from=<MaxSeq>.
	if !strings.Contains(body, "/console/sse/runs/run-dup?from=15") {
		t.Errorf("SSE data-init missing ?from=<seq>; body=\n%s",
			body)
	}
	// Events-tab fragment must render every event id exactly once.
	fragRR := httptest.NewRecorder()
	h.ServeHTTP(fragRR, httptest.NewRequest(http.MethodGet,
		"/console/api/run/run-dup/events-tab", nil))
	if fragRR.Code != http.StatusOK {
		t.Fatalf("events-tab status = %d, want 200", fragRR.Code)
	}
	fragBody := fragRR.Body.String()
	for i := 0; i < 5; i++ {
		needle := `id="run-event-row-` + strconv.Itoa(i) + `"`
		if got := strings.Count(fragBody, needle); got != 1 {
			t.Errorf("row %s appeared %d times in events-tab fragment, want 1",
				needle, got)
		}
	}
}

func TestRunDetail_notFound_rendersBackLink(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/runs/missing", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No run snapshot found") {
		t.Errorf("missing run page lacks helpful 'not found' message")
	}
	if !strings.Contains(body, `href="/console/runs"`) {
		t.Errorf("missing run page lacks back link")
	}
}

func TestRunsList_paginationClampsAndReturnsCorrectRange(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	// 30 runs, oldest first; ListRuns sorts newest-first, so the
	// resulting rendered order has run-30 first and run-1 last.
	for i := 1; i <= 30; i++ {
		fake.runs = append(fake.runs, dag.WorkflowRun{
			RunID: runIDForIndex(i), WorkflowID: "alpha",
			Status:    dag.RunStatusCompleted,
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs?page=2&size=10", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Page 2 (size 10) of the newest-first run list = items 11..20.
	// In our test setup that's run-20 down to run-11.
	wantPresent := []string{"run-20", "run-15", "run-11"}
	for _, sub := range wantPresent {
		if !strings.Contains(body, sub) {
			t.Errorf("page 2 missing %q", sub)
		}
	}
	wantAbsent := []string{"run-30", "run-21", "run-10", "run-01"}
	for _, sub := range wantAbsent {
		if strings.Contains(body, sub) {
			t.Errorf("page 2 leaked %q", sub)
		}
	}
}

func TestPaginate_clampsAndEdges(t *testing.T) {
	cases := []struct {
		total, page, size, wantStart, wantEnd int
		wantNext                              bool
	}{
		{total: 0, page: 1, size: 10, wantStart: 0, wantEnd: 0, wantNext: false},
		{total: 5, page: 1, size: 10, wantStart: 0, wantEnd: 5, wantNext: false},
		{total: 25, page: 2, size: 10, wantStart: 10, wantEnd: 20, wantNext: true},
		{total: 25, page: 3, size: 10, wantStart: 20, wantEnd: 25, wantNext: false},
		{total: 25, page: 99, size: 10, wantStart: 25, wantEnd: 25, wantNext: false},
	}
	for _, tc := range cases {
		gotStart, gotEnd, gotNext := paginate(tc.total, tc.page, tc.size)
		if gotStart != tc.wantStart || gotEnd != tc.wantEnd || gotNext != tc.wantNext {
			t.Errorf("paginate(%d,%d,%d) = (%d,%d,%v); want (%d,%d,%v)",
				tc.total, tc.page, tc.size,
				gotStart, gotEnd, gotNext,
				tc.wantStart, tc.wantEnd, tc.wantNext)
		}
	}
}

func TestParsePageAndSize_clamps(t *testing.T) {
	type want struct {
		page int
		size int
	}
	cases := map[string]struct {
		pageStr, sizeStr string
		want             want
	}{
		"defaults":            {"", "", want{1, pageSizeDefault}},
		"zero rejected":       {"0", "0", want{1, pageSizeDefault}},
		"negative rejected":   {"-3", "-10", want{1, pageSizeDefault}},
		"valid pair":          {"4", "25", want{4, 25}},
		"size clamped":        {"4", "999", want{4, pageSizeMax}},
		"page clamp":          {"99999999", "10", want{pageNumberMax, 10}},
		"non-numeric ignored": {"foo", "bar", want{1, pageSizeDefault}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			page, size := parsePageAndSize(tc.pageStr, tc.sizeStr)
			if page != tc.want.page || size != tc.want.size {
				t.Fatalf("(%q,%q) = (%d,%d); want (%d,%d)",
					tc.pageStr, tc.sizeStr,
					page, size,
					tc.want.page, tc.want.size)
			}
		})
	}
}

func TestStatusIcon_table(t *testing.T) {
	cases := map[string]string{
		"completed": "✓",
		"running":   "●",
		"failed":    "✗",
		"skipped":   "⊘",
		"cancelled": "⊘",
		"pending":   "○",
		"queued":    "○",
		"":          "○",
	}
	for in, want := range cases {
		if got := statusIcon(in); got != want {
			t.Errorf("statusIcon(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestNoExternalURLs_allPages enforces the local-first asset policy
// across the IA PR 2 introduces. Each page must reference only
// /console/-relative URLs in src/href/@import.
func TestNoExternalURLs_allPages(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{{
		RunID:      "run-1",
		WorkflowID: "alpha",
		Status:     dag.RunStatusCompleted,
		CreatedAt:  time.Now(),
	}}
	fake.triggers = []trigger.TriggerDef{{
		ID:         "cron-1",
		WorkflowID: "alpha",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "*/5 * * * *"},
	}}
	fake.deadLetters = []api.DeadLetterView{{
		DeadLetter: api.DeadLetter{
			Sequence:  42,
			Subject:   "dead.task.alpha.first",
			RunID:     "run-failed-1",
			StepID:    "first",
			Task:      "task.alpha.first",
			Error:     "task timed out",
			Timestamp: time.Now(),
			Body:      []byte(`{"x":1}`),
		},
		BodyPreserved: true,
	}}
	fake.auditEvents = []AuditEvent{{
		Time: time.Now(), Actor: "operator",
		Action: "dlq.retry", Target: "42", Outcome: "success",
	}}
	h := mountWithFake(t, fake)
	pages := []string{
		"/console/",
		"/console/workflows",
		"/console/workflows/alpha",
		"/console/runs",
		"/console/runs/run-1",
		"/console/triggers",
		"/console/triggers/cron-1",
		"/console/workers",
		"/console/kv",
		"/console/streams",
		"/console/dlq",
		"/console/dlq/42",
		"/console/audit",
		"/console/metrics",
		"/console/config",
	}
	external := regexp.MustCompile(
		`(?i)(src|href)\s*=\s*"((https?:)?//[^"]+)"`)
	atImport := regexp.MustCompile(
		`(?i)@import\s+(url\()?["']((https?:)?//[^"']+)["']?`)
	for _, page := range pages {
		t.Run(page, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, page, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			body := rr.Body.String()
			if m := external.FindStringSubmatch(body); m != nil {
				t.Errorf("external URL in src/href: %s", m[2])
			}
			if m := atImport.FindStringSubmatch(body); m != nil {
				t.Errorf("external URL in @import: %s", m[2])
			}
		})
	}
}

// TestRunDetail_rendersTabs asserts the run detail page renders the
// mockup's three-tab container (Events default-active, then IO,
// Timeline). Methodology: structural substring match against the
// tablist; we don't pin exact Basecoat class names so the CSS layer
// can evolve, but the ARIA structure is load-bearing — screen readers
// and tests depend on it. The retired Steps/Trace tabs must be absent
// (Steps absorbed into Timeline; Trace is now an action).
func TestRunDetail_rendersTabs(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-tabs", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first":  {Status: dag.StepStatusCompleted, Attempts: 1},
				"second": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, time.Now().Add(-time.Minute)),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-tabs", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, label := range []string{
		">Events<", ">IO<", ">Timeline<",
	} {
		if !strings.Contains(body, label) {
			t.Errorf("missing tab label %q", label)
		}
	}
	// Events tab must be marked default-active. Match the aria-selected
	// pair on the Events button id without pinning attribute ordering.
	if !strings.Contains(body, `id="tab-events"`) ||
		!strings.Contains(body, `aria-selected="true" aria-controls="panel-events"`) {
		t.Error("Events tab is not the default-active tab")
	}
	for _, panelID := range []string{
		`id="panel-events"`, `id="panel-io"`, `id="panel-timeline"`,
	} {
		if !strings.Contains(body, panelID) {
			t.Errorf("missing tab panel %q", panelID)
		}
	}
	// Negative space: the retired tabs must be gone.
	for _, gone := range []string{
		`id="tab-steps"`, `id="panel-steps"`,
		`id="tab-trace"`, `id="panel-trace"`,
	} {
		if strings.Contains(body, gone) {
			t.Errorf("retired tab artifact still present: %q", gone)
		}
	}
}

// TestRunDetail_headerCompactIdentity asserts the mockup's compact
// single-line identity header: run id rendered mono/accent, status
// badge, a workflow link, duration + started inline, a "Runs" back
// link, and the trailing trigger meta — all real data reflowed. The
// retired STARTED/DURATION/TRIGGER <dl> stat block must be gone.
func TestRunDetail_headerCompactIdentity(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-hdr", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, time.Now().Add(-time.Minute)),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-hdr", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`class="run-id console-mono"`, // full id, mono/accent
		`run-hdr`,                     // the full run id is rendered
		`id="run-detail-status"`,      // status badge wrapper kept
		`href="/console/workflows/alpha"`,
		`class="run-back"`, // styled back anchor
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("compact header missing %q", sub)
		}
	}
	// Negative space: the old <dl> stat block must be gone.
	if strings.Contains(body, `class="run-times"`) {
		t.Error("retired run-times stat block still rendered")
	}
}

// TestRunDetail_viewTraceAction asserts the Trace tab became a "View
// trace" action in the action bar pointing at /console/traces/<runID>
// (the standalone traces page), and that Signal/Cancel still render for
// a running run.
func TestRunDetail_viewTraceAction(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{runningRun("run-1")}
	body := renderRunDetail(t, fake, false, "run-1")
	if !strings.Contains(body, `href="/console/traces/run-1"`) {
		t.Errorf("missing View trace action href; body=%s", body)
	}
	for _, sub := range []string{`id="run-signal-btn"`, `id="run-cancel-btn"`} {
		if !strings.Contains(body, sub) {
			t.Errorf("action bar missing %q", sub)
		}
	}
	// Negative space: no Trace tab/panel.
	if strings.Contains(body, `id="tab-trace"`) {
		t.Error("Trace tab must not render; it is now an action")
	}
}

// TestRunDetail_eventsTabDefaultRendersServerSide asserts the Events
// tab is eager (default landing): event rows render in the initial page
// response with the mockup column headers Timestamp / Type / Step /
// Message — no lazy fetch round-trip required.
func TestRunDetail_eventsTabDefaultRendersServerSide(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	run := runWithSteps("run-ev", "alpha", dag.RunStatusCompleted,
		map[string]dag.StepState{
			"first": {Status: dag.StepStatusCompleted, Attempts: 1},
		}, now.Add(-time.Minute))
	fake.runs = []dag.WorkflowRun{run}
	fake.events["run-ev"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-ev",
			Timestamp: now.Add(-2 * time.Minute), Seq: 1},
		{Type: "step.completed", RunID: "run-ev", StepID: "first",
			Timestamp: now.Add(-90 * time.Second), Data: `{"out":1}`, Seq: 2},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-ev", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"<th>Timestamp</th>", "<th>Type</th>",
		"<th>Step</th>", "<th>Message</th>",
		"workflow.started", "step.completed",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("eager events tab missing %q", sub)
		}
	}
}

// TestRunDetail_timelineTabRendersBars asserts the Timeline tab renders
// per-step gantt bars positioned with inline style percentages, that a
// failed step carries the fail class, and that the bar rows keep the
// .step-list-row[data-step-id] SSE hook so live per-step patches still
// land. The empty-steps negative case is covered separately.
func TestRunDetail_timelineTabRendersBars(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	run := runWithSteps("run-tl", "alpha", dag.RunStatusFailed,
		map[string]dag.StepState{
			"first":  {Status: dag.StepStatusCompleted, Attempts: 1},
			"second": {Status: dag.StepStatusFailed, Attempts: 1, Error: "boom"},
		}, now.Add(-100*time.Second))
	fake.runs = []dag.WorkflowRun{run}
	fake.events["run-tl"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-tl",
			Timestamp: now.Add(-100 * time.Second)},
		{Type: "step.started", RunID: "run-tl", StepID: "first",
			Timestamp: now.Add(-90 * time.Second)},
		{Type: "step.completed", RunID: "run-tl", StepID: "first",
			Timestamp: now.Add(-70 * time.Second)},
		{Type: "step.started", RunID: "run-tl", StepID: "second",
			Timestamp: now.Add(-60 * time.Second)},
		{Type: "step.failed", RunID: "run-tl", StepID: "second",
			Timestamp: now.Add(-10 * time.Second), Data: "boom"},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-tl", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"timeline-bar",                      // gantt bars present
		"width:",                            // inline geometry
		"step-state-failed",                 // failed step fail class
		`data-step-id="first"`,              // SSE hook preserved
		`class="step-list-row timeline-row`, // gantt row is the hook node
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("timeline tab missing %q; body=%s", sub, body)
		}
	}
}

// TestRunDetail_timelineEmptyState asserts a run whose workflow has no
// declared steps shows an honest empty-state in the Timeline panel
// rather than a fabricated bar.
func TestRunDetail_timelineEmptyState(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{{Name: "empty"}}
	fake.runs = []dag.WorkflowRun{{
		RunID: "run-empty", WorkflowID: "empty",
		Status: dag.RunStatusCompleted, CreatedAt: time.Now().Add(-time.Minute),
	}}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-empty", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "no steps recorded") {
		t.Errorf("empty timeline missing honest empty-state; body=%s", body)
	}
	if strings.Contains(body, "timeline-bar") {
		t.Error("empty timeline must not fabricate a bar")
	}
}

// TestRunDetail_ioTabTwoColumn asserts the IO tab fragment renders the
// two-column grid with Input/Output card titles, and that the Output
// card is omitted (honest empty-state) when the run produced no output.
func TestRunDetail_ioTabTwoColumn(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	// No step output -> HasOutput false -> Output card omitted.
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-io", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, now.Add(-time.Minute)),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/api/run/run-io/io-tab", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, sub := range []string{"run-io-grid", ">Input<"} {
		if !strings.Contains(body, sub) {
			t.Errorf("io tab missing %q", sub)
		}
	}
	// Negative space: no Output card when the run has no output.
	if strings.Contains(body, `id="run-detail-output"`) {
		t.Error("output card must be omitted when run has no output")
	}
}

// TestRunDetail_failedRunShowsErrorBanner asserts that a failed run
// renders the red error banner above the tabs with the failing step
// id, error message, attempts, and a jump-to-step anchor link.
func TestRunDetail_failedRunShowsErrorBanner(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{{
		Name:    "demo",
		Version: "v1",
		Steps: []dag.StepDef{
			{ID: "fetch", Task: "echo", Timeout: time.Minute},
			{ID: "transform", Task: "echo", Timeout: time.Minute},
		},
	}}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-failed", "demo", dag.RunStatusFailed,
			map[string]dag.StepState{
				"fetch": {Status: dag.StepStatusCompleted, Attempts: 1},
				"transform": {Status: dag.StepStatusFailed,
					Attempts: 3, Error: "boom"},
			}, time.Now().Add(-time.Minute)),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-failed", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`class="alert alert-destructive run-error-banner"`,
		"transform",
		"boom",
		`href="#step-row-transform"`,
		"3 attempts",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("error banner missing %q", sub)
		}
	}
}

// TestRunDetail_completedRunHasNoBanner is the negative-space partner:
// a successful run must not render the failed-run banner.
func TestRunDetail_completedRunHasNoBanner(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-ok", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first":  {Status: dag.StepStatusCompleted, Attempts: 1},
				"second": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, time.Now().Add(-time.Minute)),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/runs/run-ok", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "run-error-banner") {
		t.Error("completed run should not show the failed-run banner")
	}
}

// TestRunDetail_lazyTabFragments asserts the three lazy-load fragment
// endpoints (events-tab, dag-tab, io-tab) return SSE patches that each
// target the matching panel id with inner-mode content.
func TestRunDetail_lazyTabFragments(t *testing.T) {
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	fake.runs = []dag.WorkflowRun{
		runWithSteps("run-lz", "alpha", dag.RunStatusCompleted,
			map[string]dag.StepState{
				"first": {Status: dag.StepStatusCompleted,
					Attempts: 1, Output: []byte(`{"ok":1}`)},
				"second": {Status: dag.StepStatusCompleted, Attempts: 1},
			}, now.Add(-time.Minute)),
	}
	fake.events["run-lz"] = []api.RunEvent{
		{Type: "workflow.started", RunID: "run-lz",
			Timestamp: now.Add(-2 * time.Minute), Seq: 1},
	}
	h := mountWithFake(t, fake)
	cases := []struct {
		name, url, wantSelector string
	}{
		{"events", "/console/api/run/run-lz/events-tab", "panel-events"},
		{"io", "/console/api/run/run-lz/io-tab", "panel-io"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(
				http.MethodGet, tc.url, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s",
					rr.Code, rr.Body.String())
			}
			body := rr.Body.String()
			// SSE wire format includes the selector and event type.
			if !strings.Contains(body, tc.wantSelector) {
				t.Errorf("fragment missing selector %q; body=%s",
					tc.wantSelector, body)
			}
		})
	}
}

// runIDForIndex names a run "run-NN" (zero-padded) so substring
// matches in tests don't confuse "run-1" with "run-10".
func runIDForIndex(i int) string {
	const padded = "00"
	s := padded[:0]
	switch {
	case i >= 100:
		s += "" // not relied on; 30-run cap means we never hit this
	case i >= 10:
		s += ""
	default:
		s = "0"
	}
	return "run-" + s + atoi(i)
}

// atoi is a tiny non-allocating int->string helper local to this
// test file. fmt.Sprintf works but the dependency feels heavy for
// a label.
func atoi(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [3]byte
	idx := len(buf)
	for i > 0 {
		idx--
		buf[idx] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[idx:])
}

// Compile-time confirmation that fakeDataSource satisfies
// the DataSource interface; if it stops, callers see an immediate
// failure instead of a far-away runtime nil dereference.
var _ DataSource = (*fakeDataSource)(nil)

// TestPrintCSS_includesMediaPrintBlock locks in the Phase-2 print
// stylesheet. Operators print run-detail pages for archival; the
// block strips nav/theme-toggle/tabs chrome and reveals URLs after
// links so the printed copy is self-referential.
func TestPrintCSS_includesMediaPrintBlock(t *testing.T) {
	cssBytes, err := os.ReadFile("assets/sources/basecoat-raw.css")
	if err != nil {
		t.Fatalf("read basecoat-raw.css: %v", err)
	}
	css := string(cssBytes)

	// Positive space: the block exists and hides the chrome we
	// intend to hide, expands tab panels, and prints URLs inline.
	wantSubstrings := []string{
		"=== Phase 2: print stylesheet ===",
		"@media print",
		"nav, .console-connection, .console-theme-toggle, .command-palette, .side-sheet",
		"display: none !important",
		".tabs-content { display: block !important",
		"a::after { content: \" (\" attr(href) \")\"",
		"=== end Phase 2: print ===",
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(css, s) {
			t.Errorf("print CSS missing expected fragment %q", s)
		}
	}

	// Negative space: the block must not accidentally re-enable
	// the chrome it just hid (a stray `display: block` on .nav
	// inside the @media print scope would defeat the rule).
	printStart := strings.Index(css, "=== Phase 2: print stylesheet ===")
	printEnd := strings.Index(css, "=== end Phase 2: print ===")
	if printStart < 0 || printEnd < 0 || printEnd <= printStart {
		t.Fatalf("could not locate Phase-2 print block boundaries: start=%d end=%d", printStart, printEnd)
	}
	block := css[printStart:printEnd]
	if strings.Contains(block, "nav { display: block") {
		t.Error("print block must not re-enable nav inside @media print")
	}
}

// TestRunCountWithin_capsInsteadOfPanicking pins the availability
// contract: the fold over the unbounded ListRuns slice that backs
// /console/workflows must degrade to a capped undercount, never a
// panic (500), when total runs exceed runsMax — matching the runs
// table header's truncation on the same data.
func TestRunCountWithin_capsInsteadOfPanicking(t *testing.T) {
	now := time.Now()
	overCap := runsMax + 10
	runs := make([]dag.WorkflowRun, overCap)
	for i := range runs {
		runs[i] = dag.WorkflowRun{
			WorkflowID: "wf",
			CreatedAt:  now.Add(-time.Minute),
		}
	}

	out := runCountWithin(runs, sparklineHours*time.Hour, now)

	// Positive space: it counted, capped at runsMax (undercount, not 500).
	if out["wf"] != runsMax {
		t.Errorf("count = %d, want %d (capped, not full %d)",
			out["wf"], runsMax, overCap)
	}
	// Negative space: no run outside the window leaked in.
	old := []dag.WorkflowRun{{WorkflowID: "wf", CreatedAt: now.Add(-48 * time.Hour)}}
	if got := runCountWithin(old, sparklineHours*time.Hour, now); got["wf"] != 0 {
		t.Errorf("out-of-window count = %d, want 0", got["wf"])
	}
}
