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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
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

	// PR 5b additions: KV inspector backing data.
	kvBuckets []KVBucketInfo
	kvKeys    map[string][]string
	kvEntries map[string][]byte
}

// triggerSetCall captures one SetTriggerEnabled invocation so tests can
// assert against the call pattern.
type triggerSetCall struct {
	ID      string
	Enabled bool
}

func newFakeDS() *fakeDataSource {
	return &fakeDataSource{
		events:       make(map[string][]api.RunEvent),
		runHistory:   make(map[string]chan HistoryEvent),
		triggerFires: make(map[string][]TriggerFireRow),
		kvKeys:       make(map[string][]string),
		kvEntries:    make(map[string][]byte),
	}
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
	return append([]trigger.TriggerDef{}, f.triggers...), nil
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
		`id="run-detail-events"`,
		`id="run-detail-steps"`,
		"workflow.started",
		"step.completed",
		"step.failed",
		"boom",
		`href="/console/workflows/alpha"`,
		"first", "second",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("run detail missing %q", sub)
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
		"/console/dlq",
		"/console/dlq/42",
		"/console/ops",
		"/console/ops/workers",
		"/console/ops/leases",
		"/console/ops/kv",
		"/console/ops/audit",
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
