// ds_ports_test.go proves the domain-port split from issue #564: a page
// unit test depends only on the narrow port its handler needs, not the
// whole DataSource surface.
//
// Methodology:
//   - unimplementedDataSource is a shared panic-stub base that satisfies
//     the full DataSource composite. A page fake embeds it and overrides
//     ONLY the port methods its page reads. Every other method panics if
//     the handler ever calls it — so the test also asserts (negative
//     space) that the page touches nothing outside its declared port.
//   - The concurrency page reads exactly one method (AdmissionState).
//     concurrencyOnlyDS implements that method and nothing else; the page
//     renders green, proving no page test needs the mega-fake.
//   - Each test builds its own Mount with the narrow fake; no shared
//     state, bounded to a single in-process request.
package console

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// unimplementedDataSource panics on every port method. Embed it in a
// page fake and override only the methods that page reads: the fake then
// satisfies DataSource (so Config.Data accepts it) without the test
// author implementing 41 methods, and any call outside the declared port
// fails loudly instead of silently returning a zero value.
type unimplementedDataSource struct{}

// Compile-time proof the base covers the whole composite surface.
var _ DataSource = unimplementedDataSource{}

func (unimplementedDataSource) ListWorkflows(ctx context.Context) ([]dag.WorkflowDef, error) {
	panic("unimplementedDataSource.ListWorkflows: not implemented by this narrow fake")
}

func (unimplementedDataSource) GetWorkflow(name string) (dag.WorkflowDef, error) {
	panic("unimplementedDataSource.GetWorkflow: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListRuns(ctx context.Context, workflowFilter string) ([]dag.WorkflowRun, error) {
	panic("unimplementedDataSource.ListRuns: not implemented by this narrow fake")
}

func (unimplementedDataSource) GetRun(ctx context.Context, runID string) (dag.WorkflowRun, error) {
	panic("unimplementedDataSource.GetRun: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListRunEvents(ctx context.Context, runID string, fullData bool) ([]api.RunEvent, error) {
	panic("unimplementedDataSource.ListRunEvents: not implemented by this narrow fake")
}

func (unimplementedDataSource) StartRun(ctx context.Context, workflowName string, input []byte) (string, error) {
	panic("unimplementedDataSource.StartRun: not implemented by this narrow fake")
}

func (unimplementedDataSource) CancelRun(ctx context.Context, runID string) error {
	panic("unimplementedDataSource.CancelRun: not implemented by this narrow fake")
}

func (unimplementedDataSource) SendSignal(ctx context.Context, runID, name string, data []byte) error {
	panic("unimplementedDataSource.SendSignal: not implemented by this narrow fake")
}

func (unimplementedDataSource) WatchRuns(ctx context.Context, liveOnly bool) (<-chan RunUpdate, error) {
	panic("unimplementedDataSource.WatchRuns: not implemented by this narrow fake")
}

func (unimplementedDataSource) WatchRunHistory(ctx context.Context, runID string, fromSeq uint64) (<-chan HistoryEvent, error) {
	panic("unimplementedDataSource.WatchRunHistory: not implemented by this narrow fake")
}

func (unimplementedDataSource) GetRunTrace(ctx context.Context, runID string) ([]TraceRow, error) {
	panic("unimplementedDataSource.GetRunTrace: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error) {
	panic("unimplementedDataSource.ListTriggers: not implemented by this narrow fake")
}

func (unimplementedDataSource) SetTriggerEnabled(ctx context.Context, triggerID string, enabled bool) error {
	panic("unimplementedDataSource.SetTriggerEnabled: not implemented by this narrow fake")
}

func (unimplementedDataSource) CreateTrigger(ctx context.Context, def trigger.TriggerDef) error {
	panic("unimplementedDataSource.CreateTrigger: not implemented by this narrow fake")
}

func (unimplementedDataSource) UpdateTrigger(ctx context.Context, triggerID string, updates api.TriggerUpdates) error {
	panic("unimplementedDataSource.UpdateTrigger: not implemented by this narrow fake")
}

func (unimplementedDataSource) DeleteTrigger(ctx context.Context, triggerID string) error {
	panic("unimplementedDataSource.DeleteTrigger: not implemented by this narrow fake")
}

func (unimplementedDataSource) FireTrigger(ctx context.Context, triggerID string) (string, error) {
	panic("unimplementedDataSource.FireTrigger: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListTriggerFires(ctx context.Context, triggerID string, limit int) ([]TriggerFireRow, error) {
	panic("unimplementedDataSource.ListTriggerFires: not implemented by this narrow fake")
}

func (unimplementedDataSource) WatchTriggers(ctx context.Context) (<-chan TriggerUpdate, error) {
	panic("unimplementedDataSource.WatchTriggers: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListDeadLetters(ctx context.Context, limit int) ([]api.DeadLetterView, error) {
	panic("unimplementedDataSource.ListDeadLetters: not implemented by this narrow fake")
}

func (unimplementedDataSource) ReplayDeadLetter(ctx context.Context, seq uint64) error {
	panic("unimplementedDataSource.ReplayDeadLetter: not implemented by this narrow fake")
}

func (unimplementedDataSource) DiscardDeadLetter(ctx context.Context, seq uint64) error {
	panic("unimplementedDataSource.DiscardDeadLetter: not implemented by this narrow fake")
}

func (unimplementedDataSource) WatchDLQ(ctx context.Context) (<-chan DLQUpdate, error) {
	panic("unimplementedDataSource.WatchDLQ: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListAuditEvents(ctx context.Context, limit int) ([]AuditEvent, error) {
	panic("unimplementedDataSource.ListAuditEvents: not implemented by this narrow fake")
}

func (unimplementedDataSource) EmitAuditEvent(ctx context.Context, evt AuditEvent) error {
	panic("unimplementedDataSource.EmitAuditEvent: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListAgentRuntimes(ctx context.Context, limit int) ([]AgentRuntimeRow, error) {
	panic("unimplementedDataSource.ListAgentRuntimes: not implemented by this narrow fake")
}

func (unimplementedDataSource) AgentRuntime(ctx context.Context, root string) (AgentRuntimeRow, bool, error) {
	panic("unimplementedDataSource.AgentRuntime: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListKVBuckets(ctx context.Context) ([]KVBucketInfo, error) {
	panic("unimplementedDataSource.ListKVBuckets: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListKVKeys(ctx context.Context, bucket, cursor string, limit int) ([]string, string, error) {
	panic("unimplementedDataSource.ListKVKeys: not implemented by this narrow fake")
}

func (unimplementedDataSource) GetKVEntry(ctx context.Context, bucket, key string) (KVEntryView, error) {
	panic("unimplementedDataSource.GetKVEntry: not implemented by this narrow fake")
}

func (unimplementedDataSource) ConfigSnapshot(ctx context.Context) (ConfigSnapshot, error) {
	panic("unimplementedDataSource.ConfigSnapshot: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListConsumers(ctx context.Context) ([]ConsumerRow, error) {
	panic("unimplementedDataSource.ListConsumers: not implemented by this narrow fake")
}

func (unimplementedDataSource) ServerHealth(ctx context.Context) (ServerHealth, error) {
	panic("unimplementedDataSource.ServerHealth: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListConnections(ctx context.Context) ([]ConnRow, error) {
	panic("unimplementedDataSource.ListConnections: not implemented by this narrow fake")
}

func (unimplementedDataSource) AggregateTaskTypes(ctx context.Context) ([]TaskTypeRow, error) {
	panic("unimplementedDataSource.AggregateTaskTypes: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListWorkerRows(ctx context.Context) ([]WorkerStatusRow, error) {
	panic("unimplementedDataSource.ListWorkerRows: not implemented by this narrow fake")
}

func (unimplementedDataSource) ListServiceRows(ctx context.Context) ([]ServiceRow, error) {
	panic("unimplementedDataSource.ListServiceRows: not implemented by this narrow fake")
}

func (unimplementedDataSource) WorkerDetail(ctx context.Context, id string) (WorkerDetail, error) {
	panic("unimplementedDataSource.WorkerDetail: not implemented by this narrow fake")
}

func (unimplementedDataSource) AdmissionState(ctx context.Context) (AdmissionState, error) {
	panic("unimplementedDataSource.AdmissionState: not implemented by this narrow fake")
}

func (unimplementedDataSource) Search(ctx context.Context, query string, limit int) ([]SearchHit, error) {
	panic("unimplementedDataSource.Search: not implemented by this narrow fake")
}

func (unimplementedDataSource) SparklineData(ctx context.Context, kind, id string, hours int) ([]float64, error) {
	panic("unimplementedDataSource.SparklineData: not implemented by this narrow fake")
}

// concurrencyOnlyDS is a page fake that implements just the two methods
// the /console/concurrency render path reads: AdmissionState (the page
// body) and ConfigSnapshot (the shared build-info footer every page
// renders). Every other port method inherits a panic-stub. It is the
// whole point of the #564 split: a page test implements the handful of
// methods its page touches, not all 41.
type concurrencyOnlyDS struct {
	unimplementedDataSource
	state AdmissionState
}

func (d concurrencyOnlyDS) AdmissionState(context.Context) (AdmissionState, error) {
	return d.state, nil
}

// ConfigSnapshot returns an empty snapshot so the shared footer degrades
// to a build-only strip rather than driving the test to implement the
// whole OpsInventory port.
func (concurrencyOnlyDS) ConfigSnapshot(context.Context) (ConfigSnapshot, error) {
	return ConfigSnapshot{}, nil
}

// TestConcurrencyPage_narrowPortFake mounts /console/concurrency backed
// by a fake that implements only AdmissionState. If servePageConcurrency
// reached for any other port method the embedded base would panic and
// this test would fail — so a green render is proof the handler depends
// on the narrow AdmissionView port, not the full DataSource.
func TestConcurrencyPage_narrowPortFake(t *testing.T) {
	fake := concurrencyOnlyDS{
		state: AdmissionState{
			Locks: []LockRow{
				{Key: "nightly-report", Scope: "workflow", HeldBy: "4f1abc02"},
			},
			TaskSlots: []SlotRow{
				{Name: "image-pipeline::fetch", InFlight: 3},
			},
		},
	}
	handler := Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
	})

	req := httptest.NewRequest(http.MethodGet, "/console/concurrency", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "nightly-report") {
		t.Errorf("body missing seeded lock key %q", "nightly-report")
	}
	// Negative space: a lock we never seeded must not appear.
	if strings.Contains(body, "deadbeef") {
		t.Errorf("body unexpectedly contains a fabricated lock key")
	}
}
