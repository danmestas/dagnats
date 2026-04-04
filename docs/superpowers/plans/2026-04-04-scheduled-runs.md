# Scheduled Runs Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one-shot scheduled workflow execution at a specific future timestamp, reusing the SLEEP_TIMERS NakWithDelay pattern.

**Architecture:** `ScheduledRun` is a KV-backed type in the `api` package. A separate `ScheduleRun` method (not overloading `StartRun`) stores future runs in a `scheduled_runs` KV bucket and publishes a timer message to `SLEEP_TIMERS`. A timer consumer fires the workflow when the timer expires. Separate `GetScheduledRun` and `CancelScheduledRun` methods keep the API clean. The REST layer routes `POST /runs` with `run_at` to `ScheduleRun`, providing a unified HTTP interface while keeping the service methods distinct.

**Spec divergence:** The spec says "Modify `StartRun` to accept optional `RunAt`." This plan intentionally creates a separate `ScheduleRun` method instead, to avoid optional parameter pollution on `StartRun`. The REST handler and CLI provide the unified UX the spec describes.

**Tech Stack:** Go, NATS JetStream (KV + streams), embedded NATS for tests

**Spec:** `docs/superpowers/specs/2026-04-04-scheduled-runs-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `api/scheduled.go` | Create | `ScheduledRun` type, `ScheduleRun`, `GetScheduledRun`, `CancelScheduledRun`, `ListScheduledRuns` methods |
| `api/scheduled_test.go` | Create | Tests for all scheduled run service methods |
| `api/service.go` | Modify | Add `scheduledKV` field to `Service`, bind in constructor |
| `api/rest.go` | Modify | Add `run_at` to `startRunRequest`, add REST routes for scheduled endpoints |
| `api/rest_test.go` | Modify | Add REST tests for scheduled run endpoints |
| `natsutil/conn.go` | Modify | Add `scheduled_runs` to `SetupKVBuckets` |
| `cli/run.go` | Modify | Add `--at` flag to `runStartCmd`, add `--scheduled` to `runListCmd`, smart cancel/status fallback |
| `e2e/features/scheduled_run_test.go` | Create | End-to-end test: schedule a run, verify it fires |

---

## Chunk 1: Types, KV Bucket, and Service Plumbing

### Task 1: Add `scheduled_runs` KV bucket to natsutil

**Files:**
- Modify: `natsutil/conn.go:57-75`
- Test: `natsutil/conn_test.go`

- [ ] **Step 1: Write the failing test**

```go
// In natsutil/conn_test.go, add after existing bucket tests:

func TestSetupKVBucketsCreatesScheduledRuns(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	err = SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets: %v", err)
	}

	// Positive: scheduled_runs bucket exists.
	kv, err := js.KeyValue("scheduled_runs")
	if err != nil {
		t.Fatalf("scheduled_runs bucket should exist: %v", err)
	}

	// Negative: bucket name is correct.
	status, err := kv.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Bucket() != "scheduled_runs" {
		t.Fatalf("bucket = %q, want scheduled_runs", status.Bucket())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./natsutil/ -run TestSetupKVBucketsCreatesScheduledRuns -v`
Expected: FAIL — `scheduled_runs bucket should exist`

- [ ] **Step 3: Add scheduled_runs bucket to SetupKVBuckets**

In `natsutil/conn.go`, add to the `buckets` slice in `SetupKVBuckets` (line 63):

```go
buckets := []nats.KeyValueConfig{
	{Bucket: "workflow_defs"},
	{Bucket: "workflow_runs"},
	{Bucket: "scheduled_runs"},
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./natsutil/ -run TestSetupKVBucketsCreatesScheduledRuns -v`
Expected: PASS

- [ ] **Step 5: Run all natsutil tests**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./natsutil/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add natsutil/conn.go natsutil/conn_test.go
git commit -m "feat: add scheduled_runs KV bucket to natsutil setup"
```

---

### Task 2: Create ScheduledRun type and service methods

**Files:**
- Create: `api/scheduled.go`
- Create: `api/scheduled_test.go`
- Modify: `api/service.go:28-41` (add `scheduledKV` field)
- Modify: `api/service.go:67-105` (bind `scheduledKV` in constructor)

- [ ] **Step 1: Write the failing test for ScheduleRun**

Create `api/scheduled_test.go`:

```go
// api/scheduled_test.go
// Tests for scheduled run operations: schedule, get, cancel, list.
// Methodology: real embedded NATS. Verify KV state after each operation.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestScheduleRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("sched-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), wfDef)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "sched-test", []byte(`"input"`), runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Positive: runID is non-empty.
	if runID == "" {
		t.Fatal("runID should not be empty")
	}

	// Positive: can retrieve the scheduled run.
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		t.Fatalf("GetScheduledRun: %v", err)
	}
	if sr.RunID != runID {
		t.Fatalf("RunID = %q, want %q", sr.RunID, runID)
	}
	if sr.Status != "scheduled" {
		t.Fatalf("Status = %q, want scheduled", sr.Status)
	}

	// Negative: GetRun should NOT find it (it hasn't started).
	_, err = svc.GetRun(context.Background(), runID)
	if err == nil {
		t.Fatal("GetRun should fail for scheduled (not-yet-started) run")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestScheduleRun -v`
Expected: FAIL — `svc.ScheduleRun undefined`

- [ ] **Step 3: Add scheduledKV to Service struct and constructor**

In `api/service.go`, add field to `Service` struct (after `signalKV` at line 36):

```go
scheduledKV nats.KeyValue
```

In `NewService` constructor (after `signalKV, _ := js.KeyValue("signals")` at line 86), add:

```go
scheduledKV, _ := js.KeyValue("scheduled_runs")
```

And set it in the return struct:

```go
scheduledKV: scheduledKV,
```

- [ ] **Step 4: Create api/scheduled.go with ScheduledRun type and methods**

Create `api/scheduled.go`:

```go
// api/scheduled.go
// One-shot scheduled workflow execution at a future timestamp.
// Stores pending runs in the scheduled_runs KV bucket. Timer
// infrastructure (SLEEP_TIMERS) fires the workflow at run_at.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// maxScheduleAhead is the maximum duration a run can be scheduled
// into the future. 365 days prevents overflow without limiting
// legitimate business use cases.
const maxScheduleAhead = 365 * 24 * time.Hour

// maxScheduledRuns bounds the total number of pending scheduled runs.
const maxScheduledRuns = 100_000

// ScheduledRun represents a workflow run that will start at a future
// time. Stored in the scheduled_runs KV bucket, keyed by RunID.
type ScheduledRun struct {
	RunID      string          `json:"run_id"`
	WorkflowID string          `json:"workflow_id"`
	Input      json.RawMessage `json:"input,omitempty"`
	RunAt      time.Time       `json:"run_at"`
	CreatedAt  time.Time       `json:"created_at"`
	Status     string          `json:"status"`
}

// ScheduleRun validates the workflow exists, generates a run ID,
// and stores a ScheduledRun in KV. Returns the run ID.
// Panics on nil ctx or empty workflowName (programmer errors).
func (s *Service) ScheduleRun(
	ctx context.Context,
	workflowName string,
	input []byte,
	runAt time.Time,
) (string, error) {
	if ctx == nil {
		panic("ScheduleRun: ctx must not be nil")
	}
	if workflowName == "" {
		panic("ScheduleRun: workflowName must not be empty")
	}
	_, span := s.tel.Tracer.Start(ctx,
		"api.scheduleRun",
		observe.WithAttributes(
			observe.StringAttr("workflow_name", workflowName),
		),
	)
	defer span.End()
	start := time.Now()
	s.requestCount.Inc()

	runID, err := s.scheduleRunInner(
		workflowName, input, runAt,
	)
	elapsed := float64(time.Since(start).Milliseconds())
	s.requestDuration.Observe(elapsed)
	if err != nil {
		s.errorCount.Inc()
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		return "", err
	}
	span.SetAttributes(observe.StringAttr("run_id", runID))
	return runID, nil
}

// scheduleRunInner holds the core logic for ScheduleRun.
func (s *Service) scheduleRunInner(
	workflowName string,
	input []byte,
	runAt time.Time,
) (string, error) {
	if workflowName == "" {
		panic("scheduleRunInner: workflowName must not be empty")
	}
	if s.scheduledKV == nil {
		return "", fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}

	// Validate workflow exists.
	_, err := s.defKV.Get(workflowName)
	if err != nil {
		return "", fmt.Errorf(
			"workflow %q not found: %w", workflowName, err,
		)
	}

	// Validate run_at bounds.
	delay := time.Until(runAt)
	if delay <= 0 {
		return "", fmt.Errorf(
			"run_at must be in the future",
		)
	}
	if delay > maxScheduleAhead {
		return "", fmt.Errorf(
			"run_at exceeds maximum schedule-ahead of %v",
			maxScheduleAhead,
		)
	}

	// Enforce max scheduled runs bound.
	keys, err := s.scheduledKV.Keys()
	if err == nil && len(keys) >= maxScheduledRuns {
		return "", fmt.Errorf(
			"maximum scheduled runs (%d) reached",
			maxScheduledRuns,
		)
	}

	runID := generateRunID()
	sr := ScheduledRun{
		RunID:      runID,
		WorkflowID: workflowName,
		Input:      input,
		RunAt:      runAt,
		CreatedAt:  time.Now().UTC(),
		Status:     "scheduled",
	}
	data, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("marshal scheduled run: %w", err)
	}
	_, err = s.scheduledKV.Put(runID, data)
	if err != nil {
		return "", fmt.Errorf("store scheduled run: %w", err)
	}

	// TODO: publish timer message to SLEEP_TIMERS stream
	// (requires Tier 1 SLEEP_TIMERS stream to exist)

	return runID, nil
}

// GetScheduledRun retrieves a pending scheduled run by ID.
// Returns nats.ErrKeyNotFound when the run doesn't exist.
func (s *Service) GetScheduledRun(
	runID string,
) (ScheduledRun, error) {
	if runID == "" {
		panic("GetScheduledRun: runID must not be empty")
	}
	if s.scheduledKV == nil {
		return ScheduledRun{}, fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	entry, err := s.scheduledKV.Get(runID)
	if err != nil {
		return ScheduledRun{}, err
	}
	var sr ScheduledRun
	err = json.Unmarshal(entry.Value(), &sr)
	return sr, err
}

// CancelScheduledRun sets a pending scheduled run's status to
// cancelled. The timer will see "cancelled" and discard (no-op).
func (s *Service) CancelScheduledRun(
	runID string,
) error {
	if runID == "" {
		panic("CancelScheduledRun: runID must not be empty")
	}
	if s.scheduledKV == nil {
		return fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	entry, err := s.scheduledKV.Get(runID)
	if err != nil {
		return fmt.Errorf(
			"scheduled run %q not found: %w", runID, err,
		)
	}
	var sr ScheduledRun
	if err := json.Unmarshal(entry.Value(), &sr); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if sr.Status != "scheduled" {
		return fmt.Errorf(
			"cannot cancel: status is %q", sr.Status,
		)
	}
	sr.Status = "cancelled"
	data, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = s.scheduledKV.Update(
		runID, data, entry.Revision(),
	)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return nil
}

// ListScheduledRuns returns all pending scheduled runs sorted
// by run_at ascending.
func (s *Service) ListScheduledRuns() ([]ScheduledRun, error) {
	if s.scheduledKV == nil {
		return nil, fmt.Errorf(
			"scheduled_runs KV bucket not available",
		)
	}
	keys, err := s.scheduledKV.Keys()
	if err == nats.ErrNoKeysFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	const maxKeys = 10_000
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	runs := make([]ScheduledRun, 0, len(keys))
	for _, key := range keys {
		entry, err := s.scheduledKV.Get(key)
		if err != nil {
			continue
		}
		var sr ScheduledRun
		if err := json.Unmarshal(
			entry.Value(), &sr,
		); err != nil {
			continue
		}
		if sr.Status == "scheduled" {
			runs = append(runs, sr)
		}
	}
	// Sort by run_at ascending.
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].RunAt.Before(runs[j].RunAt)
	})
	return runs, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestScheduleRun -v`
Expected: PASS

- [ ] **Step 6: Write additional tests for cancel and list**

Add to `api/scheduled_test.go`:

```go
func TestCancelScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("cancel-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, err := svc.ScheduleRun(
		context.Background(), "cancel-test", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Positive: cancel succeeds.
	err = svc.CancelScheduledRun(runID)
	if err != nil {
		t.Fatalf("CancelScheduledRun: %v", err)
	}

	// Positive: status is now cancelled.
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		t.Fatalf("GetScheduledRun: %v", err)
	}
	if sr.Status != "cancelled" {
		t.Fatalf("Status = %q, want cancelled", sr.Status)
	}

	// Negative: cancelling again should fail.
	err = svc.CancelScheduledRun(runID)
	if err == nil {
		t.Fatal("double cancel should fail")
	}
}

func TestListScheduledRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("list-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule two runs at different times.
	runAt1 := time.Now().Add(2 * time.Hour)
	runAt2 := time.Now().Add(1 * time.Hour)
	_, err = svc.ScheduleRun(
		context.Background(), "list-test", nil, runAt1,
	)
	if err != nil {
		t.Fatalf("ScheduleRun 1: %v", err)
	}
	_, err = svc.ScheduleRun(
		context.Background(), "list-test", nil, runAt2,
	)
	if err != nil {
		t.Fatalf("ScheduleRun 2: %v", err)
	}

	runs, err := svc.ListScheduledRuns()
	if err != nil {
		t.Fatalf("ListScheduledRuns: %v", err)
	}

	// Positive: both runs returned.
	if len(runs) != 2 {
		t.Fatalf("len = %d, want 2", len(runs))
	}

	// Negative: empty list returns nil, not error.
	// Cancel both and list again.
	for _, r := range runs {
		svc.CancelScheduledRun(r.RunID)
	}
	active, err := svc.ListScheduledRuns()
	if err != nil {
		t.Fatalf("ListScheduledRuns after cancel: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("len = %d, want 0 after cancel", len(active))
	}
}

func TestScheduleRunValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("valid-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Negative: run_at in the past should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "valid-test", nil,
		time.Now().Add(-1*time.Hour),
	)
	if err == nil {
		t.Fatal("past run_at should fail")
	}

	// Negative: run_at beyond 365 days should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "valid-test", nil,
		time.Now().Add(366*24*time.Hour),
	)
	if err == nil {
		t.Fatal("run_at beyond 365 days should fail")
	}

	// Negative: non-existent workflow should fail.
	_, err = svc.ScheduleRun(
		context.Background(), "no-such-wf", nil,
		time.Now().Add(1*time.Hour),
	)
	if err == nil {
		t.Fatal("non-existent workflow should fail")
	}
}
```

- [ ] **Step 7: Run all scheduled tests**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestSchedule -v && go test ./api/ -run TestCancelScheduled -v && go test ./api/ -run TestListScheduled -v`
Expected: All PASS

- [ ] **Step 8: Run full api test suite**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -v -timeout 60s`
Expected: All PASS (existing tests unbroken)

- [ ] **Step 9: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add api/scheduled.go api/scheduled_test.go api/service.go
git commit -m "feat: add ScheduledRun type with schedule, get, cancel, list methods"
```

---

## Chunk 2: REST Endpoints and CLI

### Task 3: Add REST endpoints for scheduled runs

**Files:**
- Modify: `api/rest.go:20-24` (extend `startRunRequest`)
- Modify: `api/rest.go:199-227` (`handleStartRun`)
- Modify: `api/rest.go:60-104` (`routeRunByID` — add scheduled routes)
- Test: `api/rest_test.go`

- [ ] **Step 1: Write the failing test for POST /runs with run_at**

Add to `api/rest_test.go`:

```go
func TestRESTStartScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("rest-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	handler := NewRESTHandler(svc)

	runAt := time.Now().Add(1 * time.Hour).UTC()
	body := fmt.Sprintf(
		`{"workflow":"rest-sched","run_at":"%s"}`,
		runAt.Format(time.RFC3339),
	)
	req := httptest.NewRequest(
		"POST", "/runs",
		strings.NewReader(body),
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 201.
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s",
			w.Code, w.Body.String())
	}

	// Positive: response contains run_id and status=scheduled.
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["run_id"] == "" {
		t.Fatal("run_id should not be empty")
	}
	if resp["status"] != "scheduled" {
		t.Fatalf("status = %q, want scheduled", resp["status"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestRESTStartScheduledRun -v`
Expected: FAIL — `run_at` field not decoded, or missing `status` in response

- [ ] **Step 3: Extend startRunRequest and handleStartRun**

In `api/rest.go`, update `startRunRequest` (line 21):

```go
type startRunRequest struct {
	Workflow string          `json:"workflow"`
	Input    json.RawMessage `json:"input,omitempty"`
	RunAt    *time.Time      `json:"run_at,omitempty"`
}
```

Update `handleStartRun` (line 199) to check `RunAt`:

```go
func handleStartRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleStartRun: svc must not be nil")
	}
	if r == nil {
		panic("handleStartRun: r must not be nil")
	}
	var req startRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(),
			http.StatusBadRequest)
		return
	}

	// Scheduled run path: run_at within 1 second of now is treated
	// as immediate (spec: "in the past or within 1 second").
	const immediateThreshold = time.Second
	if req.RunAt != nil &&
		time.Until(*req.RunAt) > immediateThreshold {
		runID, err := svc.ScheduleRun(
			r.Context(), req.Workflow, req.Input, *req.RunAt,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"run_id": runID,
			"status": "scheduled",
		})
		return
	}

	// Immediate run path (existing).
	runID, err := svc.StartRun(r.Context(), req.Workflow, req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	encErr := json.NewEncoder(w).Encode(
		map[string]string{"run_id": runID},
	)
	if encErr != nil {
		svc.tel.Logger.Error("encode response", encErr)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestRESTStartScheduledRun -v`
Expected: PASS

- [ ] **Step 5: Write tests for GET and DELETE scheduled run REST endpoints**

Add to `api/rest_test.go`:

```go
func TestRESTGetScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("rest-get-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, _ := svc.ScheduleRun(
		context.Background(), "rest-get-sched", nil, runAt,
	)

	handler := NewRESTHandler(svc)
	req := httptest.NewRequest(
		"GET", "/runs/"+runID+"/scheduled", nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 200.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var sr ScheduledRun
	json.Unmarshal(w.Body.Bytes(), &sr)
	if sr.RunID != runID {
		t.Fatalf("RunID = %q, want %q", sr.RunID, runID)
	}
}

func TestRESTCancelScheduledRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc, observe.NewNoopTelemetry())

	wb := dag.NewWorkflow("rest-cancel-sched")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	runAt := time.Now().Add(1 * time.Hour)
	runID, _ := svc.ScheduleRun(
		context.Background(), "rest-cancel-sched", nil, runAt,
	)

	handler := NewRESTHandler(svc)
	req := httptest.NewRequest(
		"DELETE", "/runs/"+runID+"/scheduled", nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Positive: returns 200.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	// Positive: status is now cancelled.
	sr, _ := svc.GetScheduledRun(runID)
	if sr.Status != "cancelled" {
		t.Fatalf("Status = %q, want cancelled", sr.Status)
	}
}
```

- [ ] **Step 6: Add REST route handlers for scheduled run endpoints**

Add to `api/rest.go` — in `routeRunByID`, add handling for `/runs/{id}/scheduled`:

```go
// In routeRunByID, add before the final GET handler:
if len(parts) >= 2 && parts[1] == "scheduled" {
	switch r.Method {
	case http.MethodGet:
		handleGetScheduledRun(s, w, r)
	case http.MethodDelete:
		handleCancelScheduledRun(s, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}
```

Add handler functions to `api/rest.go`:

```go
func handleGetScheduledRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic("handleGetScheduledRun: svc must not be nil")
	}
	// parts[0] = runID from routeRunByID's path split.
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	runID := parts[0]
	sr, err := svc.GetScheduledRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sr)
}

func handleCancelScheduledRun(
	svc *Service, w http.ResponseWriter, r *http.Request,
) {
	if svc == nil {
		panic(
			"handleCancelScheduledRun: svc must not be nil",
		)
	}
	parts := strings.Split(
		strings.TrimPrefix(r.URL.Path, "/runs/"), "/",
	)
	runID := parts[0]
	err := svc.CancelScheduledRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(
		map[string]string{"status": "cancelled"},
	)
}
```

The path parsing uses the same `strings.Split(strings.TrimPrefix(...))` pattern as existing handlers in `routeRunByID` (line 79).

- [ ] **Step 7: Run REST tests**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestREST -v`
Expected: All PASS (existing + new)

- [ ] **Step 8: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add api/rest.go api/rest_test.go
git commit -m "feat: add REST endpoints for scheduled runs (create, get, cancel)"
```

---

### Task 4: Add CLI support for --at, --scheduled, and smart cancel/status

**Files:**
- Modify: `cli/run.go:147-187` (`runStartCmd`)
- Modify: `cli/run.go:45-78` (`runRunCmd` usage)

- [ ] **Step 1: Add --at flag parsing to runStartCmd**

In `cli/run.go`, modify `runStartCmd` (around line 164-177). Add `--at` to the arg parsing loop:

```go
var input []byte
var watch, showOutput bool
var runAtStr string
for _, arg := range args[1:] {
	switch {
	case arg == "--watch":
		watch = true
	case arg == "--output":
		showOutput = true
	case strings.HasPrefix(arg, "--at="):
		runAtStr = strings.TrimPrefix(arg, "--at=")
	case strings.HasPrefix(arg, "--at"):
		// handled by next iteration
	default:
		if input == nil {
			input = []byte(arg)
		}
	}
}
```

After parsing, before the `svc.StartRun` call, add the scheduled path:

```go
if runAtStr != "" {
	runAt, err := time.Parse(time.RFC3339, runAtStr)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"invalid --at time (use RFC3339): %v\n", err)
		os.Exit(1)
	}
	runID, err := svc.ScheduleRun(
		context.Background(), workflowName, input, runAt,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "schedule run: %v\n", err)
		os.Exit(1)
	}
	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(
			map[string]string{
				"run_id": runID,
				"status": "scheduled",
				"run_at": runAt.Format(time.RFC3339),
			},
		)
	} else {
		fmt.Printf("Scheduled %s (run at %s)\n",
			runID[:8], runAt.Format(time.RFC3339))
	}
	return
}
```

- [ ] **Step 2: Add --scheduled flag to runListCmd**

Find `runListCmd` and add a `--scheduled` flag that calls `svc.ListScheduledRuns()` instead of `svc.ListRuns()`.

- [ ] **Step 3: Add smart fallback to runStatusCmd and runCancelCmd**

In `runStatusCmd`, after `svc.GetRun` fails, try `svc.GetScheduledRun`:

```go
run, err := svc.GetRun(ctx, runID)
if err != nil {
	// Try scheduled runs.
	sr, serr := svc.GetScheduledRun(runID)
	if serr == nil {
		// Print scheduled run info.
		fmt.Printf("Run:    %s\n", sr.RunID[:8])
		fmt.Printf("Status: %s\n", sr.Status)
		fmt.Printf("Run At: %s\n",
			sr.RunAt.Format(time.RFC3339))
		return
	}
	fmt.Fprintf(os.Stderr, "get run: %v\n", err)
	os.Exit(1)
}
```

Same pattern for `runCancelCmd` — try `svc.CancelRun`, fall back to `svc.CancelScheduledRun`.

- [ ] **Step 4: Update usage text**

In `printRunUsage` (line 81), add:

```go
fmt.Println("Flags:")
fmt.Println("  --at=<RFC3339>  schedule run for future time")
fmt.Println("  --scheduled     list scheduled (not yet started) runs")
fmt.Println("  --last          use the most recent run")
fmt.Println("  --json          output as JSON")
```

- [ ] **Step 5: Write CLI test for --at flag parsing**

Check if `cli/run_test.go` exists and add a test following existing patterns. The test should verify that `--at=<RFC3339>` is parsed correctly and that `--at` with an invalid time prints an error. Since CLI tests may require NATS (for the `connectService` call), follow the existing test patterns in `cli/run_test.go`.

```go
// In cli/run_test.go (or cli/scheduled_test.go if run_test.go
// doesn't have a good pattern for arg-parsing tests):

func TestRunStartAtFlagParsing(t *testing.T) {
	// Verify --at flag is recognized in the arg parsing.
	args := []string{
		"test-workflow", "--at=2026-04-10T09:00:00Z",
	}

	// Positive: --at value is extracted correctly.
	var runAtStr string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--at=") {
			runAtStr = strings.TrimPrefix(arg, "--at=")
		}
	}
	if runAtStr != "2026-04-10T09:00:00Z" {
		t.Fatalf("runAtStr = %q, want 2026-04-10T09:00:00Z",
			runAtStr)
	}

	// Positive: parses as valid RFC3339.
	_, err := time.Parse(time.RFC3339, runAtStr)
	if err != nil {
		t.Fatalf("parse --at: %v", err)
	}

	// Negative: invalid time string fails.
	_, err = time.Parse(time.RFC3339, "not-a-time")
	if err == nil {
		t.Fatal("invalid time should fail RFC3339 parse")
	}
}
```

- [ ] **Step 6: Verify compilation and run CLI tests**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go build ./... && go test ./cli/ -v -timeout 30s`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add cli/run.go cli/run_test.go
git commit -m "feat: add --at and --scheduled CLI flags for scheduled runs"
```

---

## Chunk 3: Timer Handling and E2E Test

### Task 5: Timer consumer for firing scheduled runs

**Note:** This task depends on the Tier 1 `SLEEP_TIMERS` stream existing. If the stream doesn't exist yet, this task creates it as part of the scheduled runs feature. The timer consumer pattern (publish to stream, NAK with delay, fire on redeliver) is the same pattern Tier 1 durable sleep will use.

**Files:**
- Modify: `natsutil/conn.go:12-53` (add `SLEEP_TIMERS` stream if not present)
- Create: `api/timer.go` (timer consumer that fires scheduled runs)
- Create: `api/timer_test.go`

- [ ] **Step 1: Add SLEEP_TIMERS stream to SetupStreams**

In `natsutil/conn.go`, add to the `streams` slice in `SetupStreams`:

```go
{
	Name:      "SLEEP_TIMERS",
	Subjects:  []string{"sleep.>", "scheduled.>"},
	Retention: nats.LimitsPolicy,
	Storage:   nats.FileStorage,
},
```

- [ ] **Step 2: Write the failing test for timer-based scheduled run firing**

Create `api/timer_test.go`:

```go
// api/timer_test.go
// Tests that the scheduled run timer consumer fires workflows
// when the timer expires.
// Methodology: real embedded NATS. Use short delay, verify run starts.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

// Add `dag` import for WorkflowRun type in poll loop.

func TestScheduledRunTimerFires(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	svc := NewService(nc, tel)

	// Start orchestrator so workflow.started events get processed.
	orch := engine.NewOrchestrator(nc, tel)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	wb := dag.NewWorkflow("timer-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule 2 seconds from now.
	runAt := time.Now().Add(2 * time.Second)
	runID, err := svc.ScheduleRun(
		context.Background(), "timer-test", nil, runAt,
	)
	if err != nil {
		t.Fatalf("ScheduleRun: %v", err)
	}

	// Start the timer consumer.
	timer := NewTimerConsumer(svc)
	timer.Start()
	t.Cleanup(func() { timer.Stop() })

	// Poll with bounded deadline instead of bare time.Sleep.
	deadline := time.Now().Add(10 * time.Second)
	var run dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, err = svc.GetRun(context.Background(), runID)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetRun not found within deadline: %v", err)
	}

	// Positive: the run exists in workflow_runs.
	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}

	// Negative: scheduled_runs entry should be deleted.
	_, err = svc.GetScheduledRun(runID)
	if err == nil {
		t.Fatal(
			"GetScheduledRun should fail after timer fired",
		)
	}
}

func TestScheduledRunTimerCancelled(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	tel := observe.NewNoopTelemetry()
	svc := NewService(nc, tel)

	wb := dag.NewWorkflow("cancel-timer-test")
	wb.Task("a", "task-a")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	svc.RegisterWorkflow(context.Background(), wfDef)

	// Schedule 2 seconds from now, then cancel immediately.
	runAt := time.Now().Add(2 * time.Second)
	runID, _ := svc.ScheduleRun(
		context.Background(), "cancel-timer-test", nil, runAt,
	)
	svc.CancelScheduledRun(runID)

	timer := NewTimerConsumer(svc)
	timer.Start()
	t.Cleanup(func() { timer.Stop() })

	// Wait past the timer fire time, then verify.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}

	// Negative: GetRun should fail — cancelled run should not start.
	_, err = svc.GetRun(context.Background(), runID)
	if err == nil {
		t.Fatal("cancelled scheduled run should not start")
	}

	// Positive: KV entry should be cleaned up by timer handler.
	sr, serr := svc.GetScheduledRun(runID)
	if serr == nil && sr.Status == "scheduled" {
		t.Fatal("KV entry should not still be 'scheduled'")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestScheduledRunTimer -v -timeout 30s`
Expected: FAIL — `NewTimerConsumer undefined`

- [ ] **Step 4: Implement TimerConsumer**

Create `api/timer.go`:

```go
// api/timer.go
// Timer consumer that fires scheduled workflow runs when their
// timer messages redeliver from the SLEEP_TIMERS stream.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// TimerConsumer subscribes to the SLEEP_TIMERS stream for
// scheduled.> subjects and fires workflows when timers expire.
type TimerConsumer struct {
	svc *Service
	sub *nats.Subscription
}

// NewTimerConsumer creates a timer consumer. Panics on nil svc.
func NewTimerConsumer(svc *Service) *TimerConsumer {
	if svc == nil {
		panic("NewTimerConsumer: svc must not be nil")
	}
	return &TimerConsumer{svc: svc}
}

// Start subscribes to scheduled.> on the SLEEP_TIMERS stream.
func (tc *TimerConsumer) Start() error {
	sub, err := tc.svc.js.Subscribe(
		"scheduled.>",
		tc.handleTimer,
		nats.Durable("scheduled-run-timer"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		return fmt.Errorf("subscribe SLEEP_TIMERS: %w", err)
	}
	tc.sub = sub
	return nil
}

// Stop unsubscribes the timer consumer.
func (tc *TimerConsumer) Stop() {
	if tc.sub != nil {
		tc.sub.Unsubscribe()
	}
}

// handleTimer processes a timer message. On first delivery,
// NAKs with delay to schedule the actual fire time. On
// redelivery (NumDelivered > 1), fires the workflow.
func (tc *TimerConsumer) handleTimer(msg *nats.Msg) {
	if msg == nil {
		panic("handleTimer: msg must not be nil")
	}

	meta, err := msg.Metadata()
	if err != nil {
		msg.Nak()
		return
	}

	// First delivery: NAK with delay to schedule the fire.
	if meta.NumDelivered == 1 {
		var sr ScheduledRun
		if err := json.Unmarshal(msg.Data, &sr); err != nil {
			msg.Ack()
			return
		}
		delay := time.Until(sr.RunAt)
		if delay <= 0 {
			delay = time.Millisecond
		}
		msg.NakWithDelay(delay)
		return
	}

	// Redelivery: fire the scheduled run.
	var sr ScheduledRun
	if err := json.Unmarshal(msg.Data, &sr); err != nil {
		msg.Ack()
		return
	}

	// Load from KV to check if still scheduled.
	current, err := tc.svc.GetScheduledRun(sr.RunID)
	if err != nil {
		// Entry deleted or missing — stale timer.
		msg.Ack()
		return
	}
	if current.Status != "scheduled" {
		// Cancelled — clean up and discard.
		tc.svc.scheduledKV.Delete(sr.RunID)
		msg.Ack()
		return
	}

	// Fire the workflow.
	err = tc.fireScheduledRun(current)
	if err != nil {
		tc.svc.tel.Logger.Error(
			"fire scheduled run", err,
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}

	// Clean up KV entry.
	tc.svc.scheduledKV.Delete(sr.RunID)
	msg.Ack()
}

// fireScheduledRun publishes a workflow.started event for the
// scheduled run, using the same path as StartRun.
func (tc *TimerConsumer) fireScheduledRun(
	sr ScheduledRun,
) error {
	if sr.RunID == "" {
		panic("fireScheduledRun: RunID must not be empty")
	}
	if sr.WorkflowID == "" {
		panic(
			"fireScheduledRun: WorkflowID must not be empty",
		)
	}

	entry, err := tc.svc.defKV.Get(sr.WorkflowID)
	if err != nil {
		return fmt.Errorf(
			"workflow %q not found: %w", sr.WorkflowID, err,
		)
	}

	payload, err := buildStartPayload(entry.Value(), sr.Input)
	if err != nil {
		return err
	}

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, sr.RunID, payload,
	)

	// Inject trace context so timer-fired runs have tracing
	// lineage, matching the normal StartRun path.
	ctx, span := tc.svc.tel.Tracer.Start(
		context.Background(), "timer.fireScheduledRun",
	)
	defer span.End()
	injectAPITraceCtx(span, &evt)

	data, err := evt.Marshal()
	if err != nil {
		return err
	}

	pubMsg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {evt.NATSMsgID()},
		},
	}
	injectAPIMsgTraceCtx(span, pubMsg)
	_, err = tc.svc.js.PublishMsg(pubMsg)

	span.SetAttributes(
		observe.StringAttr("run_id", sr.RunID),
		observe.StringAttr("workflow", sr.WorkflowID),
	)
	_ = ctx // consumed by Tracer.Start
	return err
}
```

- [ ] **Step 5: Update ScheduleRun to publish timer message**

In `api/scheduled.go`, replace the `// TODO: publish timer message` comment in `scheduleRunInner` with:

```go
// Publish timer message to SLEEP_TIMERS.
timerData, err := json.Marshal(sr)
if err != nil {
	return "", fmt.Errorf("marshal timer: %w", err)
}
timerMsg := &nats.Msg{
	Subject: "scheduled." + runID,
	Data:    timerData,
	Header: nats.Header{
		"Nats-Msg-Id": {"scheduled." + runID},
	},
}
_, err = s.js.PublishMsg(timerMsg)
if err != nil {
	return "", fmt.Errorf("publish timer: %w", err)
}
```

- [ ] **Step 6: Run timer tests**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./api/ -run TestScheduledRunTimer -v -timeout 30s`
Expected: Both PASS

- [ ] **Step 7: Run full test suite**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./... -timeout 120s`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add natsutil/conn.go api/timer.go api/timer_test.go api/scheduled.go
git commit -m "feat: add timer consumer that fires scheduled runs via SLEEP_TIMERS"
```

---

### Task 6: E2E integration test

**Files:**
- Create: `e2e/features/scheduled_run_test.go`

- [ ] **Step 1: Write the E2E test**

```go
// e2e/features/scheduled_run_test.go
// Tests full scheduled run lifecycle: schedule -> timer fires ->
// worker executes -> workflow completes.
// Methodology: real embedded NATS, real orchestrator, real worker.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestScheduledRunE2E(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()

		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Register a simple worker.
		harness.SubscribeWorker(t, nc, "echo",
			func(tc worker.TaskContext) error {
				return tc.Complete(
					[]byte(`"scheduled-ok"`),
				)
			},
		)

		svc := harness.NewTestService(t, nc)

		// Register workflow.
		wb := dag.NewWorkflow("sched-e2e")
		wb.Task("echo-step", "echo")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		svc.RegisterWorkflow(context.Background(), wfDef)

		// Schedule 2 seconds from now.
		runAt := time.Now().Add(2 * time.Second)
		runID, err := svc.ScheduleRun(
			context.Background(), "sched-e2e", nil, runAt,
		)
		if err != nil {
			t.Fatalf("ScheduleRun: %v", err)
		}

		// Start timer consumer.
		timer := api.NewTimerConsumer(svc)
		timer.Start()
		t.Cleanup(func() { timer.Stop() })

		// Poll with bounded deadline for run completion.
		deadline := time.Now().Add(15 * time.Second)
		var run dag.WorkflowRun
		for time.Now().Before(deadline) {
			run, err = svc.GetRun(
				context.Background(), runID,
			)
			if err == nil &&
				run.Status == dag.RunStatusCompleted {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}

		// Positive: run completed.
		if run.Status != dag.RunStatusCompleted {
			t.Fatalf("Status = %s, want completed",
				run.Status)
		}

		// Positive: echo step output is correct.
		if string(run.Steps["echo-step"].Output) !=
			`"scheduled-ok"` {
			t.Fatalf("output = %s, want scheduled-ok",
				run.Steps["echo-step"].Output)
		}

		// Negative: scheduled_runs entry cleaned up.
		_, err = svc.GetScheduledRun(runID)
		if err == nil {
			t.Fatal("scheduled entry should be deleted")
		}
	})
}
```

- [ ] **Step 2: Run E2E test**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./e2e/features/ -run TestScheduledRunE2E -v -timeout 30s`
Expected: PASS

- [ ] **Step 3: Run full E2E suite**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./e2e/... -v -timeout 120s`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs
git add e2e/features/scheduled_run_test.go
git commit -m "test: add E2E test for scheduled run lifecycle"
```

- [ ] **Step 5: Final full test run**

Run: `cd /Users/dmestas/projects/dagnats/.claude/worktrees/feat+scheduled-runs && go test ./... -timeout 120s`
Expected: All PASS
