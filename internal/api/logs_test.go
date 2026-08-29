// internal/api/logs_test.go
// Tests for GET /runs/{id}/logs (#624): non-follow paging, after_seq,
// from=failure, unknown-run/step 404s, empty-but-known-step 200, SSE
// follow, and the follow-concurrency 503.
// Methodology: real embedded NATS server + orchestrator + a real
// worker writing through worker.TaskContext.LogOut()/Fail(), driven
// over httptest.Server. Bounded timeouts on every wait.
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
)

// newLogsRestFixture spins an embedded server + orchestrator + REST
// handler, registers a single-step workflow ("a" → taskType), and
// (unless deferWorker) starts a worker running handler. When
// deferWorker is true, the returned start func must be called to begin
// running tasks — used by the "no chunks yet" test to observe a
// queued-but-unstarted step.
func newLogsRestFixture(
	t *testing.T, wfName, taskType string,
	handler worker.HandlerFunc, deferWorker bool,
) (svc *Service, server *httptest.Server, startWorker func()) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	w := worker.NewWorker(nc)
	w.Handle(taskType, handler)
	startWorker = func() {
		w.Start()
		t.Cleanup(w.Stop)
	}
	if !deferWorker {
		startWorker()
	}

	svc = NewService(nc)
	server = httptest.NewServer(NewRESTHandler(svc))
	t.Cleanup(server.Close)

	wb := dag.NewWorkflow(wfName)
	wb.Task("a", taskType)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, server, startWorker
}

func startLogsTestRun(t *testing.T, svc *Service, wfName string) string {
	t.Helper()
	runID, err := svc.StartRun(context.Background(), wfName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return runID
}

// waitForStepKnown polls svc.GetRun until stepID appears in the run
// snapshot — StartRun returns before the orchestrator has necessarily
// processed workflow.started and created the run's Steps map, so a
// GET .../logs issued immediately after StartRun can race a genuine
// 404 "step not found" rather than the race this test wants to avoid.
func waitForStepKnown(
	t *testing.T, svc *Service, runID, stepID string, timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			if _, ok := run.Steps[stepID]; ok {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("step %q never appeared on run %q", stepID, runID)
}

func getLogsPage(t *testing.T, base, runID, query string) (logsResponse, *http.Response) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/runs/%s/logs%s", base, runID, query))
	if err != nil {
		t.Fatalf("GET logs: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return logsResponse{}, resp
	}
	var out logsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode logs response: %v", err)
	}
	return out, resp
}

// waitForLogsNonEmpty polls GET .../logs?step=a until it returns at
// least want chunks or the deadline elapses.
func waitForLogsNonEmpty(
	t *testing.T, base, runID string, want int, timeout time.Duration,
) logsResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last logsResponse
	for time.Now().Before(deadline) {
		page, resp := getLogsPage(t, base, runID, "?step=a")
		if resp.StatusCode == http.StatusOK && len(page.Chunks) >= want {
			return page
		}
		last = page
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d chunks; last=%+v", want, last)
	return last
}

func TestGetRunLogs_NonFollowReturnsChunksAndEOF(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-basic", "logs-basic-task",
		func(tc worker.TaskContext) error {
			tc.LogOut().Write([]byte("line1"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-basic")

	page := waitForLogsNonEmpty(t, server.URL, runID, 1, 10*time.Second)
	if len(page.Chunks) != 1 || string(page.Chunks[0].Data) != "line1" {
		t.Fatalf("Chunks = %+v, want 1 chunk %q", page.Chunks, "line1")
	}
	if page.NextSeq != 1 {
		t.Fatalf("NextSeq = %d, want 1", page.NextSeq)
	}
	// eof requires the step to be terminal too — poll until Complete
	// has actually landed (StartRun races the worker).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !page.EOF {
		time.Sleep(50 * time.Millisecond)
		page, _ = getLogsPage(t, server.URL, runID, "?step=a")
	}
	if !page.EOF {
		t.Fatalf("EOF never became true: %+v", page)
	}
}

func TestGetRunLogs_AfterSeqSkips(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-afterseq", "logs-afterseq-task",
		func(tc worker.TaskContext) error {
			// Sleep past the 250ms flush deadline between writes so
			// each becomes its own chunk (see the worker package's
			// TestLogOutLogErr_WritesLandInWriteOrder for the same
			// reasoning) instead of racing the ticker against Complete.
			tc.LogOut().Write([]byte("a"))
			time.Sleep(300 * time.Millisecond)
			tc.LogOut().Write([]byte("b"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-afterseq")

	waitForLogsNonEmpty(t, server.URL, runID, 2, 10*time.Second)
	page, resp := getLogsPage(t, server.URL, runID, "?step=a&after_seq=0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(page.Chunks) != 1 || page.Chunks[0].Seq != 1 {
		t.Fatalf("Chunks = %+v, want exactly seq=1", page.Chunks)
	}
}

func TestGetRunLogs_FromFailureStartsAtMarker(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-failure", "logs-failure-task",
		func(tc worker.TaskContext) error {
			tc.LogOut().Write([]byte("before"))
			tc.Fail(fmt.Errorf("boom"))
			return nil
		}, false)
	runID := startLogsTestRun(t, svc, "logs-failure")

	waitForLogsNonEmpty(t, server.URL, runID, 2, 10*time.Second)
	page, resp := getLogsPage(t, server.URL, runID, "?step=a&from=failure")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(page.Chunks) != 1 ||
		page.Chunks[0].Stream != protocol.LogStreamMarker ||
		string(page.Chunks[0].Data) != protocol.LogMarkerFailed {
		t.Fatalf("Chunks = %+v, want just the failed marker", page.Chunks)
	}
}

func TestGetRunLogs_UnknownRunIs404(t *testing.T) {
	_, server, _ := newLogsRestFixture(t, "logs-404run", "logs-404run-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	_, resp := getLogsPage(t, server.URL, "no-such-run", "?step=a")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestGetRunLogs_UnknownStepIs404(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-404step", "logs-404step-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	runID := startLogsTestRun(t, svc, "logs-404step")
	_, resp := getLogsPage(t, server.URL, runID, "?step=does-not-exist")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestGetRunLogs_MissingStepParamIs400(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-400", "logs-400-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	runID := startLogsTestRun(t, svc, "logs-400")
	_, resp := getLogsPage(t, server.URL, runID, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetRunLogs_KnownStepNoChunksYetIs200Empty(t *testing.T) {
	svc, server, startWorker := newLogsRestFixture(
		t, "logs-empty", "logs-empty-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, true,
	)
	runID := startLogsTestRun(t, svc, "logs-empty")

	// Give the engine a moment to dispatch the step (so it's "queued",
	// a known step) without any worker running to consume it yet.
	time.Sleep(200 * time.Millisecond)
	page, resp := getLogsPage(t, server.URL, runID, "?step=a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(page.Chunks) != 0 {
		t.Fatalf("Chunks = %+v, want empty", page.Chunks)
	}
	if page.EOF {
		t.Fatal("EOF = true before the step ever ran")
	}
	startWorker()
}

func TestGetRunLogs_FollowStreamsChunksThenEOF(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-follow", "logs-follow-task",
		func(tc worker.TaskContext) error {
			time.Sleep(150 * time.Millisecond) // let follow attach first
			tc.LogOut().Write([]byte("streamed"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-follow")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	req, err := http.NewRequest(
		"GET", fmt.Sprintf("%s/runs/%s/logs?step=a&follow=1", server.URL, runID), nil,
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("follow request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var sawChunk, sawEOF bool
	var event string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: chunk"):
			event = "chunk"
		case strings.HasPrefix(line, "event: eof"):
			event = "eof"
		case strings.HasPrefix(line, "data: ") && event == "chunk":
			sawChunk = true
		}
		if event == "eof" && strings.HasPrefix(line, "data:") {
			sawEOF = true
			break
		}
	}
	if !sawChunk {
		t.Fatal("never observed a chunk event")
	}
	if !sawEOF {
		t.Fatal("never observed the eof event")
	}
}

func TestGetRunLogs_FollowConcurrencyCapReturns503(t *testing.T) {
	svc, server, _ := newLogsRestFixture(t, "logs-cap", "logs-cap-task",
		func(tc worker.TaskContext) error {
			time.Sleep(2 * time.Second)
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-cap")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	origCap := logFollowConcurrentMax
	logFollowConcurrentMax = 1
	defer func() { logFollowConcurrentMax = origCap }()

	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/runs/%s/logs?step=a&follow=1", server.URL, runID)

	first, err := client.Get(url)
	if err != nil {
		t.Fatalf("first follow: %v", err)
	}
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first follow status = %d, want 200", first.StatusCode)
	}

	second, err := client.Get(url)
	if err != nil {
		t.Fatalf("second follow: %v", err)
	}
	defer second.Body.Close()
	if second.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second follow status = %d, want 503", second.StatusCode)
	}
}
