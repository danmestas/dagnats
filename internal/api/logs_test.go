// internal/api/logs_test.go
// Tests for GET /runs/{id}/logs (#624 + review): non-follow paging via
// stream-sequence cursor, from=failure resolved via GetLastMsgForSubject,
// unknown-run/step 404s, empty-but-known-step 200, SSE follow over a
// SINGLE consumer, the follow-concurrency 503, and the review's
// headline pagination regression test (2100 chunks, three pages, no
// gaps/duplicates, eof only on the last page).
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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// newLogsRestFixture spins an embedded server + orchestrator + REST
// handler, registers a single-step workflow ("a" → taskType), and
// (unless deferWorker) starts a worker running handler. When
// deferWorker is true, the returned start func must be called to begin
// running tasks — used by the "no chunks yet" test to observe a
// queued-but-unstarted step. Also returns the raw *nats.Conn so tests
// that need to seed BUILD_LOGS directly (bypassing the worker SDK) or
// inspect consumers can do so.
func newLogsRestFixture(
	t *testing.T, wfName, taskType string,
	handler worker.HandlerFunc, deferWorker bool,
) (svc *Service, server *httptest.Server, nc *nats.Conn, startWorker func()) {
	t.Helper()
	_, nc = natsutil.StartTestServer(t)
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
	return svc, server, nc, startWorker
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

// waitForStepTerminal polls svc.GetRun until stepID's status is
// terminal (dag.StepStatusCompleted/Failed/etc).
func waitForStepTerminal(
	t *testing.T, svc *Service, runID, stepID string, timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil {
			if state, ok := run.Steps[stepID]; ok && stepTerminal(state.Status) {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("step %q on run %q never reached a terminal status", stepID, runID)
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
	svc, server, _, _ := newLogsRestFixture(t, "logs-basic", "logs-basic-task",
		func(tc worker.TaskContext) error {
			tc.LogOut().Write([]byte("line1"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-basic")

	// 1 data chunk + the "completed" terminal marker every
	// attempt-ending TaskContext method now emits (#624 review).
	page := waitForLogsNonEmpty(t, server.URL, runID, 2, 10*time.Second)
	if len(page.Chunks) != 2 || string(page.Chunks[0].Data) != "line1" {
		t.Fatalf("Chunks = %+v, want 2 chunks starting with %q", page.Chunks, "line1")
	}
	if page.Chunks[1].Stream != protocol.LogStreamMarker ||
		string(page.Chunks[1].Data) != protocol.LogMarkerCompleted {
		t.Fatalf("Chunks[1] = %+v, want marker=completed", page.Chunks[1])
	}
	// next_cursor is an opaque JetStream stream sequence now (not a
	// small per-attempt counter) — assert it's positive and advancing,
	// not a specific value.
	if page.NextCursor == 0 {
		t.Fatalf("NextCursor = 0, want positive")
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

func TestGetRunLogs_CursorSkips(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-cursor", "logs-cursor-task",
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
	runID := startLogsTestRun(t, svc, "logs-cursor")

	// 3 chunks total: "a", "b", the completed marker.
	first := waitForLogsNonEmpty(t, server.URL, runID, 3, 10*time.Second)
	if len(first.Chunks) < 2 {
		t.Fatalf("first page = %+v, want at least 2 chunks", first.Chunks)
	}
	// Cursor to skip past the first chunk ("a").
	firstChunkCursor := first.NextCursor - uint64(len(first.Chunks)) + 1
	page, resp := getLogsPage(t, server.URL, runID,
		fmt.Sprintf("?step=a&cursor=%d", firstChunkCursor))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(page.Chunks) < 1 || string(page.Chunks[0].Data) != "b" {
		t.Fatalf("Chunks = %+v, want first chunk data=b", page.Chunks)
	}
}

func TestGetRunLogs_FromFailureStartsAtMarker(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-failure", "logs-failure-task",
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
	// GetLastMsgForSubject resolves the marker directly (O(1), #624
	// review) — by the drain-before-resolve invariant it is the true
	// last message, so from=failure returns exactly that one chunk.
	if len(page.Chunks) != 1 ||
		page.Chunks[0].Stream != protocol.LogStreamMarker ||
		string(page.Chunks[0].Data) != protocol.LogMarkerFailed {
		t.Fatalf("Chunks = %+v, want just the failed marker", page.Chunks)
	}
}

// TestGetRunLogs_FromFailureOnCompletedAttemptIs404 is the #624 review
// round-2 regression test for point 3: from=failure must be STRICT —
// a completed (non-failed) attempt's last message is a "completed"
// marker, not "failed", so from=failure must 404 rather than silently
// starting at whatever the last message happens to be.
func TestGetRunLogs_FromFailureOnCompletedAttemptIs404(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-failure-404", "logs-failure-404-task",
		func(tc worker.TaskContext) error {
			tc.LogOut().Write([]byte("all good"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-failure-404")

	waitForLogsNonEmpty(t, server.URL, runID, 2, 10*time.Second)
	resp, err := http.Get(
		fmt.Sprintf("%s/runs/%s/logs?step=a&from=failure", server.URL, runID),
	)
	if err != nil {
		t.Fatalf("GET logs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error != "attempt has no failure marker" {
		t.Fatalf("error = %q, want %q", body.Error, "attempt has no failure marker")
	}
}

func TestGetRunLogs_UnknownRunIs404(t *testing.T) {
	_, server, _, _ := newLogsRestFixture(t, "logs-404run", "logs-404run-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	_, resp := getLogsPage(t, server.URL, "no-such-run", "?step=a")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestGetRunLogs_UnknownStepIs404(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-404step", "logs-404step-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	runID := startLogsTestRun(t, svc, "logs-404step")
	_, resp := getLogsPage(t, server.URL, runID, "?step=does-not-exist")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestGetRunLogs_MissingStepParamIs400(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-400", "logs-400-task",
		func(tc worker.TaskContext) error { return tc.Complete(nil) }, false)
	runID := startLogsTestRun(t, svc, "logs-400")
	_, resp := getLogsPage(t, server.URL, runID, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetRunLogs_KnownStepNoChunksYetIs200Empty(t *testing.T) {
	svc, server, _, startWorker := newLogsRestFixture(
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
	svc, server, _, _ := newLogsRestFixture(t, "logs-follow", "logs-follow-task",
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
	svc, server, _, _ := newLogsRestFixture(t, "logs-cap", "logs-cap-task",
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

// countBuildLogsConsumers lists the live consumer names on BUILD_LOGS.
func countBuildLogsConsumers(t *testing.T, nc *nats.Conn) int {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "BUILD_LOGS")
	if err != nil {
		t.Fatalf("Stream(BUILD_LOGS): %v", err)
	}
	lister := stream.ConsumerNames(ctx)
	count := 0
	for range lister.Name() {
		count++
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ConsumerNames: %v", err)
	}
	return count
}

// buildLogsConsumerNames returns the set of live consumer names on
// BUILD_LOGS.
func buildLogsConsumerNames(t *testing.T, nc *nats.Conn) map[string]bool {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "BUILD_LOGS")
	if err != nil {
		t.Fatalf("Stream(BUILD_LOGS): %v", err)
	}
	lister := stream.ConsumerNames(ctx)
	names := make(map[string]bool)
	for name := range lister.Name() {
		names[name] = true
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ConsumerNames: %v", err)
	}
	return names
}

// TestGetRunLogs_FollowEndsWithErrorEventOnConsumerDeleted is the
// #624 review round-2 regression test for point 4: deleting the
// follow's underlying JetStream consumer out from under it must end
// the SSE stream with an event: error within one Next() wait, not
// silently loop as "idle" until the 1h duration cap.
func TestGetRunLogs_FollowEndsWithErrorEventOnConsumerDeleted(t *testing.T) {
	svc, server, nc, _ := newLogsRestFixture(t, "logs-consdel", "logs-consdel-task",
		func(tc worker.TaskContext) error {
			time.Sleep(3 * time.Second) // keep the follow open
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-consdel")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	before := buildLogsConsumerNames(t, nc)

	req, err := http.NewRequest(
		"GET", fmt.Sprintf("%s/runs/%s/logs?step=a&follow=1", server.URL, runID), nil,
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("follow request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Find the new consumer this follow opened, and delete it — the
	// mid-flight failure the review's regression targets.
	var newName string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && newName == "" {
		after := buildLogsConsumerNames(t, nc)
		for name := range after {
			if !before[name] {
				newName = name
				break
			}
		}
		if newName == "" {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if newName == "" {
		t.Fatal("never observed the follow's own consumer appear on BUILD_LOGS")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "BUILD_LOGS")
	if err != nil {
		t.Fatalf("Stream(BUILD_LOGS): %v", err)
	}
	if err := stream.DeleteConsumer(ctx, newName); err != nil {
		t.Fatalf("DeleteConsumer(%s): %v", newName, err)
	}

	// The stream must end with event: error well within the 15s
	// keepalive wait — bounded generously to absorb scheduling jitter,
	// but nowhere near the 1h duration cap the pre-fix busy loop would
	// have run into.
	scanner := bufio.NewScanner(resp.Body)
	var sawError bool
	var event string
	scanDeadline := time.Now().Add(20 * time.Second)
	for scanner.Scan() {
		if time.Now().After(scanDeadline) {
			break
		}
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: error"):
			event = "error"
		case strings.HasPrefix(line, "event: eof"), strings.HasPrefix(line, "event: chunk"):
			event = ""
		}
		if event == "error" && strings.HasPrefix(line, "data:") {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Fatal("never observed an event: error after the consumer was deleted")
	}
}

// TestGetRunLogs_FollowUsesExactlyOneConsumer is the #624 review's
// regression test for point 3: the old implementation opened a fresh
// ordered consumer every 500ms for the life of a follow connection.
// This asserts BUILD_LOGS's live consumer count grows by exactly 1
// while a follow is attached, not by many over a few seconds of idle
// polling.
func TestGetRunLogs_FollowUsesExactlyOneConsumer(t *testing.T) {
	svc, server, nc, _ := newLogsRestFixture(t, "logs-oneconsumer", "logs-oneconsumer-task",
		func(tc worker.TaskContext) error {
			time.Sleep(3 * time.Second) // keep the follow open across idle cycles
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-oneconsumer")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	before := countBuildLogsConsumers(t, nc)

	req, err := http.NewRequest(
		"GET", fmt.Sprintf("%s/runs/%s/logs?step=a&follow=1", server.URL, runID), nil,
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("follow request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Sample consumer count a couple of times while the connection is
	// still open and idle — it must never exceed before+1.
	for i := 0; i < 3; i++ {
		time.Sleep(400 * time.Millisecond)
		during := countBuildLogsConsumers(t, nc)
		if during > before+1 {
			t.Fatalf("BUILD_LOGS consumer count = %d after %v, want <= %d (exactly one per follow)",
				during, time.Duration(i+1)*400*time.Millisecond, before+1)
		}
	}
}

// TestGetRunLogs_PagesWithoutGapsOrDuplicates is the #624 review's
// headline pagination regression test: 2100 chunks on one attempt's
// subject, paged in three requests of up to LogReadChunksMax (1024)
// each via the cursor (1024 + 1024 + 53, the last including the
// terminal marker), must together cover every message exactly once
// (no gaps, no duplicates), with eof true ONLY on the final page.
//
// The 2100 chunks are seeded directly onto the real step's BUILD_LOGS
// subject via a raw publish (bypassing the worker SDK, which would
// take far too long flushing one chunk per ~250ms tick for 2100
// writes) so this stays a fast, deterministic unit-shaped test; the
// step is then actually completed through a real worker so eof's
// "step terminal" half of the condition is genuine, not faked.
func TestGetRunLogs_PagesWithoutGapsOrDuplicates(t *testing.T) {
	const chunkCount = 2100
	svc, server, nc, startWorker := newLogsRestFixture(
		t, "logs-1500", "logs-1500-task",
		func(tc worker.TaskContext) error {
			// A log lane (and its terminal marker) is only created on
			// first LogOut()/LogErr() Write (#624: "a handler that
			// never logs never pays for the ticker") — write one line
			// so Complete()'s drain actually has a marker to emit.
			tc.LogOut().Write([]byte("final-line"))
			return tc.Complete(nil)
		}, true,
	)
	runID := startLogsTestRun(t, svc, "logs-1500")
	waitForStepKnown(t, svc, runID, "a", 5*time.Second)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	// attempt=1: a fresh single dispatch resolves to AttemptNumber 1
	// (worker/context.go's resolveAttemptNumber, #624 review) — must
	// match the subject the real worker's own "completed" marker
	// lands on below, or the two would occupy disjoint subjects.
	subject := attemptSubject(runID, "a", 1)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer seedCancel()
	for i := 0; i < chunkCount; i++ {
		chunk := protocol.LogChunk{
			Seq: uint64(i), Attempt: 1, TS: time.Now(),
			Stream: protocol.LogStreamOut, Data: []byte(fmt.Sprintf("line-%d", i)),
		}
		data, err := json.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal seed chunk %d: %v", i, err)
		}
		msgID := fmt.Sprintf("test-seed-%s-%d", runID, i)
		if _, err := js.Publish(seedCtx, subject, data, jetstream.WithMsgID(msgID)); err != nil {
			t.Fatalf("seed publish %d: %v", i, err)
		}
	}

	// Now let the real worker complete the step — its "completed"
	// marker becomes chunk 2100 (message 2101 overall) on the SAME
	// subject via its own independent seq counter and Msg-Id scheme,
	// so it cannot collide with the seeded messages above.
	startWorker()
	waitForStepTerminal(t, svc, runID, "a", 10*time.Second)

	var allChunks []protocol.LogChunk
	seenCursors := make(map[uint64]bool)
	cursor := uint64(0)
	pages := 0
	const maxPages = 10 // bounded loop guard, well above the expected 3
	for pages < maxPages {
		pages++
		page, resp := getLogsPage(t, server.URL, runID,
			fmt.Sprintf("?step=a&cursor=%d", cursor))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d: status = %d", pages, resp.StatusCode)
		}
		if len(page.Chunks) == 0 && !page.EOF {
			t.Fatalf("page %d: empty page but eof=false", pages)
		}
		if pages < 3 && page.EOF {
			t.Fatalf("page %d: eof=true too early (want it only on the last page)", pages)
		}
		t.Logf("page %d: got=%d eof=%v nextCursor=%d cursorSent=%d",
			pages, len(page.Chunks), page.EOF, page.NextCursor, cursor)
		allChunks = append(allChunks, page.Chunks...)
		if seenCursors[page.NextCursor] && len(page.Chunks) > 0 {
			t.Fatalf("page %d: NextCursor %d repeats a prior page's cursor (stalled paging)",
				pages, page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		if page.EOF {
			break
		}
		cursor = page.NextCursor
	}
	if pages != 3 {
		t.Fatalf("paged in %d requests, want exactly 3 (1024 + 1024 + 54)", pages)
	}
	// No gaps, no duplicates: every seeded line appears exactly once,
	// plus the real worker's own "final-line" write and exactly one
	// terminal marker — total chunkCount+2.
	const wantTotal = chunkCount + 2
	if len(allChunks) != wantTotal {
		t.Fatalf("total chunks across pages = %d, want %d", len(allChunks), wantTotal)
	}
	seenData := make(map[string]int, chunkCount+1)
	markerCount := 0
	for _, c := range allChunks {
		if c.Stream == protocol.LogStreamMarker {
			markerCount++
			continue
		}
		seenData[string(c.Data)]++
	}
	if markerCount != 1 {
		t.Fatalf("marker count = %d, want exactly 1", markerCount)
	}
	const wantDistinctData = chunkCount + 1 // 2100 seeded lines + "final-line"
	if len(seenData) != wantDistinctData {
		t.Fatalf("distinct data lines = %d, want %d (gap or duplicate)", len(seenData), wantDistinctData)
	}
	for line, n := range seenData {
		if n != 1 {
			t.Fatalf("line %q appeared %d times, want exactly 1", line, n)
		}
	}
}

// TestGetRunLogs_CursorPastEndIsEmptyAndEOF asserts a cursor beyond the
// last stored message on a terminal step's attempt returns an empty
// page with eof=true, rather than hanging or erroring.
func TestGetRunLogs_CursorPastEndIsEmptyAndEOF(t *testing.T) {
	svc, server, _, _ := newLogsRestFixture(t, "logs-pastend", "logs-pastend-task",
		func(tc worker.TaskContext) error {
			tc.LogOut().Write([]byte("only line"))
			return tc.Complete(nil)
		}, false)
	runID := startLogsTestRun(t, svc, "logs-pastend")
	waitForStepTerminal(t, svc, runID, "a", 10*time.Second)

	last := waitForLogsNonEmpty(t, server.URL, runID, 2, 10*time.Second)
	pastEnd := last.NextCursor + 1_000_000
	page, resp := getLogsPage(t, server.URL, runID,
		fmt.Sprintf("?step=a&cursor=%d", pastEnd))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(page.Chunks) != 0 {
		t.Fatalf("Chunks = %+v, want empty", page.Chunks)
	}
	if !page.EOF {
		t.Fatal("EOF = false for a cursor past the end of a terminal step's log")
	}
}
