// worker/log_writer_e2e_test.go
// End-to-end tests for the BUILD_LOGS hot lane (#624): a real Worker,
// dispatched against a real embedded NATS server, driving LogOut()/
// LogErr() through to published protocol.LogChunk messages on
// logs.{runID}.{stepID}.{attempt}. Methodology: publish a task message
// directly to the worker's task subject (bypassing the engine, matching
// the existing consumer_subscribe_test.go pattern), drain an ordered
// consumer over BUILD_LOGS, assert chunk shape/ordering/markers.
// Bounded timeouts on every wait.
package worker

import (
	"context"
	"encoding/json"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// logsSubject builds the attempt-scoped BUILD_LOGS subject the same
// way worker/log_writer.go's newLogLane does. iteration is always 0
// here — none of this file's tests exercise an agent-loop step; that
// scenario (iteration as a second subject dimension, #624 review
// round 3) is covered end-to-end in
// internal/api/logs_test.go's TestAgentLoop_ContinueTwiceKeepsEachIterationOnItsOwnSubject,
// which drives a real engine-dispatched Continue loop rather than the
// synthetic direct-publish harness this file uses.
func logsSubject(runID, stepID string, attempt int) string {
	return "logs." + runID + "." + stepID + "." + strconv.Itoa(attempt) + ".0"
}

// collectLogChunks drains up to want chunks from
// logs.{runID}.{stepID}.{attempt} within timeout, in stream order.
func collectLogChunks(
	t *testing.T, js jetstream.JetStream,
	runID, stepID string, attempt, want int, timeout time.Duration,
) []protocol.LogChunk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	subject := logsSubject(runID, stepID, attempt)
	cons, err := js.OrderedConsumer(ctx, "BUILD_LOGS", jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("OrderedConsumer: %v", err)
	}
	var chunks []protocol.LogChunk
	for len(chunks) < want {
		if ctx.Err() != nil {
			t.Fatalf("timed out with %d/%d chunks: %+v", len(chunks), want, chunks)
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			continue
		}
		var chunk protocol.LogChunk
		if err := json.Unmarshal(msg.Data(), &chunk); err != nil {
			t.Fatalf("unmarshal LogChunk: %v", err)
		}
		msg.Ack()
		chunks = append(chunks, chunk)
	}
	return chunks
}

// countLogChunks drains everything currently on
// logs.{runID}.{stepID}.{attempt} within a short idle window and
// returns how many messages arrived — used for exact-count negative
// assertions ("no further chunk after the terminal marker").
func countLogChunks(
	t *testing.T, js jetstream.JetStream, runID, stepID string, attempt int,
) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	subject := logsSubject(runID, stepID, attempt)
	cons, err := js.OrderedConsumer(ctx, "BUILD_LOGS", jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("OrderedConsumer: %v", err)
	}
	count := 0
	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(300 * time.Millisecond))
		if err != nil {
			break
		}
		msg.Ack()
		count++
	}
	return count
}

func publishTask(
	t *testing.T, js jetstream.JetStream,
	taskType, runID, stepID string, attempt int,
) {
	t.Helper()
	payload := protocol.TaskPayload{
		TaskID:  runID + "." + stepID,
		RunID:   runID,
		StepID:  stepID,
		Attempt: attempt,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal TaskPayload: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subject := "task." + taskType + "." + runID
	if _, err := js.Publish(ctx, subject, data); err != nil {
		t.Fatalf("publish task: %v", err)
	}
}

func TestLogOutLogErr_WritesLandInWriteOrder(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-1", "step-1"
	done := make(chan error, 1)
	w := NewWorker(nc)
	w.Handle("logtask", func(tc TaskContext) error {
		// Sleep past the flush deadline between writes so each Write
		// becomes its own chunk, deterministically, rather than racing
		// the ticker against Complete()'s drain.
		tc.LogOut().Write([]byte("line1"))
		time.Sleep(300 * time.Millisecond)
		tc.LogOut().Write([]byte("line2"))
		time.Sleep(300 * time.Millisecond)
		tc.LogOut().Write([]byte("line3"))
		time.Sleep(300 * time.Millisecond)
		tc.LogErr().Write([]byte("err1"))
		err := tc.Complete([]byte(`"ok"`))
		done <- err
		return err
	})
	w.Start()
	defer w.Stop()

	publishTask(t, js, "logtask", runID, stepID, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	// 4 data chunks + the "completed" terminal marker every attempt-
	// ending path now emits (#624 review).
	chunks := collectLogChunks(t, js, runID, stepID, 1, 5, 10*time.Second)
	if len(chunks) != 5 {
		t.Fatalf("got %d chunks, want 5: %+v", len(chunks), chunks)
	}
	wantStream := []string{
		protocol.LogStreamOut, protocol.LogStreamOut,
		protocol.LogStreamOut, protocol.LogStreamErr,
		protocol.LogStreamMarker,
	}
	wantData := []string{"line1", "line2", "line3", "err1", protocol.LogMarkerCompleted}
	for i, c := range chunks {
		if c.Seq != uint64(i) {
			t.Fatalf("chunk %d: Seq = %d, want %d", i, c.Seq, i)
		}
		if c.Attempt != 1 {
			t.Fatalf("chunk %d: Attempt = %d, want 1 (resolved AttemptNumber for a fresh dispatch)",
				i, c.Attempt)
		}
		if c.Stream != wantStream[i] {
			t.Fatalf("chunk %d: Stream = %q, want %q", i, c.Stream, wantStream[i])
		}
		if string(c.Data) != wantData[i] {
			t.Fatalf("chunk %d: Data = %q, want %q", i, c.Data, wantData[i])
		}
	}
}

func TestLogOut_SplitsOversizedWriteIntoMultipleChunks(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-2", "step-1"
	big := make([]byte, protocol.LogChunkBytesMax+100)
	for i := range big {
		big[i] = 'x'
	}
	done := make(chan error, 1)
	w := NewWorker(nc)
	w.Handle("bigtask", func(tc TaskContext) error {
		tc.LogOut().Write(big)
		err := tc.Complete([]byte(`"ok"`))
		done <- err
		return err
	})
	w.Start()
	defer w.Stop()

	publishTask(t, js, "bigtask", runID, stepID, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	// 2 data chunks (split) + the "completed" terminal marker.
	chunks := collectLogChunks(t, js, runID, stepID, 1, 3, 10*time.Second)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3: sizes=%v",
			len(chunks), chunkSizes(chunks))
	}
	total := len(chunks[0].Data) + len(chunks[1].Data)
	if total != len(big) {
		t.Fatalf("total chunk bytes = %d, want %d", total, len(big))
	}
	if len(chunks[0].Data) != protocol.LogChunkBytesMax {
		t.Fatalf("chunk[0] size = %d, want %d",
			len(chunks[0].Data), protocol.LogChunkBytesMax)
	}
	if chunks[2].Stream != protocol.LogStreamMarker ||
		string(chunks[2].Data) != protocol.LogMarkerCompleted {
		t.Fatalf("chunks[2] = %+v, want marker=completed", chunks[2])
	}
}

func chunkSizes(chunks []protocol.LogChunk) []int {
	sizes := make([]int, len(chunks))
	for i, c := range chunks {
		sizes[i] = len(c.Data)
	}
	return sizes
}

func TestFail_EmitsFailedMarkerBeforeStepFailedEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-3", "step-1"
	done := make(chan struct{})
	w := NewWorker(nc)
	w.Handle("failtask", func(tc TaskContext) error {
		tc.LogOut().Write([]byte("about to fail"))
		tc.Fail(assertionError("boom"))
		close(done)
		return nil
	})
	w.Start()
	defer w.Stop()

	// Subscribe to the step's history subject BEFORE publishing so the
	// step.failed event isn't missed by a slow subscriber start.
	histCtx, histCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer histCancel()
	histCons, err := js.OrderedConsumer(histCtx, "WORKFLOW_HISTORY",
		jetstream.OrderedConsumerConfig{FilterSubjects: []string{"history." + runID}})
	if err != nil {
		t.Fatalf("OrderedConsumer(WORKFLOW_HISTORY): %v", err)
	}

	publishTask(t, js, "failtask", runID, stepID, 0)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 1, 2, 10*time.Second)
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2 (log line + marker): %+v", len(chunks), chunks)
	}
	if chunks[0].Stream != protocol.LogStreamOut {
		t.Fatalf("chunks[0].Stream = %q, want out", chunks[0].Stream)
	}
	if chunks[1].Stream != protocol.LogStreamMarker ||
		string(chunks[1].Data) != protocol.LogMarkerFailed {
		t.Fatalf("chunks[1] = %+v, want marker=failed", chunks[1])
	}

	// The marker's timestamp must not be AFTER the step.failed event's
	// — it was published strictly before the resolution publish call,
	// so it can never race ahead in real time.
	var failedTS time.Time
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := histCons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			t.Fatalf("unmarshal Event: %v", err)
		}
		msg.Ack()
		if evt.Type == protocol.EventStepFailed {
			failedTS = evt.Timestamp
			break
		}
	}
	if failedTS.IsZero() {
		t.Fatal("step.failed event never observed")
	}
	if chunks[1].TS.After(failedTS) {
		t.Fatalf("failed marker TS %v is after step.failed TS %v",
			chunks[1].TS, failedTS)
	}
}

// assertionError is a minimal error for Fail() calls in these tests.
type assertionError string

func (e assertionError) Error() string { return string(e) }

func TestLogOut_ExceedingStepBudgetEmitsOneTruncatedMarker(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Shrink the per-step budget for this test so it runs fast — a
	// real 64 MiB write would be slow and wasteful to exercise this
	// path. Restored via defer; tests in this package must not run
	// this one in parallel with others touching the same var.
	origMax := logStepBytesMax
	logStepBytesMax = 200
	defer func() { logStepBytesMax = origMax }()

	const runID, stepID = "run-log-4", "step-1"
	done := make(chan error, 1)
	w := NewWorker(nc)
	w.Handle("trunctask", func(tc TaskContext) error {
		payload := make([]byte, 150)
		for i := range payload {
			payload[i] = 'a'
		}
		tc.LogOut().Write(payload) // 150 bytes, under 200
		tc.LogOut().Write(payload) // pushes total to 300, over 200
		tc.LogOut().Write(payload) // must be a dropped no-op
		err := tc.Complete([]byte(`"ok"`))
		done <- err
		return err
	})
	w.Start()
	defer w.Stop()

	publishTask(t, js, "trunctask", runID, stepID, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	// Positive: exactly 3 chunks — one data chunk (truncated to fit
	// the 200-byte budget), the truncated marker, then the
	// attempt-ending "completed" marker (Complete() still runs after
	// truncation — truncation stops OUTPUT chunks, not the attempt).
	// No chunk from the dropped third write ever appears.
	chunks := collectLogChunks(t, js, runID, stepID, 1, 3, 10*time.Second)
	if chunks[1].Stream != protocol.LogStreamMarker ||
		string(chunks[1].Data) != protocol.LogMarkerTruncated {
		t.Fatalf("chunks[1] = %+v, want marker=truncated", chunks[1])
	}
	if chunks[2].Stream != protocol.LogStreamMarker ||
		string(chunks[2].Data) != protocol.LogMarkerCompleted {
		t.Fatalf("chunks[2] = %+v, want marker=completed", chunks[2])
	}
	// Negative: no further chunk shows up after the terminal marker.
	if count := countLogChunks(t, js, runID, stepID, 1); count != 3 {
		t.Fatalf("stream has %d chunks after drain, want exactly 3", count)
	}
}

func TestComplete_DrainsBufferedBytesBeforeResolution(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-5", "step-1"
	done := make(chan struct{})
	w := NewWorker(nc)
	w.Handle("draintask", func(tc TaskContext) error {
		// Written immediately before Complete(), with no sleep: only
		// drain-before-resolve guarantees this byte reaches the stream
		// before the resolution.
		tc.LogOut().Write([]byte("final line"))
		if err := tc.Complete([]byte(`"ok"`)); err != nil {
			return err
		}
		close(done)
		return nil
	})
	w.Start()
	defer w.Stop()

	publishTask(t, js, "draintask", runID, stepID, 0)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 1, 2, 10*time.Second)
	if string(chunks[0].Data) != "final line" {
		t.Fatalf("chunks[0].Data = %q, want %q", chunks[0].Data, "final line")
	}
	if chunks[1].Stream != protocol.LogStreamMarker ||
		string(chunks[1].Data) != protocol.LogMarkerCompleted {
		t.Fatalf("chunks[1] = %+v, want marker=completed", chunks[1])
	}
}

// TestRetry_ProducesChunksOnDistinctAttemptSubjects is the #624 review's
// headline regression test: the first attempt fails (writing a log
// line first), a retry is dispatched as a genuinely separate task
// message and completes. Before the fix, both attempts published to
// the SAME bare logs.{runID}.{stepID} subject and BOTH used seq
// starting at 0 under the SAME Nats-Msg-Id shape, so the retry's
// chunks silently collided with (and were dropped as duplicates of)
// the first attempt's inside BUILD_LOGS's 2-minute dedup window. This
// test proves chunks from BOTH attempts are independently observable,
// and that each attempt's terminal marker is the last message on ITS
// OWN subject.
//
// Payload.Attempt values mirror the engine's real dispatch shape
// (internal/engine/task_publish.go's collectReadyMessages publishes
// the first attempt with Attempt: 0; internal/engine/sleeptimer.go's
// republishTask publishes a retry with Attempt: tm.Attempt+1, where
// tm.Attempt was already the POST-first-attempt Attempts value — so a
// retry's payload.Attempt is 2, not 1, in practice). The resolved
// AttemptNumber worker/log_writer.go's subject actually uses
// (resolveAttemptNumber) is 1 for the first dispatch (payload.Attempt
// 0 falls back to NATS NumDelivered) and 2 for the retry
// (payload.Attempt 2 is used directly) — the two numbers this test
// asserts against.
func TestRetry_ProducesChunksOnDistinctAttemptSubjects(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-retry", "step-1"
	attemptSeen := make(chan int, 2)
	w := NewWorker(nc)
	w.Handle("retrytask", func(tc TaskContext) error {
		attempt := tc.RetryCount()
		attemptSeen <- attempt
		if attempt == 0 {
			tc.LogOut().Write([]byte("attempt-1-line"))
			return tc.Fail(assertionError("first attempt fails"))
		}
		tc.LogOut().Write([]byte("attempt-2-line"))
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	publishTask(t, js, "retrytask", runID, stepID, 0)
	select {
	case got := <-attemptSeen:
		if got != 0 {
			t.Fatalf("first attempt payload.Attempt = %d, want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first attempt never dispatched")
	}
	// The first attempt resolves to AttemptNumber 1 (NumDelivered
	// fallback for a fresh message) — its terminal marker (failed)
	// must be the last message on ITS subject before the retry is
	// even published.
	firstAttemptChunks := collectLogChunks(t, js, runID, stepID, 1, 2, 10*time.Second)
	if firstAttemptChunks[0].Attempt != 1 || string(firstAttemptChunks[0].Data) != "attempt-1-line" {
		t.Fatalf("firstAttemptChunks[0] = %+v, want attempt=1 data=attempt-1-line",
			firstAttemptChunks[0])
	}
	if firstAttemptChunks[1].Stream != protocol.LogStreamMarker ||
		string(firstAttemptChunks[1].Data) != protocol.LogMarkerFailed {
		t.Fatalf("firstAttemptChunks[1] = %+v, want marker=failed", firstAttemptChunks[1])
	}
	if count := countLogChunks(t, js, runID, stepID, 1); count != 2 {
		t.Fatalf("attempt-1 subject has %d chunks, want exactly 2 (no retry bleed-through)", count)
	}

	// Dispatch the retry exactly as the engine's republishTask would:
	// a fresh task message with Attempt: 2 (see the doc comment above
	// for why it's 2, not 1).
	publishTask(t, js, "retrytask", runID, stepID, 2)
	select {
	case got := <-attemptSeen:
		if got != 2 {
			t.Fatalf("retry payload.Attempt = %d, want 2", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry never dispatched")
	}
	retryChunks := collectLogChunks(t, js, runID, stepID, 2, 2, 10*time.Second)
	if retryChunks[0].Seq != 0 || retryChunks[0].Attempt != 2 ||
		string(retryChunks[0].Data) != "attempt-2-line" {
		t.Fatalf("retryChunks[0] = %+v, want seq=0 attempt=2 data=attempt-2-line",
			retryChunks[0])
	}
	if retryChunks[1].Stream != protocol.LogStreamMarker ||
		string(retryChunks[1].Data) != protocol.LogMarkerCompleted {
		t.Fatalf("retryChunks[1] = %+v, want marker=completed", retryChunks[1])
	}
	// The first attempt's subject is untouched by the retry's chunks —
	// the collision this test guards against would show up here as
	// extra or overwritten messages.
	if count := countLogChunks(t, js, runID, stepID, 1); count != 2 {
		t.Fatalf("attempt-1 subject has %d chunks after the retry ran, want exactly 2 (unchanged)", count)
	}
}

// TestHandlerReturningNilWithoutResolving_DrainsLogLaneNoLeak is the
// #624 review round-2 regression test for the goroutine leak: a
// handler that writes to LogOut() but returns nil WITHOUT ever calling
// Complete/Fail*/Continue/Pause (a bug — TaskContext's contract calls
// for exactly one of them, but nothing enforces it) used to leave
// tc.logLane's ticker goroutine running forever, since only those
// methods stopped it. handleMessage's defensive drainLogs call
// (worker/worker.go) must still emit a "completed" marker and stop the
// ticker even when the handler itself never resolves.
func TestHandlerReturningNilWithoutResolving_DrainsLogLaneNoLeak(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID, stepID = "run-log-noresolve", "step-1"
	const iterations = 5 // bounded loop
	done := make(chan struct{}, iterations)
	w := NewWorker(nc)
	w.Handle("noresolvetask", func(tc TaskContext) error {
		tc.LogOut().Write([]byte("wrote but never resolved"))
		done <- struct{}{}
		return nil // bug under test: handler never resolves.
	})
	w.Start()
	defer w.Stop()

	// Dispatch one attempt-scoped instance per iteration (distinct
	// stepID suffix -> distinct log lane/ticker) so a real leak
	// accumulates iterations worth of stuck goroutines, not just one —
	// a much more reliable signal than a single before/after snapshot,
	// which is noisy against unrelated background goroutines (NATS
	// client internals, OTel batching, HTTP keep-alive) that a fixed
	// small tolerance can't distinguish from a genuine single leak.
	//
	// Goroutine sampling happens in THIS loop, before any
	// collectLogChunks call — collectLogChunks itself opens a fresh
	// OrderedConsumer per call (a real, uncleaned-up subscription in
	// this test helper) that would otherwise confound the sample with
	// growth that has nothing to do with the log lane. Marker
	// correctness is checked in a separate pass below, after sampling
	// is done.
	iterStepIDs := make([]string, iterations)
	var samples []int
	for i := 0; i < iterations; i++ {
		iterStepIDs[i] = stepID + "-" + strconv.Itoa(i)
		publishTask(t, js, "noresolvetask", runID, iterStepIDs[i], 0)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: handler did not run in time", i)
		}
		// Let this iteration's (correctly stopped) ticker goroutine
		// actually finish exiting before sampling.
		time.Sleep(50 * time.Millisecond)
		samples = append(samples, runtime.NumGoroutine())
	}

	// Positive: the defensive drain emitted the marker for every
	// instance, proving it actually ran (not just that no panic
	// occurred).
	for i, iterStepID := range iterStepIDs {
		chunks := collectLogChunks(t, js, runID, iterStepID, 1, 2, 10*time.Second)
		if chunks[1].Stream != protocol.LogStreamMarker ||
			string(chunks[1].Data) != protocol.LogMarkerCompleted {
			t.Fatalf("iteration %d: chunks[1] = %+v, want marker=completed", i, chunks[1])
		}
	}

	// Negative: goroutine count must NOT trend upward across
	// iterations — a real leak of one ticker per iteration would show
	// samples growing roughly by 1 each time; unrelated background
	// noise stays roughly flat. Compare the last sample against the
	// FIRST (post-iteration-0) sample rather than a pre-test baseline,
	// so one-time startup goroutines (the worker's own subscriptions,
	// etc.) don't count against the tolerance.
	first, last := samples[0], samples[len(samples)-1]
	const growthToleranceMax = 2
	if last > first+growthToleranceMax {
		t.Fatalf(
			"goroutine count grew from %d to %d over %d iterations (samples=%v) — log lane ticker leaked",
			first, last, iterations, samples,
		)
	}
}
