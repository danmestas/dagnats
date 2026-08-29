// worker/log_writer_e2e_test.go
// End-to-end tests for the BUILD_LOGS hot lane (#624): a real Worker,
// dispatched against a real embedded NATS server, driving LogOut()/
// LogErr() through to published protocol.LogChunk messages on
// logs.{runID}.{stepID}. Methodology: publish a task message directly
// to the worker's task subject (bypassing the engine, matching the
// existing consumer_subscribe_test.go pattern), drain an ordered
// consumer over BUILD_LOGS, assert chunk shape/ordering/markers.
// Bounded timeouts on every wait.
package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// collectLogChunks drains up to want chunks from logs.{runID}.{stepID}
// within timeout, in stream order.
func collectLogChunks(
	t *testing.T, js jetstream.JetStream,
	runID, stepID string, want int, timeout time.Duration,
) []protocol.LogChunk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	subject := "logs." + runID + "." + stepID
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

func publishTask(
	t *testing.T, js jetstream.JetStream,
	taskType, runID, stepID string,
) {
	t.Helper()
	payload := protocol.TaskPayload{
		TaskID: runID + "." + stepID,
		RunID:  runID,
		StepID: stepID,
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

	publishTask(t, js, "logtask", runID, stepID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 4, 10*time.Second)
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4: %+v", len(chunks), chunks)
	}
	wantStream := []string{
		protocol.LogStreamOut, protocol.LogStreamOut,
		protocol.LogStreamOut, protocol.LogStreamErr,
	}
	wantData := []string{"line1", "line2", "line3", "err1"}
	for i, c := range chunks {
		if c.Seq != uint64(i) {
			t.Fatalf("chunk %d: Seq = %d, want %d", i, c.Seq, i)
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

	publishTask(t, js, "bigtask", runID, stepID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 2, 10*time.Second)
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2: sizes=%v",
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

	publishTask(t, js, "failtask", runID, stepID)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 2, 10*time.Second)
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

	publishTask(t, js, "trunctask", runID, stepID)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	// Positive: exactly 2 chunks — one data chunk (truncated to fit
	// the 200-byte budget) then the truncated marker. No third chunk
	// from the dropped write ever appears.
	chunks := collectLogChunks(t, js, runID, stepID, 2, 10*time.Second)
	if chunks[1].Stream != protocol.LogStreamMarker ||
		string(chunks[1].Data) != protocol.LogMarkerTruncated {
		t.Fatalf("chunks[1] = %+v, want marker=truncated", chunks[1])
	}
	// Negative: no further chunk shows up after the marker.
	extraCtx, extraCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer extraCancel()
	subject := "logs." + runID + "." + stepID
	extraCons, err := js.OrderedConsumer(extraCtx, "BUILD_LOGS",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject}, DeliverPolicy: jetstream.DeliverAllPolicy,
		})
	if err != nil {
		t.Fatalf("OrderedConsumer: %v", err)
	}
	count := 0
	for {
		msg, err := extraCons.Next(jetstream.FetchMaxWait(300 * time.Millisecond))
		if err != nil {
			break
		}
		msg.Ack()
		count++
	}
	if count != 2 {
		t.Fatalf("stream has %d chunks after drain, want exactly 2", count)
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

	publishTask(t, js, "draintask", runID, stepID)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete in time")
	}

	chunks := collectLogChunks(t, js, runID, stepID, 1, 10*time.Second)
	if string(chunks[0].Data) != "final line" {
		t.Fatalf("chunks[0].Data = %q, want %q", chunks[0].Data, "final line")
	}
}
