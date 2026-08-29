// examples/log-offload/offload_test.go
// Methodology: unit test over a real embedded NATS server (repo
// convention). Seeds a BUILD_LOGS stream and publishes chunks
// directly (standing in for #624's not-yet-merged publisher), then
// asserts offloadRunLogs drains them into the expected NDJSON file(s)
// in stream order.
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// setupBuildLogsStream creates a minimal BUILD_LOGS stream for the
// test — #624/#652 have not merged, so natsutil has no provisioning
// helper for it yet; this mirrors the shape the eventual helper will
// create (subjects "logs.>").
func setupBuildLogsStream(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     buildLogsStream,
		Subjects: []string{buildLogsSubjectPrefix + ">"},
	})
	if err != nil {
		t.Fatalf("create BUILD_LOGS: %v", err)
	}
}

// publishChunk publishes one logChunk to logs.{runID}.{stepID}.{attempt}.
func publishChunk(
	t *testing.T, js jetstream.JetStream,
	runID, stepID string, attempt int, chunk logChunk,
) {
	t.Helper()
	data, err := json.Marshal(chunk)
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}
	subject := buildLogsSubjectPrefix + runID + "." + stepID + "." +
		strconv.Itoa(attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, data); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

func TestOffloadRunLogs_WritesOneFileInOrder(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	setupBuildLogsStream(t, js)

	runID, stepID, attempt := "run-offload-1", "build", 1
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	chunks := []logChunk{
		{Seq: 1, Ts: base, Attempt: attempt, Stream: "out", Data: "line one"},
		{Seq: 2, Ts: base.Add(time.Second), Attempt: attempt, Stream: "out", Data: "line two"},
		{Seq: 3, Ts: base.Add(2 * time.Second), Attempt: attempt, Stream: "marker", Data: "completed"},
	}
	for _, c := range chunks {
		publishChunk(t, js, runID, stepID, attempt, c)
	}

	outDir := t.TempDir()
	out, err := offloadRunLogs(context.Background(), js, runID, outDir)
	if err != nil {
		t.Fatalf("offloadRunLogs: %v", err)
	}

	// Positive: exactly one file, one chunk count of 3.
	if len(out.Files) != 1 {
		t.Fatalf("Files = %v, want 1 entry", out.Files)
	}
	if out.ChunkCount != 3 {
		t.Fatalf("ChunkCount = %d, want 3", out.ChunkCount)
	}

	path := filepath.Join(outDir, "build.1.ndjson")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3: %q", len(lines), string(data))
	}

	// Positive: order preserved — decode each line and check Seq
	// ascends 1,2,3 and the terminal marker is last.
	for i, line := range lines {
		var got logChunk
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("unmarshal line %d: %v", i, err)
		}
		if got.Seq != uint64(i+1) {
			t.Fatalf("line %d Seq = %d, want %d", i, got.Seq, i+1)
		}
	}
	var last logChunk
	if err := json.Unmarshal([]byte(lines[2]), &last); err != nil {
		t.Fatalf("unmarshal last line: %v", err)
	}
	// Negative: the marker line is the terminal one — not "out".
	if last.Stream != "marker" || last.Data != "completed" {
		t.Fatalf("last chunk = %+v, want marker/completed", last)
	}
}

func TestOffloadRunLogs_SeparatesStepsAndAttempts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	setupBuildLogsStream(t, js)

	runID := "run-offload-2"
	base := time.Now().UTC()
	publishChunk(t, js, runID, "build", 1, logChunk{
		Seq: 1, Ts: base, Attempt: 1, Stream: "out", Data: "attempt one",
	})
	publishChunk(t, js, runID, "build", 2, logChunk{
		Seq: 1, Ts: base, Attempt: 2, Stream: "out", Data: "attempt two (retry)",
	})
	publishChunk(t, js, runID, "test", 1, logChunk{
		Seq: 1, Ts: base, Attempt: 1, Stream: "out", Data: "other step",
	})

	outDir := t.TempDir()
	out, err := offloadRunLogs(context.Background(), js, runID, outDir)
	if err != nil {
		t.Fatalf("offloadRunLogs: %v", err)
	}

	// Positive: three distinct (step, attempt) files.
	if len(out.Files) != 3 {
		t.Fatalf("Files = %v, want 3 entries", out.Files)
	}
	for _, name := range []string{"build.1", "build.2", "test.1"} {
		if _, err := os.Stat(
			filepath.Join(outDir, name+".ndjson"),
		); err != nil {
			t.Fatalf("expected file %s: %v", name, err)
		}
	}
}

func TestOffloadRunLogs_RetryDoesNotDuplicateLines(t *testing.T) {
	// Methodology: offloadRunLogs creates a fresh ephemeral consumer
	// per call, so calling it twice against the SAME seeded chunks
	// (its own ack state does not carry over between calls) is
	// exactly what a retried offload step does — reads the whole
	// backlog again. #634 review nit 9: a naive append-mode writer
	// would duplicate every line on the second call; the
	// temp-file-then-rename writer must not.
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	setupBuildLogsStream(t, js)

	runID := "run-offload-retry"
	base := time.Now().UTC()
	chunks := []logChunk{
		{Seq: 1, Ts: base, Attempt: 1, Stream: "out", Data: "line one"},
		{Seq: 2, Ts: base, Attempt: 1, Stream: "marker", Data: "completed"},
	}
	for _, c := range chunks {
		publishChunk(t, js, runID, "build", 1, c)
	}

	outDir := t.TempDir()
	if _, err := offloadRunLogs(context.Background(), js, runID, outDir); err != nil {
		t.Fatalf("first offloadRunLogs: %v", err)
	}
	if _, err := offloadRunLogs(context.Background(), js, runID, outDir); err != nil {
		t.Fatalf("second (retry) offloadRunLogs: %v", err)
	}

	path := filepath.Join(outDir, "build.1.ndjson")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	// Negative: still 2 lines, not 4 -- the retry replaced the file
	// instead of appending onto it.
	if len(lines) != 2 {
		t.Fatalf("lines after retry = %d, want 2 (no duplication): %q",
			len(lines), string(data))
	}
}
