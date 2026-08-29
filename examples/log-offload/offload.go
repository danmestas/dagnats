// examples/log-offload/offload.go
// offloadRunLogs drains every BUILD_LOGS chunk for one run and writes
// it to a local file, one file per (step, attempt), in stream order.
// Split from main.go so the unit test (offload_test.go) can exercise
// it directly against an embedded NATS server without a real Worker.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// offloadRunLogs creates an ephemeral pull consumer filtered to
// logs.{runID}.>, drains it (bounded — see fetchIdleAttemptsMax /
// fetchTotalChunksMax), and writes each chunk as one NDJSON line to
// outDir/{stepID}.{attempt}.ndjson via writeChunk. Returns the file
// list and total chunk count for the step's output.
func offloadRunLogs(
	ctx context.Context, js jetstream.JetStream, runID, outDir string,
) (offloadOutput, error) {
	if js == nil {
		panic("offloadRunLogs: js must not be nil")
	}
	if runID == "" {
		panic("offloadRunLogs: runID must not be empty")
	}

	stream, err := js.Stream(ctx, buildLogsStream)
	if err != nil {
		return offloadOutput{}, fmt.Errorf("stream %s: %w", buildLogsStream, err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject: buildLogsSubjectPrefix + runID + ".>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return offloadOutput{}, fmt.Errorf("ephemeral consumer: %w", err)
	}

	files := map[string]*os.File{}
	defer closeAll(files)

	chunkCount, idleAttempts := 0, 0
	for chunkCount < fetchTotalChunksMax && idleAttempts < fetchIdleAttemptsMax {
		msgs, fetchErr := cons.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchMaxWait))
		if fetchErr != nil {
			return offloadOutput{}, fmt.Errorf("fetch: %w", fetchErr)
		}
		batchCount, err := drainBatch(msgs, runID, outDir, files)
		if err != nil {
			return offloadOutput{}, err
		}
		chunkCount += batchCount
		if batchCount == 0 {
			idleAttempts++
		} else {
			idleAttempts = 0
		}
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	return offloadOutput{RunID: runID, Files: names, ChunkCount: chunkCount}, nil
}

// drainBatch consumes msgs to completion, writing each to its
// (stepID, attempt) file and acking it. Returns the count consumed —
// jetstream.MessagesContext already bounds this to one Fetch batch
// (fetchBatchSize), so the loop here is bounded by that call's
// contract, not by anything drainBatch itself enforces.
func drainBatch(
	msgs jetstream.MessageBatch, runID, outDir string, files map[string]*os.File,
) (int, error) {
	if msgs == nil {
		panic("drainBatch: msgs must not be nil")
	}
	if outDir == "" {
		panic("drainBatch: outDir must not be empty")
	}
	count := 0
	for msg := range msgs.Messages() {
		if err := writeChunk(msg, runID, outDir, files); err != nil {
			return count, err
		}
		if err := msg.Ack(); err != nil {
			return count, fmt.Errorf("ack: %w", err)
		}
		count++
	}
	if err := msgs.Error(); err != nil {
		return count, fmt.Errorf("message batch: %w", err)
	}
	return count, nil
}

// writeChunk appends one NDJSON line to outDir/{stepID}.{attempt}.ndjson,
// opening the file (append mode) the first time that key is seen.
// THIS is the function to replace with your own store's writer — the
// rest of this file (consumer setup, drain loop) is storage-agnostic.
func writeChunk(
	msg jetstream.Msg, runID, outDir string, files map[string]*os.File,
) error {
	if msg == nil {
		panic("writeChunk: msg must not be nil")
	}
	if runID == "" {
		panic("writeChunk: runID must not be empty")
	}

	stepID, attempt, err := parseLogSubject(msg.Subject(), runID)
	if err != nil {
		return err
	}
	var chunk logChunk
	if err := json.Unmarshal(msg.Data(), &chunk); err != nil {
		return fmt.Errorf("unmarshal chunk %s: %w", msg.Subject(), err)
	}

	key := stepID + "." + strconv.Itoa(attempt)
	f, ok := files[key]
	if !ok {
		path := filepath.Join(outDir, key+".ndjson")
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		files[key] = f
	}
	if _, err := f.Write(append(msg.Data(), '\n')); err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	return nil
}

// parseLogSubject extracts stepID and attempt from a
// logs.{runID}.{stepID}.{attempt} subject. runID is passed in
// (already known from the trigger's input) rather than re-derived
// from the subject, so a step ID that happens to contain characters
// resembling the runID token cannot confuse the split.
func parseLogSubject(subject, runID string) (string, int, error) {
	if subject == "" {
		panic("parseLogSubject: subject must not be empty")
	}
	if runID == "" {
		panic("parseLogSubject: runID must not be empty")
	}
	prefix := buildLogsSubjectPrefix + runID + "."
	if !strings.HasPrefix(subject, prefix) {
		return "", 0, fmt.Errorf("subject %q missing prefix %q", subject, prefix)
	}
	rest := strings.TrimPrefix(subject, prefix)
	idx := strings.LastIndex(rest, ".")
	if idx < 0 {
		return "", 0, fmt.Errorf("subject %q missing attempt segment", subject)
	}
	stepID, attemptStr := rest[:idx], rest[idx+1:]
	attempt, err := strconv.Atoi(attemptStr)
	if err != nil {
		return "", 0, fmt.Errorf("subject %q: bad attempt %q: %w",
			subject, attemptStr, err)
	}
	return stepID, attempt, nil
}

// closeAll closes every open file, best-effort — called from a defer
// after offloadRunLogs has already decided its return value, so a
// close failure here is logged-nowhere-else-to-report but must not
// panic (Fsync-on-close failures are rare and non-fatal for a
// reference NDJSON writer).
func closeAll(files map[string]*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}
