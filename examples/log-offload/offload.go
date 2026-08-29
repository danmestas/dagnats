// examples/log-offload/offload.go
// offloadRunLogs drains every BUILD_LOGS chunk for one run and writes
// it to a local file, one file per (step, attempt), in stream order.
// Split from main.go so the unit test (offload_test.go) can exercise
// it directly against an embedded NATS server without a real Worker.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// offloadRunLogs creates an ephemeral pull consumer filtered to
// logs.{runID}.>, drains it (bounded — see fetchIdleAttemptsMax /
// fetchTotalChunksMax), buffers each (step, attempt)'s chunks in
// memory in stream order, then flushes each buffer to
// outDir/{stepID}.{attempt}.ndjson via a temp-file-then-rename (see
// flushBuffers). Returns the file list and total chunk count for the
// step's output.
//
// Buffering the whole run's logs in memory before writing (rather
// than streaming straight to an appended file) is what makes the
// step's OWN 3 retries (workflow.json) safe: writeChunk appending
// directly to an already-partially-written file would duplicate every
// line a failed first attempt had already written. A reference
// implementation trades memory for that correctness; a production
// writer targeting an actual store should use whatever idempotent-
// write primitive that store offers instead (e.g. an object key that
// this run+step+attempt always maps to, overwritten wholesale).
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

	buffers := map[string]*bytes.Buffer{}

	chunkCount, idleAttempts := 0, 0
	for chunkCount < fetchTotalChunksMax && idleAttempts < fetchIdleAttemptsMax {
		msgs, fetchErr := cons.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchMaxWait))
		if fetchErr != nil {
			return offloadOutput{}, fmt.Errorf("fetch: %w", fetchErr)
		}
		batchCount, err := drainBatch(msgs, runID, buffers)
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

	names, err := flushBuffers(outDir, buffers)
	if err != nil {
		return offloadOutput{}, err
	}
	return offloadOutput{RunID: runID, Files: names, ChunkCount: chunkCount}, nil
}

// drainBatch consumes msgs to completion, appending each chunk to its
// (stepID, attempt) in-memory buffer and acking it. Returns the count
// consumed — jetstream.MessagesContext already bounds this to one
// Fetch batch (fetchBatchSize), so the loop here is bounded by that
// call's contract, not by anything drainBatch itself enforces.
func drainBatch(
	msgs jetstream.MessageBatch, runID string, buffers map[string]*bytes.Buffer,
) (int, error) {
	if msgs == nil {
		panic("drainBatch: msgs must not be nil")
	}
	if buffers == nil {
		panic("drainBatch: buffers must not be nil")
	}
	count := 0
	for msg := range msgs.Messages() {
		if err := bufferChunk(msg, runID, buffers); err != nil {
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

// bufferChunk validates one chunk and appends it (as one NDJSON line)
// to its (stepID, attempt) in-memory buffer, creating the buffer the
// first time that key is seen.
func bufferChunk(
	msg jetstream.Msg, runID string, buffers map[string]*bytes.Buffer,
) error {
	if msg == nil {
		panic("bufferChunk: msg must not be nil")
	}
	if runID == "" {
		panic("bufferChunk: runID must not be empty")
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
	buf, ok := buffers[key]
	if !ok {
		buf = &bytes.Buffer{}
		buffers[key] = buf
	}
	buf.Write(msg.Data())
	buf.WriteByte('\n')
	return nil
}

// flushBuffers writes each buffered (step, attempt) log to
// outDir/{key}.ndjson via a temp-file-then-rename: THIS is the
// function to replace with your own store's writer — the rest of
// this file (consumer setup, drain loop) is storage-agnostic. Rename
// is what makes a retried offload step idempotent instead of
// duplicating lines: each attempt writes a fresh temp file and
// atomically replaces the destination, rather than appending onto
// whatever a previous, failed attempt already wrote (#634 review,
// nit 9).
func flushBuffers(
	outDir string, buffers map[string]*bytes.Buffer,
) ([]string, error) {
	if outDir == "" {
		panic("flushBuffers: outDir must not be empty")
	}
	if buffers == nil {
		panic("flushBuffers: buffers must not be nil")
	}
	names := make([]string, 0, len(buffers))
	for key, buf := range buffers {
		if err := flushOne(outDir, key, buf); err != nil {
			return nil, err
		}
		names = append(names, key)
	}
	return names, nil
}

// flushOne writes one buffer to outDir/{key}.ndjson via a sibling
// temp file plus os.Rename (same-filesystem rename is atomic on every
// platform this reference targets). The temp file is removed on any
// failure so a partial write never leaves stray *.tmp-* files behind.
func flushOne(outDir, key string, buf *bytes.Buffer) error {
	if outDir == "" {
		panic("flushOne: outDir must not be empty")
	}
	if key == "" {
		panic("flushOne: key must not be empty")
	}

	tmp, err := os.CreateTemp(outDir, key+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", key, err)
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		closeLogErr(tmp)
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp for %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp for %s: %w", key, err)
	}

	finalPath := filepath.Join(outDir, key+".ndjson")
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, finalPath, err)
	}
	return nil
}

// closeLogErr closes f and logs any error — used on failure paths
// where the write already failed and the caller is about to return
// that error, so a Close failure here is a SECOND problem worth
// surfacing (#634 review, nit 10) rather than silently swallowing.
func closeLogErr(f *os.File) {
	if f == nil {
		panic("closeLogErr: f must not be nil")
	}
	if err := f.Close(); err != nil {
		log.Printf("close %s: %v", f.Name(), err)
	}
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
