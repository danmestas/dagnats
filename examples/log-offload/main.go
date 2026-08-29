// examples/log-offload/main.go
// Reference implementation of the "something else owns log
// retention" half of #634. dagnats owns the BUILD_LOGS hot lane
// (#624) for the duration of a run; this worker is what an operator
// points a run_terminal trigger at to copy those chunks somewhere
// durable once the source run finishes. The filesystem writer below
// is the reference target ONLY — replace writeChunk (and nothing
// else) with a call into your own store (S3, GCS, a database, ...).
// dagnats never learns what storage you chose; that is the point.
//
// TTL constraint (state this loudly, it is easy to violate): BUILD_LOGS'
// hot retention must exceed the offload workflow's own retry horizon.
// A chronically failing offload run (retries=3, step timeout 10m —
// see workflow.json) has roughly 30-40 minutes of headroom before it
// gives up; the default 7-day hot TTL a dagnats deployment ships with
// is comfortable, but a deployment that shortens BUILD_LOGS retention
// below that horizon will silently lose logs for any run whose
// offload step keeps failing.
//
// Build note (#634 review): PR #652 (#624 build logs) had not merged
// when this was written, and its shape changed mid-review to subject
// logs.{runID}.{stepID}.{attempt} with protocol.LogChunk{seq, ts,
// attempt, stream, data} — stream one of out/err/marker, with marker
// chunks carrying completed/failed/paused/continued as the LAST chunk
// of an attempt. This file is written against exactly that shape
// (logChunk below mirrors protocol.LogChunk field-for-field) so it
// can switch to importing the real type with a one-line change once
// #652 lands; it does not import protocol.LogChunk today because
// that type does not exist on main yet.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// buildLogsStream and buildLogsSubjectPrefix name the #624 stream and
// subject this worker reads from. Duplicated here (not imported)
// because #624/#652 have not merged — see the package doc.
const (
	buildLogsStream        = "BUILD_LOGS"
	buildLogsSubjectPrefix = "logs."
)

// logOffloadDirEnv names the directory this reference worker writes
// one NDJSON file per (step, attempt) into. Required — see main().
const logOffloadDirEnv = "LOG_OFFLOAD_DIR"

// fetchBatchSize and fetchMaxWait bound each pull-consumer Fetch call.
// fetchIdleAttemptsMax bounds the read loop: the source run is already
// terminal by the time this worker runs (a run_terminal trigger only
// fires after that), so BUILD_LOGS for runID is a closed, finite
// backlog — an empty Fetch means "drained", not "wait longer for a
// live tail." One empty Fetch is enough to stop; the retry allowance
// exists only to absorb an ordinary NATS scheduling hiccup, not to
// wait for a producer that might still be writing.
const (
	fetchBatchSize       = 100
	fetchMaxWait         = 2 * time.Second
	fetchIdleAttemptsMax = 2
	fetchTotalChunksMax  = 1_000_000 // matches #624's documented per-run cap
)

// logChunk mirrors protocol.LogChunk's wire shape (see package doc).
type logChunk struct {
	Seq     uint64    `json:"seq"`
	Ts      time.Time `json:"ts"`
	Attempt int       `json:"attempt"`
	Stream  string    `json:"stream"` // "out" | "err" | "marker"
	Data    string    `json:"data"`
}

// offloadInput is the run_terminal trigger's chain-start Input shape
// (internal/trigger's runTerminalChainInput, #634) — the flat
// {run_id, workflow_id, status, labels} object, not the generic
// TriggerEnvelope every other trigger type produces.
type offloadInput struct {
	RunID      string            `json:"run_id"`
	WorkflowID string            `json:"workflow_id"`
	Status     string            `json:"status"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// offloadOutput reports what got written, for the step's history record.
type offloadOutput struct {
	RunID      string   `json:"run_id"`
	Files      []string `json:"files"`
	ChunkCount int      `json:"chunk_count"`
}

func main() {
	outDir := os.Getenv(logOffloadDirEnv)
	if outDir == "" {
		fmt.Fprintf(os.Stderr, "%s must be set\n", logOffloadDirEnv)
		os.Exit(1)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", outDir, err)
		os.Exit(1)
	}

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jetstream: %v\n", err)
		os.Exit(1)
	}

	w := worker.NewWorker(nc)
	worker.HandleTyped(w, "logs.offload",
		func(ctx worker.TaskContext, in offloadInput) (offloadOutput, error) {
			if in.RunID == "" {
				return offloadOutput{}, worker.NewNonRetryableError(
					fmt.Errorf("input.run_id must not be empty"),
				)
			}
			fmt.Printf("[logs.offload] draining %s for run %s\n",
				buildLogsStream, in.RunID)
			return offloadRunLogs(ctx.Context(), js, in.RunID, outDir)
		},
	)

	fmt.Println("log-offload worker ready. Waiting for tasks...")
	w.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("\nShutting down...")
	w.Stop()
}
