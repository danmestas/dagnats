package protocol

import "time"

// Bounds for the BUILD_LOGS hot lane (#624). dagnats owns the
// JetStream hot lane only — no S3, no offloader, no cache. Retention
// past the hot TTL is a consumer's job, the same way run history and
// telemetry already work (docs/wire-protocol.md, "Consumer contract:
// build logs").
const (
	// LogChunkBytesMax bounds a single LogChunk's Data payload. A
	// worker Write() larger than this is split into multiple chunks
	// (worker/log_writer.go) rather than growing one chunk unbounded.
	LogChunkBytesMax = 64 * 1024

	// LogStepBytesMax bounds the total bytes (both out and err
	// streams combined) captured for one step. Once hit, exactly one
	// LogMarkerTruncated chunk is emitted and further writes for that
	// step are dropped — bounded storage beats an unbounded log flood
	// from a runaway process.
	LogStepBytesMax = 64 * 1024 * 1024

	// LogReadChunksMax bounds a single non-follow GET /runs/{id}/logs
	// response.
	LogReadChunksMax = 1024

	// LogFollowDurationMax bounds a single SSE follow connection.
	LogFollowDurationMax = 1 * time.Hour

	// LogFollowConcurrentMax bounds concurrent SSE follow connections
	// per server process.
	LogFollowConcurrentMax = 256
)

// Stream tags a LogChunk's origin.
const (
	LogStreamOut    = "out"
	LogStreamErr    = "err"
	LogStreamMarker = "marker"
)

// Marker values, carried in LogChunk.Data when Stream == LogStreamMarker.
// Every path that ends a task attempt/iteration (worker/context.go's
// Complete, Fail, FailPermanent, FailRetryAfter, Continue, Pause) emits
// exactly one of these as the LAST chunk on that
// logs.{runID}.{stepID}.{attempt}.{iteration} subject (#624 review) —
// the drain-before-resolve invariant guarantees no write for that
// attempt/iteration lands after it. GET .../logs's follow mode and
// from=failure both depend on this: a marker is a reliable end-of-
// attempt/iteration signal, not one a consumer has to infer from a
// separate history stream.
const (
	// LogMarkerCompleted is emitted by Complete.
	LogMarkerCompleted = "completed"
	// LogMarkerFailed is emitted by Fail, FailPermanent, and
	// FailRetryAfter — all three represent this attempt ending in
	// failure, whether or not the engine will retry. BEFORE the
	// resolution publish, so a consumer following the lane sees the
	// marker before (or with) the terminal step.failed event and
	// GET .../logs?from=failure has a recorded position to resolve via
	// GetLastMsgForSubject rather than a scan.
	LogMarkerFailed = "failed"
	// LogMarkerContinued is emitted by Continue (agent-loop iteration
	// boundary) — this attempt's subject is done; the next iteration
	// gets a new attempt-scoped subject.
	LogMarkerContinued = "continued"
	// LogMarkerPaused is emitted by Pause — this attempt is done; a
	// resumed step (LoadCheckpoint) gets a new attempt-scoped subject.
	LogMarkerPaused = "paused"
	// LogMarkerTruncated is emitted at most once, the moment
	// LogStepBytesMax is reached, BEFORE the attempt-ending marker
	// (which still lands last) — no out/err chunk follows it, but the
	// terminal marker always does.
	LogMarkerTruncated = "truncated"
)

// LogChunk is a single unit of captured step output on the BUILD_LOGS
// stream, subject logs.{runID}.{stepID}.{attempt}.{iteration} (#624
// review round 3: iteration is a SECOND dimension of dispatch identity,
// distinct from attempt — an agent-loop step's Continue re-dispatches
// the SAME attempt with a new iteration, never bumping Attempts (doing
// so would consume retry budget the step never spent), so iteration
// must be part of the subject/Msg-Id too or iteration 2+'s chunks
// collide with iteration 0's within the dedup window exactly the way
// un-scoped attempts collided before #624 review round 2. iteration is
// 0 for a non-loop step. Seq is monotonic per (ATTEMPT, ITERATION) pair
// and shared across the out/err streams (worker/log_writer.go assigns
// it from one counter), so ordering by Seq reconstructs write order
// even though out and err are independently buffered.
//
// Data marshals to a base64 string over JSON (encoding/json's default
// []byte handling) so both text and arbitrary binary output round-trip
// safely.
type LogChunk struct {
	Seq       uint64    `json:"seq"`
	Attempt   int       `json:"attempt"`
	Iteration int       `json:"iteration"`
	TS        time.Time `json:"ts"`
	Stream    string    `json:"stream"`
	Data      []byte    `json:"data"`
}
