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
const (
	// LogMarkerFailed is emitted by Fail/FailPermanent BEFORE the
	// resolution publish, so a consumer following the lane sees the
	// marker before (or with) the terminal step.failed event and
	// GET .../logs?from=failure has a recorded position to start
	// from rather than an inferred one.
	LogMarkerFailed = "failed"
	// LogMarkerTruncated is emitted exactly once, the moment
	// LogStepBytesMax is reached; no further chunks follow it.
	LogMarkerTruncated = "truncated"
)

// LogChunk is a single unit of captured step output on the BUILD_LOGS
// stream, subject logs.{runID}.{stepID}. Seq is monotonic per step and
// shared across the out/err streams (worker/log_writer.go assigns it
// from one counter), so ordering by Seq reconstructs write order even
// though out and err are independently buffered.
//
// Data marshals to a base64 string over JSON (encoding/json's default
// []byte handling) so both text and arbitrary binary output round-trip
// safely.
type LogChunk struct {
	Seq    uint64    `json:"seq"`
	TS     time.Time `json:"ts"`
	Stream string    `json:"stream"`
	Data   []byte    `json:"data"`
}
