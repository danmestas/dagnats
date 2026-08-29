package natsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// buildLogsTTLDefault, buildLogsTTLMin, buildLogsTTLMax bound
// DAGNATS_BUILD_LOGS_TTL (#624): the hot lane's retention window.
// Retention past this window is a consumer's job (docs/wire-protocol.md,
// "Consumer contract: build logs") — dagnats itself never persists log
// bytes beyond it. Default 168h (7d) mirrors a typical CI-log-retention
// expectation without demanding an operator configure anything; the
// [1h, 8760h] (1h..1y) range keeps a misconfigured value from either
// discarding logs before any consumer could plausibly drain them or
// growing the stream unboundedly under the ceiling's proportional share.
const (
	buildLogsTTLDefault = 168 * time.Hour
	buildLogsTTLMin     = 1 * time.Hour
	buildLogsTTLMax     = 8760 * time.Hour
)

// resolveBuildLogsTTL parses val (the raw DAGNATS_BUILD_LOGS_TTL value)
// into a validated TTL. Empty resolves to buildLogsTTLDefault. Any parse
// error, or a value outside [buildLogsTTLMin, buildLogsTTLMax], is
// returned as an error rather than silently clamped — the design calls
// for a bad value to refuse startup, matching applyRunsMaxAgeEnv's
// fail-fast precedent (server/config.go) rather than guessing at what
// the operator meant.
func resolveBuildLogsTTL(val string) (time.Duration, error) {
	if val == "" {
		return buildLogsTTLDefault, nil
	}
	dur, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid DAGNATS_BUILD_LOGS_TTL %q: %w", val, err)
	}
	if dur < buildLogsTTLMin || dur > buildLogsTTLMax {
		return 0, fmt.Errorf(
			"invalid DAGNATS_BUILD_LOGS_TTL %q: must be in [%s, %s]",
			val, buildLogsTTLMin, buildLogsTTLMax,
		)
	}
	return dur, nil
}

// SetupBuildLogsStream creates the BUILD_LOGS stream (#624): the
// per-attempt hot lane for worker stdout/stderr, subjects
// logs.{runID}.{stepID}.{attempt}. LimitsPolicy + FileStorage matches
// the other history-shaped streams (WORKFLOW_HISTORY, EVENTS); ttl is
// the caller's resolved DAGNATS_BUILD_LOGS_TTL (see
// resolveBuildLogsTTL). maxStoreBytes is the JetStreamMaxStore budget;
// the byte ceiling is a fraction of it (see the fraction table's
// comment block above) — a budget of 0 (or less) disables the
// ceiling, same as every other file stream here.
//
// Duplicates uses a >=2min window: LogChunk.Seq collisions from a
// redelivered task message (worker crash between publish and ack) must
// dedup on Nats-Msg-Id ("log-{runID}-{stepID}-{attempt}-{seq}"), and 2
// minutes comfortably covers the AckWait-driven redelivery interval a
// worker task can see. attempt is part of both the subject and the
// Msg-Id (#624 review round 2) — without it, a retry's fresh Msg-Id
// counter starting back at seq 0 would collide with the prior
// attempt's within this same window.
//
// No AllowDirect: unlike TASK_QUEUES (#632's queue-depth API), nothing
// reads BUILD_LOGS via direct-get — the tail API (internal/api) reads it
// through ordered consumers, so there's no reason to pay for direct-get
// support on this stream.
func SetupBuildLogsStream(
	js jetstream.JetStream,
	maxStoreBytes int64,
	ttl time.Duration,
	replicas int,
) error {
	if js == nil {
		panic("SetupBuildLogsStream: js must not be nil")
	}
	if ttl <= 0 {
		panic("SetupBuildLogsStream: ttl must be positive")
	}
	cfg := jetstream.StreamConfig{
		Name:      "BUILD_LOGS",
		Subjects:  []string{"logs.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    ttl,
		MaxBytes: proportionalMaxBytes(
			maxStoreBytes, fractionBuildLogs,
		),
		Duplicates: 2 * time.Minute,
		Replicas:   replicas,
	}
	if cfg.Name == "" {
		panic("SetupBuildLogsStream: stream name must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	_, err := js.CreateOrUpdateStream(ctx, cfg)
	return err
}
