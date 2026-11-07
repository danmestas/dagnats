// natsutil/ceilings_test.go
// Tests for proportional per-stream byte ceilings (#441 follow-up). Each
// file-based stream gets a MaxBytes sized as a fraction of the configured
// JetStreamMaxStore budget, so the sum of all MaxBytes stays comfortably
// under the budget on any host/cluster — the absolute ceilings that broke
// the 2 GiB e2e cluster (err 10047) are gone. Methodology: provision the
// streams against an embedded server with an explicit budget, assert each
// MaxBytes == floor(budget*fraction), assert the sum < budget, and assert
// budget==0 disables the ceiling. Bounded 5-second timeout on all ops.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fileStreamFractions mirrors the proportional table in conn.go. Keeping a
// copy in the test makes the expected MaxBytes literal and the sum check
// self-contained.
var fileStreamFractions = map[string]float64{
	"WORKFLOW_HISTORY": fractionWorkflowHistory,
	"EVENTS":           fractionEvents,
	"TASK_QUEUES":      fractionTaskQueues,
	"TELEMETRY":        fractionTelemetry,
	"BUILD_LOGS":       fractionBuildLogs,
	"DEAD_LETTERS":     fractionDeadLetters,
	"TRIGGER_HISTORY":  fractionTriggerHistory,
	"SLEEP_TIMERS":     fractionSleepTimers,
	AgentJournalStream: fractionAgentJournal,
}

// streamConfigsWithBudget provisions all file streams against a budget and
// returns their live configs keyed by name.
func streamConfigsWithBudget(
	t *testing.T, budget int64,
) map[string]jetstream.StreamConfig {
	t.Helper()
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := SetupStreams(js, 1, budget); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}
	if err := SetupTelemetryStream(js, budget, 1); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}
	if err := SetupTriggerHistoryStream(js, budget, 1); err != nil {
		t.Fatalf("SetupTriggerHistoryStream: %v", err)
	}
	if err := SetupBuildLogsStream(
		js, budget, buildLogsTTLDefault, 1,
	); err != nil {
		t.Fatalf("SetupBuildLogsStream: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	out := make(map[string]jetstream.StreamConfig, len(fileStreamFractions))
	for name := range fileStreamFractions {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			t.Fatalf("Stream(%s): %v", name, err)
		}
		out[name] = stream.CachedInfo().Config
	}
	return out
}

func TestProportionalMaxBytesPerStream(t *testing.T) {
	// Size against the embedded server's own budget: the assertions are
	// budget-independent math, and a budget exceeding the server's would
	// fail stream creation on the storage check rather than the sizing.
	const budget = testServerMaxStoreBytes
	cfgs := streamConfigsWithBudget(t, budget)

	for name, fraction := range fileStreamFractions {
		want := int64(float64(budget) * fraction)
		got := cfgs[name].MaxBytes
		if got != want {
			t.Fatalf("%s MaxBytes = %d, want %d (budget*%.2f)",
				name, got, want, fraction)
		}
		// Negative: a real ceiling must be set (never 0 with a budget).
		if got <= 0 {
			t.Fatalf("%s MaxBytes = %d, want positive ceiling", name, got)
		}
	}
}

// TestMaxBytesSumUnderBudget is the core safety guard: the sum of every
// file stream's MaxBytes must stay comfortably under the budget so stream
// creation never hits err 10047 (insufficient storage resources).
func TestMaxBytesSumUnderBudget(t *testing.T) {
	const budget = testServerMaxStoreBytes
	cfgs := streamConfigsWithBudget(t, budget)

	var sum int64
	for _, cfg := range cfgs {
		sum += cfg.MaxBytes
	}
	// The submissions bucket (ADR-027) is the one KV bucket with a
	// ceiling from the same budget; it counts against the guard too.
	sum += submissionsBucketConfig(1, budget).MaxBytes
	if sum >= budget {
		t.Fatalf("sum of MaxBytes = %d, want < budget %d", sum, budget)
	}
	// Negative: the sum must also leave real headroom (<= 85% of budget),
	// not creep up to the edge of the budget.
	if sum > budget*85/100 {
		t.Fatalf("sum of MaxBytes = %d exceeds 85%% of budget %d", sum, budget)
	}
}

// TestZeroBudgetDisablesCeiling guards the budget==0 path: an unset/zero
// budget must skip MaxBytes (the config sends 0), leaving the stream
// unbounded. JetStream reports an unbounded MaxBytes as -1, so the live
// config must carry no positive ceiling.
func TestZeroBudgetDisablesCeiling(t *testing.T) {
	cfgs := streamConfigsWithBudget(t, 0)

	for name, cfg := range cfgs {
		if cfg.MaxBytes > 0 {
			t.Fatalf("%s MaxBytes = %d, want no ceiling (<= 0)",
				name, cfg.MaxBytes)
		}
	}

	// Direct guard on the sizing helper itself: budget 0 → 0 (skip).
	if got := proportionalMaxBytes(0, fractionTelemetry); got != 0 {
		t.Fatalf("proportionalMaxBytes(0, _) = %d, want 0", got)
	}
}

// runsShareCommittedMax is the percentage of the budget the stream
// ceilings plus the workflow_runs share may commit, leaving the rest for
// the unbudgeted KV buckets.
const runsShareCommittedMax = 93

// TestRunsShareFitsHeadroom guards the workflow_runs share. The bucket gets
// no MaxBytes (a full KV rejects writes), so its share is a pruning target
// rather than a reservation: it comes out of the headroom the stream
// ceilings leave, and must leave part of that headroom for the other,
// unbudgeted KV buckets.
func TestRunsShareFitsHeadroom(t *testing.T) {
	const budget = testServerMaxStoreBytes
	share := RunsShareBytes(budget)
	if share != proportionalMaxBytes(budget, fractionWorkflowRuns) {
		t.Fatalf("RunsShareBytes = %d, want budget*%.2f", share, fractionWorkflowRuns)
	}
	var reserved int64
	for _, cfg := range streamConfigsWithBudget(t, budget) {
		reserved += cfg.MaxBytes
	}
	reserved += submissionsBucketConfig(1, budget).MaxBytes
	if reserved+share > budget*runsShareCommittedMax/100 {
		t.Fatalf("reserved %d + runs share %d exceeds %d%% of budget %d",
			reserved, share, runsShareCommittedMax, budget)
	}
	if RunsShareBytes(0) != 0 {
		t.Fatalf("RunsShareBytes(0) = %d, want 0 (no budget, no share)", RunsShareBytes(0))
	}
}
