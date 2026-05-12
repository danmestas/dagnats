// cli/dlq_watch_test.go
// Tests for DLQ watch command components: replay tracker and output formatting.
// Methodology: pure unit tests with no NATS dependency. The watch loop
// itself is not tested here — only its composable building blocks.
package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

func TestReplayTrackerShouldReplayUnderMax(t *testing.T) {
	tracker := newReplayTracker(3)

	// Positive: fresh sequence should be replayable.
	if !tracker.shouldReplay(1) {
		t.Fatal("fresh sequence should be replayable")
	}

	// Positive: after one replay, still under max.
	tracker.record(1)
	if !tracker.shouldReplay(1) {
		t.Fatal("sequence with 1 replay should still be replayable")
	}
}

func TestReplayTrackerShouldReplayAtMax(t *testing.T) {
	tracker := newReplayTracker(2)

	tracker.record(5)
	tracker.record(5)

	// Positive: at max replays, should not be replayable.
	if tracker.shouldReplay(5) {
		t.Fatal("sequence at max replays should not be replayable")
	}

	// Negative: different sequence should still be replayable.
	if !tracker.shouldReplay(99) {
		t.Fatal("untracked sequence should be replayable")
	}
}

func TestReplayTrackerRecordIncrements(t *testing.T) {
	tracker := newReplayTracker(5)

	tracker.record(10)
	tracker.record(10)
	tracker.record(10)

	// Positive: count should be 3 after three records.
	if tracker.counts[10] != 3 {
		t.Fatalf("expected count 3, got %d", tracker.counts[10])
	}

	// Negative: untracked sequence should have zero count.
	if tracker.counts[999] != 0 {
		t.Fatalf("untracked sequence should have count 0, got %d",
			tracker.counts[999])
	}
}

func TestReplayTrackerExhaustedCount(t *testing.T) {
	tracker := newReplayTracker(1)

	// Exhaust two sequences.
	tracker.record(1)
	tracker.record(2)

	// Positive: two sequences should be exhausted.
	if tracker.exhausted() != 2 {
		t.Fatalf("expected 2 exhausted, got %d",
			tracker.exhausted())
	}

	// Negative: total tracked should be 2, not more.
	if len(tracker.counts) != 2 {
		t.Fatalf("expected 2 tracked sequences, got %d",
			len(tracker.counts))
	}
}

func TestReplayTrackerExhaustedZeroWhenNone(t *testing.T) {
	tracker := newReplayTracker(3)
	tracker.record(1)

	// Positive: no exhausted sequences yet.
	if tracker.exhausted() != 0 {
		t.Fatalf("expected 0 exhausted, got %d",
			tracker.exhausted())
	}

	// Negative: should still have one tracked entry.
	if len(tracker.counts) != 1 {
		t.Fatalf("expected 1 tracked, got %d",
			len(tracker.counts))
	}
}

func TestReplayTrackerPanicsOnOverflow(t *testing.T) {
	tracker := newReplayTracker(3)

	// Fill to capacity.
	const maxTracked = 10000
	for i := uint64(0); i < maxTracked; i++ {
		tracker.record(i)
	}

	// Positive: should panic on exceeding bound.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on overflow")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T", r)
		}
		// Negative: panic message should mention the bound.
		if !strings.Contains(msg, "10000") {
			t.Fatalf("panic should mention bound, got: %s", msg)
		}
	}()

	tracker.record(maxTracked) // 10001st entry
}

func TestFormatDLQWatchAction(t *testing.T) {
	letter := api.DeadLetterView{
		DeadLetter: api.DeadLetter{
			Sequence:  42,
			Subject:   "dead.my-task.run-1.step-a",
			RunID:     "run-1",
			StepID:    "step-a",
			Task:      "my-task",
			Error:     "timeout",
			Timestamp: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
		},
	}

	var buf bytes.Buffer
	FormatDLQWatchAction(&buf, letter, 1, 3)
	output := buf.String()

	// Positive: should contain sequence and task.
	if !strings.Contains(output, "42") {
		t.Fatal("output should contain sequence number")
	}
	if !strings.Contains(output, "my-task") {
		t.Fatal("output should contain task name")
	}

	// Negative: should not contain raw subject.
	if strings.Contains(output, "dead.my-task.run-1.step-a") {
		t.Fatal("output should not contain raw subject")
	}
}

func TestFormatDLQWatchActionSkipped(t *testing.T) {
	letter := api.DeadLetterView{
		DeadLetter: api.DeadLetter{
			Sequence: 7,
			Task:     "stuck-task",
		},
	}

	var buf bytes.Buffer
	FormatDLQWatchActionSkipped(&buf, letter, 3)
	output := buf.String()

	// Positive: should indicate exhausted/skipped.
	if !strings.Contains(output, "skip") &&
		!strings.Contains(output, "exhausted") {
		t.Fatal("output should indicate skip or exhausted")
	}

	// Negative: should not say "replayed".
	if strings.Contains(output, "replayed") {
		t.Fatal("skipped output should not say replayed")
	}
}

func TestFormatDLQWatchSummary(t *testing.T) {
	var buf bytes.Buffer
	FormatDLQWatchSummary(&buf, 10, 2)
	output := buf.String()

	// Positive: should contain the replay count.
	if !strings.Contains(output, "10") {
		t.Fatal("summary should contain replayed count")
	}

	// Positive: should contain the exhausted count.
	if !strings.Contains(output, "2") {
		t.Fatal("summary should contain exhausted count")
	}
}

func TestFormatDLQWatchActionJSON(t *testing.T) {
	letter := api.DeadLetterView{
		DeadLetter: api.DeadLetter{
			Sequence: 42,
			Task:     "my-task",
			RunID:    "run-1",
		},
	}

	var buf bytes.Buffer
	FormatDLQWatchActionJSON(&buf, letter, "replayed", 1)
	output := buf.String()

	// Positive: should be valid-looking JSON with action field.
	if !strings.Contains(output, `"action"`) {
		t.Fatal("JSON output should contain action field")
	}
	if !strings.Contains(output, `"replayed"`) {
		t.Fatal("JSON output should contain replayed action")
	}

	// Negative: should not contain human text formatting.
	if strings.Contains(output, "Replaying") {
		t.Fatal("JSON output should not contain human text")
	}
}

func TestFormatDLQWatchSummaryJSON(t *testing.T) {
	var buf bytes.Buffer
	FormatDLQWatchSummaryJSON(&buf, 5, 1)
	output := buf.String()

	// Positive: should contain replayed count.
	if !strings.Contains(output, `"total_replayed"`) {
		t.Fatal("JSON summary should contain total_replayed")
	}

	// Negative: should not contain human text.
	if strings.Contains(output, "Summary") {
		t.Fatal("JSON summary should not contain human text")
	}
}
