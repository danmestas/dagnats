// cli/run_count_test.go
// Integration tests for the #452 honesty surface on `dagnats run`:
// the --json envelope and human "showing N of M" line on `run list`,
// the --since filter, and the new `run count` subcommand.
// Methodology: spin an embedded NATS server via dagnatstest.NewHarness,
// drive real runs with RunFixture, then exercise the CLI in-process via
// CLIFixture. Each test gets its own server (no sharing). Assertions
// cover both positive (expected shape present) and negative (old shape
// or unwanted rows absent) space.
package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dagnatstest"
)

func newRunCountFixtures(t *testing.T) (
	*dagnatstest.RunFixture, *dagnatstest.CLIFixture,
) {
	t.Helper()
	h := dagnatstest.NewHarness(t)
	runs := dagnatstest.NewRunFixture(h)
	cli := dagnatstest.NewCLIFixture(h, Run, SwapExitFunc)
	return runs, cli
}

// TestRunListJSONEnvelope proves --json now emits the envelope object
// (runs/total/returned/truncated), not a bare array.
func TestRunListJSONEnvelope(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 3)

	out := cli.Run(t, "run", "list", "--json")

	var env struct {
		Runs      []map[string]any `json:"runs"`
		Total     int              `json:"total"`
		Returned  int              `json:"returned"`
		Truncated bool             `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("envelope must be a JSON object: %v\noutput=%s",
			err, out)
	}
	// Positive: the envelope reports all three runs.
	if env.Total != 3 {
		t.Fatalf("total = %d, want 3:\n%s", env.Total, out)
	}
	if env.Returned != 3 || len(env.Runs) != 3 {
		t.Fatalf("returned=%d len=%d, want 3:\n%s",
			env.Returned, len(env.Runs), out)
	}
	// Negative: a full window is not truncated, and the output is an
	// object — a bare array would fail the unmarshal above and lack
	// the "total" key here.
	if env.Truncated {
		t.Fatalf("truncated must be false for full window:\n%s", out)
	}
	if !strings.Contains(out, "\"total\"") {
		t.Fatalf("envelope must carry a total key:\n%s", out)
	}
}

// TestRunListJSONEnvelopeTruncated proves total > returned sets the
// truncated flag honestly.
func TestRunListJSONEnvelopeTruncated(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 4)

	out := cli.Run(t, "run", "list", "--limit", "2", "--json")
	var env struct {
		Total     int  `json:"total"`
		Returned  int  `json:"returned"`
		Truncated bool `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("envelope unmarshal: %v\n%s", err, out)
	}
	// Positive: returned is capped, total reflects the real count.
	if env.Returned != 2 {
		t.Fatalf("returned = %d, want 2:\n%s", env.Returned, out)
	}
	if env.Total != 4 {
		t.Fatalf("total = %d, want 4:\n%s", env.Total, out)
	}
	// Negative: truncated must be set when total exceeds returned.
	if !env.Truncated {
		t.Fatalf("truncated must be true (4>2):\n%s", out)
	}
}

// TestRunListHumanShowingLine proves the human path prints a
// "showing <returned> of <total>" line.
func TestRunListHumanShowingLine(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 3)

	out := cli.Run(t, "run", "list")
	// Positive: the showing line names returned and total.
	if !strings.Contains(out, "showing 3 of 3") {
		t.Fatalf("expected 'showing 3 of 3':\n%s", out)
	}
	// Negative: it must not claim a different total.
	if strings.Contains(out, "of 0") {
		t.Fatalf("showing line must not report zero total:\n%s", out)
	}
}

// TestRunCountSubcommand proves `run count` returns the aggregate
// without printing rows, and respects --json.
func TestRunCountSubcommand(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 2)
	runs.SubmitAndAdvanceTo(t, "failed", 1)

	out := cli.Run(t, "run", "count", "--json")
	var got struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("count --json must be an object: %v\n%s", err, out)
	}
	// Positive: counts every run.
	if got.Count != 3 {
		t.Fatalf("count = %d, want 3:\n%s", got.Count, out)
	}
	// Negative: count output must not materialize the run table.
	if strings.Contains(out, "RUN_ID") {
		t.Fatalf("count must not print the run table:\n%s", out)
	}
}

// TestRunCountStateFilter proves --state narrows the count.
func TestRunCountStateFilter(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 2)
	runs.SubmitAndAdvanceTo(t, "failed", 3)

	out := cli.Run(t, "run", "count", "--state", "failed", "--json")
	var got struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("count --json: %v\n%s", err, out)
	}
	// Positive: only the failed runs are counted.
	if got.Count != 3 {
		t.Fatalf("failed count = %d, want 3:\n%s", got.Count, out)
	}
	// Negative: the filtered count is strictly below the total of 5.
	if got.Count >= 5 {
		t.Fatalf("filtered count must be < 5:\n%s", out)
	}
}

// TestRunListSinceDuration proves --since interprets a duration as
// "since now - dur": a wide window keeps recent runs.
func TestRunListSinceDuration(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunCountFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 2)

	// Positive: a 100h-wide window covers runs just created.
	out := cli.Run(t, "run", "count", "--since", "100h", "--json")
	var got struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("count --json: %v\n%s", err, out)
	}
	if got.Count != 2 {
		t.Fatalf("since 100h count = %d, want 2:\n%s", got.Count, out)
	}

	// Negative: a 1-nanosecond window excludes runs created earlier.
	tiny := cli.Run(t, "run", "count", "--since", "1ns", "--json")
	var none struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(tiny), &none); err != nil {
		t.Fatalf("count --json (tiny): %v\n%s", err, tiny)
	}
	if none.Count != 0 {
		t.Fatalf("since 1ns count = %d, want 0:\n%s", none.Count, tiny)
	}
}

// TestParseSinceFlag pins the --since parse contract: duration first,
// then RFC3339, else an error naming the flag. Pure unit, no NATS.
func TestParseSinceFlag(t *testing.T) {
	// Positive: a duration is interpreted as "now - dur".
	durTime, err := parseSinceFlag("30m")
	if err != nil {
		t.Fatalf("parseSinceFlag(30m): %v", err)
	}
	if durTime.IsZero() {
		t.Fatal("duration form must yield a non-zero cutoff")
	}
	// Positive: an RFC3339 absolute timestamp parses verbatim.
	absTime, err := parseSinceFlag("2026-01-02T15:04:05Z")
	if err != nil {
		t.Fatalf("parseSinceFlag(rfc3339): %v", err)
	}
	if absTime.Year() != 2026 {
		t.Fatalf("rfc3339 year = %d, want 2026", absTime.Year())
	}
	// Negative: garbage errors and the message names the flag.
	_, err = parseSinceFlag("not-a-time")
	if err == nil {
		t.Fatal("garbage --since must error")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Fatalf("error must name --since flag; got %q", err.Error())
	}
}
