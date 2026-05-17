// internal/console/runs_window_test.go
// Methodology: table-driven tests over the runs-list since/until
// URL params introduced by PR 8 for the anomaly-marker click handler.
// Both the parser and the filter are pure functions — no NATS or
// HTTP fixture needed.
package console

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestParseUnixSecsParam_validInput(t *testing.T) {
	cases := map[string]int64{
		"1700000000": 1700000000,
		"42":         42,
		"1":          1,
	}
	for in, want := range cases {
		if got := parseUnixSecsParam(in); got != want {
			t.Errorf("parseUnixSecsParam(%q) = %d, want %d",
				in, got, want)
		}
	}
}

func TestParseUnixSecsParam_invalidInput(t *testing.T) {
	for _, in := range []string{
		"", "notanumber", "-5", "1.5", "0",
		"99999999999999999999999",
	} {
		if got := parseUnixSecsParam(in); got != 0 {
			t.Errorf("parseUnixSecsParam(%q) = %d, want 0",
				in, got)
		}
	}
}

func TestFilterRunsByWindow_inclusiveBounds(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	runs := []dag.WorkflowRun{
		{RunID: "before", CreatedAt: t0.Add(-time.Hour)},
		{RunID: "inside", CreatedAt: t0},
		{RunID: "after", CreatedAt: t0.Add(time.Hour)},
	}
	got := filterRunsByWindow(runs,
		t0.Add(-time.Minute).Unix(),
		t0.Add(time.Minute).Unix(),
	)
	if len(got) != 1 {
		t.Fatalf("want 1 run, got %d", len(got))
	}
	if got[0].RunID != "inside" {
		t.Errorf("want inside, got %s", got[0].RunID)
	}
}

func TestFilterRunsByWindow_openSince(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	runs := []dag.WorkflowRun{
		{RunID: "early", CreatedAt: t0.Add(-2 * time.Hour)},
		{RunID: "late", CreatedAt: t0.Add(time.Hour)},
	}
	// since=0 → no lower bound. until cuts off "late".
	got := filterRunsByWindow(runs, 0, t0.Unix())
	if len(got) != 1 || got[0].RunID != "early" {
		t.Fatalf("want [early], got %+v", got)
	}
}

func TestFilterRunsByWindow_emptyBothBounds(t *testing.T) {
	runs := []dag.WorkflowRun{{RunID: "anything"}}
	// 0, 0 → pass-through (no filter applied).
	if got := filterRunsByWindow(runs, 0, 0); len(got) != 1 {
		t.Fatalf("expected pass-through, got %d", len(got))
	}
}
