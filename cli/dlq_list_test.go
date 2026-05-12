// cli/dlq_list_test.go
// Integration tests for `dagnats dlq list` defaults, --all, truncation
// footer, and delivery_count/consumer field rendering.
//
// Methodology: spin an embedded NATS server via dagnatstest.NewHarness,
// seed N synthetic DLQ entries via DLQFixture.Seed, then exercise the
// CLI in-process via CLIFixture. Asserts cover default safety cap,
// explicit --limit, --all, and field surfacing in --json. Closes #203.
package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dagnatstest"
)

// newDLQListFixtures bundles a Harness, DLQFixture, and CLIFixture so
// each test in this file gets a clean embedded NATS server with the
// right helper surfaces wired up.
func newDLQListFixtures(t *testing.T) (
	*dagnatstest.DLQFixture, *dagnatstest.CLIFixture,
) {
	t.Helper()
	h := dagnatstest.NewHarness(t)
	dlq := dagnatstest.NewDLQFixture(h)
	cli := dagnatstest.NewCLIFixture(h, Run, SwapExitFunc)
	return dlq, cli
}

// seedBelowCap is a row count comfortably above the legacy 50-entry
// hard limit and below dlqListSafetyCap (500). The test asserts the
// default returns all of them.
const seedBelowCap = 121

func TestDLQList_DefaultReturnsAllUpToSafetyCap(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dlq, cli := newDLQListFixtures(t)
	dlq.Seed(t, seedBelowCap)

	out := cli.Run(t, "dlq", "list", "--json")
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("output must be JSON: %v\n%s", err, out)
	}
	if len(rows) != seedBelowCap {
		t.Fatalf("default must return all entries below safety cap; "+
			"got %d want %d", len(rows), seedBelowCap)
	}
}

func TestDLQList_TruncationFooterToStderr(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dlq, cli := newDLQListFixtures(t)
	dlq.Seed(t, 10)

	stdout, stderr := cli.RunSplit(t,
		"dlq", "list", "--limit", "5", "--json")
	if !strings.Contains(stderr, "truncated") {
		t.Fatalf("truncation footer must go to stderr; "+
			"got stderr=%q", stderr)
	}
	if !strings.Contains(stderr, "10") {
		t.Fatalf("footer must name stream-total; got stderr=%q", stderr)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatalf("stdout must remain valid JSON: %v\n%s", err, stdout)
	}
	if len(rows) != 5 {
		t.Fatalf("explicit --limit honoured; got %d rows want 5",
			len(rows))
	}
}

func TestDLQList_AllReturnsEverything(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dlq, cli := newDLQListFixtures(t)
	dlq.Seed(t, 7)

	out := cli.Run(t, "dlq", "list", "--all", "--json")
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("--all must produce JSON: %v\n%s", err, out)
	}
	if len(rows) != 7 {
		t.Fatalf("--all must return every entry; got %d want 7",
			len(rows))
	}
}

func TestDLQList_DeliveryCountAndConsumerSurfaced(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	dlq, cli := newDLQListFixtures(t)
	dlq.Seed(t, 1)

	out := cli.Run(t, "dlq", "list", "--json")
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("output must be JSON: %v\n%s", err, out)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	if _, ok := rows[0]["delivery_count"]; !ok {
		t.Fatalf("delivery_count must appear in --json output: %v",
			rows[0])
	}
	if _, ok := rows[0]["consumer"]; !ok {
		t.Fatalf("consumer must appear in --json output: %v", rows[0])
	}
}
