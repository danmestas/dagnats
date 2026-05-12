// cli/run_state_test.go
// Integration tests for `dagnats run list --state <s>` flag parsing
// and client-side filtering.
// Methodology: spin an embedded NATS server via dagnatstest.NewHarness,
// drive runs into mixed terminal states with RunFixture, then exercise
// the CLI in-process via CLIFixture. Asserts cover canonical lowercase,
// equals-form, uppercase, unknown-value error, --status alias, and the
// no-flag baseline. Closes #201.
package cli

import (
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dagnatstest"
)

// newRunStateFixtures bundles a Harness, RunFixture, and CLIFixture
// so each test in this file gets a clean embedded NATS server with
// the right helper surfaces wired up.
func newRunStateFixtures(t *testing.T) (
	*dagnatstest.RunFixture, *dagnatstest.CLIFixture,
) {
	t.Helper()
	h := dagnatstest.NewHarness(t)
	runs := dagnatstest.NewRunFixture(h)
	cli := dagnatstest.NewCLIFixture(h, Run, SwapExitFunc)
	return runs, cli
}

func TestRunList_StateFlag_FiltersByStatus(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunStateFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 3)
	runs.SubmitAndAdvanceTo(t, "failed", 2)

	out := cli.Run(t, "run", "list", "--state", "failed")
	failed := strings.Count(out, "failed")
	if failed < 2 {
		t.Fatalf("expected at least 2 failed rows; got %d:\n%s",
			failed, out)
	}
	if strings.Contains(out, "completed") {
		t.Fatalf("--state failed must not return completed:\n%s",
			out)
	}
}

func TestRunList_StateFlag_EqualsForm(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunStateFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 1)
	runs.SubmitAndAdvanceTo(t, "failed", 1)

	out := cli.Run(t, "run", "list", "--state=failed")
	if !strings.Contains(out, "failed") {
		t.Fatalf("--state=failed must show failed:\n%s", out)
	}
	if strings.Contains(out, "completed") {
		t.Fatalf("--state=failed must not show completed:\n%s",
			out)
	}
}

func TestRunList_StateFlag_CaseInsensitive(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunStateFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 1)
	runs.SubmitAndAdvanceTo(t, "failed", 1)

	out := cli.Run(t, "run", "list", "--state", "FAILED")
	if !strings.Contains(out, "failed") {
		t.Fatalf("--state FAILED must match failed:\n%s", out)
	}
	if strings.Contains(out, "completed") {
		t.Fatalf("--state FAILED must filter out completed:\n%s",
			out)
	}
}

func TestRunList_StateFlag_UnknownValueErrors(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	_, cli := newRunStateFixtures(t)
	_, err := cli.RunErr(t, "run", "list", "--state", "foobar")
	if err == nil {
		t.Fatalf("unknown state must exit non-zero")
	}
	msg := err.Error()
	if !strings.Contains(msg, "valid:") {
		t.Fatalf("error must list valid states; got %q", msg)
	}
	if !strings.Contains(msg, "failed") {
		t.Fatalf("error must mention failed state; got %q", msg)
	}
}

func TestRunList_StateFlag_NoFlagReturnsAll(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunStateFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 2)
	runs.SubmitAndAdvanceTo(t, "failed", 1)

	out := cli.Run(t, "run", "list")
	if !strings.Contains(out, "completed") {
		t.Fatalf("no flag must include completed:\n%s", out)
	}
	if !strings.Contains(out, "failed") {
		t.Fatalf("no flag must include failed:\n%s", out)
	}
}

func TestRunList_StatusAlias_MatchesState(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	runs, cli := newRunStateFixtures(t)
	runs.SubmitAndAdvanceTo(t, "completed", 1)
	runs.SubmitAndAdvanceTo(t, "failed", 1)

	withStatus := cli.Run(t, "run", "list", "--status", "failed")
	withState := cli.Run(t, "run", "list", "--state", "failed")

	if !strings.Contains(withStatus, "failed") {
		t.Fatalf("--status alias must filter; got:\n%s",
			withStatus)
	}
	if strings.Contains(withStatus, "completed") {
		t.Fatalf("--status alias must exclude completed:\n%s",
			withStatus)
	}
	// Both forms should agree on the filtered runs. We compare
	// the count of "failed" tokens since RUN IDs differ between
	// invocations only by table formatting headers.
	if strings.Count(withStatus, "failed") !=
		strings.Count(withState, "failed") {
		t.Fatalf(
			"alias mismatch:\n--status: %s\n--state: %s",
			withStatus, withState,
		)
	}
}
