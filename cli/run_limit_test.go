// cli/run_limit_test.go
// Tests for `dagnats run list --limit` flag parsing and end-to-end
// truncation notice behavior (issue #257).
// Methodology: pure unit tests for parseRunListFlags so the validation
// contract is pinned without spinning up NATS, plus an integration
// test via dagnatstest.CLIFixture that drives real runs and asserts
// the stderr notice fires when len(runs) == limit.
package cli

import (
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/api"
)

// TestRunListLimitFlagParsing covers the valid/invalid surfaces of
// the --limit flag without invoking the service. parseRunListFlags
// owns the validation contract.
func TestRunListLimitFlagParsing(t *testing.T) {
	// Positive: equals form parses.
	flags, err := parseRunListFlags([]string{"--limit=50"})
	if err != nil {
		t.Fatalf("--limit=50 unexpected error: %v", err)
	}
	if flags.limit != 50 {
		t.Fatalf("flags.limit = %d, want 50", flags.limit)
	}

	// Positive: space form parses.
	flags, err = parseRunListFlags([]string{"--limit", "250"})
	if err != nil {
		t.Fatalf("--limit 250 unexpected error: %v", err)
	}
	if flags.limit != 250 {
		t.Fatalf("flags.limit = %d, want 250", flags.limit)
	}

	// Positive: default applies when --limit is absent.
	flags, err = parseRunListFlags([]string{"--workflow=wf"})
	if err != nil {
		t.Fatalf("no --limit unexpected error: %v", err)
	}
	if flags.limit != api.DefaultRunsLimit {
		t.Fatalf("default limit = %d, want %d",
			flags.limit, api.DefaultRunsLimit)
	}

	// Negative: zero rejected.
	if _, err := parseRunListFlags(
		[]string{"--limit=0"},
	); err == nil {
		t.Fatal("--limit=0 must error")
	}

	// Negative: out-of-range rejected.
	if _, err := parseRunListFlags(
		[]string{"--limit=99999"},
	); err == nil {
		t.Fatal("--limit=99999 must error")
	}

	// Negative: non-integer rejected.
	if _, err := parseRunListFlags(
		[]string{"--limit=abc"},
	); err == nil {
		t.Fatal("--limit=abc must error")
	}

	// Negative: ceiling is exactly accepted, one above is not.
	flags, err = parseRunListFlags([]string{
		"--limit=" + itoa(api.MaxRunsLimitCeiling),
	})
	if err != nil {
		t.Fatalf("--limit at ceiling must accept: %v", err)
	}
	if flags.limit != api.MaxRunsLimitCeiling {
		t.Fatalf("limit at ceiling = %d, want %d",
			flags.limit, api.MaxRunsLimitCeiling)
	}
	if _, err := parseRunListFlags([]string{
		"--limit=" + itoa(api.MaxRunsLimitCeiling+1),
	}); err == nil {
		t.Fatal("--limit one above ceiling must error")
	}
}

// itoa is a tiny local helper. We avoid importing strconv into the
// test file to keep the diff small; the values are bounded so a
// hand-rolled base-10 is trivially correct.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

// TestRunListPrintsTruncationNotice drives real runs through the CLI
// fixture and asserts the truncation notice fires ONLY when the true
// total exceeds the returned window (total > returned), not merely
// when len(runs) == limit. #452 fixed the prior latent bug where an
// exact-fit limit (limit == total) spuriously emitted the notice.
func TestRunListPrintsTruncationNotice(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	h := dagnatstest.NewHarness(t)
	runs := dagnatstest.NewRunFixture(h)
	cli := dagnatstest.NewCLIFixture(h, Run, SwapExitFunc)

	runs.SubmitAndAdvanceTo(t, "completed", 3)

	// Positive: --limit=2 against 3 runs genuinely truncates (3 > 2),
	// so the notice fires and rows still land on stdout.
	stdout, stderr := cli.RunSplit(
		t, "run", "list", "--limit=2",
	)
	if !strings.Contains(stderr, "truncated") {
		t.Fatalf(
			"expected truncation notice on stderr; got:\n%s",
			stderr,
		)
	}
	if !strings.Contains(stderr, "--limit=2") {
		t.Fatalf(
			"notice should echo the requested limit; got:\n%s",
			stderr,
		)
	}
	if !strings.Contains(stdout, "completed") {
		t.Fatalf(
			"stdout should still contain run rows; got:\n%s",
			stdout,
		)
	}

	// Negative: an exact-fit limit (limit == total) is NOT truncated
	// under the honest total>returned predicate — no notice.
	_, stderrExact := cli.RunSplit(
		t, "run", "list", "--limit=3",
	)
	if strings.Contains(stderrExact, "truncated") {
		t.Fatalf(
			"exact-fit limit must not emit truncation notice; got:\n%s",
			stderrExact,
		)
	}

	// Negative: with comfortable headroom, no notice either.
	_, stderrBig := cli.RunSplit(
		t, "run", "list", "--limit=100",
	)
	if strings.Contains(stderrBig, "truncated") {
		t.Fatalf(
			"unexpected truncation notice with headroom; got:\n%s",
			stderrBig,
		)
	}
}
