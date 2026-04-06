// cli/completion_test.go
// Tests for shell completion logic. Methodology: unit tests for the
// static completion resolution functions. No NATS required -- dynamic
// completions are tested only for the fallback (no connection) path.
// Each test validates both positive and negative space.
package cli

import (
	"testing"
)

// TestResolveCompletions_TopLevel verifies that an empty word list
// returns all top-level commands.
func TestResolveCompletions_TopLevel(t *testing.T) {
	got := resolveCompletions([]string{})

	// Positive: must contain known commands.
	if !contains(got, "run") {
		t.Errorf(
			"expected 'run' in completions, got %v", got,
		)
	}
	if !contains(got, "workflow") {
		t.Errorf(
			"expected 'workflow' in completions, got %v",
			got,
		)
	}

	// Negative: must not contain hidden commands.
	if contains(got, "__complete") {
		t.Errorf(
			"'__complete' should not appear in top-level",
		)
	}
}

// TestResolveCompletions_PartialTopLevel verifies that a partial
// top-level word filters to matching commands.
func TestResolveCompletions_PartialTopLevel(t *testing.T) {
	got := resolveCompletions([]string{"wo"})

	// Positive: "workflow" and "workers" match "wo".
	if !contains(got, "workflow") {
		t.Errorf(
			"expected 'workflow' for prefix 'wo', got %v",
			got,
		)
	}
	if !contains(got, "workers") {
		t.Errorf(
			"expected 'workers' for prefix 'wo', got %v",
			got,
		)
	}

	// Negative: "run" does not match "wo".
	if contains(got, "run") {
		t.Errorf(
			"'run' should not match prefix 'wo'",
		)
	}
}

// TestResolveCompletions_RunSubcommands verifies that typing
// "run" then TAB returns run subcommands.
func TestResolveCompletions_RunSubcommands(t *testing.T) {
	got := resolveCompletions([]string{"run", ""})

	// Positive: must include known run subcommands.
	if !contains(got, "start") {
		t.Errorf(
			"expected 'start' in run subs, got %v", got,
		)
	}
	if !contains(got, "status") {
		t.Errorf(
			"expected 'status' in run subs, got %v", got,
		)
	}

	// Negative: must not include workflow subcommands.
	if contains(got, "register") {
		t.Errorf(
			"'register' should not appear in run subs",
		)
	}
}

// TestResolveCompletions_PartialRunSubcommand verifies partial
// subcommand matching under "run".
func TestResolveCompletions_PartialRunSubcommand(t *testing.T) {
	got := resolveCompletions([]string{"run", "st"})

	// Positive: "start" and "status" match "st".
	if !contains(got, "start") {
		t.Errorf(
			"expected 'start' for prefix 'st', got %v",
			got,
		)
	}
	if !contains(got, "status") {
		t.Errorf(
			"expected 'status' for prefix 'st', got %v",
			got,
		)
	}

	// Negative: "cancel" does not match "st".
	if contains(got, "cancel") {
		t.Errorf(
			"'cancel' should not match prefix 'st'",
		)
	}
}

// TestResolveCompletions_WorkflowSubcommands verifies workflow
// subcommand completions.
func TestResolveCompletions_WorkflowSubcommands(t *testing.T) {
	got := resolveCompletions([]string{"workflow", ""})

	// Positive: known subcommands present.
	if !contains(got, "list") {
		t.Errorf(
			"expected 'list' in workflow subs, got %v",
			got,
		)
	}
	if !contains(got, "show") {
		t.Errorf(
			"expected 'show' in workflow subs, got %v",
			got,
		)
	}

	// Negative: run subcommands absent.
	if contains(got, "start") {
		t.Errorf(
			"'start' should not appear in workflow subs",
		)
	}
}

// TestResolveCompletions_TriggerSubcommands verifies trigger
// subcommand completions.
func TestResolveCompletions_TriggerSubcommands(t *testing.T) {
	got := resolveCompletions([]string{"trigger", ""})

	// Positive: known subcommands present.
	if !contains(got, "create") {
		t.Errorf(
			"expected 'create' in trigger subs, got %v",
			got,
		)
	}
	if !contains(got, "enable") {
		t.Errorf(
			"expected 'enable' in trigger subs, got %v",
			got,
		)
	}

	// Negative: dlq subcommands absent.
	if contains(got, "replay") {
		t.Errorf(
			"'replay' should not appear in trigger subs",
		)
	}
}

// TestResolveCompletions_DLQSubcommands verifies dlq subcommand
// completions.
func TestResolveCompletions_DLQSubcommands(t *testing.T) {
	got := resolveCompletions([]string{"dlq", ""})

	// Positive: known subcommands present.
	if !contains(got, "list") {
		t.Errorf(
			"expected 'list' in dlq subs, got %v", got,
		)
	}
	if !contains(got, "replay") {
		t.Errorf(
			"expected 'replay' in dlq subs, got %v", got,
		)
	}

	// Negative: only 3 dlq subcommands.
	if len(got) != 3 {
		t.Errorf(
			"expected 3 dlq subs, got %d: %v",
			len(got), got,
		)
	}
}

// TestResolveCompletions_CompletionSubcommands verifies the
// completion command's own subcommands.
func TestResolveCompletions_CompletionSubcommands(t *testing.T) {
	got := resolveCompletions([]string{"completion", ""})

	// Positive: bash and zsh present.
	if !contains(got, "bash") {
		t.Errorf(
			"expected 'bash' in completion subs, got %v",
			got,
		)
	}
	if !contains(got, "zsh") {
		t.Errorf(
			"expected 'zsh' in completion subs, got %v",
			got,
		)
	}
}

// TestResolveCompletions_FlagCompletion verifies that flags are
// completed when the current word starts with "-".
func TestResolveCompletions_FlagCompletion(t *testing.T) {
	got := resolveCompletions(
		[]string{"run", "start", "--"},
	)

	// Positive: must include run.start flags.
	if !contains(got, "--watch") {
		t.Errorf(
			"expected '--watch' in flags, got %v", got,
		)
	}
	if !contains(got, "--json") {
		t.Errorf(
			"expected '--json' in flags, got %v", got,
		)
	}

	// Negative: must not include flags from other commands.
	if contains(got, "--last") {
		t.Errorf(
			"'--last' should not appear in run.start flags",
		)
	}
}

// TestResolveCompletions_UnknownCommand verifies that an unknown
// top-level command returns empty completions.
func TestResolveCompletions_UnknownCommand(t *testing.T) {
	got := resolveCompletions(
		[]string{"nonexistent", ""},
	)

	// Positive: result must be nil (no completions).
	if got != nil {
		t.Errorf(
			"expected nil for unknown command, got %v", got,
		)
	}

	// Negative: must have zero length.
	if len(got) != 0 {
		t.Errorf(
			"expected 0 completions, got %d", len(got),
		)
	}
}

// TestResolveCompletions_NoMatchPartial verifies that a partial
// word with no matches returns empty.
func TestResolveCompletions_NoMatchPartial(t *testing.T) {
	got := resolveCompletions([]string{"zzz"})

	// Positive: empty result for non-matching prefix.
	if len(got) != 0 {
		t.Errorf(
			"expected 0 for prefix 'zzz', got %v", got,
		)
	}

	// Negative: must not panic or return stale data.
	if contains(got, "run") {
		t.Errorf("'run' should not match prefix 'zzz'")
	}
}

// TestStripSeparator verifies "--" removal from args.
func TestStripSeparator(t *testing.T) {
	got := stripSeparator([]string{"--", "run", "start"})

	// Positive: separator removed, rest preserved.
	if len(got) != 2 {
		t.Fatalf("expected 2 args, got %d", len(got))
	}
	if got[0] != "run" {
		t.Errorf("expected 'run', got %q", got[0])
	}

	// Negative: no separator means no change.
	got2 := stripSeparator([]string{"run", "start"})
	if len(got2) != 2 {
		t.Errorf("expected 2 args unchanged, got %d", len(got2))
	}
}

// TestFilterPrefix verifies prefix filtering on string slices.
func TestFilterPrefix(t *testing.T) {
	input := []string{"start", "status", "cancel"}
	got := filterPrefix(input, "st")

	// Positive: "start" and "status" match.
	if len(got) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(got), got)
	}

	// Negative: "cancel" does not match.
	if contains(got, "cancel") {
		t.Errorf("'cancel' should not match prefix 'st'")
	}
}

// TestFilterPrefix_EmptyPrefix returns all candidates.
func TestFilterPrefix_EmptyPrefix(t *testing.T) {
	input := []string{"a", "b", "c"}
	got := filterPrefix(input, "")

	// Positive: all returned.
	if len(got) != 3 {
		t.Errorf("expected 3, got %d", len(got))
	}

	// Negative: must equal original length.
	if len(got) != len(input) {
		t.Errorf("length mismatch: %d vs %d",
			len(got), len(input))
	}
}

// TestHandleCompleteCmd_WithDagnatsPrefix verifies that the
// program name "dagnats" is stripped from COMP_WORDS.
func TestHandleCompleteCmd_WithDagnatsPrefix(t *testing.T) {
	// Simulate: dagnats __complete -- dagnats run <TAB>
	// The shell passes COMP_WORDS including the empty current word.
	// handleCompleteCmd receives ["--", "dagnats", "run", ""]
	// and should resolve subcommands for "run".
	words := []string{"--", "dagnats", "run", ""}
	cleaned := stripSeparator(words)
	if cleaned[0] == "dagnats" {
		cleaned = cleaned[1:]
	}
	got := resolveCompletions(cleaned)

	// Positive: should return run subcommands.
	if !contains(got, "start") {
		t.Errorf(
			"expected 'start' after stripping dagnats, got %v",
			got,
		)
	}

	// Negative: should not contain top-level commands.
	if contains(got, "workflow") {
		t.Errorf(
			"'workflow' should not appear in run subcommands",
		)
	}
}

// TestDynamicCompletions_NoNATS verifies that dynamic completions
// return nil when NATS is unavailable (silent failure).
func TestDynamicCompletions_NoNATS(t *testing.T) {
	got := fetchDynamicCompletions("run.start", "")

	// Positive: nil result when NATS is down.
	if got != nil {
		t.Errorf(
			"expected nil without NATS, got %v", got,
		)
	}

	// Negative: must not panic.
	got2 := fetchDynamicCompletions("run.status", "prefix")
	if got2 != nil {
		t.Errorf(
			"expected nil without NATS, got %v", got2,
		)
	}
}

// TestDynamicCompletions_NonDynamicKey verifies that keys without
// dynamic completion return nil.
func TestDynamicCompletions_NonDynamicKey(t *testing.T) {
	got := fetchDynamicCompletions("run.list", "")

	// Positive: nil for non-dynamic key.
	if got != nil {
		t.Errorf(
			"expected nil for run.list, got %v", got,
		)
	}

	// Negative: unknown key also returns nil.
	got2 := fetchDynamicCompletions("fake.cmd", "")
	if got2 != nil {
		t.Errorf(
			"expected nil for fake.cmd, got %v", got2,
		)
	}
}

// contains checks whether a string slice includes a value.
func contains(ss []string, val string) bool {
	for _, s := range ss {
		if s == val {
			return true
		}
	}
	return false
}
