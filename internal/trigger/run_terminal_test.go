// internal/trigger/run_terminal_test.go
// Methodology: pure unit tests for run_terminal config validation,
// default-status application, and subject derivation. No NATS
// dependency — the e2e fire/dedup/depth-cap behavior lives in
// run_terminal_e2e_test.go against a real embedded server.
package trigger

import (
	"strings"
	"testing"
)

func TestValidateRunTerminalConfig_SelfTargetRejected(t *testing.T) {
	// Methodology: table of config validation cases. Each case
	// checks both the pass/fail outcome (positive/negative space)
	// and, where relevant, the error content.

	// Negative: filter workflow equals the trigger's own target
	// workflow — this is exactly the offload-retriggers-itself loop
	// the issue calls out. The error must name both workflows so an
	// operator can see the cycle without re-reading the trigger def.
	def := TriggerDef{
		ID:         "t-self",
		WorkflowID: "offload-wf",
		RunTerminal: &RunTerminalConfig{
			Workflow: "offload-wf",
		},
	}
	err := Validate(def)
	if err == nil {
		t.Fatal("expected error for self-targeting run_terminal trigger")
	}
	if !strings.Contains(err.Error(), "offload-wf") {
		t.Fatalf("error %q should name the shared workflow", err)
	}
}

func TestValidateRunTerminalConfig_EmptyOrWildcardWorkflowRejected(t *testing.T) {
	// Negative: empty workflow filter.
	defEmpty := TriggerDef{
		ID:          "t-empty",
		WorkflowID:  "target-wf",
		RunTerminal: &RunTerminalConfig{Workflow: ""},
	}
	if err := Validate(defEmpty); err == nil {
		t.Fatal("expected error for empty workflow filter")
	}

	// Negative: wildcard workflow filter — the filter must name one
	// workflow, not match every workflow's terminal events.
	for _, wildcard := range []string{"*", ">", "wf-*", "wf.>"} {
		defWild := TriggerDef{
			ID:         "t-wild",
			WorkflowID: "target-wf",
			RunTerminal: &RunTerminalConfig{
				Workflow: wildcard,
			},
		}
		if err := Validate(defWild); err == nil {
			t.Fatalf(
				"expected error for wildcard workflow filter %q",
				wildcard,
			)
		}
	}
}

func TestValidateRunTerminalConfig_UnknownStatusRejected(t *testing.T) {
	// Negative: an unrecognized status name.
	def := TriggerDef{
		ID:         "t-status",
		WorkflowID: "target-wf",
		RunTerminal: &RunTerminalConfig{
			Workflow: "source-wf",
			Statuses: []string{"completed", "bogus"},
		},
	}
	if err := Validate(def); err == nil {
		t.Fatal("expected error for unknown status")
	}
}

func TestValidateRunTerminalConfig_ValidPasses(t *testing.T) {
	// Positive: a well-formed config with an explicit status subset
	// on a distinct source/target pair passes.
	def := TriggerDef{
		ID:         "t-ok",
		WorkflowID: "offload-wf",
		RunTerminal: &RunTerminalConfig{
			Workflow: "build-wf",
			Statuses: []string{"completed", "failed"},
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("expected valid config to pass, got %v", err)
	}
}

func TestRunTerminalConfig_EffectiveStatuses(t *testing.T) {
	// Positive: defaults applied when Statuses is empty — all three
	// terminal statuses.
	empty := RunTerminalConfig{Workflow: "wf"}
	got := empty.EffectiveStatuses()
	want := []string{"completed", "failed", "cancelled"}
	if len(got) != len(want) {
		t.Fatalf("EffectiveStatuses() = %v, want %v", got, want)
	}
	for i, s := range want {
		if got[i] != s {
			t.Fatalf("EffectiveStatuses()[%d] = %q, want %q",
				i, got[i], s)
		}
	}

	// Positive: explicit subset passes through unchanged.
	explicit := RunTerminalConfig{
		Workflow: "wf",
		Statuses: []string{"failed"},
	}
	got2 := explicit.EffectiveStatuses()
	if len(got2) != 1 || got2[0] != "failed" {
		t.Fatalf("EffectiveStatuses() = %v, want [failed]", got2)
	}
}

func TestRunTerminalSubject(t *testing.T) {
	// Positive: a plain workflow name derives the expected wildcard
	// filter subject on the EVENTS stream.
	got := runTerminalSubject("build-wf")
	want := "event.run.build-wf.*.*"
	if got != want {
		t.Fatalf("runTerminalSubject(%q) = %q, want %q",
			"build-wf", got, want)
	}

	// Positive: characters outside the NATS-subject-safe set are
	// sanitized the same way engine's subjectToken sanitizes them
	// when publishing event.run.* — otherwise the trigger's filter
	// subject would never match the published subject for a workflow
	// name containing e.g. a space or a dot.
	got2 := runTerminalSubject("build wf.v2")
	want2 := "event.run.build_wf_v2.*.*"
	if got2 != want2 {
		t.Fatalf("runTerminalSubject(%q) = %q, want %q",
			"build wf.v2", got2, want2)
	}
}
