// dag/validate_respond_test.go
//
// Methodology: pure unit tests, no NATS. Each test builds a small
// WorkflowDef and asserts the warning kinds returned by
// ValidateRespondReachability. Two assertions per test: presence (or
// absence) of the expected warning plus a sanity check on the rest of
// the warning list.
package dag

import (
	"encoding/json"
	"testing"
)

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func TestValidateRespondNonHTTPTriggerNoWarning(t *testing.T) {
	def := WorkflowDef{
		Name: "non-http", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
		},
	}
	got := ValidateRespondReachability(def, false)
	if len(got) != 0 {
		t.Fatalf("non-http trigger should produce no warnings: %v", got)
	}
}

func TestValidateRespondNonHTTPWithRespondStepNoWarning(t *testing.T) {
	def := WorkflowDef{
		Name: "non-http-respond", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, false)
	if len(got) != 0 {
		t.Fatalf("respond is legal everywhere: %v", got)
	}
}

func TestValidateRespondHTTPTriggerWithReachableRespond(t *testing.T) {
	def := WorkflowDef{
		Name: "http-ok", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 0 {
		t.Fatalf("single reachable respond — no warnings: %v", got)
	}
}

func TestValidateRespondHTTPTriggerMissingRespond(t *testing.T) {
	def := WorkflowDef{
		Name: "http-no-respond", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "b", Task: "task-b",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 1 {
		t.Fatalf("want 1 warning, got %d: %v", len(got), got)
	}
	if got[0].Kind != WarnMissingRespond {
		t.Fatalf(
			"want WarnMissingRespond, got %q", got[0].Kind,
		)
	}
}

func TestValidateRespondHTTPTriggerDuplicateRespond(t *testing.T) {
	// Two respond steps with no mutually-exclusive gating: both will
	// fire on the same execution. ADR-013 names this duplicate_respond.
	def := WorkflowDef{
		Name: "dup", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r1", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r2", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 1 {
		t.Fatalf("want 1 warning, got %d: %v", len(got), got)
	}
	if got[0].Kind != WarnDuplicateRespond {
		t.Fatalf(
			"want WarnDuplicateRespond, got %q", got[0].Kind,
		)
	}
}

func TestValidateRespondHTTPTriggerBranchPerOutcomeNoWarning(t *testing.T) {
	// happy and error branches each have their own respond. The two
	// respond steps are mutually exclusive because each is gated on an
	// opposite SkipIf condition over the same upstream's output.
	def := WorkflowDef{
		Name: "branched", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID:        "happy",
				Task:      "task-happy",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  false,
				},
			},
			{
				ID:        "err",
				Task:      "task-err",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  true,
				},
			},
			{
				ID: "r-ok", Type: StepTypeRespond,
				DependsOn: []string{"happy"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r-err", Type: StepTypeRespond,
				DependsOn: []string{"err"},
				Config:    mustMarshal(t, RespondConfig{Status: 500}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 0 {
		t.Fatalf(
			"branch-per-outcome should produce no warnings: %v", got,
		)
	}
}

func TestValidateRespondHTTPTriggerNoSteps(t *testing.T) {
	def := WorkflowDef{
		Name: "empty", Version: "1",
		Steps: []StepDef{},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 1 {
		t.Fatalf("want 1 warning, got %d: %v", len(got), got)
	}
	if got[0].Kind != WarnMissingRespond {
		t.Fatalf("want WarnMissingRespond, got %q", got[0].Kind)
	}
}

func TestValidateRespondHTTPTriggerOneOfThreeRespondsBranched(t *testing.T) {
	// Two responds gated by mutually-exclusive SkipIf, plus a third
	// respond that's reachable on every path. The bare respond fires
	// simultaneously with whichever branch wins → duplicate_respond.
	def := WorkflowDef{
		Name: "mixed", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID:        "happy",
				Task:      "task-happy",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  false,
				},
			},
			{
				ID:        "err",
				Task:      "task-err",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  true,
				},
			},
			{
				ID: "r-ok", Type: StepTypeRespond,
				DependsOn: []string{"happy"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r-err", Type: StepTypeRespond,
				DependsOn: []string{"err"},
				Config:    mustMarshal(t, RespondConfig{Status: 500}),
			},
			{
				ID: "r-other", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 202}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 1 {
		t.Fatalf("want 1 warning, got %d: %v", len(got), got)
	}
	if got[0].Kind != WarnDuplicateRespond {
		t.Fatalf("want WarnDuplicateRespond, got %q", got[0].Kind)
	}
}
