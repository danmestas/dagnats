// dag/approval_test.go
// Tests for ApprovalConfig parsing, validation, and builder integration.
// Methodology: unit tests for config round-trip, validation boundary
// conditions, and builder panic guards. No NATS dependency.
package dag

import (
	"testing"
	"time"
)

func TestParseApprovalConfig_Valid(t *testing.T) {
	step := StepDef{
		ID:   "gate",
		Type: StepTypeApproval,
		Config: MarshalConfig(&ApprovalConfig{
			Timeout: 30 * time.Minute,
			Subject: "approvals.deploy",
		}),
	}
	cfg, err := ParseApprovalConfig(step)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Positive: fields round-trip correctly.
	if cfg.Timeout != 30*time.Minute {
		t.Fatalf("expected 30m timeout, got %v", cfg.Timeout)
	}
	if cfg.Subject != "approvals.deploy" {
		t.Fatalf(
			"expected subject 'approvals.deploy', got %q",
			cfg.Subject,
		)
	}
}

func TestParseApprovalConfig_WrongType(t *testing.T) {
	step := StepDef{
		ID:   "gate",
		Type: StepTypeNormal,
		Config: MarshalConfig(&ApprovalConfig{
			Timeout: time.Minute,
			Subject: "test",
		}),
	}
	_, err := ParseApprovalConfig(step)

	// Positive: error returned for wrong type.
	if err == nil {
		t.Fatal("expected error for wrong step type")
	}
	// Negative: should not succeed silently.
}

func TestParseApprovalConfig_NilConfig(t *testing.T) {
	step := StepDef{
		ID:   "gate",
		Type: StepTypeApproval,
	}
	_, err := ParseApprovalConfig(step)

	// Positive: error returned for nil config.
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestValidateApprovalStep_Valid(t *testing.T) {
	b := NewWorkflow("test-approval")
	b.Approval("gate", ApprovalConfig{
		Timeout: time.Hour,
		Subject: "approvals.test",
	})
	_, err := b.Build()

	// Positive: valid approval config passes validation.
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateApprovalStep_EmptySubject(t *testing.T) {
	step := StepDef{
		ID:   "gate",
		Type: StepTypeApproval,
		Config: MarshalConfig(&ApprovalConfig{
			Timeout: time.Hour,
			Subject: "",
		}),
	}
	err := validateApprovalStep(step)

	// Positive: error for empty subject.
	if err == nil {
		t.Fatal("expected error for empty subject")
	}
	// Negative: non-approval steps should pass.
	normalStep := StepDef{ID: "x", Type: StepTypeNormal}
	if validateApprovalStep(normalStep) != nil {
		t.Fatal("non-approval step should pass validation")
	}
}

func TestValidateApprovalStep_TimeoutBounds(t *testing.T) {
	// Zero timeout.
	step := StepDef{
		ID:   "gate",
		Type: StepTypeApproval,
		Config: MarshalConfig(&ApprovalConfig{
			Timeout: 0,
			Subject: "test",
		}),
	}
	err := validateApprovalStep(step)
	if err == nil {
		t.Fatal("expected error for zero timeout")
	}

	// Exceeds max (> 168h).
	step.Config = MarshalConfig(&ApprovalConfig{
		Timeout: 169 * time.Hour,
		Subject: "test",
	})
	err = validateApprovalStep(step)
	if err == nil {
		t.Fatal("expected error for timeout exceeding max")
	}

	// Exactly at max — should pass.
	step.Config = MarshalConfig(&ApprovalConfig{
		Timeout: 168 * time.Hour,
		Subject: "test",
	})
	err = validateApprovalStep(step)
	if err != nil {
		t.Fatalf("expected no error at max timeout: %v", err)
	}
}

func TestBuilderApproval_PanicsOnEmpty(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panic for empty id.
		if r == nil {
			t.Fatal("expected panic for empty id")
		}
	}()
	b := NewWorkflow("test")
	b.Approval("", ApprovalConfig{Timeout: time.Hour})
}

func TestBuilderApproval_PanicsOnNegativeTimeout(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panic for non-positive timeout.
		if r == nil {
			t.Fatal("expected panic for non-positive timeout")
		}
	}()
	b := NewWorkflow("test")
	b.Approval("gate", ApprovalConfig{Timeout: -time.Second})
}
