// dag/waitforevent_test.go

// Tests for WaitForEvent: Match and ResolvedMatch evaluation, resolution,
// and validation. Methodology: unit tests with synthetic JSON data for
// match evaluation and resolution against step outputs and workflow input.
package dag

import (
	"strings"
	"testing"
	"time"
)

func TestResolvedMatchEvaluate(t *testing.T) {
	match := ResolvedMatch{
		Left:  "data.status",
		Op:    MatchOpEq,
		Right: "ready",
	}

	eventData := []byte(`{"data":{"status":"ready"}}`)
	result, err := match.Evaluate(eventData)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	// Positive: match succeeds when values are equal
	if !result {
		t.Fatal("expected match to succeed, got false")
	}

	// Negative: match fails when values differ
	eventData2 := []byte(`{"data":{"status":"pending"}}`)
	result2, err := match.Evaluate(eventData2)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if result2 {
		t.Fatal("expected match to fail, got true")
	}
}

func TestResolvedMatchEvaluateMissingField(t *testing.T) {
	match := ResolvedMatch{
		Left:  "data.missing",
		Op:    MatchOpEq,
		Right: "value",
	}

	eventData := []byte(`{"data":{"status":"ready"}}`)
	result, err := match.Evaluate(eventData)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	// Positive: missing field returns false (no match)
	if result {
		t.Fatal("expected no match for missing field, got true")
	}

	// Negative: verify it's not an error, just false
	if err != nil {
		t.Fatalf("expected no error for missing field, got: %v", err)
	}
}

func TestMatchResolveFromStepOutput(t *testing.T) {
	match := Match{
		Left:  "event.data.X",
		Op:    MatchOpEq,
		Right: "step.prev.output.result",
	}

	stepOutputs := map[string][]byte{
		"prev": []byte(`{"result":"success"}`),
	}
	workflowInput := []byte(`{}`)

	resolved, err := match.Resolve(stepOutputs, workflowInput)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Positive: resolved Right is extracted value
	if resolved.Right != "success" {
		t.Fatalf("resolved.Right = %v, want success", resolved.Right)
	}

	// Positive: Left and Op are copied unchanged
	if resolved.Left != "event.data.X" {
		t.Fatalf("resolved.Left = %v, want event.data.X", resolved.Left)
	}
}

func TestMatchResolveFromInput(t *testing.T) {
	match := Match{
		Left:  "event.data.X",
		Op:    MatchOpEq,
		Right: "input.expected_value",
	}

	stepOutputs := map[string][]byte{}
	workflowInput := []byte(`{"expected_value":"foo"}`)

	resolved, err := match.Resolve(stepOutputs, workflowInput)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Positive: resolved Right is input value
	if resolved.Right != "foo" {
		t.Fatalf("resolved.Right = %v, want foo", resolved.Right)
	}

	// Negative: step outputs were not used
	if len(stepOutputs) > 0 && resolved.Right == "success" {
		t.Fatal("should not have used step outputs")
	}
}

func TestMatchResolveMissingStep(t *testing.T) {
	match := Match{
		Left:  "event.data.X",
		Op:    MatchOpEq,
		Right: "step.missing.output.field",
	}

	stepOutputs := map[string][]byte{}
	workflowInput := []byte(`{}`)

	_, err := match.Resolve(stepOutputs, workflowInput)
	// Positive: error for missing step
	if err == nil {
		t.Fatal("expected error for missing step, got nil")
	}
	// Negative: error message mentions the step
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should mention missing step: %v", err)
	}
}

func TestValidateWaitForEventMissingFields(t *testing.T) {
	ids := map[string]bool{"a": true}

	// Missing Event
	step := StepDef{
		ID:   "wait",
		Type: StepTypeWaitForEvent,
		WaitForEvent: &WaitForEventOpts{
			Event:   "",
			Match:   Match{Left: "x", Op: MatchOpEq, Right: "input.y"},
			Timeout: time.Second,
		},
	}
	err := validateWaitForEventStep(step, ids)
	if err == nil {
		t.Fatal("expected error for missing Event, got nil")
	}

	// Missing Match.Left
	step.WaitForEvent.Event = "test.event"
	step.WaitForEvent.Match.Left = ""
	err = validateWaitForEventStep(step, ids)
	if err == nil {
		t.Fatal("expected error for missing Match.Left, got nil")
	}

	// Missing Match.Op
	step.WaitForEvent.Match.Left = "event.data.X"
	step.WaitForEvent.Match.Op = ""
	err = validateWaitForEventStep(step, ids)
	if err == nil {
		t.Fatal("expected error for missing Match.Op, got nil")
	}
}

func TestValidateWaitForEventInvalidStepRef(t *testing.T) {
	ids := map[string]bool{"a": true}

	step := StepDef{
		ID:   "wait",
		Type: StepTypeWaitForEvent,
		WaitForEvent: &WaitForEventOpts{
			Event: "test.event",
			Match: Match{
				Left:  "event.data.X",
				Op:    MatchOpEq,
				Right: "step.missing.output.Y",
			},
			Timeout: time.Second,
		},
	}

	err := validateWaitForEventStep(step, ids)
	// Positive: error for missing step reference
	if err == nil {
		t.Fatal("expected error for invalid step reference, got nil")
	}
	// Negative: error mentions the missing step
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should mention missing step: %v", err)
	}
}

func TestValidateWaitForEventValidStep(t *testing.T) {
	ids := map[string]bool{"prev": true}

	step := StepDef{
		ID:   "wait",
		Type: StepTypeWaitForEvent,
		WaitForEvent: &WaitForEventOpts{
			Event: "test.event",
			Match: Match{
				Left:  "event.data.X",
				Op:    MatchOpEq,
				Right: "step.prev.output.Y",
			},
			Timeout: 5 * time.Second,
		},
	}

	err := validateWaitForEventStep(step, ids)
	// Positive: valid config passes
	if err != nil {
		t.Fatalf("expected no error for valid step, got: %v", err)
	}

	// Negative: try with input path
	step.WaitForEvent.Match.Right = "input.Z"
	err = validateWaitForEventStep(step, ids)
	if err != nil {
		t.Fatalf("expected no error for input path, got: %v", err)
	}
}

func TestResolvedMatchEvaluateEmptyLeftPanics(t *testing.T) {
	match := ResolvedMatch{Left: "", Op: MatchOpEq, Right: "x"}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty Left")
		}
	}()
	_, _ = match.Evaluate([]byte(`{}`))
}

func TestResolvedMatchEvaluateEmptyOpPanics(t *testing.T) {
	match := ResolvedMatch{Left: "x", Op: "", Right: "y"}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty Op")
		}
	}()
	_, _ = match.Evaluate([]byte(`{}`))
}

func TestMatchResolveEmptyRightPanics(t *testing.T) {
	match := Match{Left: "x", Op: MatchOpEq, Right: ""}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty Right")
		}
	}()
	_, _ = match.Resolve(map[string][]byte{}, []byte(`{}`))
}
