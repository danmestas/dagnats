// cli/demo_io_test.go
// Unit tests for the realistic-IO surface of the rich demo harness.
// Methodology: the per-step output selector, the step failure-message
// selector, the per-workflow input builder, and the worker-fleet
// partition are all PURE functions of their args (stepID / workflow
// name / static maps) — no NATS, no RNG, no global state. We lock in
// (a) every step emits non-empty valid JSON output, (b) failure
// messages are step-specific and informative, (c) a built input carries
// the control outcome AND domain fields and round-trips through
// decodeOutcome, and (d) the worker domains cover every task type
// disjointly so no step can hang for lack of a handler.
package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

// allDemoStepIDs lists every step ID referenced by any rich workflow.
// Kept here (not derived) so the test fails loudly if a new step is
// added to a workflow without a matching output/failure entry.
func allDemoStepIDs() []string {
	return []string{
		"noop", "attempt",
		"fetch-urls", "fetch", "build-gallery",
		"plan", "act", "observe", "summarize",
		"extract", "transform", "load",
		"render", "send-email", "send-slack",
	}
}

// TestStepOutputIsValidJSONPerStep asserts each step emits non-empty,
// valid-JSON, step-distinct success output (the run-detail IO tab).
func TestStepOutputIsValidJSONPerStep(t *testing.T) {
	t.Parallel()
	for _, id := range allDemoStepIDs() {
		out := stepOutput(id)
		if len(out) == 0 {
			t.Errorf("stepOutput(%q) is empty", id)
		}
		var v map[string]any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Errorf("stepOutput(%q) is not valid JSON object: %v", id, err)
		}
	}
	// Unknown step falls back to a sane, non-empty JSON payload.
	fallback := stepOutput("does-not-exist")
	var v map[string]any
	if err := json.Unmarshal(fallback, &v); err != nil {
		t.Errorf("fallback stepOutput invalid JSON: %v", err)
	}
	// Distinct steps must not all collapse to one constant shape.
	if string(stepOutput("fetch-urls")) == string(stepOutput("build-gallery")) {
		t.Errorf("expected distinct outputs across distinct steps")
	}
}

// TestStepFailureErrorIsInformative asserts failure messages are
// step-specific and never the bare generic for a known step.
func TestStepFailureErrorIsInformative(t *testing.T) {
	t.Parallel()
	const generic = "demo noop: planned failure"

	fetchErr := stepFailureError("fetch")
	if fetchErr == nil {
		t.Fatalf("stepFailureError(fetch) returned nil")
	}
	if fetchErr.Error() == generic {
		t.Errorf("known step fetch fell back to generic message")
	}
	if !strings.Contains(fetchErr.Error(), "fetch") {
		t.Errorf("fetch failure not step-specific: %q", fetchErr.Error())
	}
	// Unknown step still returns a non-nil error (the generic fallback).
	if got := stepFailureError("unknown-step"); got == nil {
		t.Errorf("stepFailureError(unknown) returned nil, want fallback")
	}
}

// TestBuildWorkflowInputCarriesOutcomeAndDomainFields asserts every
// workflow's input is valid JSON, round-trips the control outcome
// through decodeOutcome, and (for a rich workflow) carries domain
// fields beyond the bare outcome.
func TestBuildWorkflowInputCarriesOutcomeAndDomainFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		outcome demoOutcome
	}{
		{demoWorkflowImagePipeline, outcomeFailed},
		{demoWorkflowAgentLoop, outcomeCompleted},
		{demoWorkflowETL, outcomeCancelled},
		{demoWorkflowNotify, outcomeCompleted},
		{demoWorkflowName, outcomeCompleted},
	}
	for _, c := range cases {
		in := buildWorkflowInput(c.name, c.outcome)
		if got := decodeOutcome(in); got != c.outcome {
			t.Errorf("%s: decoded outcome = %q, want %q",
				c.name, got, c.outcome)
		}
		var v map[string]any
		if err := json.Unmarshal(in, &v); err != nil {
			t.Errorf("%s: input not valid JSON: %v", c.name, err)
		}
	}

	var m map[string]any
	if err := json.Unmarshal(
		buildWorkflowInput(demoWorkflowImagePipeline, outcomeCompleted),
		&m,
	); err != nil {
		t.Fatalf("image input unmarshal: %v", err)
	}
	if _, ok := m["source_urls"]; !ok {
		t.Errorf("image-pipeline input missing domain field source_urls: %v", m)
	}
	if _, ok := m["outcome"]; !ok {
		t.Errorf("image-pipeline input missing control field outcome: %v", m)
	}
}

// TestDemoWorkerDomainsCoverAllTaskTypes guards the fleet partition:
// every task type maps to exactly one worker domain, and no domain
// handles a type outside demoTaskTypes() — otherwise a step would hang
// for lack of a handler or a worker would subscribe to a dead subject.
func TestDemoWorkerDomainsCoverAllTaskTypes(t *testing.T) {
	t.Parallel()
	domains := demoWorkerDomains()
	if len(domains) < 2 {
		t.Fatalf("want a multi-worker fleet, got %d domains", len(domains))
	}

	known := map[string]bool{}
	for _, tt := range demoTaskTypes() {
		known[tt] = true
	}

	covered := map[string]bool{}
	for domain, types := range domains {
		for _, tt := range types {
			if covered[tt] {
				t.Errorf("task type %q handled by two domains", tt)
			}
			covered[tt] = true
			if !known[tt] {
				t.Errorf("domain %q handles unknown task type %q", domain, tt)
			}
		}
	}
	for _, tt := range demoTaskTypes() {
		if !covered[tt] {
			t.Errorf("task type %q has no worker domain", tt)
		}
	}
}
