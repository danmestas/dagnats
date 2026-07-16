// Methodology: pure unit tests for the capability-grant policy. No NATS.
// Exercises deny-by-default (nil policy denies everything), grant/promote
// set membership, and the pure effectiveCapabilities strip — including the
// invariant that the shared input slice is never mutated.
package engine

import (
	"slices"
	"testing"
)

func TestGrantPolicyGrantsControlPlane(t *testing.T) {
	p := NewGrantPolicy([]string{"planner", "supervisor"}, []string{"supervisor"})

	if !p.GrantsControlPlane("planner") {
		t.Fatal("planner should be granted control-plane")
	}
	if p.GrantsControlPlane("worker") {
		t.Fatal("ungranted workflow must not be granted control-plane")
	}
}

func TestGrantPolicyAllowsPromote(t *testing.T) {
	p := NewGrantPolicy([]string{"planner", "supervisor"}, []string{"supervisor"})

	if !p.AllowsPromote("supervisor") {
		t.Fatal("supervisor should be allowed to promote")
	}
	// planner is granted control-plane but NOT in the promote list.
	if p.AllowsPromote("planner") {
		t.Fatal("planner is not in promote list and must be denied promote")
	}
}

func TestNilGrantPolicyDeniesEverything(t *testing.T) {
	var p *GrantPolicy // nil: deny-by-default, no error path.

	if p.GrantsControlPlane("planner") {
		t.Fatal("nil policy must deny control-plane grant")
	}
	if p.AllowsPromote("supervisor") {
		t.Fatal("nil policy must deny promote")
	}
}

func TestEffectiveCapabilitiesStripsWhenUngranted(t *testing.T) {
	p := NewGrantPolicy([]string{"planner"}, nil)
	input := []string{"control-plane", "gpu"}

	got := effectiveCapabilities(input, "worker", p)

	if slices.Contains(got, "control-plane") {
		t.Fatalf("control-plane must be stripped for ungranted workflow, got %v", got)
	}
	if !slices.Contains(got, "gpu") {
		t.Fatalf("non-control-plane caps must be preserved, got %v", got)
	}
	// Negative space: the shared input slice must NOT be mutated.
	if !slices.Equal(input, []string{"control-plane", "gpu"}) {
		t.Fatalf("input slice was mutated: %v", input)
	}
}

func TestEffectiveCapabilitiesPreservesWhenGranted(t *testing.T) {
	p := NewGrantPolicy([]string{"planner"}, nil)
	input := []string{"control-plane", "gpu"}

	got := effectiveCapabilities(input, "planner", p)

	if !slices.Contains(got, "control-plane") {
		t.Fatalf("granted workflow must keep control-plane, got %v", got)
	}
	if len(got) != 2 || got[0] != "control-plane" || got[1] != "gpu" {
		t.Fatalf("order must be preserved, got %v", got)
	}
}

func TestEffectiveCapabilitiesNilPolicyStrips(t *testing.T) {
	input := []string{"control-plane", "gpu"}

	got := effectiveCapabilities(input, "planner", nil)

	if slices.Contains(got, "control-plane") {
		t.Fatalf("nil policy must strip control-plane, got %v", got)
	}
	if !slices.Contains(got, "gpu") {
		t.Fatalf("nil policy must preserve other caps, got %v", got)
	}
}

func TestEffectiveCapabilitiesUnchangedWhenNoControlPlane(t *testing.T) {
	p := NewGrantPolicy(nil, nil)
	input := []string{"gpu", "ssd"}

	got := effectiveCapabilities(input, "worker", p)

	if !slices.Equal(got, input) {
		t.Fatalf("caps without control-plane must be returned unchanged, got %v", got)
	}
}

func TestStripControlPlaneCapabilityRemovesItUnconditionally(t *testing.T) {
	// #513: the strip is unconditional -- no GrantPolicy involved at all,
	// proving the deny is categorical (data-level), not policy-conditional.
	input := []string{"control-plane", "gpu"}

	got := stripControlPlaneCapability(input)

	// Positive: control-plane capability is gone.
	if slices.Contains(got, "control-plane") {
		t.Fatalf("control-plane must be stripped unconditionally, got %v", got)
	}
	// Negative space: unrelated capabilities survive untouched.
	if !slices.Contains(got, "gpu") {
		t.Fatalf("non-control-plane caps must be preserved, got %v", got)
	}
}

func TestStripControlPlaneCapabilityDoesNotMutateInput(t *testing.T) {
	input := []string{"control-plane", "gpu"}
	before := slices.Clone(input)

	_ = stripControlPlaneCapability(input)

	// Positive + negative: the shared input slice is unchanged after the call.
	if !slices.Equal(input, before) {
		t.Fatalf("input slice was mutated: got %v, want %v", input, before)
	}
}

func TestStripControlPlaneCapabilityNoOpWhenAbsent(t *testing.T) {
	input := []string{"gpu"}

	got := stripControlPlaneCapability(input)

	// Positive: pass-through value equality.
	if !slices.Equal(got, input) {
		t.Fatalf("no-op strip must return an equal slice, got %v, want %v", got, input)
	}
	// Nice-to-have (not a hard gate): same underlying array, no reallocation,
	// on the hot no-capabilities-changed path. Only checked for non-empty input.
	if len(input) > 0 && &input[0] != &got[0] {
		t.Logf("stripControlPlaneCapability reallocated on a no-op path (not fatal)")
	}
}
