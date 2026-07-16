package engine

import "slices"

// controlPlaneCapability is the capability token that, when present on a
// step, asks the worker to hand the task a ControlPlane handle. The grant
// policy decides whether that request is honored. Mirrors the worker-side
// constant of the same value (worker.controlPlaneCapability); kept local so
// the engine does not import the worker package.
const controlPlaneCapability = "control-plane"

// GrantPolicy is a deny-by-default authorization policy for the control
// plane. It answers two questions: may a workflow receive a ControlPlane
// handle at all (GrantsControlPlane), and may it promote a runtime-defined
// workflow to the durable catalog (AllowsPromote). The zero value is not
// usable; construct with NewGrantPolicy. A nil *GrantPolicy denies
// everything — there is no "policy not loaded" error path, because the safe
// default is to grant nothing.
type GrantPolicy struct {
	grant   map[string]struct{}
	promote map[string]struct{}
}

// NewGrantPolicy builds a policy from the granted and promote-allowed
// workflow-name lists. Membership is exact-match on workflow name. The
// caller (config validation) has already bounded and de-duplicated the
// lists; this constructor copies them into sets.
func NewGrantPolicy(grant, promote []string) *GrantPolicy {
	p := &GrantPolicy{
		grant:   make(map[string]struct{}, len(grant)),
		promote: make(map[string]struct{}, len(promote)),
	}
	for _, name := range grant {
		p.grant[name] = struct{}{}
	}
	for _, name := range promote {
		p.promote[name] = struct{}{}
	}
	return p
}

// GrantsControlPlane reports whether the named workflow may receive a
// ControlPlane handle. A nil policy denies (deny-by-default).
func (p *GrantPolicy) GrantsControlPlane(workflowName string) bool {
	if p == nil {
		return false
	}
	// An empty name can never be granted: workflow names are non-empty by
	// construction, so an empty argument is a caller bug or an unnamed step —
	// fail closed rather than risk an empty-string grant entry matching.
	if workflowName == "" {
		return false
	}
	_, ok := p.grant[workflowName]
	return ok
}

// AllowsPromote reports whether the named workflow may promote a
// runtime-defined workflow to the durable catalog. A nil policy denies.
func (p *GrantPolicy) AllowsPromote(workflowName string) bool {
	if p == nil {
		return false
	}
	// Empty name fails closed — see GrantsControlPlane.
	if workflowName == "" {
		return false
	}
	_, ok := p.promote[workflowName]
	return ok
}

// effectiveCapabilities returns the capabilities a step should actually be
// dispatched with, given the grant policy. If the step does not declare the
// control-plane capability, caps is returned unchanged. If it does declare
// it but the workflow is not granted, a NEW slice with control-plane removed
// is returned — the shared input slice is never mutated. This is the
// deny-by-default gate at the payload source: an ungranted step's task
// message never carries the control-plane capability, so the unchanged
// worker gate withholds the handle.
func effectiveCapabilities(
	caps []string, workflowName string, p *GrantPolicy,
) []string {
	if !slices.Contains(caps, controlPlaneCapability) {
		return caps
	}
	if p.GrantsControlPlane(workflowName) {
		return caps
	}
	stripped := make([]string, 0, len(caps))
	for _, capability := range caps {
		if capability == controlPlaneCapability {
			continue
		}
		stripped = append(stripped, capability)
	}
	return stripped
}

// stripControlPlaneCapability removes "control-plane" from caps
// unconditionally -- no GrantPolicy, no workflow name. #513: map instances
// must categorically never hold control-plane (#380), so the deny is
// expressed directly at the data level (capability absent from the step)
// rather than as a side effect of passing an empty workflow name into
// effectiveCapabilities. Never mutates caps. Returns caps unchanged
// (same underlying array, no allocation) when control-plane is already
// absent -- mirrors effectiveCapabilities's own short-circuit so the common
// no-capabilities step pays nothing on the hot path.
func stripControlPlaneCapability(caps []string) []string {
	if !slices.Contains(caps, controlPlaneCapability) {
		return caps
	}
	stripped := make([]string, 0, len(caps))
	for _, capability := range caps {
		if capability == controlPlaneCapability {
			continue
		}
		stripped = append(stripped, capability)
	}
	return stripped
}
