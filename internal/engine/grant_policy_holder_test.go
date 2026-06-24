// Methodology: pure unit tests for the hot-reloadable grant-policy holder.
// An unset holder Loads nil (deny-by-default); Store swaps the active policy
// atomically so a live config reload flips the grant with no new machinery.
package engine

import "testing"

func TestGrantPolicyHolderUnsetLoadsNil(t *testing.T) {
	var h GrantPolicyHolder

	if h.Load() != nil {
		t.Fatal("unset holder must Load nil (deny-by-default)")
	}
	// Negative space: a nil-loaded policy denies everything.
	if h.Load().GrantsControlPlane("planner") {
		t.Fatal("nil-loaded policy must deny control-plane")
	}
}

func TestGrantPolicyHolderStoreSwaps(t *testing.T) {
	var h GrantPolicyHolder
	h.Store(NewGrantPolicy([]string{"planner"}, nil))

	if !h.Load().GrantsControlPlane("planner") {
		t.Fatal("after Store, planner must be granted")
	}
	// Swap to an empty policy: grant flips off live.
	h.Store(NewGrantPolicy(nil, nil))
	if h.Load().GrantsControlPlane("planner") {
		t.Fatal("after re-Store with empty grant, planner must be denied")
	}
}
