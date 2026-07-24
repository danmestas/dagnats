// e2e/harness/respond_timeout_test.go
// Methodology: pure unit test for RespondTimeoutMs, the topology-aware
// HTTP-respond timeout budget helper (#574). RunE2E names each subtest
// topo.Name(), so the helper recovers the topology from t.Name(); these
// tests drive it through t.Run subtests named like the real topologies
// and assert the supercluster subtest gets the larger budget while the
// faster topologies (and any non-topology caller) get the base budget.
package harness

import "testing"

func TestRespondTimeoutMs_SuperclusterGetsLargerBudget(t *testing.T) {
	t.Run("supercluster", func(t *testing.T) {
		got := RespondTimeoutMs(t)
		if got != superclusterRespondTimeoutMs {
			t.Fatalf(
				"RespondTimeoutMs under supercluster = %d, want %d",
				got, superclusterRespondTimeoutMs,
			)
		}
		// Negative space: the supercluster budget must be strictly
		// larger than the base, or the helper isn't buying any headroom.
		if got <= baseRespondTimeoutMs {
			t.Fatalf(
				"supercluster budget %d must exceed base %d",
				got, baseRespondTimeoutMs,
			)
		}
	})
}

func TestRespondTimeoutMs_FastTopologiesGetBaseBudget(t *testing.T) {
	for _, name := range []string{"embedded", "local_cluster"} {
		t.Run(name, func(t *testing.T) {
			if got := RespondTimeoutMs(t); got != baseRespondTimeoutMs {
				t.Fatalf(
					"RespondTimeoutMs under %s = %d, want base %d",
					name, got, baseRespondTimeoutMs,
				)
			}
		})
	}
	// Negative space: a caller outside any RunE2E topology subtest
	// (t.Name() has no topology segment) also gets the base budget.
	if got := RespondTimeoutMs(t); got != baseRespondTimeoutMs {
		t.Fatalf(
			"RespondTimeoutMs outside a topology subtest = %d, "+
				"want base %d",
			got, baseRespondTimeoutMs,
		)
	}
}
