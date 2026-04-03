// e2e/harness/harness.go
// RunE2E runs a test function against all enabled topologies.
// Topology selection via E2E_TOPOLOGY env var: "embedded",
// "local_cluster", "supercluster", or empty for all.
package harness

import (
	"os"
	"testing"

	"github.com/nats-io/nats.go"
)

// E2ETest is the signature for topology-agnostic E2E tests.
type E2ETest func(t *testing.T, nc *nats.Conn)

// RunE2E runs the test function against each enabled topology.
// Panics if no topologies are enabled (e.g., typo in E2E_TOPOLOGY).
func RunE2E(t *testing.T, test E2ETest) {
	t.Helper()
	topos := enabledTopologies(t)
	if len(topos) == 0 {
		t.Fatal(
			"RunE2E: no topologies enabled — check E2E_TOPOLOGY",
		)
	}
	for _, topo := range topos {
		t.Run(topo.Name(), func(t *testing.T) {
			nc := topo.Connect(t)
			topo.Setup(t, nc)
			test(t, nc)
		})
	}
}

// enabledTopologies returns the topologies selected by E2E_TOPOLOGY.
// Empty string means all topologies. Panics on unrecognized values.
func enabledTopologies(t *testing.T) []Topology {
	t.Helper()
	env := os.Getenv("E2E_TOPOLOGY")

	all := []Topology{
		NewEmbedded(),
		NewLocalCluster(),
		NewSupercluster(),
	}

	if env == "" {
		return all
	}

	valid := map[string]bool{
		"embedded":      true,
		"local_cluster": true,
		"supercluster":  true,
	}
	if !valid[env] {
		t.Fatalf(
			"enabledTopologies: unknown E2E_TOPOLOGY=%q "+
				"(valid: embedded, local_cluster, supercluster)",
			env,
		)
	}

	for _, topo := range all {
		if topo.Name() == env {
			return []Topology{topo}
		}
	}
	return nil
}

