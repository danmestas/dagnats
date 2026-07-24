// e2e/harness/respond_timeout.go
// Topology-aware HTTP-respond timeout budget (#574). The synchronous
// HTTP-respond round-trip (POST -> orchestrator dispatch -> respond
// step -> reply) completes in well under a second on a healthy system
// (measured p99 < 650ms even under concurrent load). Its client-side
// budget therefore only ever binds under pathological contention, and
// the budget that is safe there differs by topology — so it lives here,
// keyed off the topology RunE2E is currently exercising.
package harness

import (
	"strings"
	"testing"
)

// baseRespondTimeoutMs is the per-request HTTP-respond budget for the
// embedded and local_cluster topologies. Both complete a respond
// round-trip in well under a second, so 10s is ~15x headroom and still
// tight enough to catch a genuine slow-path regression.
const baseRespondTimeoutMs = 10_000

// superclusterRespondTimeoutMs is the per-request budget for the
// supercluster topology. Under full-suite `-p 4` oversubscription a
// CPU-starved JetStream op in the orchestrator's dispatch path can time
// out; the orchestrator then NAKs the WORKFLOW_HISTORY event with
// historyRedeliverSchedule[0]=5s and retries. One such NAK adds ~5s and
// a second ~10s to a round-trip that is otherwise <1s, so the base 10s
// budget tolerates one redelivery retry but not two and intermittently
// returns 504 workflow_timeout (#574). supercluster is hit specifically
// because its cross-cluster gateway + leaf replication makes each
// JetStream op slower, and thus likelier to breach a client timeout
// when starved, than the embedded/local_cluster round-trip. 30s is ~2x
// the two-NAK worst case (base <1s + 5s + 10s ≈ 16s) and matches the
// load-heavy respond budget already used in http_concurrency_test.go.
// This sizes only the test's client timeout to the topology's realistic
// worst-case dispatch latency — it does NOT change orchestrator or
// topology behavior.
const superclusterRespondTimeoutMs = 30_000

// RespondTimeoutMs returns the per-request HTTP-respond timeout budget
// (milliseconds) an e2e test should configure for the topology it is
// currently running under. RunE2E names each subtest topo.Name(), so
// the topology is recoverable from t.Name(); a caller outside a RunE2E
// topology subtest gets the base budget.
func RespondTimeoutMs(t *testing.T) int {
	if t == nil {
		panic("RespondTimeoutMs: t must not be nil")
	}
	name := t.Name()
	if name == "" {
		panic("RespondTimeoutMs: t.Name() must not be empty")
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "supercluster" {
			return superclusterRespondTimeoutMs
		}
	}
	return baseRespondTimeoutMs
}
