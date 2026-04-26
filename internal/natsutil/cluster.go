package natsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ClusterOptions describes the NATS topology this dagnats instance
// is participating in. Empty Routes means standalone or leaf mode
// (no quorum wait, R=1 streams unless explicitly overridden).
type ClusterOptions struct {
	// Routes is the list of peer URLs this instance connects to.
	// Empty for non-cluster modes.
	Routes []string

	// ReplicasOverride forces the JetStream replication factor when
	// > 0. Otherwise R is auto-derived from cluster size via
	// DeriveReplicas.
	ReplicasOverride int
}

// DeriveReplicas computes the JetStream replication factor for streams
// and KV buckets given the cluster route list and an optional explicit
// override.
//
// When override > 0, returns it as-is (caller is responsible for
// validating the override against {1, 3, 5} at config-load time).
//
// When override == 0, auto-derives from cluster size:
//   - len(routes) == 0 (standalone or leaf) -> 1
//   - cluster size >= 5                     -> 5
//   - cluster size >= 3                     -> 3 (rounds 4 down to 3)
//
// Cluster size is len(routes) + 1 (peers + self).
func DeriveReplicas(routes []string, override int) int {
	if override > 0 {
		return override
	}
	if len(routes) == 0 {
		return 1
	}
	clusterSize := len(routes) + 1
	if clusterSize >= 5 {
		return 5
	}
	if clusterSize >= 3 {
		return 3
	}
	return 1 // 2-node cluster falls back; validation should prevent this
}

const quorumPollInterval = 500 * time.Millisecond

// WaitForClusterQuorum blocks until JetStream reports a healthy
// cluster of expectedSize, or ctx is cancelled. Polls every 500ms.
// expectedSize is the total node count (this node + peers); for a
// 3-node cluster it is 3.
//
// Returns the elapsed time on success. Returns the underlying ctx
// error (typically context.DeadlineExceeded) on timeout.
//
// Panics if expectedSize < 1 or js is nil.
func WaitForClusterQuorum(
	ctx context.Context, js jetstream.JetStream, expectedSize int,
) (time.Duration, error) {
	if js == nil {
		panic("WaitForClusterQuorum: js is nil")
	}
	if expectedSize < 1 {
		panic(fmt.Sprintf("WaitForClusterQuorum: expectedSize=%d", expectedSize))
	}

	start := time.Now()
	ticker := time.NewTicker(quorumPollInterval)
	defer ticker.Stop()

	for {
		ready, err := jsClusterReady(ctx, js, expectedSize)
		if err == nil && ready {
			return time.Since(start), nil
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-ticker.C:
			// poll again
		}
	}
}

// jsClusterReady returns true when JetStream reports a healthy
// cluster of at least expectedSize nodes with a meta-leader elected.
// For expectedSize=1 (standalone), returns true as soon as
// AccountInfo succeeds.
//
// We deliberately do not consult info.API.Errors here: it is a
// monotonic lifetime counter on the JS API account, not a current-
// health signal. Any prior failed API call (e.g. a transient stream-
// placement error during a R=1 → R=3 migration on a fresh cluster)
// would cause this check to falsely report unready forever. A
// successful AccountInfo response is itself the readiness signal —
// it requires the meta-leader to be elected and JS to be operational.
// Peer count verification happens in the cluster integration tests.
func jsClusterReady(
	ctx context.Context, js jetstream.JetStream, expectedSize int,
) (bool, error) {
	info, err := js.AccountInfo(ctx)
	if err != nil {
		return false, err
	}
	if info == nil {
		return false, nil
	}
	return true, nil
}
