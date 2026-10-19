package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
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

// ReplicaProbeStreamPrefix names the throwaway streams detectReplicas
// creates and deletes. Exported so tests can assert none is left behind.
const ReplicaProbeStreamPrefix = "DAGNATS_REPLICA_PROBE_"

// replicaProbeAttemptsMax bounds how often pickReplicas re-asks for three
// replicas. With the provisioning backoff that is about 40s of waiting.
const replicaProbeAttemptsMax = 12

// replicaProbeMaxBytes keeps a probe's store reservation negligible.
const replicaProbeMaxBytes = 64 << 10

// detectReplicas derives the replication factor from the JetStream the
// connection is attached to, for a process that has no cluster routes of
// its own to count (dagnats-engine, dagnats-api). A server outside any
// cluster gets 1 without a round trip. In a cluster, the meta leader is
// the only authority on how many peers can hold a replica, and no
// non-system API reports that count, so this asks it directly: a
// throwaway stream is created at five, then at three (see pickReplicas
// for why three is retried), and deleted once placed.
func detectReplicas(ctx context.Context, nc *nats.Conn) (int, error) {
	if ctx == nil {
		panic("detectReplicas: ctx must not be nil")
	}
	if nc == nil {
		panic("detectReplicas: nc must not be nil")
	}
	if nc.ConnectedClusterName() == "" {
		return 1, nil
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return 0, err
	}
	probe := func(ctx context.Context, replicas int) (bool, error) {
		return probeReplicaPlacement(ctx, js, replicas)
	}
	// connect_urls name the other servers in the connected server's own
	// cluster (not gateway peers, and placement stays in that cluster).
	// Fewer than three known servers can never place three replicas, so
	// a refusal there is final rather than a cluster still booting.
	knownServers := len(nc.DiscoveredServers()) + 1
	replicas, err := pickReplicas(
		ctx, probe, provisionRetryPause, knownServers >= 3,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"detect JetStream replicas (set "+
				"DAGNATS_NATS_JETSTREAM_REPLICAS to skip): %w", err,
		)
	}
	if replicas == 1 {
		slog.Warn("natsutil: connected cluster cannot place 3 replicas; "+
			"provisioning at 1. Set DAGNATS_NATS_JETSTREAM_REPLICAS=3 if "+
			"more nodes are still joining",
			"cluster", nc.ConnectedClusterName(), "known_servers", knownServers)
	}
	return replicas, nil
}

// pickReplicas chooses the factor for a server that belongs to a cluster,
// mirroring DeriveReplicas (5+ nodes -> 5, 3-4 -> 3). A refusal at five is
// an answer: three is still replicated. A refusal at three is retried with
// pause between attempts, because a booting cluster's meta leader refuses
// placement until it has seen enough peers (#772). Settling on one replica
// there would create every new stream and bucket unreplicated for good, so
// a refusal that never clears is an error instead. With fewer than three
// known servers (canPlaceThree false) the first refusal answers one.
func pickReplicas(
	ctx context.Context,
	probe func(context.Context, int) (bool, error),
	pause func(attempt int) time.Duration,
	canPlaceThree bool,
) (int, error) {
	if ctx == nil {
		panic("pickReplicas: ctx must not be nil")
	}
	if probe == nil || pause == nil {
		panic("pickReplicas: probe and pause must not be nil")
	}
	placed, err := probe(ctx, 5)
	if err != nil {
		return 0, err
	}
	if placed {
		return 5, nil
	}
	for attempt := 0; attempt < replicaProbeAttemptsMax; attempt++ {
		placed, err := probe(ctx, 3)
		if err != nil {
			return 0, err
		}
		if placed {
			return 3, nil
		}
		if !canPlaceThree {
			return 1, nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("cluster refused 3 replicas: %w", ctx.Err())
		case <-time.After(pause(attempt)):
		}
	}
	return 0, fmt.Errorf(
		"cluster refused 3 replicas on %d attempts", replicaProbeAttemptsMax,
	)
}

// probeReplicaPlacement reports whether the cluster can place a stream at
// replicas. A refusal for lack of peers is an answer (false), not an
// error; anything else (permissions, no reply) is returned.
func probeReplicaPlacement(
	ctx context.Context, js jetstream.JetStream, replicas int,
) (bool, error) {
	if js == nil {
		panic("probeReplicaPlacement: js must not be nil")
	}
	if replicas < 2 {
		panic("probeReplicaPlacement: replicas must be at least 2")
	}
	name := ReplicaProbeStreamPrefix + nuid.Next()
	attemptCtx, cancel := context.WithTimeout(ctx, provisionAttemptTimeout)
	defer cancel()
	_, err := js.CreateStream(attemptCtx, jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{"_dagnats.replica_probe." + name},
		Storage:  jetstream.FileStorage,
		MaxBytes: replicaProbeMaxBytes,
		Replicas: replicas,
	})
	if isPlacementRefusal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := js.DeleteStream(attemptCtx, name); err != nil {
		return false, fmt.Errorf("delete probe stream %s: %w", name, err)
	}
	return true, nil
}

// isPlacementRefusal reports whether err says the cluster lacks the peers
// (or the clustering) for the requested replica count.
func isPlacementRefusal(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode {
	case jsErrCodeNoSuitablePeers, jsErrCodeInsufficientResources,
		jsErrCodeReplicasNotSupported:
		return true
	}
	return false
}

// jsErrCodeInsufficientResources is JetStream error 10023.
const jsErrCodeInsufficientResources jetstream.ErrorCode = 10023

// jsErrCodeReplicasNotSupported is JetStream error 10074, "replicas > 1
// not supported in non-clustered mode".
const jsErrCodeReplicasNotSupported jetstream.ErrorCode = 10074

const quorumPollInterval = 500 * time.Millisecond

// quorumProbeTimeout bounds one AccountInfo probe. A clustered server with
// no meta leader yet may drop the request instead of answering, so an
// unbounded probe would burn the whole quorum budget on its first attempt
// and the poll loop would never retry.
const quorumProbeTimeout = 2 * time.Second

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
		ready, err := jsClusterReady(ctx, js)
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

// jsClusterReady returns true when JetStream's API responds to
// AccountInfo. Caller-supplied expectedSize is intentionally NOT
// inspected here — peer-count verification is left to the cluster
// integration tests (and to the placement probe in dagnatstest).
//
// We deliberately do not consult info.API.Errors either: it is a
// monotonic lifetime counter on the JS API account, not a current-
// health signal. Any prior failed API call (e.g. a transient stream-
// placement error during a R=1 → R=3 migration on a fresh cluster)
// would cause this check to falsely report unready forever. A
// successful AccountInfo response is itself the readiness signal —
// it requires the meta-leader to be elected and JS to be operational.
func jsClusterReady(
	ctx context.Context, js jetstream.JetStream,
) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, quorumProbeTimeout)
	defer cancel()
	info, err := js.AccountInfo(probeCtx)
	if err != nil {
		return false, err
	}
	if info == nil {
		return false, nil
	}
	return true, nil
}
