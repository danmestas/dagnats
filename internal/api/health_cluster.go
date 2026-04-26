// api/health_cluster.go
// HTTP handler for GET /health/cluster — reports embedded NATS cluster
// health. Standalone deployments return a minimal "ok" response; clustered
// deployments include peer counts, leader presence, and per-stream replica
// info derived from JetStream.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// knownStreams enumerates the JetStream streams DagNats provisions in
// natsutil.SetupStreams. Health reporting walks this list to report
// per-stream replica counts.
var knownStreams = []string{
	"WORKFLOW_HISTORY",
	"TASK_QUEUES",
	"EVENTS",
	"DEAD_LETTERS",
	"SLEEP_TIMERS",
}

// knownKVBuckets enumerates the KV buckets natsutil.SetupKVBuckets
// provisions plus those added at server startup. Listed for visibility in
// /health/cluster output; cluster-formation does not depend on these.
var knownKVBuckets = []string{
	"workflow_defs",
	"workflow_runs",
}

// clusterHealthTimeout bounds JetStream lookups inside the handler.
// Longer than the default REST handler budget because cluster operations
// may hop across nodes; shorter than client-facing request timeouts.
const clusterHealthTimeout = 5 * time.Second

// clusterStreamInfo reports replica config for a single JetStream stream.
type clusterStreamInfo struct {
	Replicas int `json:"replicas"`
	InSync   int `json:"in_sync"`
}

// clusterJetStreamInfo aggregates JetStream-layer cluster signals: leader
// presence (derived from API errors), per-stream replicas, and per-bucket
// replicas.
type clusterJetStreamInfo struct {
	LeaderElected bool                         `json:"leader_elected"`
	Streams       map[string]clusterStreamInfo `json:"streams"`
	KVBuckets     map[string]int               `json:"kv_buckets"`
}

// clusterHealthResponse is the JSON body for GET /health/cluster.
// Mode is "standalone" when no routes are configured, "cluster" otherwise.
// Empty pointer/zero fields are omitted from the response so standalone
// payloads stay minimal.
type clusterHealthResponse struct {
	Mode           string                `json:"mode"`
	ExpectedPeers  int                   `json:"expected_peers,omitempty"`
	ConnectedPeers int                   `json:"connected_peers,omitempty"`
	Leader         string                `json:"leader,omitempty"`
	JetStream      *clusterJetStreamInfo `json:"jetstream,omitempty"`
	OK             bool                  `json:"ok"`
}

// NewClusterHealthHandler returns an http.Handler serving GET
// /health/cluster. routes is the list of cluster routes from server config;
// nil or empty means standalone deployment. Panics if nc is nil — a NATS
// connection is always required, even for standalone mode reporting.
func NewClusterHealthHandler(
	nc *nats.Conn, routes []string,
) http.Handler {
	if nc == nil {
		panic("NewClusterHealthHandler: nc must not be nil")
	}
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		serveClusterHealth(w, r, nc, routes)
	})
}

// serveClusterHealth builds and writes the cluster health response.
// Splits standalone vs cluster paths; clustered path performs JetStream
// AccountInfo + per-stream lookups inside a bounded context. Any failure
// in the cluster path flips OK to false and downgrades to HTTP 503.
func serveClusterHealth(
	w http.ResponseWriter,
	r *http.Request,
	nc *nats.Conn,
	routes []string,
) {
	if w == nil {
		panic("serveClusterHealth: w must not be nil")
	}
	if r == nil {
		panic("serveClusterHealth: r must not be nil")
	}

	if len(routes) == 0 {
		writeClusterJSON(w, clusterHealthResponse{
			Mode: "standalone",
			OK:   true,
		}, http.StatusOK)
		return
	}

	resp, status := buildClusterReport(r.Context(), nc, routes)
	writeClusterJSON(w, resp, status)
}

// buildClusterReport populates a clusterHealthResponse for clustered
// deployments. Returns the response plus the appropriate HTTP status:
// 200 when all JetStream calls succeed, 503 when any fail (degraded).
// Optimistic for v1: ConnectedPeers == len(routes) and InSync == Replicas;
// real peer-state tracking is deferred to v1.1.
func buildClusterReport(
	parent context.Context,
	nc *nats.Conn,
	routes []string,
) (clusterHealthResponse, int) {
	if nc == nil {
		panic("buildClusterReport: nc must not be nil")
	}
	if len(routes) == 0 {
		panic("buildClusterReport: routes must not be empty")
	}

	resp := clusterHealthResponse{
		Mode:           "cluster",
		ExpectedPeers:  len(routes),
		ConnectedPeers: len(routes),
		OK:             true,
		JetStream: &clusterJetStreamInfo{
			Streams:   make(map[string]clusterStreamInfo, len(knownStreams)),
			KVBuckets: make(map[string]int, len(knownKVBuckets)),
		},
	}

	ctx, cancel := context.WithTimeout(parent, clusterHealthTimeout)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		resp.OK = false
		return resp, http.StatusServiceUnavailable
	}

	info, err := js.AccountInfo(ctx)
	if err != nil {
		resp.OK = false
		return resp, http.StatusServiceUnavailable
	}
	resp.JetStream.LeaderElected = info.API.Errors == 0

	if err := collectStreamReplicas(ctx, js, resp.JetStream); err != nil {
		resp.OK = false
		return resp, http.StatusServiceUnavailable
	}

	collectKVReplicas(ctx, js, resp.JetStream)

	return resp, http.StatusOK
}

// collectStreamReplicas fills out per-stream replica counts on the report.
// Returns the first stream-lookup error so the caller can flip OK/status.
// InSync is reported optimistically as Replicas for v1.
func collectStreamReplicas(
	ctx context.Context,
	js jetstream.JetStream,
	out *clusterJetStreamInfo,
) error {
	if js == nil {
		panic("collectStreamReplicas: js must not be nil")
	}
	if out == nil {
		panic("collectStreamReplicas: out must not be nil")
	}
	for _, name := range knownStreams {
		s, err := js.Stream(ctx, name)
		if err != nil {
			return err
		}
		cached := s.CachedInfo()
		if cached == nil {
			return jetstream.ErrStreamNotFound
		}
		replicas := cached.Config.Replicas
		out.Streams[name] = clusterStreamInfo{
			Replicas: replicas,
			InSync:   replicas,
		}
	}
	return nil
}

// collectKVReplicas adds per-bucket replica counts. JetStream KV buckets
// are backed by streams named "KV_<bucket>"; we read replica config from
// that stream. Errors are tolerated (KV missing is not fatal for cluster
// health) and simply skipped.
func collectKVReplicas(
	ctx context.Context,
	js jetstream.JetStream,
	out *clusterJetStreamInfo,
) {
	if js == nil {
		panic("collectKVReplicas: js must not be nil")
	}
	if out == nil {
		panic("collectKVReplicas: out must not be nil")
	}
	for _, name := range knownKVBuckets {
		s, err := js.Stream(ctx, "KV_"+name)
		if err != nil {
			continue
		}
		cached := s.CachedInfo()
		if cached == nil {
			continue
		}
		out.KVBuckets[name] = cached.Config.Replicas
	}
}

// writeClusterJSON serializes the report and writes status. Encoding
// errors are logged but cannot be surfaced (header/body already flushed).
func writeClusterJSON(
	w http.ResponseWriter, body clusterHealthResponse, status int,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("encode cluster health response", "error", err)
	}
}
