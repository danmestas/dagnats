// api/health_cluster.go
// Cluster health report for the embedded NATS cluster, served over HTTP
// (GET /health/cluster) and NATS request/reply (api.cluster.health) from
// one ClusterReporter. Standalone deployments return a minimal "ok"
// response; clustered deployments include member nodes, meta-leader, peer
// counts, and per-stream replica sync derived from JetStream.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// errUnavailableAssets marks a degraded report: some stream or bucket
// could not be reached. The names are in JetStream.Unavailable.
var errUnavailableAssets = errors.New("some jetstream assets unavailable")

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
	// Unavailable names replicated streams/buckets that did not answer:
	// quorum is lost for them. Non-empty means 503.
	Unavailable []string `json:"unavailable,omitempty"`
	// Degraded names unreachable single-replica assets. The cluster still
	// has quorum, so this is reported but stays 200: a load balancer that
	// health-checks every node must not drain the survivors because one
	// node's unreplicated asset went down with it.
	Degraded []string `json:"degraded,omitempty"`
}

// clusterHealthResponse is the JSON body for GET /health/cluster.
// Mode is "standalone" when no routes are configured, "cluster" otherwise.
// Empty pointer/zero fields are omitted from the response so standalone
// payloads stay minimal.
type clusterHealthResponse struct {
	Mode           string `json:"mode"`
	ExpectedPeers  int    `json:"expected_peers,omitempty"`
	ConnectedPeers int    `json:"connected_peers,omitempty"`
	Leader         string `json:"leader,omitempty"`
	// Node is the server that answered. Every node answers for itself, so
	// a NATS request (no queue group) reaches whichever replies first.
	Node          string                `json:"node,omitempty"`
	NodesExpected int                   `json:"nodes_expected,omitempty"`
	NodesUp       int                   `json:"nodes_up,omitempty"`
	Members       []ClusterMember       `json:"members,omitempty"`
	JetStream     *clusterJetStreamInfo `json:"jetstream,omitempty"`
	OK            bool                  `json:"ok"`
}

// ClusterMember is one node in the embedded cluster as seen from the
// reporting node.
type ClusterMember struct {
	Name           string `json:"name"`
	Self           bool   `json:"self,omitempty"`
	RouteConnected bool   `json:"route_connected"`
	MetaLeader     bool   `json:"meta_leader,omitempty"`
	// Current and LagOps are only meaningful when the answering node is
	// the meta leader: a follower's view reports the other followers as
	// not current. Current is nil (omitted) otherwise.
	Current *bool  `json:"current,omitempty"`
	Offline bool   `json:"offline,omitempty"`
	LagOps  uint64 `json:"lag_ops,omitempty"`
}

// ClusterMembership is a point-in-time membership view. Members includes
// the reporting node itself (Self true).
type ClusterMembership struct {
	Self       string
	MetaLeader string
	Members    []ClusterMember
}

// ClusterMembershipFunc reads live membership from the embedded server.
// The api package cannot see the server, so the server package supplies
// it; nil means membership is unavailable (library embedders).
type ClusterMembershipFunc func() (ClusterMembership, error)

// ClusterReporter builds the cluster health report. It is the single
// implementation behind both transports (HTTP and NATS request/reply).
type ClusterReporter struct {
	nc      *nats.Conn
	routes  []string
	members ClusterMembershipFunc
}

// NewClusterReporter returns a reporter. routes is the configured
// cluster route list (empty means standalone); members may be nil.
// Panics if nc is nil — a NATS connection is always required.
func NewClusterReporter(
	nc *nats.Conn, routes []string, members ClusterMembershipFunc,
) *ClusterReporter {
	if nc == nil {
		panic("NewClusterReporter: nc must not be nil")
	}
	return &ClusterReporter{nc: nc, routes: routes, members: members}
}

// NewClusterHealthHandler returns an http.Handler serving GET
// /health/cluster from reporter. Panics if reporter is nil.
func NewClusterHealthHandler(reporter *ClusterReporter) http.Handler {
	if reporter == nil {
		panic("NewClusterHealthHandler: reporter must not be nil")
	}
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, status := reporter.report(r.Context())
		writeClusterJSON(w, body, status)
	})
}

// report builds the response and the HTTP-style status (200 healthy,
// 503 degraded). Standalone short-circuits with a minimal body; the
// clustered path performs JetStream lookups inside a bounded context.
func (r *ClusterReporter) report(
	parent context.Context,
) (clusterHealthResponse, int) {
	if r == nil {
		panic("ClusterReporter.report: r must not be nil")
	}
	if parent == nil {
		panic("ClusterReporter.report: parent must not be nil")
	}
	if len(r.routes) == 0 {
		return clusterHealthResponse{Mode: "standalone", OK: true},
			http.StatusOK
	}
	resp := clusterHealthResponse{
		Mode:           "cluster",
		ExpectedPeers:  len(r.routes),
		ConnectedPeers: len(r.routes),
		OK:             true,
		JetStream: &clusterJetStreamInfo{
			Streams:   map[string]clusterStreamInfo{},
			KVBuckets: map[string]int{},
		},
	}
	if r.members != nil && !applyMembership(&resp, r.members) {
		return resp, http.StatusServiceUnavailable
	}
	ctx, cancel := context.WithTimeout(parent, clusterHealthTimeout)
	defer cancel()
	if err := r.collectJetStream(ctx, &resp); err != nil {
		if !errors.Is(err, errUnavailableAssets) {
			slog.Warn("cluster health: jetstream lookup failed", "error", err)
		}
		resp.OK = false
		return resp, http.StatusServiceUnavailable
	}
	return resp, http.StatusOK
}

// applyMembership fills member rows, the meta leader and the derived
// node/peer counts. Returns false (and marks resp not OK) on failure so
// the caller degrades rather than reporting numbers it could not verify.
func applyMembership(
	resp *clusterHealthResponse, source ClusterMembershipFunc,
) bool {
	if resp == nil || source == nil {
		panic("applyMembership: resp and source must not be nil")
	}
	view, err := source()
	if err != nil {
		slog.Warn("cluster health: membership lookup failed", "error", err)
		resp.OK = false
		return false
	}
	resp.Members = view.Members
	resp.Node = view.Self
	resp.Leader = view.MetaLeader
	resp.JetStream.LeaderElected = view.MetaLeader != ""
	resp.NodesExpected = resp.ExpectedPeers + 1
	resp.ConnectedPeers = 0
	for _, m := range view.Members {
		if m.RouteConnected && !m.Offline {
			resp.NodesUp++
			if !m.Self {
				resp.ConnectedPeers++
			}
		}
	}
	return true
}

// collectJetStream fills per-stream and per-bucket replica sync from the
// names natsutil provisions. Without a membership source, leader presence
// falls back to the account API error counter.
func (r *ClusterReporter) collectJetStream(
	ctx context.Context, resp *clusterHealthResponse,
) error {
	js, err := jetstream.New(r.nc)
	if err != nil {
		return err
	}
	info, err := js.AccountInfo(ctx)
	if err != nil {
		return err
	}
	if r.members == nil {
		resp.JetStream.LeaderElected = info.API.Errors == 0
	}
	collectAssets(ctx, js, resp.JetStream)
	if classifyUnavailable(resp.JetStream) {
		resp.OK = false
		return errUnavailableAssets
	}
	return nil
}

// streamInSync counts replicas that are current: the leader plus every
// non-offline follower caught up. Falls back to the configured count when
// the server reports no cluster block (single-node stream).
func streamInSync(info *jetstream.StreamInfo) int {
	if info.Cluster == nil {
		return info.Config.Replicas
	}
	inSync := 1
	for _, peer := range info.Cluster.Replicas {
		if peer.Current && !peer.Offline {
			inSync++
		}
	}
	return inSync
}

// classifyUnavailable decides whether unreachable assets mean lost quorum.
// If nothing answered, or any answering asset is replicated, the
// unreachable ones are expected to be replicated too and their loss is a
// quorum failure (returns true; names stay in Unavailable). If every
// answering asset is single-replica (an operator override, or a
// standalone-style layout), the unreachable ones are merely down with
// their one node: they move to Degraded and the report stays healthy.
func classifyUnavailable(out *clusterJetStreamInfo) bool {
	if out == nil {
		panic("classifyUnavailable: out must not be nil")
	}
	if len(out.Unavailable) == 0 {
		return false
	}
	answered := len(out.Streams) + len(out.KVBuckets)
	replicasMax := 0
	for _, st := range out.Streams {
		replicasMax = max(replicasMax, st.Replicas)
	}
	for _, replicas := range out.KVBuckets {
		replicasMax = max(replicasMax, replicas)
	}
	if answered == 0 || replicasMax > 1 {
		return true
	}
	out.Degraded = out.Unavailable
	out.Unavailable = nil
	return false
}

// assetLookup is one stream (or KV backing stream) to inspect.
type assetLookup struct {
	label    string // name as reported: stream name or KV bucket name
	stream   string // JetStream stream name to query
	isBucket bool
	// optional assets are created outside SetupAll and may legitimately
	// not exist; absence is left out of the report, not called unavailable.
	optional bool
}

// provisionedAssets lists every stream and KV bucket natsutil provisions,
// plus the console_audit bucket, which the console creates (auditkv.NewKV)
// and a split dagnats-api deployment never has.
func provisionedAssets() []assetLookup {
	streams := natsutil.ProvisionedStreamNames()
	buckets := natsutil.ProvisionedKVBucketNames()
	out := make([]assetLookup, 0, len(streams)+len(buckets)+1)
	for _, name := range streams {
		out = append(out, assetLookup{label: name, stream: name})
	}
	for _, name := range buckets {
		out = append(out, assetLookup{
			label: name, stream: "KV_" + name, isBucket: true,
		})
	}
	return append(out, assetLookup{
		label: auditkv.Bucket, stream: "KV_" + auditkv.Bucket,
		isBucket: true, optional: true,
	})
}

// collectAssets looks up every provisioned stream and bucket in parallel
// (the list is a fixed, small bound) inside ctx. An asset that cannot be
// reached — typically an R1 asset whose only replica is on a dead node, or
// one without a leader — is listed in Unavailable instead of aborting the
// report, so one dead stream cannot hide the state of all the others.
func collectAssets(
	ctx context.Context,
	js jetstream.JetStream,
	out *clusterJetStreamInfo,
) {
	if js == nil {
		panic("collectAssets: js must not be nil")
	}
	if out == nil {
		panic("collectAssets: out must not be nil")
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, asset := range provisionedAssets() {
		wg.Add(1)
		go func(a assetLookup) {
			defer wg.Done()
			s, err := js.Stream(ctx, a.stream)
			// js.Stream already fetched fresh StreamInfo (incl. the
			// cluster block); another Info round trip would only add
			// latency across routes.
			mu.Lock()
			defer mu.Unlock()
			if a.optional && errors.Is(err, jetstream.ErrStreamNotFound) {
				return
			}
			if err != nil || s.CachedInfo() == nil {
				out.Unavailable = append(out.Unavailable, a.label)
				return
			}
			info := s.CachedInfo()
			if a.isBucket {
				out.KVBuckets[a.label] = info.Config.Replicas
				return
			}
			out.Streams[a.label] = clusterStreamInfo{
				Replicas: info.Config.Replicas,
				InSync:   streamInSync(info),
			}
		}(asset)
	}
	wg.Wait()
	sort.Strings(out.Unavailable)
}

// clusterHealthSubject is the micro endpoint subject for the same report.
const clusterHealthSubject = "api.cluster.health"

// SetClusterReporter enables the api.cluster.health endpoint. Must be
// called before Start; without it the endpoint is not registered.
func (n *NATSAPI) SetClusterReporter(r *ClusterReporter) {
	if n.micro != nil {
		panic("NATSAPI.SetClusterReporter: call before Start")
	}
	n.cluster = r
}

// addClusterEndpoint registers api.cluster.health when a reporter is set.
func (n *NATSAPI) addClusterEndpoint(srv micro.Service) {
	if n.cluster == nil {
		return
	}
	err := srv.AddEndpoint(
		"cluster-health", micro.HandlerFunc(n.handleClusterHealth),
		micro.WithEndpointSubject(clusterHealthSubject),
		micro.WithEndpointQueueGroupDisabled(),
	)
	if err != nil {
		panic("NATSAPI.Start: AddEndpoint failed for " +
			clusterHealthSubject + ": " + err.Error())
	}
}

// handleClusterHealth replies with the JSON report. Degraded state is in
// the body (ok=false); NATS replies have no status line.
func (n *NATSAPI) handleClusterHealth(req micro.Request) {
	if req == nil {
		panic("handleClusterHealth: req must not be nil")
	}
	if n.cluster == nil {
		panic("handleClusterHealth: reporter must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), clusterHealthTimeout,
	)
	defer cancel()
	body, _ := n.cluster.report(ctx)
	n.reply(req, body)
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
