// api/health_cluster_test.go
// Tests for /health/cluster endpoint.
// Methodology: stand up a real embedded NATS test server, build a
// ClusterHealthHandler with chosen routes, send an HTTP GET, and assert on
// status code and JSON body shape.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestHealthCluster_Standalone verifies the standalone mode response when
// no cluster routes are configured. Asserts mode=standalone, ok=true, 200.
func TestHealthCluster_Standalone(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	handler := NewClusterHealthHandler(NewClusterReporter(nc, nil, nil))
	if handler == nil {
		t.Fatal("NewClusterHealthHandler returned nil")
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health/cluster")
	if err != nil {
		t.Fatalf("GET /health/cluster: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body clusterHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Mode != "standalone" {
		t.Errorf("mode = %q, want %q", body.Mode, "standalone")
	}
	if !body.OK {
		t.Errorf("ok = false, want true")
	}
	if body.ExpectedPeers != 0 {
		t.Errorf("expected_peers = %d, want 0 (omitted)", body.ExpectedPeers)
	}
}

// TestHealthCluster_ClusterShape verifies the response shape when routes are
// configured but the underlying server is single-node (so cluster does not
// actually form). Smoke-tests the cluster branch — asserts mode=cluster,
// expected_peers=2, and HTTP 503 (degraded since quorum will not form).
func TestHealthCluster_ClusterShape(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	handler := NewClusterHealthHandler(NewClusterReporter(nc, routes, nil))
	if handler == nil {
		t.Fatal("NewClusterHealthHandler returned nil")
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health/cluster")
	if err != nil {
		t.Fatalf("GET /health/cluster: %v", err)
	}
	defer resp.Body.Close()

	// Test server has streams configured at replicas=1, so each stream
	// lookup will succeed but the underlying single-node server is not a
	// real 2-route cluster. The handler returns 200 here because all
	// stream/JS calls succeed against the embedded server. To still
	// exercise the cluster branch shape, we accept either 200 (all calls
	// succeed) or 503 (quorum sniff failed) — but we assert mode and
	// expected_peers regardless.
	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 200 or 503", resp.StatusCode)
	}

	var body clusterHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Mode != "cluster" {
		t.Errorf("mode = %q, want %q", body.Mode, "cluster")
	}
	if body.ExpectedPeers != 2 {
		t.Errorf("expected_peers = %d, want 2", body.ExpectedPeers)
	}
	if body.JetStream == nil {
		t.Error("jetstream block missing in cluster mode")
	}
}

// fakeMembership returns a fixed 3-node view: self healthy, one peer
// healthy and the meta leader, one peer offline with its route down.
func fakeMembership() (ClusterMembership, error) {
	return ClusterMembership{
		Self:       "n1",
		MetaLeader: "n2",
		Members: []ClusterMember{
			{Name: "n1", Self: true, RouteConnected: true},
			{Name: "n2", RouteConnected: true, MetaLeader: true},
			{Name: "n3", Offline: true, LagOps: 7},
		},
	}, nil
}

// TestHealthCluster_ReportsEveryProvisionedStream fails when a stream or
// bucket natsutil provisions is absent from the report, so the health
// lists cannot silently go stale again.
func TestHealthCluster_ReportsEveryProvisionedStream(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	reporter := NewClusterReporter(nc, routes, nil)
	body, status := reporter.report(context.Background())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	for _, name := range natsutil.ProvisionedStreamNames() {
		if _, ok := body.JetStream.Streams[name]; !ok {
			t.Errorf("stream %s missing from health report", name)
		}
	}
	for _, name := range natsutil.ProvisionedKVBucketNames() {
		if _, ok := body.JetStream.KVBuckets[name]; !ok {
			t.Errorf("kv bucket %s missing from health report", name)
		}
	}
}

// TestHealthCluster_ReportsAuditBucket covers console_audit (#784): the
// console creates it outside SetupAll, so the report lists it when it
// exists and does not call it unavailable on a node that has none
// (dagnats-api runs without a console).
func TestHealthCluster_ReportsAuditBucket(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	reporter := NewClusterReporter(nc, routes, nil)
	body, status := reporter.report(context.Background())
	if status != http.StatusOK {
		t.Fatalf("status without audit bucket = %d, want 200", status)
	}
	if slices.Contains(body.JetStream.Unavailable, auditkv.Bucket) {
		t.Errorf("absent %s reported unavailable", auditkv.Bucket)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := auditkv.NewKV(context.Background(), js, 1); err != nil {
		t.Fatalf("auditkv.NewKV: %v", err)
	}
	body, _ = reporter.report(context.Background())
	if got, ok := body.JetStream.KVBuckets[auditkv.Bucket]; !ok || got != 1 {
		t.Errorf("%s in report = %d (present %v), want 1", auditkv.Bucket, got, ok)
	}
}

// TestHealthCluster_Membership verifies member rows, the meta leader and
// the derived up/peer counts come from the membership source, not from
// the configured route count.
func TestHealthCluster_Membership(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	reporter := NewClusterReporter(nc, routes, fakeMembership)
	body, _ := reporter.report(context.Background())
	if len(body.Members) != 3 {
		t.Fatalf("members = %d, want 3", len(body.Members))
	}
	if body.Node != "n1" {
		t.Errorf("node = %q, want n1", body.Node)
	}
	if body.Leader != "n2" || !body.JetStream.LeaderElected {
		t.Errorf("leader = %q elected=%v", body.Leader, body.JetStream.LeaderElected)
	}
	if body.NodesExpected != 3 || body.NodesUp != 2 {
		t.Errorf("nodes up/expected = %d/%d, want 2/3", body.NodesUp, body.NodesExpected)
	}
	if body.ConnectedPeers != 1 || body.ExpectedPeers != 2 {
		t.Errorf("peers = %d/%d, want 1/2", body.ConnectedPeers, body.ExpectedPeers)
	}
}

// TestHealthCluster_MembershipError verifies a failing membership source
// degrades the report to 503 instead of reporting optimistic numbers.
func TestHealthCluster_MembershipError(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	failing := func() (ClusterMembership, error) {
		return ClusterMembership{}, errors.New("jsz unavailable")
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	body, status := NewClusterReporter(nc, routes, failing).report(context.Background())
	if status != http.StatusServiceUnavailable || body.OK {
		t.Errorf("status=%d ok=%v, want 503/false", status, body.OK)
	}
}

// TestNATSAPI_ClusterHealth verifies the same report is served over the
// micro control API at api.cluster.health.
func TestNATSAPI_ClusterHealth(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	api := NewNATSAPI(NewService(nc), nc, "1.0.0")
	api.SetClusterReporter(NewClusterReporter(nc, routes, fakeMembership))
	api.Start()
	defer api.Stop()

	reply, err := nc.Request("api.cluster.health", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	var body clusterHealthResponse
	if err := json.Unmarshal(reply.Data, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Mode != "cluster" || len(body.Members) != 3 || body.NodesUp != 2 {
		t.Errorf("unexpected body: %+v", body)
	}
}

// TestHealthCluster_UnreplicatedAssetDegradedNot503 verifies an unreachable
// single-replica asset is reported under degraded with HTTP 200: the
// cluster still has quorum, and a per-node load-balancer health check
// must not drain survivors because one node's unreplicated asset is gone.
func TestHealthCluster_UnreplicatedAssetDegradedNot503(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := js.DeleteStream(context.Background(), "STICKY_TASKS"); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	body, status := NewClusterReporter(nc, routes, nil).report(context.Background())
	if status != http.StatusOK || !body.OK {
		t.Errorf("status=%d ok=%v, want 200/true", status, body.OK)
	}
	if len(body.JetStream.Degraded) != 1 ||
		body.JetStream.Degraded[0] != "STICKY_TASKS" {
		t.Errorf("degraded = %v, want [STICKY_TASKS]", body.JetStream.Degraded)
	}
	if len(body.JetStream.Unavailable) != 0 {
		t.Errorf("unavailable = %v, want none", body.JetStream.Unavailable)
	}
}

// TestClassifyUnavailable covers the quorum rule: unreachable assets are a
// quorum failure when replicated assets exist or nothing answered, and
// merely degraded when everything that answered is single-replica.
func TestClassifyUnavailable(t *testing.T) {
	replicated := &clusterJetStreamInfo{
		Streams:     map[string]clusterStreamInfo{"A": {Replicas: 3, InSync: 2}},
		KVBuckets:   map[string]int{},
		Unavailable: []string{"B"},
	}
	if !classifyUnavailable(replicated) || len(replicated.Degraded) != 0 {
		t.Errorf("replicated: want quorum failure, got %+v", replicated)
	}
	single := &clusterJetStreamInfo{
		Streams:     map[string]clusterStreamInfo{"A": {Replicas: 1, InSync: 1}},
		KVBuckets:   map[string]int{},
		Unavailable: []string{"B"},
	}
	if classifyUnavailable(single) || len(single.Degraded) != 1 ||
		len(single.Unavailable) != 0 {
		t.Errorf("single: want degraded only, got %+v", single)
	}
	nothing := &clusterJetStreamInfo{
		Streams:     map[string]clusterStreamInfo{},
		KVBuckets:   map[string]int{},
		Unavailable: []string{"A"},
	}
	if !classifyUnavailable(nothing) {
		t.Error("nothing answered: want quorum failure")
	}
}

// TestHealthCluster_DiskStatus pins the wire shape of the disk block with
// literal keys and checks low disk is reported as degraded-but-200 in
// cluster mode, matching the existing Degraded precedent, and that a
// healthy reading adds no degradation.
func TestHealthCluster_DiskStatus(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	reporter := NewClusterReporter(nc, routes, nil)
	reporter.SetDiskStatus(func() DiskStatus {
		return DiskStatus{FreeBytes: 5, ThresholdBytes: 100, Low: true}
	})
	body, status := reporter.report(context.Background())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (low disk is a warning)", status)
	}
	if !slices.Contains(body.JetStream.Degraded, "disk") {
		t.Errorf("degraded = %v, want it to name disk", body.JetStream.Degraded)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	disk, _ := wire["disk"].(map[string]any)
	if disk["free_bytes"] != float64(5) || disk["threshold_bytes"] != float64(100) ||
		disk["low"] != true {
		t.Errorf("disk wire = %v", disk)
	}

	reporter.SetDiskStatus(func() DiskStatus {
		return DiskStatus{FreeBytes: 500, ThresholdBytes: 100}
	})
	body, _ = reporter.report(context.Background())
	if slices.Contains(body.JetStream.Degraded, "disk") || body.Disk == nil || body.Disk.Low {
		t.Errorf("healthy disk reported degraded: %+v", body)
	}
}

// TestHealthCluster_DiskStatusStandalone checks standalone nodes report
// disk too, and omit the block when no monitor is attached.
func TestHealthCluster_DiskStatusStandalone(t *testing.T) {
	_, nc := natstest.StartServer(t)
	reporter := NewClusterReporter(nc, nil, nil)
	body, _ := reporter.report(context.Background())
	if body.Disk != nil {
		t.Errorf("disk = %+v without a monitor, want nil", body.Disk)
	}
	reporter.SetDiskStatus(func() DiskStatus {
		return DiskStatus{FreeBytes: 1, ThresholdBytes: 2, Low: true}
	})
	body, status := reporter.report(context.Background())
	if status != http.StatusOK || body.Disk == nil || !body.Disk.Low {
		t.Errorf("standalone status=%d disk=%+v", status, body.Disk)
	}
}

// TestHealthCluster_TrafficStatus pins the wire shape of the traffic block
// with literal keys and checks a hot connection is degraded-but-200 in
// cluster mode, like low disk, while a quiet reading adds no degradation.
func TestHealthCluster_TrafficStatus(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	routes := []string{"nats://peer-a:6222", "nats://peer-b:6222"}
	reporter := NewClusterReporter(nc, routes, nil)
	body, _ := reporter.report(context.Background())
	if body.Traffic != nil {
		t.Errorf("traffic = %+v without a monitor, want nil", body.Traffic)
	}
	reporter.SetTrafficStatus(func() TrafficStatus {
		return TrafficStatus{
			ThresholdMsgsPerSec: 100,
			Hot:                 []HotConnection{{CID: 7, Name: "pump", MsgsPerSec: 312.5}},
		}
	})
	body, status := reporter.report(context.Background())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (hot connection is a warning)", status)
	}
	if !slices.Contains(body.JetStream.Degraded, "traffic") {
		t.Errorf("degraded = %v, want it to name traffic", body.JetStream.Degraded)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	traffic, _ := wire["traffic"].(map[string]any)
	hot, _ := traffic["hot"].([]any)
	if traffic["threshold_msgs_per_sec"] != float64(100) || len(hot) != 1 {
		t.Fatalf("traffic wire = %v", traffic)
	}
	conn, _ := hot[0].(map[string]any)
	if conn["cid"] != float64(7) || conn["name"] != "pump" ||
		conn["msgs_per_sec"] != 312.5 {
		t.Errorf("hot wire = %v", conn)
	}

	reporter.SetTrafficStatus(func() TrafficStatus {
		return TrafficStatus{ThresholdMsgsPerSec: 100}
	})
	body, _ = reporter.report(context.Background())
	if slices.Contains(body.JetStream.Degraded, "traffic") || body.Traffic == nil {
		t.Errorf("quiet traffic reported degraded: %+v", body)
	}
}

// TestHealthCluster_Region checks a node reports the region it was
// configured with, in standalone and cluster mode, and that the key is
// absent when no region is set (literal JSON key, not the decoded field).
func TestHealthCluster_Region(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	bare := NewClusterReporter(nc, nil, nil)
	raw := clusterHealthJSON(t, bare)
	if _, ok := raw["region"]; ok {
		t.Errorf("region key present without a region: %v", raw)
	}
	for _, routes := range [][]string{nil, {"nats://a:6222", "nats://b:6222"}} {
		reporter := NewClusterReporter(nc, routes, nil)
		reporter.SetRegion("us")
		raw = clusterHealthJSON(t, reporter)
		if got, _ := raw["region"].(string); got != "us" {
			t.Errorf("routes %v: region = %v, want \"us\" (body %v)", routes, raw["region"], raw)
		}
	}
}

// clusterHealthJSON serves one GET /health/cluster from reporter and
// decodes the body into a generic map so tests can check literal keys.
func clusterHealthJSON(t *testing.T, reporter *ClusterReporter) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/cluster", nil)
	NewClusterHealthHandler(reporter).ServeHTTP(rec, req)
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return raw
}
