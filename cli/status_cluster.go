// cli/status_cluster.go
// Probes /health/cluster on the dagnats HTTP endpoint and prints cluster
// summary lines (mode/peers/leader/streams) when the server reports
// mode=cluster. Standalone, leaf, 404 (older builds), and any transport
// error are treated as "skip silently" — the existing status output stays
// unchanged so the CLI stays compatible with mismatched server versions.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

// clusterHealthURL returns the URL the CLI should probe for cluster
// health. Reads DAGNATS_HTTP_ADDR, falling back to localhost:8080 (the
// server default). Bare ":port" forms are rewritten to localhost so
// HTTP requests resolve.
func clusterHealthURL() string {
	addr := os.Getenv("DAGNATS_HTTP_ADDR")
	if addr == "" {
		addr = "localhost:8080"
	}
	if len(addr) > 0 && addr[0] == ':' {
		addr = "localhost" + addr
	}
	return "http://" + addr + "/health/cluster"
}

// clusterStreamReport mirrors api.clusterStreamInfo for unmarshaling.
type clusterStreamReport struct {
	Replicas int `json:"replicas"`
	InSync   int `json:"in_sync"`
}

// clusterJetStreamReport mirrors api.clusterJetStreamInfo.
type clusterJetStreamReport struct {
	LeaderElected bool                           `json:"leader_elected"`
	Streams       map[string]clusterStreamReport `json:"streams"`
	KVBuckets     map[string]int                 `json:"kv_buckets"`
}

// clusterHealthReport mirrors api.clusterHealthResponse for the CLI.
// Only fields the CLI actually prints are tracked; unknown fields are
// ignored by encoding/json.
type clusterHealthReport struct {
	Mode           string                  `json:"mode"`
	ExpectedPeers  int                     `json:"expected_peers"`
	ConnectedPeers int                     `json:"connected_peers"`
	Leader         string                  `json:"leader"`
	JetStream      *clusterJetStreamReport `json:"jetstream"`
	OK             bool                    `json:"ok"`
}

// clusterProbeTimeout bounds the HTTP GET so a stalled or unreachable
// server cannot freeze `dagnats status`. Short by design — the endpoint
// is local and any slowness is a server problem we'd rather skip past.
const clusterProbeTimeout = 2 * time.Second

// clusterProbeMaxBytes caps the response body read. The real handler
// produces well under 4KB, but a malicious or misconfigured proxy could
// stream forever otherwise.
const clusterProbeMaxBytes = 64 * 1024

// fetchClusterHealth performs the HTTP GET. Returns nil (no error) for
// any condition the CLI must treat as "skip silently": connection
// failure, non-200 status, malformed JSON. Errors only surface for
// programmer mistakes (e.g., bad URL); the caller can ignore them
// safely.
func fetchClusterHealth(url string) (*clusterHealthReport, error) {
	if url == "" {
		panic("fetchClusterHealth: url must not be empty")
	}
	if len(url) > 1024 {
		panic("fetchClusterHealth: url exceeds max length")
	}

	client := &http.Client{Timeout: clusterProbeTimeout}
	resp, err := client.Get(url)
	if err != nil {
		// Connection refused, DNS failure, timeout — silently skip.
		return nil, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// 404 (older server without endpoint) or any other status
		// is treated as "no cluster info available".
		return nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, clusterProbeMaxBytes))
	if err != nil {
		return nil, nil
	}

	var report clusterHealthReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, nil
	}
	return &report, nil
}

// printClusterStatus prints cluster summary lines when mode=cluster.
// All other modes (standalone, leaf, empty) and a nil report are
// no-ops — preserving the existing status output for backwards
// compatibility with older servers and non-cluster deployments.
func printClusterStatus(report *clusterHealthReport) {
	if report == nil {
		return
	}
	if report.Mode != "cluster" {
		return
	}

	fmt.Println("mode:        cluster")
	fmt.Printf("peers:       %d/%d connected\n",
		report.ConnectedPeers, report.ExpectedPeers,
	)
	if report.Leader != "" {
		fmt.Printf("leader:      %s\n", report.Leader)
	}
	if report.JetStream != nil {
		printClusterStreams(report.JetStream)
	}
}

// printClusterStreams renders the per-stream replica summary as a
// single condensed "X/Y in-sync at R=Z" line. When streams report
// uniform replica counts (the v1 case), R is the shared value;
// otherwise the line is omitted to avoid misleading output.
func printClusterStreams(js *clusterJetStreamReport) {
	if js == nil {
		panic("printClusterStreams: js must not be nil")
	}
	if len(js.Streams) == 0 {
		return
	}
	if len(js.Streams) > 1000 {
		panic("printClusterStreams: streams exceeds max bound")
	}

	totalReplicas, totalInSync, replicaSet := summarizeStreams(js.Streams)
	if len(replicaSet) != 1 {
		// Mixed replica counts: skip the summary line rather than
		// pretending there's a single "R=" value to report.
		return
	}
	r := replicaSet[0]
	fmt.Printf("streams:     %d/%d in-sync at R=%d\n",
		totalInSync, totalReplicas, r,
	)
}

// summarizeStreams aggregates per-stream replica/in-sync counts and
// returns the sorted set of distinct replica values. Sorted output
// keeps the "single replica value" check deterministic across runs.
func summarizeStreams(
	streams map[string]clusterStreamReport,
) (int, int, []int) {
	if streams == nil {
		panic("summarizeStreams: streams must not be nil")
	}

	totalReplicas := 0
	totalInSync := 0
	seen := make(map[int]struct{}, 4)
	for _, s := range streams {
		totalReplicas += s.Replicas
		totalInSync += s.InSync
		seen[s.Replicas] = struct{}{}
	}
	replicaSet := make([]int, 0, len(seen))
	for r := range seen {
		replicaSet = append(replicaSet, r)
	}
	sort.Ints(replicaSet)
	return totalReplicas, totalInSync, replicaSet
}
