// cli/status_cluster_test.go
// Methodology: each test stands up an httptest.NewServer that mimics the
// dagnats /health/cluster handler with a canned JSON body. Tests then
// point DAGNATS_HTTP_ADDR at that test server and invoke the CLI helpers
// directly. Asserts cover positive (cluster mode → output includes
// summary lines) and negative (standalone, 404, network error → no
// extra output) cases so the CLI stays compatible with mismatched
// server/client versions.
package cli

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

// withClusterServer points DAGNATS_HTTP_ADDR at the given test server
// for the duration of the test, restoring the prior value via t.Cleanup.
func withClusterServer(t *testing.T, ts *httptest.Server) {
	t.Helper()
	if ts == nil {
		t.Fatal("withClusterServer: ts must not be nil")
	}
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	prev, hadPrev := os.LookupEnv("DAGNATS_HTTP_ADDR")
	if err := os.Setenv("DAGNATS_HTTP_ADDR", u.Host); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	t.Cleanup(func() {
		if hadPrev {
			_ = os.Setenv("DAGNATS_HTTP_ADDR", prev)
			return
		}
		_ = os.Unsetenv("DAGNATS_HTTP_ADDR")
	})
}

// clusterJSONSample is a canonical /health/cluster response for a
// 3-node cluster — taken from the spec sample in the task plan.
const clusterJSONSample = `{
  "mode": "cluster",
  "expected_peers": 2,
  "connected_peers": 2,
  "leader": "node-2",
  "jetstream": {
    "leader_elected": true,
    "streams": {
      "WORKFLOW_HISTORY": {"replicas": 3, "in_sync": 3},
      "TASK_QUEUES":      {"replicas": 3, "in_sync": 3}
    }
  },
  "ok": true
}`

func TestFetchClusterHealthClusterMode(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health/cluster" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(clusterJSONSample))
		},
	))
	defer ts.Close()
	withClusterServer(t, ts)

	report, err := fetchClusterHealth(clusterHealthURL())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Positive: report parses and reports cluster mode.
	if report == nil {
		t.Fatal("expected non-nil report for cluster mode")
	}
	if report.Mode != "cluster" {
		t.Fatalf("Mode = %q, want %q", report.Mode, "cluster")
	}
	if report.ConnectedPeers != 2 {
		t.Fatalf("ConnectedPeers = %d, want 2", report.ConnectedPeers)
	}

	// Negative: leader must not be empty in the cluster sample.
	if report.Leader == "" {
		t.Fatal("expected non-empty leader for cluster sample")
	}
}

func TestPrintClusterStatusClusterMode(t *testing.T) {
	report := &clusterHealthReport{
		Mode:           "cluster",
		ExpectedPeers:  2,
		ConnectedPeers: 2,
		Leader:         "node-2",
		JetStream: &clusterJetStreamReport{
			LeaderElected: true,
			Streams: map[string]clusterStreamReport{
				"WORKFLOW_HISTORY": {Replicas: 3, InSync: 3},
				"TASK_QUEUES":      {Replicas: 3, InSync: 3},
			},
		},
		OK: true,
	}

	output := captureOutput(func() {
		printClusterStatus(report)
	})

	// Positive: all four expected lines must appear.
	wants := []string{"mode:", "cluster", "peers:", "leader:", "streams:"}
	for _, w := range wants {
		if !strings.Contains(output, w) {
			t.Fatalf("expected %q in output, got: %s", w, output)
		}
	}

	// Negative: peers count must be the connected/expected pair.
	if !strings.Contains(output, "2/2 connected") {
		t.Fatalf("expected '2/2 connected' in output, got: %s", output)
	}
}

func TestPrintClusterStatusStandaloneMode(t *testing.T) {
	report := &clusterHealthReport{Mode: "standalone", OK: true}

	output := captureOutput(func() {
		printClusterStatus(report)
	})

	// Positive: standalone produces no extra output.
	if output != "" {
		t.Fatalf("expected empty output for standalone, got: %q", output)
	}

	// Negative: the cluster lines must never appear.
	for _, banned := range []string{"mode:", "peers:", "leader:", "streams:"} {
		if strings.Contains(output, banned) {
			t.Fatalf(
				"unexpected %q in standalone output: %s",
				banned, output,
			)
		}
	}
}

func TestPrintClusterStatusLeafMode(t *testing.T) {
	report := &clusterHealthReport{Mode: "leaf", OK: true}

	output := captureOutput(func() {
		printClusterStatus(report)
	})

	// Positive: leaf is silent like standalone.
	if output != "" {
		t.Fatalf("expected empty output for leaf, got: %q", output)
	}

	// Negative: must not print mode line for leaf.
	if strings.Contains(output, "mode:") {
		t.Fatal("leaf mode must not print mode line")
	}
}

func TestFetchClusterHealthEndpoint404(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	))
	defer ts.Close()
	withClusterServer(t, ts)

	report, err := fetchClusterHealth(clusterHealthURL())

	// Positive: 404 must return nil report with no error so the CLI
	// stays compatible with older builds missing the endpoint.
	if err != nil {
		t.Fatalf("unexpected error on 404: %v", err)
	}
	if report != nil {
		t.Fatalf("expected nil report on 404, got: %+v", report)
	}
}

func TestFetchClusterHealthMalformedJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not json"))
		},
	))
	defer ts.Close()
	withClusterServer(t, ts)

	report, err := fetchClusterHealth(clusterHealthURL())

	// Positive: malformed JSON degrades to nil report, no error.
	if err != nil {
		t.Fatalf("unexpected error on bad JSON: %v", err)
	}

	// Negative: must not surface a partial report.
	if report != nil {
		t.Fatalf("expected nil report on malformed JSON, got: %+v", report)
	}
}

func TestFetchClusterHealthConnectionRefused(t *testing.T) {
	// Bind to an ephemeral port that's immediately closed so the GET
	// fails with connection refused. Reuses httptest.NewServer to get
	// a known-good port, then closes it before the probe.
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {},
	))
	withClusterServer(t, ts)
	ts.Close()

	report, err := fetchClusterHealth(clusterHealthURL())

	// Positive: network errors degrade silently — never block status.
	if err != nil {
		t.Fatalf("expected silent skip on network error, got: %v", err)
	}
	if report != nil {
		t.Fatalf(
			"expected nil report on network error, got: %+v", report,
		)
	}
}

func TestPrintClusterStreamsMixedReplicas(t *testing.T) {
	js := &clusterJetStreamReport{
		Streams: map[string]clusterStreamReport{
			"A": {Replicas: 3, InSync: 3},
			"B": {Replicas: 1, InSync: 1},
		},
	}

	output := captureOutput(func() {
		printClusterStreams(js)
	})

	// Positive: mixed replicas must skip the summary line entirely.
	if output != "" {
		t.Fatalf(
			"expected empty output for mixed replica counts, got: %q",
			output,
		)
	}

	// Negative: must not print any "R=" claim when replicas differ.
	if strings.Contains(output, "R=") {
		t.Fatal("mixed replicas must not print R= summary")
	}
}

func TestClusterHealthURLDefault(t *testing.T) {
	prev, hadPrev := os.LookupEnv("DAGNATS_HTTP_ADDR")
	_ = os.Unsetenv("DAGNATS_HTTP_ADDR")
	t.Cleanup(func() {
		if hadPrev {
			_ = os.Setenv("DAGNATS_HTTP_ADDR", prev)
		}
	})

	got := clusterHealthURL()

	// Positive: default URL points at localhost:8080.
	if !strings.Contains(got, "localhost:8080") {
		t.Fatalf(
			"expected default URL to contain localhost:8080, got: %s",
			got,
		)
	}

	// Negative: never returns a bare ":port" form.
	if strings.HasPrefix(got, "http://:") {
		t.Fatalf("URL must not start with 'http://:', got: %s", got)
	}
}
