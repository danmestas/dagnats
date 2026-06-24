// api/micro_test.go
// Tests that the control plane is exposed as a nats-micro service so that
// $SRV.PING/INFO/STATS discovery works alongside the existing subjects.
// Methodology: real embedded NATS, drive $SRV control subjects via
// request/reply, parse the micro wire types, assert positive + negative.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/micro"
)

func TestMicroPingRespondsAfterStartTimesOutAfterStop(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")

	// Negative: before Start there is no service to ping.
	_, err := nc.Request("$SRV.PING.dagnats-api", nil, 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected no PING responder before Start")
	}

	natsAPI.Start()

	// Positive: after Start the service answers PING with its identity.
	reply, err := nc.Request("$SRV.PING.dagnats-api", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("PING after Start failed: %v", err)
	}
	var ping micro.Ping
	if err := json.Unmarshal(reply.Data, &ping); err != nil {
		t.Fatalf("unmarshal ping: %v", err)
	}
	if ping.Name != "dagnats-api" {
		t.Fatalf("ping name = %q, want dagnats-api", ping.Name)
	}

	natsAPI.Stop()

	// Negative: after Stop the PING responder is gone.
	_, err = nc.Request("$SRV.PING.dagnats-api", nil, 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected no PING responder after Stop")
	}
}

func TestMicroInfoListsExactlyThreeEndpointSubjects(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()

	reply, err := nc.Request("$SRV.INFO.dagnats-api", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("INFO request failed: %v", err)
	}
	var info micro.Info
	if err := json.Unmarshal(reply.Data, &info); err != nil {
		t.Fatalf("unmarshal info: %v", err)
	}

	want := map[string]bool{
		"api.workflows.register": true,
		"api.runs.start":         true,
		"api.runs.get":           true,
	}
	got := make(map[string]bool, len(info.Endpoints))
	for _, ep := range info.Endpoints {
		got[ep.Subject] = true
	}

	// Positive: the three control-plane subjects are present.
	for subject := range want {
		if !got[subject] {
			t.Fatalf("INFO missing endpoint subject %q", subject)
		}
	}
	// Negative: no unexpected subjects beyond the three.
	if len(info.Endpoints) != len(want) {
		t.Fatalf(
			"endpoint count = %d, want %d (subjects: %v)",
			len(info.Endpoints), len(want), got,
		)
	}
}

func TestMicroStatsCountsPerEndpointRequests(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()

	// Drive exactly one call against the get-run endpoint.
	if _, err := nc.Request(
		"api.runs.get", []byte("no-such-run"), 2*time.Second,
	); err != nil {
		t.Fatalf("priming api.runs.get failed: %v", err)
	}

	reply, err := nc.Request("$SRV.STATS.dagnats-api", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("STATS request failed: %v", err)
	}
	var stats micro.Stats
	if err := json.Unmarshal(reply.Data, &stats); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}

	bysubject := make(map[string]*micro.EndpointStats, len(stats.Endpoints))
	for _, es := range stats.Endpoints {
		bysubject[es.Subject] = es
	}

	getStats := bysubject["api.runs.get"]
	if getStats == nil {
		t.Fatal("STATS missing api.runs.get endpoint")
	}
	// Positive: the called endpoint recorded at least one request.
	if getStats.NumRequests < 1 {
		t.Fatalf("api.runs.get NumRequests = %d, want >=1", getStats.NumRequests)
	}
	// Negative: a sibling endpoint we never called stayed at zero.
	startStats := bysubject["api.runs.start"]
	if startStats == nil {
		t.Fatal("STATS missing api.runs.start endpoint")
	}
	if startStats.NumRequests != 0 {
		t.Fatalf(
			"api.runs.start NumRequests = %d, want 0 (per-endpoint routing)",
			startStats.NumRequests,
		)
	}
}

func TestMicroVersion(t *testing.T) {
	cases := []struct {
		name  string
		build string
		want  string
	}{
		{"plain semver passes through", "1.2.3", "1.2.3"},
		{"zero semver passes through", "0.0.0", "0.0.0"},
		{"dev sentinel", "dev", "0.0.0-dev"},
		{"empty sentinel", "", "0.0.0-dev"},
		{"prerelease passes through", "1.2.3-4-gabcdef", "1.2.3-4-gabcdef"},
		{"build metadata passes through", "1.2.3+build.7", "1.2.3+build.7"},
		{"v-prefixed collapses", "v1.2.3", "0.0.0-dev"},
		{"git describe with v collapses", "v1.2.3-4-gabcdef", "0.0.0-dev"},
		{"two-component collapses", "1.2", "0.0.0-dev"},
		{"non-semver collapses", "main", "0.0.0-dev"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := microVersion(tc.build)
			if got != tc.want {
				t.Fatalf("microVersion(%q) = %q, want %q", tc.build, got, tc.want)
			}
			// Contract: every output round-trips through micro's
			// validation -- it is a valid SemVer or the dev sentinel.
			if !microVersionRegexp.MatchString(got) &&
				got != microVersionDevSentinel {
				t.Fatalf(
					"microVersion(%q) = %q is not micro-acceptable",
					tc.build, got,
				)
			}
		})
	}
}

// TestStartWithDevBuildDoesNotPanic proves the dev sentinel produced by
// microVersion is actually accepted by micro.AddService (F5 integration
// assertion) -- an un-stamped "dev" build must not crash Start.
func TestStartWithDevBuildDoesNotPanic(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)
	svc := NewService(nc)
	natsAPI := NewNATSAPI(svc, nc, "dev")

	natsAPI.Start()
	defer natsAPI.Stop()

	// Positive: PING works, proving AddService accepted the sentinel.
	reply, err := nc.Request("$SRV.PING.dagnats-api", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("PING after dev-build Start failed: %v", err)
	}
	var ping micro.Ping
	if err := json.Unmarshal(reply.Data, &ping); err != nil {
		t.Fatalf("unmarshal ping: %v", err)
	}
	// Negative: the reported version is the sentinel, not the raw "dev".
	if ping.Version == "dev" {
		t.Fatal("expected sanitized version, got raw 'dev'")
	}
	if ping.Version != "0.0.0-dev" {
		t.Fatalf("version = %q, want 0.0.0-dev", ping.Version)
	}
}
