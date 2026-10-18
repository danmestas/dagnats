// Methodology: pure unit tests for the clustered server_name. The name
// must be identical across restarts (JetStream tracks peers by it) yet
// distinct for nodes sharing a host, and an explicit override must win.
package server

import (
	"os"
	"strings"
	"testing"
)

func TestClusterServerName_StableAcrossCalls(t *testing.T) {
	cfg := Config{NATSClusterName: "mesh"}
	first := clusterServerName(cfg, 26222)
	second := clusterServerName(cfg, 26222)
	if first != second {
		t.Fatalf("name changed between calls: %q vs %q", first, second)
	}
	if !strings.HasPrefix(first, "mesh-") || !strings.HasSuffix(first, "-26222") {
		t.Fatalf("name = %q, want mesh-<hostname>-26222", first)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	// A pid-derived name passes the two checks above only by luck; this
	// pins the hostname, which is what survives a restart.
	sanitized := strings.Map(func(r rune) rune {
		if isServerNameRune(r) {
			return r
		}
		return '-'
	}, hostname)
	if want := "mesh-" + sanitized + "-26222"; first != want {
		t.Fatalf("name = %q, want %q", first, want)
	}
}

func TestClusterServerName_UniquePerPortAndOverride(t *testing.T) {
	cfg := Config{NATSClusterName: "mesh"}
	if clusterServerName(cfg, 6222) == clusterServerName(cfg, 6223) {
		t.Fatal("two nodes on one host got the same name")
	}
	cfg.NATSServerName = "eu-1"
	if got := clusterServerName(cfg, 6222); got != "eu-1" {
		t.Fatalf("override ignored: got %q", got)
	}
	for _, r := range clusterServerName(Config{NATSClusterName: "m"}, 1) {
		if !isServerNameRune(r) {
			t.Fatalf("derived name contains %q", r)
		}
	}
}

func TestWithRouteCredentials_AddsTokenKeepsExplicit(t *testing.T) {
	routes, err := parseClusterRoutes([]string{
		"nats://10.0.0.2:6222", "nats://ops:pw@10.0.0.3:6222",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	withRouteCredentials(routes, "tok")
	// Positive: a bare route now authenticates with the token.
	if got := routes[0].User.Username(); got != "tok" {
		t.Fatalf("bare route user = %q, want tok", got)
	}
	// Negative: explicit userinfo on a route is left alone.
	if pw, _ := routes[1].User.Password(); routes[1].User.Username() != "ops" || pw != "pw" {
		t.Fatalf("explicit route credentials overwritten: %v", routes[1].User)
	}
}
