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
