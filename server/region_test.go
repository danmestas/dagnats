// region: config parsing and validation as pure unit tests. Each test
// loads a config file through ConfigWithPath, the same path dagnats serve
// takes, and checks both that a region fills nats_cluster_name and
// jetstream_domain and that any disagreement is a load error naming both
// keys.
package server

import (
	"strings"
	"testing"
)

func TestRegionConfig_DefaultsClusterNameAndDomain(t *testing.T) {
	if got := DefaultConfig().Region; got != "" {
		t.Fatalf("default region = %q, want empty", got)
	}
	cfg, _, err := ConfigWithPath(writeConfigFile(t, "region: us\n"))
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.Region != "us" || cfg.NATSClusterName != "us" || cfg.JetStreamDomain != "us" {
		t.Fatalf("region us: region=%q cluster=%q domain=%q, want us for all three",
			cfg.Region, cfg.NATSClusterName, cfg.JetStreamDomain)
	}
	t.Setenv("DAGNATS_REGION", "eu")
	cfg, _, err = ConfigWithPath(writeConfigFile(t, "region: us\n"))
	if err != nil {
		t.Fatalf("ConfigWithPath with env: %v", err)
	}
	if cfg.Region != "eu" || cfg.NATSClusterName != "eu" || cfg.JetStreamDomain != "eu" {
		t.Fatalf("DAGNATS_REGION eu: region=%q cluster=%q domain=%q, want eu",
			cfg.Region, cfg.NATSClusterName, cfg.JetStreamDomain)
	}
}

func TestRegionConfig_UnsetLeavesKeysAlone(t *testing.T) {
	cfg, _, err := ConfigWithPath(writeConfigFile(t,
		"nats_cluster_name: mesh\njetstream_domain: hub\n"))
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.Region != "" || cfg.NATSClusterName != "mesh" || cfg.JetStreamDomain != "hub" {
		t.Fatalf("no region: region=%q cluster=%q domain=%q, want \"\" mesh hub",
			cfg.Region, cfg.NATSClusterName, cfg.JetStreamDomain)
	}
}

func TestRegionConfig_ExplicitMatchingKeysAccepted(t *testing.T) {
	cfg, _, err := ConfigWithPath(writeConfigFile(t,
		"region: us\nnats_cluster_name: us\njetstream_domain: us\n"))
	if err != nil {
		t.Fatalf("matching explicit keys rejected: %v", err)
	}
	if cfg.NATSClusterName != "us" || cfg.JetStreamDomain != "us" {
		t.Fatalf("cluster=%q domain=%q, want us", cfg.NATSClusterName, cfg.JetStreamDomain)
	}
}

func TestRegionConfig_ConflictIsLoadError(t *testing.T) {
	cases := []struct{ name, body, key string }{
		{"cluster name", "region: us\nnats_cluster_name: eu\n", "nats_cluster_name"},
		{"domain", "region: us\njetstream_domain: eu\n", "jetstream_domain"},
	}
	for _, tc := range cases {
		_, _, err := ConfigWithPath(writeConfigFile(t, tc.body))
		if err == nil {
			t.Errorf("%s: conflicting value accepted", tc.name)
			continue
		}
		msg := err.Error()
		for _, want := range []string{"region", tc.key, `"us"`, `"eu"`} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s: err %q does not mention %s", tc.name, msg, want)
			}
		}
	}
	t.Setenv("DAGNATS_NATS_CLUSTER_NAME", "eu")
	_, _, err := ConfigWithPath(writeConfigFile(t, "region: us\n"))
	if err == nil || !strings.Contains(err.Error(), "nats_cluster_name") {
		t.Fatalf("env cluster name conflict: err = %v, want nats_cluster_name error", err)
	}
}

func TestRegionConfig_RejectsNonToken(t *testing.T) {
	for _, value := range []string{"us.east", "us*", "us east"} {
		_, _, err := ConfigWithPath(writeConfigFile(t, "region: "+value+"\n"))
		if err == nil || !strings.Contains(err.Error(), "region") {
			t.Errorf("region %q: err = %v, want a region error", value, err)
		}
	}
	if err := validateRegion("us"); err != nil {
		t.Fatalf("region us rejected: %v", err)
	}
}
