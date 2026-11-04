// server/region_config.go
// The region key: one name for a regional cluster that becomes both its
// NATS cluster name and its JetStream domain (ADR-031), so the two cannot
// drift apart.
package server

import (
	"fmt"
	"os"
)

// applyRegion applies DAGNATS_REGION, then defaults nats_cluster_name and
// jetstream_domain to the region. Either key set explicitly to another
// value is an error: a node whose cluster name and domain disagree joins
// one region's routes but answers the other region's JetStream API.
func applyRegion(cfg *Config) error {
	if cfg == nil {
		panic("applyRegion: cfg is nil")
	}
	if val := os.Getenv("DAGNATS_REGION"); val != "" {
		cfg.Region = val
	}
	if cfg.Region == "" {
		return nil
	}
	if err := validateRegion(cfg.Region); err != nil {
		return err
	}
	keys := []struct {
		name  string
		value *string
	}{
		{"nats_cluster_name", &cfg.NATSClusterName},
		{"jetstream_domain", &cfg.JetStreamDomain},
	}
	for _, key := range keys {
		if *key.value != "" && *key.value != cfg.Region {
			return fmt.Errorf("region %q conflicts with %s %q: a region is "+
				"both the cluster name and the JetStream domain; unset %s "+
				"or set it to %q", cfg.Region, key.name, *key.value,
				key.name, cfg.Region)
		}
		*key.value = cfg.Region
	}
	if cfg.NATSClusterName != cfg.Region || cfg.JetStreamDomain != cfg.Region {
		panic("applyRegion: region not applied to both keys")
	}
	return nil
}

// validateRegion accepts a single NATS subject token, since the region
// becomes the domain token in $JS.<domain>.API.
func validateRegion(region string) error {
	return validateSubjectToken("region", region)
}
