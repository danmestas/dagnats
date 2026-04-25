package natsutil

// DeriveReplicas computes the JetStream replication factor for streams
// and KV buckets given the cluster route list and an optional explicit
// override.
//
// When override > 0, returns it as-is (caller is responsible for
// validating the override against {1, 3, 5} at config-load time).
//
// When override == 0, auto-derives from cluster size:
//   - len(routes) == 0 (standalone or leaf) -> 1
//   - cluster size >= 5                     -> 5
//   - cluster size >= 3                     -> 3 (rounds 4 down to 3)
//
// Cluster size is len(routes) + 1 (peers + self).
func DeriveReplicas(routes []string, override int) int {
	if override > 0 {
		return override
	}
	if len(routes) == 0 {
		return 1
	}
	clusterSize := len(routes) + 1
	if clusterSize >= 5 {
		return 5
	}
	if clusterSize >= 3 {
		return 3
	}
	return 1 // 2-node cluster falls back; validation should prevent this
}
