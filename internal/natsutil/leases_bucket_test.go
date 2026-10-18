// natsutil/leases_bucket_test.go
// Methodology: SetupAll against a real embedded NATS server, then read
// the provisioned leases bucket's stream config back from the server.
// The lease protocol (ADR-024) depends on three server-side settings --
// per-message TTL, delete markers on expiry, and the republish of every
// change onto dagnats.lease.changed.> -- so each is asserted directly
// rather than inferred from behavior (behavior lives in internal/lease).
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSetupAllCreatesLeasesBucket(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()

	err := SetupAll(nc)
	assert(t, err == nil, "SetupAll must succeed: %v", err)
	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New must succeed: %v", err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "KV_"+LeasesBucket)
	assert(t, err == nil, "leases bucket stream must exist: %v", err)
	cfg := stream.CachedInfo().Config

	assert(t, cfg.AllowMsgTTL, "leases bucket must allow per-key TTL")
	assert(t, cfg.SubjectDeleteMarkerTTL == LeaseMarkerTTL,
		"expiry must leave a delete marker for %v, got %v",
		LeaseMarkerTTL, cfg.SubjectDeleteMarkerTTL)
	assert(t, cfg.MaxMsgsPerSubject == 1,
		"leases keep only the current revision, got history %d",
		cfg.MaxMsgsPerSubject)
	assert(t, cfg.RePublish != nil, "leases bucket must republish changes")
	assert(t, cfg.RePublish.Destination == LeaseChangedSubjectPrefix+".>",
		"republish destination = %q", cfg.RePublish.Destination)
	assert(t, !cfg.RePublish.HeadersOnly,
		"republished events must carry the lease value")
}
