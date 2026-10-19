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
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// oldServerJS stands in for a pre-2.11 server: nats.go refuses a
// LimitMarkerTTL bucket client-side with err. Only the methods the
// provisioner calls are implemented; anything else panics (nil embed).
type oldServerJS struct {
	jetstream.JetStream
	err error
}

// Stream reports no existing bucket stream, so the provisioner has no
// replica count to keep and goes straight to the create.
func (f oldServerJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}

func (f oldServerJS) CreateOrUpdateKeyValue(
	context.Context, jetstream.KeyValueConfig,
) (jetstream.KeyValue, error) {
	return nil, f.err
}

func TestLeasesBucketSkippedOnServerWithoutTTL(t *testing.T) {
	ctx := context.Background()
	old := oldServerJS{err: jetstream.ErrLimitMarkerTTLNotSupported}
	err := provisionLeasesBucket(ctx, old, 1)
	assert(t, err == nil, "pre-2.11 server must not fail setup: %v", err)

	other := errors.New("insufficient resources")
	err = provisionLeasesBucket(ctx, oldServerJS{err: other}, 1)
	assert(t, errors.Is(err, other),
		"any other provisioning error must still fail setup, got %v", err)
}

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
