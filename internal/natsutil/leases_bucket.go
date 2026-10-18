package natsutil

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// LeasesBucket is the KV bucket backing coordination leases (ADR-024).
// One key per lease; the key's KV revision is the fencing token.
const LeasesBucket = "leases"

// LeaseChangedSubjectPrefix is where the server republishes every write
// to the leases bucket: dagnats.lease.changed.<key>. Republish is done by
// the stream leader only, so a cluster emits each change once, not once
// per dagnats node -- which is why no dagnats-side watcher exists.
const LeaseChangedSubjectPrefix = "dagnats.lease.changed"

// LeaseMarkerTTL is how long a released or expired lease's delete marker
// survives. The marker is what makes expiry observable (it is the
// "expired" event), and it ages out so dead keys do not accumulate.
const LeaseMarkerTTL = time.Minute

// leaseValueSizeMax bounds one lease record. The value is a small JSON
// object (holder, acquired_at, ttl_ms); anything larger is a bug.
const leaseValueSizeMax = 1024

// leasesBucketConfig is the leases bucket definition. LimitMarkerTTL
// turns on per-message TTL (so the server, not the holder's clock,
// enforces expiry) and server-written delete markers on expiry.
func leasesBucketConfig(replicas int) jetstream.KeyValueConfig {
	if replicas != 1 && replicas != 3 && replicas != 5 {
		panic(fmt.Sprintf(
			"leasesBucketConfig: replicas must be 1, 3, or 5; got %d",
			replicas,
		))
	}
	cfg := jetstream.KeyValueConfig{
		Bucket:         LeasesBucket,
		History:        1,
		MaxValueSize:   leaseValueSizeMax,
		LimitMarkerTTL: LeaseMarkerTTL,
		Replicas:       replicas,
		RePublish: &jetstream.RePublish{
			Source:      "$KV." + LeasesBucket + ".>",
			Destination: LeaseChangedSubjectPrefix + ".>",
		},
	}
	if cfg.LimitMarkerTTL < time.Second {
		panic("leasesBucketConfig: marker TTL below the 1s server minimum")
	}
	return cfg
}
