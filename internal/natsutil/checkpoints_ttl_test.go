// natsutil/checkpoints_ttl_test.go
// Methodology: real embedded NATS. The checkpoints bucket holds Pause
// markers and bridge checkpoints that nothing deletes once a run ends,
// so it carries a TTL like history does. Two cases: a fresh install
// gets the TTL, and a bucket that predates it (created with no TTL, a
// value in it) is upgraded in place by the next SetupAll with its
// entries intact. Negative space: a sibling bucket without a TTL stays
// without one.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSetupAllGivesCheckpointsBucketATTL(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()
	assert(t, SetupAll(nc) == nil, "SetupAll must succeed")
	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New: %v", err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checkpoints := kvTTL(ctx, t, js, "checkpoints")
	assert(t, checkpoints == checkpointsTTL,
		"checkpoints TTL = %v, want %v", checkpoints, checkpointsTTL)
	assert(t, checkpointsTTL == 30*24*time.Hour, "checkpoints TTL must be 30 days")
	signals := kvTTL(ctx, t, js, "signals")
	assert(t, signals == 0, "signals must stay without a TTL, got %v", signals)
}

func TestSetupAllUpgradesCheckpointsBucketInPlace(t *testing.T) {
	s, nc := StartTestServer(t)
	defer s.Shutdown()
	defer nc.Close()
	js, err := jetstream.New(nc)
	assert(t, err == nil, "jetstream.New: %v", err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	legacy, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "checkpoints"})
	assert(t, err == nil, "create legacy bucket: %v", err)
	_, err = legacy.Put(ctx, "run.step", []byte(`{"kept":true}`))
	assert(t, err == nil, "put legacy entry: %v", err)
	assert(t, kvTTL(ctx, t, js, "checkpoints") == 0, "legacy bucket must start without a TTL")

	assert(t, SetupAll(nc) == nil, "SetupAll over a legacy bucket must succeed")

	got := kvTTL(ctx, t, js, "checkpoints")
	assert(t, got == checkpointsTTL, "upgraded TTL = %v, want %v", got, checkpointsTTL)
	entry, err := legacy.Get(ctx, "run.step")
	assert(t, err == nil, "legacy entry must survive the upgrade: %v", err)
	assert(t, string(entry.Value()) == `{"kept":true}`, "entry changed: %s", entry.Value())
}

func kvTTL(
	ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string,
) time.Duration {
	t.Helper()
	kv, err := js.KeyValue(ctx, bucket)
	assert(t, err == nil, "open %s: %v", bucket, err)
	status, err := kv.Status(ctx)
	assert(t, err == nil, "status %s: %v", bucket, err)
	return status.TTL()
}
