// Methodology: real embedded NATS server per test, KV bucket created
// with jetstream's own CreateKeyValue (so AllowDirect is set exactly as
// in production). Each case writes through the KeyValue API and reads
// back through KVGetLeader, asserting value, revision, and the
// not-found mapping for missing, deleted, and purged keys. Leader
// routing itself (vs a lagging replica) is not observable on one node;
// the read path is the same STREAM.MSG.GET request lease reads use.
package natsutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestKVGetLeader(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "leader_read"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	if _, err := kv.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	rev, err := kv.Put(ctx, "k", []byte("v2"))
	if err != nil {
		t.Fatalf("put v2: %v", err)
	}
	got, err := KVGetLeader(ctx, nc, "leader_read", "k")
	if err != nil || string(got.Value) != "v2" || got.Revision != rev {
		t.Fatalf("got %q@%d err=%v, want v2@%d", got.Value, got.Revision, err, rev)
	}
	_, err = KVGetLeader(ctx, nc, "leader_read", "missing")
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("missing key: err=%v, want ErrKeyNotFound", err)
	}
	if err := kv.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = KVGetLeader(ctx, nc, "leader_read", "k")
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("deleted key: err=%v, want ErrKeyNotFound", err)
	}
	if _, err := kv.Put(ctx, "p", []byte("x")); err != nil {
		t.Fatalf("put p: %v", err)
	}
	if err := kv.Purge(ctx, "p"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	_, err = KVGetLeader(ctx, nc, "leader_read", "p")
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("purged key: err=%v, want ErrKeyNotFound", err)
	}
}

func TestKVGetLeader_MissingBucketIsAnError(t *testing.T) {
	_, nc := StartTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := KVGetLeader(ctx, nc, "no_such_bucket", "k")
	if err == nil {
		t.Fatal("read from a missing bucket must fail")
	}
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("missing bucket must not look like a missing key: %v", err)
	}
}
