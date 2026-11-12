// Methodology: real embedded NATS server per test. A plain stream is
// written with jetstream's own Publish and read back through
// StreamMsgLeader with the two STREAM.MSG.GET shapes callers use
// (seq + next_by_subj, last_by_subj), asserting the message, its
// sequence and the nil answer when nothing matches. A KV bucket checks
// that KVGetLeaderOrMarker tells a deleted or purged key from one that
// was never written, which KVGetLeader deliberately does not (#1006).
package natsutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamMsgLeader(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "LEADER_READ", Subjects: []string{"lr.>"},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	for _, msg := range []struct{ subject, data string }{
		{"lr.a", "a1"}, {"lr.b", "b1"}, {"lr.a", "a2"},
	} {
		if _, err := js.Publish(ctx, msg.subject, []byte(msg.data)); err != nil {
			t.Fatalf("publish %s: %v", msg.subject, err)
		}
	}
	first, err := StreamMsgLeader(ctx, nc, "LEADER_READ",
		map[string]any{"seq": 1, "next_by_subj": "lr.a"})
	if err != nil || first == nil || string(first.Data) != "a1" || first.Sequence != 1 {
		t.Fatalf("first lr.a = %+v err=%v, want a1@1", first, err)
	}
	next, err := StreamMsgLeader(ctx, nc, "LEADER_READ",
		map[string]any{"seq": first.Sequence + 1, "next_by_subj": "lr.a"})
	if err != nil || next == nil || string(next.Data) != "a2" || next.Sequence != 3 {
		t.Fatalf("next lr.a = %+v err=%v, want a2@3", next, err)
	}
	last, err := StreamMsgLeader(ctx, nc, "LEADER_READ",
		map[string]any{"last_by_subj": "lr.b"})
	if err != nil || last == nil || string(last.Data) != "b1" {
		t.Fatalf("last lr.b = %+v err=%v, want b1", last, err)
	}
	none, err := StreamMsgLeader(ctx, nc, "LEADER_READ",
		map[string]any{"seq": next.Sequence + 1, "next_by_subj": "lr.a"})
	if err != nil || none != nil {
		t.Fatalf("past the end = %+v err=%v, want nil, nil", none, err)
	}
	if _, err := StreamMsgLeader(ctx, nc, "NO_SUCH_STREAM",
		map[string]any{"last_by_subj": "lr.a"}); err == nil {
		t.Fatal("a read from a missing stream must fail, not look empty")
	}
}

func TestKVGetLeaderOrMarker(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "marker_read"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	for _, key := range []string{"live", "deleted", "purged"} {
		if _, err := kv.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := kv.Delete(ctx, "deleted"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := kv.Purge(ctx, "purged"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	entry, deleted, err := KVGetLeaderOrMarker(ctx, nc, "marker_read", "live")
	if err != nil || deleted || string(entry.Value) != "v" {
		t.Fatalf("live = %q deleted=%v err=%v", entry.Value, deleted, err)
	}
	for _, key := range []string{"deleted", "purged"} {
		_, deleted, err := KVGetLeaderOrMarker(ctx, nc, "marker_read", key)
		if !errors.Is(err, jetstream.ErrKeyNotFound) || !deleted {
			t.Errorf("%s: deleted=%v err=%v, want true, ErrKeyNotFound", key, deleted, err)
		}
	}
	_, deleted, err = KVGetLeaderOrMarker(ctx, nc, "marker_read", "never")
	if !errors.Is(err, jetstream.ErrKeyNotFound) || deleted {
		t.Fatalf("never written: deleted=%v err=%v, want false, ErrKeyNotFound", deleted, err)
	}
	// A wildcard key would match every key in last_by_subj: refused.
	for _, key := range []string{"*", ">"} {
		got, err := KVGetLeader(ctx, nc, "marker_read", key)
		if !errors.Is(err, jetstream.ErrInvalidKey) {
			t.Errorf("KVGetLeader(%q) = %q err=%v, want ErrInvalidKey", key, got.Value, err)
		}
	}
}
