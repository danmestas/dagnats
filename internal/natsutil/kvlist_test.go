// internal/natsutil/kvlist_test.go
// Tests for ListKeys (#698): consistent KV key enumeration from a
// bucket's backing stream subject state, moved here from
// worker/directory.go's private listKeys (#699) so every name-keyed
// registry can share it. Methodology: integration tests against a real
// embedded NATS server for the concurrency probe and the dotted-key
// contract, plus deterministic unit tests against stub jetstream.Stream
// implementations for the delete-marker and bound-exceeded contracts.
// Bounded 5s timeouts throughout.
package natsutil

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvlistConcurrencyPuts bounds the probe. Mirrors
// worker/directory_list_concurrency_test.go's listConcurrencyReads
// rationale: a statistical regression probe, not a guarantee -- it
// cannot fail against correct code, but only catches a reintroduction
// of the watcher-snapshot race with high probability.
const kvlistConcurrencyReads = 1500

// kvlistConcurrencyWriters is the number of goroutines replaying a Put
// against the hot key while ListKeys is read concurrently.
const kvlistConcurrencyWriters = 4

func TestListKeysNeverMissesLiveKeyUnderConcurrentPuts(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kvlist_probe",
		History: 1,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_kvlist_probe")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := kv.Put(ctx, "hot", []byte("1")); err != nil {
		t.Fatalf("seed Put hot: %v", err)
	}
	if _, err := kv.Put(ctx, "quiet", []byte("1")); err != nil {
		t.Fatalf("seed Put quiet: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(kvlistConcurrencyWriters)
	for range kvlistConcurrencyWriters {
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := kv.Put(ctx, "hot", []byte("2")); err != nil {
					return
				}
			}
		}()
	}

	hotMisses, quietMisses, empties := 0, 0, 0
	for range kvlistConcurrencyReads {
		keys, err := ListKeys(ctx, stream, "kvlist_probe")
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("ListKeys: %v", err)
		}
		if len(keys) == 0 {
			empties++
		}
		if !containsKey(keys, "hot") {
			hotMisses++
		}
		if !containsKey(keys, "quiet") {
			quietMisses++
		}
	}
	stop.Store(true)
	wg.Wait()

	// Positive space: both keys are Put-registered and never deleted,
	// so every ListKeys call must report both. Negative space: a race
	// on "hot" must not drop the untouched "quiet" key, and no call may
	// come back empty while two keys exist.
	if hotMisses != 0 {
		t.Fatalf("ListKeys missed live key %q %d/%d times", "hot", hotMisses, kvlistConcurrencyReads)
	}
	if quietMisses != 0 {
		t.Fatalf("ListKeys missed untouched key %q %d/%d times", "quiet", quietMisses, kvlistConcurrencyReads)
	}
	if empties != 0 {
		t.Fatalf("ListKeys returned empty %d/%d times", empties, kvlistConcurrencyReads)
	}
}

// TestListKeysEnumeratesDottedKeys pins that a key spanning several
// subject tokens is recovered by stripping the bucket prefix, not by
// taking the last token.
func TestListKeysEnumeratesDottedKeys(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kvlist_dotted",
		History: 1,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_kvlist_dotted")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	ids := []string{"plain", "host.example.com", "a.b.c.d"}
	for _, id := range ids {
		if _, err := kv.Put(ctx, id, []byte("1")); err != nil {
			t.Fatalf("Put(%q): %v", id, err)
		}
	}
	keys, err := ListKeys(ctx, stream, "kvlist_dotted")
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	for _, id := range ids {
		if !containsKey(keys, id) {
			t.Fatalf("ListKeys missing %q, got %v", id, keys)
		}
	}
	// Negative space: no key may still carry the bucket subject prefix.
	for _, k := range keys {
		if strings.HasPrefix(k, "$KV.") {
			t.Fatalf("ListKeys returned un-stripped subject %q", k)
		}
	}
	if len(keys) != len(ids) {
		t.Fatalf("ListKeys = %d keys, want %d", len(keys), len(ids))
	}
}

// TestListKeysIncludesDeleteMarkerSubjects pins the hazard callers
// must handle: a deleted key's subject still holds its delete marker
// and so still appears in ListKeys' output -- the caller distinguishes
// it with a per-key Get that checks for jetstream.ErrKeyNotFound, the
// same way worker.Directory.List() and every migrated call site does.
func TestListKeysIncludesDeleteMarkerSubjects(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kvlist_deleted",
		History: 1,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_kvlist_deleted")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := kv.Put(ctx, "gone", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := kv.Delete(ctx, "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	keys, err := ListKeys(ctx, stream, "kvlist_deleted")
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if !containsKey(keys, "gone") {
		t.Fatalf("ListKeys must still report the delete-marker subject %q, got %v", "gone", keys)
	}
	if _, err := kv.Get(ctx, "gone"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("Get(gone) error = %v, want ErrKeyNotFound", err)
	}
}

// stubKVListStream reports a fixed, fully-controlled subject set so the
// bound-exceeded path can be pinned deterministically.
type stubKVListStream struct {
	jetstream.Stream
	subjects map[string]uint64
	infoErr  error
}

func (s stubKVListStream) Info(
	_ context.Context, _ ...jetstream.StreamInfoOpt,
) (*jetstream.StreamInfo, error) {
	if s.infoErr != nil {
		return nil, s.infoErr
	}
	return &jetstream.StreamInfo{
		State: jetstream.StreamState{Subjects: s.subjects},
	}, nil
}

// TestListKeysPropagatesInfoError pins that a stream Info failure
// (timeout, disconnect) is returned to the caller rather than
// swallowed into an empty list.
func TestListKeysPropagatesInfoError(t *testing.T) {
	errBroke := errors.New("stream info broke")
	stream := stubKVListStream{infoErr: errBroke}
	keys, err := ListKeys(context.Background(), stream, "b")
	if !errors.Is(err, errBroke) {
		t.Fatalf("ListKeys error = %v, want %v", err, errBroke)
	}
	if keys != nil {
		t.Fatalf("ListKeys keys = %v, want nil on error", keys)
	}
}

// containsKey reports whether keys holds key.
func containsKey(keys []string, key string) bool {
	if key == "" {
		panic("containsKey: key must not be empty")
	}
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
