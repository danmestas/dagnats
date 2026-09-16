// store_loadall_test.go
// Tests for Store.loadAll's #698 migration to natsutil.ListKeys:
// a deleted token must not resurrect into the cache, and a per-key Get
// failure that is NOT ErrKeyNotFound must fail Open loudly rather than
// silently warm a short cache. Methodology: the deleted-key case runs
// against a real embedded NATS server (loadAll's enumeration source is
// the bucket's live subject state); the Get-error case is a
// deterministic unit test against a stub jetstream.KeyValue/Stream
// pair, mirroring worker/directory_list_concurrency_test.go's stubKV.
package workertoken

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestLoadAllSkipsDeletedKeys pins that a token deleted before a fresh
// Store opens does not appear in that Store's cache, even though its
// subject still carries a delete marker and so still surfaces from
// natsutil.ListKeys' enumeration.
func TestLoadAllSkipsDeletedKeys(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seed, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open seed: %v", err)
	}
	goneID, _, err := seed.Mint(ctx, "gone", []string{"echo"}, nil, "tester")
	if err != nil {
		t.Fatalf("Mint gone: %v", err)
	}
	keptID, _, err := seed.Mint(ctx, "kept", []string{"echo"}, nil, "tester")
	if err != nil {
		t.Fatalf("Mint kept: %v", err)
	}
	if err := seed.kv.Delete(ctx, goneID); err != nil {
		t.Fatalf("Delete gone: %v", err)
	}
	seed.Close()

	fresh, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open fresh: %v", err)
	}
	defer fresh.Close()
	if _, ok := fresh.Lookup(goneID); ok {
		t.Fatalf("loadAll resurrected deleted token %q", goneID)
	}
	if _, ok := fresh.Lookup(keptID); !ok {
		t.Fatalf("loadAll dropped live token %q", keptID)
	}
}

// errLoadAllGetBroke stands in for a transient KV Get failure that is
// not a missing key.
var errLoadAllGetBroke = errors.New("kv get broke")

// stubLoadAllKV serves Bucket() and fails every Get with a
// non-NotFound error.
type stubLoadAllKV struct {
	jetstream.KeyValue
}

func (stubLoadAllKV) Bucket() string { return bucketName }

func (stubLoadAllKV) Get(
	_ context.Context, _ string,
) (jetstream.KeyValueEntry, error) {
	return nil, errLoadAllGetBroke
}

// stubLoadAllStream reports one subject so loadAll has exactly one key
// to Get.
type stubLoadAllStream struct {
	jetstream.Stream
}

func (stubLoadAllStream) Info(
	_ context.Context, _ ...jetstream.StreamInfoOpt,
) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{
		State: jetstream.StreamState{
			Subjects: map[string]uint64{"$KV." + bucketName + ".t1": 1},
		},
	}, nil
}

// TestLoadAllPropagatesGetError pins that a Get failure which is NOT
// ErrKeyNotFound fails loadAll (and so Open) instead of being
// swallowed into a short cache -- the exact failure mode the original
// blanket "continue // deleted between ListKeys and Get" would have
// reintroduced under natsutil.ListKeys, which (unlike kv.ListKeys) also
// surfaces delete-marker subjects.
func TestLoadAllPropagatesGetError(t *testing.T) {
	s := &Store{
		kv:     stubLoadAllKV{},
		stream: stubLoadAllStream{},
		tokens: make(map[string]Token),
	}
	err := s.loadAll(context.Background())
	if !errors.Is(err, errLoadAllGetBroke) {
		t.Fatalf("loadAll error = %v, want %v", err, errLoadAllGetBroke)
	}
	// Negative space: a propagated failure must not also leave a
	// partial cache a caller might read from.
	if len(s.tokens) != 0 {
		t.Fatalf("loadAll tokens = %+v, want empty on error", s.tokens)
	}
}
