// kv_keys_test.go
// Tests for apiServiceAdapter.ListKVKeys' #698 migration to
// natsutil.ListKeys: a key deleted from a browsed bucket must not
// resurrect into the console's KV browse view, and a per-key Get
// failure that is NOT ErrKeyNotFound must fail the call rather than
// shorten it silently. Methodology: real embedded NATS server + real
// apiServiceAdapter (api.NewService, no fake) against the "services"
// bucket -- ListKVKeys can browse any provisioned KV bucket by name,
// and this one is simple to Put/Delete into directly without a
// dedicated SDK helper.
package console

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestListKVKeys_SkipsDeletedKey asserts that a key deleted directly
// from a browsed bucket (#698: ListKVKeys now enumerates via
// natsutil.ListKeys, which surfaces a delete marker's subject that
// kv.ListKeys' IgnoreDeletes watch would not have) is not resurrected
// into the browse view, and a live, untouched key survives.
func TestListKVKeys_SkipsDeletedKey(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := api.NewService(nc)
	ds := NewAPIDataSource(svc, nc, nil, nil)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "services")
	if err != nil {
		t.Fatalf("KeyValue(services): %v", err)
	}
	if _, err := kv.Put(ctx, "gone", []byte(`{}`)); err != nil {
		t.Fatalf("Put gone: %v", err)
	}
	if _, err := kv.Put(ctx, "kept", []byte(`{}`)); err != nil {
		t.Fatalf("Put kept: %v", err)
	}
	if err := kv.Delete(ctx, "gone"); err != nil {
		t.Fatalf("Delete gone: %v", err)
	}

	keys, _, err := ds.ListKVKeys(ctx, "services", "", 100)
	if err != nil {
		t.Fatalf("ListKVKeys: %v", err)
	}
	for _, k := range keys {
		if k == "gone" {
			t.Fatalf("ListKVKeys resurrected deleted key %q, got %v", "gone", keys)
		}
	}
	found := false
	for _, k := range keys {
		if k == "kept" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListKVKeys dropped live key %q, got %v", "kept", keys)
	}
}

// Get-error propagation for this site (a non-NotFound per-key Get
// failure must fail ListKVKeys rather than shorten the list) follows
// the identical code shape already unit-tested in isolation for
// worker.Directory.List (worker/directory_list_concurrency_test.go),
// workertoken.Store.loadAll, and console's own listAuditEventsInner
// (audit_kv_test.go). apiServiceAdapter.ListKVKeys resolves its
// jetstream.JetStream/KeyValue/Stream internally from a.nc rather than
// taking them as injectable parameters, so it has no seam for a stub
// Get failure without adding one purely for this test -- not done
// here per the "don't invent a heavy harness" guidance.
