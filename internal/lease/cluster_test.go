// lease/cluster_test.go
// Methodology: the lease protocol on a real 3-node embedded cluster with
// the bucket at R3, the topology cluster mode provisions. R3 matters for
// two reasons a single node cannot show: CAS conflicts come back as
// 10164 rather than 10071, and reads must be routed to the stream
// leader. External test package because dagnatstest imports the api
// package, which imports this one.
package lease_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/lease"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestLeaseLifecycleOnR3Cluster(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	routes := []string{"a", "b"} // only len matters for replica derivation
	if err := natsutil.SetupAll(
		nc, natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_"+natsutil.LeasesBucket)
	if err != nil || stream.CachedInfo().Config.Replicas != 3 {
		t.Fatalf("leases bucket must be R3 in cluster mode: err=%v", err)
	}

	store := lease.NewStore(nc)
	held, granted, err := store.Acquire(ctx, "leader", "node-a", time.Second)
	if err != nil || !granted {
		t.Fatalf("acquire: granted=%v err=%v", granted, err)
	}
	_, granted, err = store.Acquire(ctx, "leader", "node-b", time.Second)
	if granted || err != nil {
		t.Fatalf("contender must be denied while node-a holds: err=%v", err)
	}
	renewed, err := store.Renew(ctx, "leader", "node-a", held.Revision, time.Second)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	_, err = store.Renew(ctx, "leader", "node-a", held.Revision, time.Second)
	if !errors.Is(err, lease.ErrLost) {
		t.Fatalf("stale renew on R3 (10164): err=%v, want ErrLost", err)
	}

	next := acquireAfterExpiry(t, ctx, store)
	if next.Revision <= renewed.Revision {
		t.Fatalf("fence must advance: %d <= %d", next.Revision, renewed.Revision)
	}
	again, granted, err := store.Acquire(ctx, "leader", "node-a", time.Second)
	if err != nil || granted || again.Holder != "node-b" {
		t.Fatalf("old holder re-acquire: %+v granted=%v err=%v, want denied",
			again, granted, err)
	}
}

// acquireAfterExpiry polls (bounded) until node-b wins the lease that
// node-a let lapse.
func acquireAfterExpiry(t *testing.T, ctx context.Context, store *lease.Store) lease.Lease {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		l, granted, err := store.Acquire(ctx, "leader", "node-b", 10*time.Second)
		if err != nil {
			t.Fatalf("acquire after expiry: %v", err)
		}
		if granted {
			return l
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("lease did not expire within 10s")
	return lease.Lease{}
}
