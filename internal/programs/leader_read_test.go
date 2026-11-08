// leader_read_test.go
// Methodology: real embedded NATS. A direct get on a replicated bucket
// may be answered by any replica, including one that has not applied a
// write already acknowledged; reading a pending record back that way
// right after the confirm promoted it told program.Run "confirmation
// still pending" (lode's first start on the eu mesh). The registrar must
// read its records through the stream leader. Which API a read used is
// observable on one server: a plain subscriber on the API subjects sees
// every request. Positive: register, confirm and lookup read through
// STREAM.MSG.GET. Negative: none of them sends a DIRECT.GET for the
// programs bucket.
package programs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestRegistrar_ReadsRecordsThroughTheStreamLeader(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	var direct, leader atomic.Int64
	count := func(n *atomic.Int64) nats.MsgHandler {
		return func(*nats.Msg) { n.Add(1) }
	}
	for subject, n := range map[string]*atomic.Int64{
		"$JS.API.DIRECT.GET.KV_" + BucketName:        &direct,
		"$JS.API.DIRECT.GET.KV_" + BucketName + ".>": &direct,
		"$JS.API.STREAM.MSG.GET.KV_" + BucketName:    &leader,
	} {
		sub, err := f.nc.Subscribe(subject, count(n))
		if err != nil {
			t.Fatalf("subscribe %s: %v", subject, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	if err := f.nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	req := request(t, "leaderread", "")
	if _, err := f.register(t, req); err != nil {
		t.Fatalf("register and confirm: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, found, err := f.registrar.lookup(ctx, req.Manifest.Name)
	if err != nil || !found || record.Pending {
		t.Fatalf("lookup after confirm: found=%v pending=%v err=%v", found, record.Pending, err)
	}
	if err := f.nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if n := direct.Load(); n != 0 {
		t.Fatalf("%d direct gets on the programs bucket, want 0", n)
	}
	if leader.Load() == 0 {
		t.Fatal("no stream-leader read of the programs bucket")
	}
}
