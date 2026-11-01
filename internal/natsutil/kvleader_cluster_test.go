// Methodology: read-your-writes for workflow_defs on a real 3-node
// embedded cluster with the bucket at R3, as cluster mode provisions
// it. A key is written through one connection and read back at once
// through a connection to each node in turn. jetstream's KeyValue.Get
// is a direct get that any replica may answer, and a follower that has
// not applied the write answers not-found (measured: 1 to 11 misses
// per 1000 per node). LeaderReadKV must never miss. External test
// package: dagnatstest imports natsutil.
package natsutil_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const readYourWritesPerNode = 1000

func TestLeaderReadKV_ReadsYourWritesOnEveryNode(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	if err := natsutil.SetupAll(nc, natsutil.WithCluster(
		natsutil.ClusterOptions{Routes: []string{"a", "b"}},
	)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	writer, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if writer.Bucket() != "workflow_defs" {
		t.Fatal("bound the wrong bucket")
	}
	servers := nc.Servers()
	if len(servers) != 3 {
		t.Fatalf("servers = %v, want 3", servers)
	}
	for _, url := range servers {
		readFromNode(ctx, t, writer, url)
	}
}

// readFromNode writes keys through writer and reads each back through
// a leader-read bucket on a connection to url.
func readFromNode(ctx context.Context, t *testing.T, writer jetstream.KeyValue, url string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	bound, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	reader := natsutil.LeaderReadKV(nc, bound)
	misses := 0
	for i := 0; i < readYourWritesPerNode; i++ {
		key := fmt.Sprintf("wf-%s-%d", nc.ConnectedServerName(), i)
		rev, err := writer.Put(ctx, key, []byte("def"))
		if err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		entry, err := reader.Get(ctx, key)
		if err != nil || entry.Revision() != rev {
			misses++
		}
	}
	if misses != 0 {
		t.Fatalf("reader on %s: %d of %d reads missed a write already acknowledged",
			nc.ConnectedServerName(), misses, readYourWritesPerNode)
	}
}
