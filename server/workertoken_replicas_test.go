// Methodology: real in-process 3-node cluster. The bucket is provisioned
// at R3 (as natsutil.SetupAll does), then Open runs as a node start would.
// Asserts Open binds to the existing bucket instead of resetting it to R1,
// the regression that silently dropped HA for worker_tokens in clusters.
package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOpenKeepsClusterReplicas(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "worker_tokens", History: 1, Replicas: 3,
	})
	if err != nil {
		t.Fatalf("create bucket at R3: %v", err)
	}
	store, err := workertoken.Open(ctx, js)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)

	stream, err := js.Stream(ctx, "KV_worker_tokens")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := stream.CachedInfo().Config.Replicas; got != 3 {
		t.Errorf("replicas = %d after Open, want 3 (Open reset it)", got)
	}
}
