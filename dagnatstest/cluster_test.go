package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStartTestCluster_3Nodes(t *testing.T) {
	nc := StartTestCluster(t, 3)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := js.AccountInfo(ctx)
	if err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	if info == nil {
		t.Fatal("AccountInfo nil")
	}
	if info.API.Errors > 0 {
		t.Errorf("API.Errors = %d, want 0", info.API.Errors)
	}
}
