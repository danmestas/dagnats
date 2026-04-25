package natsutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestDeriveReplicas(t *testing.T) {
	cases := []struct {
		name     string
		routes   []string
		override int
		want     int
	}{
		{"no routes, no override", nil, 0, 1},
		{"no routes, override 3", nil, 3, 3},
		{"3-node cluster (2 routes), auto", []string{"a", "b"}, 0, 3},
		{"4-node cluster (3 routes), auto rounds down", []string{"a", "b", "c"}, 0, 3},
		{"5-node cluster (4 routes), auto", []string{"a", "b", "c", "d"}, 0, 5},
		{"6-node cluster (5 routes), auto caps at 5", []string{"a", "b", "c", "d", "e"}, 0, 5},
		{"override beats auto", []string{"a", "b", "c", "d"}, 3, 3},
		{"override 1 in cluster", []string{"a", "b"}, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveReplicas(tc.routes, tc.override)
			if got != tc.want {
				t.Errorf("DeriveReplicas(%v, %d) = %d, want %d",
					tc.routes, tc.override, got, tc.want)
			}
		})
	}
}

func TestWaitForClusterQuorum_StandaloneSucceedsImmediately(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Single-node cluster (expectedSize=1) should report ready immediately.
	elapsed, err := WaitForClusterQuorum(ctx, js, 1)
	if err != nil {
		t.Fatalf("WaitForClusterQuorum: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("standalone quorum took %v, want <1s", elapsed)
	}
}

// TestWaitForClusterQuorum_TimesOut verifies the ctx-cancellation branch by
// passing a pre-cancelled context. We use a pre-cancelled context (rather
// than waiting for a real deadline against a 1-node server with
// expectedSize=3) because the conservative jsClusterReady check returns
// true once AccountInfo succeeds — it cannot query peer count via the
// public jetstream.JetStream API. Full peer-count verification lives in
// the cluster integration tests in a later commit.
func TestWaitForClusterQuorum_TimesOut(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel to force the ctx.Done() path

	_, err = WaitForClusterQuorum(ctx, js, 3)
	if err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
