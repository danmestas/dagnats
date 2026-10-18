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

func TestSetupAll_StandaloneNoOptionUsesR1(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	// Verify a known stream exists at R=1.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := s.CachedInfo().Config.Replicas; got != 1 {
		t.Errorf("Replicas = %d, want 1", got)
	}
}

func TestSetupAll_WithClusterOverrideHonored(t *testing.T) {
	_, nc := StartTestServer(t)
	// Standalone single node + WithCluster(ReplicasOverride=1) — verifies
	// override path doesn't break standalone (no actual cluster needed for R=1).
	err := SetupAll(nc, WithCluster(ClusterOptions{ReplicasOverride: 1}))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
}

// hangOnceJS blocks its first AccountInfo until the caller's context ends,
// like a clustered server that drops $JS.API.INFO before a meta leader is
// elected; later calls succeed.
type hangOnceJS struct {
	jetstream.JetStream
	calls int
}

func (h *hangOnceJS) AccountInfo(
	ctx context.Context,
) (*jetstream.AccountInfo, error) {
	h.calls++
	if h.calls == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &jetstream.AccountInfo{}, nil
}

// TestWaitForClusterQuorum_RetriesHungProbe guards the root cause of
// multi-process clusters never forming: one unanswered AccountInfo used the
// whole quorum budget, so the poll loop never got a second attempt.
func TestWaitForClusterQuorum_RetriesHungProbe(t *testing.T) {
	js := &hangOnceJS{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := WaitForClusterQuorum(ctx, js, 3); err != nil {
		t.Fatalf("WaitForClusterQuorum: %v (calls=%d)", err, js.calls)
	}
	if js.calls < 2 {
		t.Errorf("calls = %d, want a retry after the hung probe", js.calls)
	}
}
