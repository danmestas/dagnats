// Methodology: provision a real embedded server with SetupAll, ask
// JetStream what actually exists, and compare against the exported name
// lists. The lists feed /health/cluster; this test is the only thing
// keeping them honest, so it must fail on drift in either direction.
package natsutil

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestProvisionedNamesMatchSetupAll fails if SetupAll creates a stream or
// KV bucket that the exported lists omit, or the lists name one it never
// creates.
func TestProvisionedNamesMatchSetupAll(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var gotStreams []string
	streams := js.StreamNames(ctx)
	for n := range streams.Name() {
		// KV buckets are backed by KV_<bucket> streams; compared below.
		if !strings.HasPrefix(n, "KV_") {
			gotStreams = append(gotStreams, n)
		}
	}
	if err := streams.Err(); err != nil {
		t.Fatalf("StreamNames: %v", err)
	}
	var gotKV []string
	kvNames := js.KeyValueStoreNames(ctx)
	for n := range kvNames.Name() {
		gotKV = append(gotKV, n)
	}

	wantStreams := ProvisionedStreamNames()
	wantKV := ProvisionedKVBucketNames()
	slices.Sort(gotStreams)
	slices.Sort(gotKV)
	slices.Sort(wantStreams)
	slices.Sort(wantKV)
	if !slices.Equal(gotStreams, wantStreams) {
		t.Errorf("streams drifted:\n created %v\n listed  %v", gotStreams, wantStreams)
	}
	if !slices.Equal(gotKV, wantKV) {
		t.Errorf("kv buckets drifted:\n created %v\n listed  %v", gotKV, wantKV)
	}
}

// TestSetupAllFailsFastWithoutJetStream guards the standalone posture: a
// server with JetStream disabled must error immediately, not sit in a
// retry budget sized for cluster elections.
func TestSetupAllFailsFastWithoutJetStream(t *testing.T) {
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	start := time.Now()
	if err := SetupAll(nc); err == nil {
		t.Fatal("SetupAll succeeded without JetStream")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("SetupAll took %v, want fast failure", elapsed)
	}
}
