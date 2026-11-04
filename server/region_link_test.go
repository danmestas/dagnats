// region_link_test.go: two three-node regions (ADR-031), each its own
// cluster and JetStream domain, joined by leaf from every us node to every
// eu node's leaf listener. Integration tests with real embedded servers,
// none shared between tests. They check the properties the design rests
// on: each region provisions dagnats' streams on its own, a request to
// $JS.<domain>.API crosses the link in both directions, and a region keeps
// creating streams and consumers when the other region is gone.
package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// regionRoutes is any two-route list: SetupAll only counts routes to pick
// R3.
var regionRoutes = []string{"a", "b"}

func TestRegions_ProvisionIndependentlyAndReachByDomain(t *testing.T) {
	eu, us := startRegionPair(t, nil)
	for _, region := range []*testRegion{eu, us} {
		if err := natsutil.SetupAll(region.connect(t, 1),
			natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
		); err != nil {
			t.Fatalf("SetupAll in %s: %v", region.name, err)
		}
	}
	ctx := regionCtx(t)
	for _, region := range []*testRegion{eu, us} {
		assertHistoryInRegion(t, region.jetStream(t, 0, ""), region.name)
	}
	// Each region reaches the other's JetStream by domain, over the link.
	assertHistoryInRegion(t, us.jetStream(t, 2, "eu"), "eu")
	assertHistoryInRegion(t, eu.jetStream(t, 2, "us"), "us")
	// An unknown domain has no responder: requests do not leak elsewhere.
	_, err := us.jetStream(t, 0, "ap").Stream(ctx, "WORKFLOW_HISTORY")
	if err == nil {
		t.Fatal("domain ap answered; no region is named ap")
	}
}

// assertHistoryInRegion checks WORKFLOW_HISTORY, seen through js, is an R3
// stream placed in the cluster named region.
func assertHistoryInRegion(t *testing.T, js jetstream.JetStream, region string) {
	t.Helper()
	stream, err := js.Stream(regionCtx(t), "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("WORKFLOW_HISTORY via region %s: %v", region, err)
	}
	info := stream.CachedInfo()
	if info.Cluster == nil || info.Cluster.Name != region || info.Config.Replicas != 3 {
		t.Fatalf("WORKFLOW_HISTORY: cluster %+v replicas %d, want %s R3",
			info.Cluster, info.Config.Replicas, region)
	}
}

// With eu gone, us still creates streams and consumers (its own meta group
// keeps quorum) and re-runs SetupAll as a restarting node would, while a
// request to eu's domain fails.
func TestRegions_KeepCreatingConsumersWhileLinkDown(t *testing.T) {
	eu, us := startRegionPair(t, nil)
	if err := natsutil.SetupAll(us.connect(t, 0),
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
	); err != nil {
		t.Fatalf("SetupAll in us: %v", err)
	}
	eu.shutdown()
	waitRegionLeafCount(t, us, func(n int) bool { return n == 0 }, "no leaf links")

	js := us.jetStream(t, 1, "")
	ctx := regionCtx(t)
	if err := natsutil.SetupAll(us.connect(t, 2),
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
	); err != nil {
		t.Fatalf("SetupAll in us with the link down: %v", err)
	}
	for i := range 3 {
		consumer, err := js.CreateOrUpdateConsumer(ctx, "WORKFLOW_HISTORY",
			jetstream.ConsumerConfig{Durable: fmt.Sprintf("link_down_%d", i)})
		if err != nil {
			t.Fatalf("create consumer %d with the link down: %v", i, err)
		}
		if consumer.CachedInfo().Cluster.Name != "us" {
			t.Fatalf("consumer %d placed in %+v, want us", i, consumer.CachedInfo().Cluster)
		}
	}
	_, err := js.OrderedConsumer(ctx, "WORKFLOW_HISTORY", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("ordered consumer with the link down: %v", err)
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = us.jetStream(t, 0, "eu").Stream(shortCtx, "WORKFLOW_HISTORY")
	if err == nil || errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("eu by domain with eu gone: err = %v, want no responders or timeout", err)
	}
}
