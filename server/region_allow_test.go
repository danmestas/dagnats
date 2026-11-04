// region_allow_test.go: the region link's allow-list (ADR-031) end to end.
// Two three-node regions from region_harness_test.go, every us node linked
// to every eu leaf listener, with real embedded servers per test. With the
// allow-list on eu's leaf login, dagnats subjects stay in their region in
// both directions while $JS.<domain>.API, its acks and replies cross; the
// unset case shows the same publish does cross, so the isolation checks
// are not vacuous. The link must also survive a program-user reload.
package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/programs"
	"github.com/danmestas/dagnats/program"
)

// crossWait bounds how long a test waits for a message that must not
// cross: far above a loopback round trip.
const crossWait = 750 * time.Millisecond

func withRegionAllow(cfg *Config) { cfg.LeafListenAllow = regionLinkAllow }

// startProvisionedPair starts both regions and runs SetupAll in each.
func startProvisionedPair(t *testing.T, euMutate func(*Config)) (*testRegion, *testRegion) {
	t.Helper()
	eu, us := startRegionPair(t, euMutate)
	for _, region := range []*testRegion{eu, us} {
		if err := natsutil.SetupAll(region.connect(t, 0),
			natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
		); err != nil {
			t.Fatalf("SetupAll in %s: %v", region.name, err)
		}
	}
	return eu, us
}

func TestRegionAllow_DagnatsSubjectsStayInRegion(t *testing.T) {
	eu, us := startProvisionedPair(t, withRegionAllow)
	for _, pair := range [][2]*testRegion{{us, eu}, {eu, us}} {
		from, to := pair[0], pair[1]
		for _, prefix := range []string{"task", "history"} {
			subject := prefix + ".leak." + from.name
			if crossed(t, from, to, subject) {
				t.Errorf("%s published in %s reached %s", subject, from.name, to.name)
			}
			assertStored(t, from, subject, 1)
			assertStored(t, to, subject, 0)
		}
	}
}

// Without the allow-list the link carries everything: the same publish
// lands in the other region's stream. This is the leak ADR-031 guards.
func TestRegionAllow_UnsetCarriesDagnatsSubjects(t *testing.T) {
	eu, us := startProvisionedPair(t, nil)
	if !crossed(t, us, eu, "task.leak.x") {
		t.Fatal("task.leak.x did not reach eu with no allow-list")
	}
	assertStored(t, eu, "task.leak.x", 1)
}

// crossed publishes subject on from (node 1) and reports whether a core
// subscriber on to (node 2) received it within crossWait.
func crossed(t *testing.T, from, to *testRegion, subject string) bool {
	t.Helper()
	sub, err := to.connect(t, 2).SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe %s in %s: %v", subject, to.name, err)
	}
	// Let the subscription's interest reach every server and leaf.
	time.Sleep(200 * time.Millisecond)
	publisher := from.connect(t, 1)
	if err := publisher.Publish(subject, []byte("x")); err != nil {
		t.Fatalf("publish %s in %s: %v", subject, from.name, err)
	}
	if err := publisher.Flush(); err != nil {
		t.Fatalf("flush in %s: %v", from.name, err)
	}
	_, err = sub.NextMsg(crossWait)
	return err == nil
}

// assertStored checks how many messages on subject region's stream holds.
func assertStored(t *testing.T, region *testRegion, subject string, want uint64) {
	t.Helper()
	stream := streamFor(subject)
	ctx := regionCtx(t)
	js := region.jetStream(t, 0, "")
	deadline := time.Now().Add(crossWait)
	for {
		s, err := js.Stream(ctx, stream)
		if err != nil {
			t.Fatalf("%s in %s: %v", stream, region.name, err)
		}
		info, err := s.Info(ctx, jetstream.WithSubjectFilter(subject))
		if err != nil {
			t.Fatalf("%s info in %s: %v", stream, region.name, err)
		}
		got := info.State.Subjects[subject]
		// Zero must hold for the whole window; a positive count is final.
		if got == want && (want > 0 || time.Now().After(deadline)) {
			return
		}
		if got != want && time.Now().After(deadline) {
			t.Fatalf("%s in %s holds %d messages on %s, want %d",
				stream, region.name, got, subject, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func streamFor(subject string) string {
	if strings.HasPrefix(subject, "task.") {
		return "TASK_QUEUES"
	}
	return "WORKFLOW_HISTORY"
}

// A us client works an eu stream through eu's domain: create a consumer,
// fetch and ack. The ack crosses on $JS.ACK.>, so eu sees it settle.
func TestRegionAllow_DomainConsumerFetchAndAck(t *testing.T) {
	eu, us := startProvisionedPair(t, withRegionAllow)
	ctx := regionCtx(t)
	if _, err := eu.jetStream(t, 1, "").Publish(ctx, "history.cross.1", []byte("x")); err != nil {
		t.Fatalf("publish in eu: %v", err)
	}
	remote := us.jetStream(t, 2, "eu")
	consumer, err := remote.CreateOrUpdateConsumer(ctx, "WORKFLOW_HISTORY",
		jetstream.ConsumerConfig{Durable: "from_us", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("create eu consumer from us: %v", err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch from us: %v", err)
	}
	fetched := 0
	for msg := range batch.Messages() {
		fetched++
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("ack from us: %v", err)
		}
	}
	if fetched != 1 || batch.Error() != nil {
		t.Fatalf("fetched %d messages (err %v), want 1", fetched, batch.Error())
	}
	local, err := eu.jetStream(t, 0, "").Consumer(ctx, "WORKFLOW_HISTORY", "from_us")
	if err != nil {
		t.Fatalf("consumer in eu: %v", err)
	}
	info, err := local.Info(ctx)
	if err != nil || info.AckFloor.Stream != 1 || info.NumAckPending != 0 {
		t.Fatalf("eu consumer after the ack: %+v (err %v), want ack floor 1", info, err)
	}
}

// A program-user reload on an eu node keeps the region link and its
// allow-list: the reload starts from the live options, leaf user included.
func TestRegionAllow_SurvivesProgramUserReload(t *testing.T) {
	eu, us := startProvisionedPair(t, withRegionAllow)
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	manifest := (&tinyProgram{}).Manifest()
	record := programs.Record{
		Manifest: manifest, PublicKey: key.PublicKey(), RegisteredAt: time.Now(),
	}
	for _, node := range eu.nodes {
		if err := newUserSet(node).put(manifest.Name, record, false); err != nil {
			t.Fatalf("reload program users: %v", err)
		}
	}
	waitRegionLinked(t, us, 1)
	assertHistoryInRegion(t, us.jetStream(t, 0, "eu"), "eu")
	if crossed(t, us, eu, "task.after.reload") {
		t.Fatal("task.> crossed after a program-user reload")
	}
	nc, err := nats.Connect(eu.nodes[0].ClientURL(), key.ConnectOption())
	if err != nil {
		t.Fatalf("program key refused after the reload: %v", err)
	}
	nc.Close()
}

// Each us node holds one link that fails over: when the eu node it is
// linked to stops, it links to another eu node within a bounded wait and
// requests by domain work again.
func TestRegionAllow_LinkFailsOverToAnotherHubNode(t *testing.T) {
	eu, us := startProvisionedPair(t, withRegionAllow)
	hub := linkedHubNode(t, eu, "us-0")
	eu.nodes[hub].Shutdown()
	eu.nodes[hub].WaitForShutdown()
	// The hub registers the link a moment before the dialing side counts
	// it, so wait for both.
	deadline := time.Now().Add(testRegionLinkWait)
	for linkedHubNodeOrNone(eu, "us-0") < 0 || us.nodes[0].NumLeafNodes() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("us-0 did not relink to eu within %s", testRegionLinkWait)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if next := linkedHubNodeOrNone(eu, "us-0"); next == hub {
		t.Fatalf("us-0 relinked to the stopped node %d", hub)
	}
	if links := us.nodes[0].NumLeafNodes(); links != 1 {
		t.Fatalf("us-0 holds %d leaf links after failover, want 1", links)
	}
	assertHistoryInRegion(t, us.jetStream(t, 0, "eu"), "eu")
}

// linkedHubNode is the index of the eu node us server name links to.
func linkedHubNode(t *testing.T, eu *testRegion, name string) int {
	t.Helper()
	hub := linkedHubNodeOrNone(eu, name)
	if hub < 0 {
		t.Fatalf("%s holds no link to eu", name)
	}
	return hub
}

// linkedHubNodeOrNone is linkedHubNode, or -1 when no running eu node
// holds a link from name. More than one link is a test failure elsewhere;
// this returns the first.
func linkedHubNodeOrNone(eu *testRegion, name string) int {
	for i, node := range eu.nodes {
		if !node.Running() {
			continue
		}
		leafz, err := node.Leafz(nil)
		if err != nil {
			continue
		}
		for _, leaf := range leafz.Leafs {
			if leaf.Name == name {
				return i
			}
		}
	}
	return -1
}
