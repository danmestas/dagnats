// region_rehearsal_test.go: a rehearsal of the live dagnats-mesh migration
// (ADR-031 section 7) on real embedded three-node clusters, none shared
// between tests. First the running cluster, which has no JetStream domain,
// gains jetstream_domain=eu by a rolling restart on its own data
// directories; then a us region (region=us) links to the eu listeners with
// the allow-list the runbook deploys, loaded from testdata/region-eu.env
// rather than restated here. Each test pairs what must work (state kept
// through the roll, notify's agents traffic and a KV watch crossing the
// link, failover) with what must not (no domain API before the roll,
// dagnats subjects and other services staying in their region). Every wait
// is bounded.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const (
	// liveClusterName is the live eu cluster's name, kept by the migration.
	liveClusterName = "dagnats-mesh"
	// regionEUEnvPath is the env file the runbook loads on every eu node.
	regionEUEnvPath = "testdata/region-eu.env"
	// rehearsalWait bounds every wait that spans an election or a relink.
	rehearsalWait = 30 * time.Second
	// rehearsalHistoryCount is how many history messages the roll keeps.
	rehearsalHistoryCount = 20
	// rehearsalCardCount is how many KV values the roll keeps.
	rehearsalCardCount = 10
	// watchFailoverWait bounds a KV watch's recovery after the eu node it
	// is linked through stops. When that node also held the watch's R1
	// ordered consumer, the watch hears nothing until nats.go's missed
	// heartbeat check recreates the consumer: about 20s, measured.
	watchFailoverWait = 45 * time.Second
)

// asLiveMesh shapes a node like the live eu cluster before the migration:
// its own cluster name, no region and no JetStream domain.
func asLiveMesh(cfg *Config) {
	cfg.NATSClusterName, cfg.Region, cfg.JetStreamDomain = liveClusterName, "", ""
}

// regionKeyOnly leaves only the region key set, as a us node's config
// file does, and derives cluster name and domain the way loading does.
func regionKeyOnly(cfg *Config) {
	cfg.NATSClusterName, cfg.JetStreamDomain = "", ""
	if err := applyRegion(cfg); err != nil {
		panic(fmt.Sprintf("regionKeyOnly: %v", err))
	}
}

// loadRegionAllow reads DAGNATS_LEAF_LISTEN_ALLOW from the runbook's env
// file and parses it as a node would, through applyLeafEnv.
func loadRegionAllow(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(regionEUEnvPath)
	if err != nil {
		t.Fatalf("read %s: %v", regionEUEnvPath, err)
	}
	value := ""
	for _, line := range strings.Split(string(data), "\n") {
		name, val, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && name == "DAGNATS_LEAF_LISTEN_ALLOW" {
			value = strings.Trim(val, "'\"")
		}
	}
	if value == "" {
		t.Fatalf("%s sets no DAGNATS_LEAF_LISTEN_ALLOW", regionEUEnvPath)
	}
	t.Setenv("DAGNATS_LEAF_LISTEN_ALLOW", value)
	var cfg Config
	if err := applyLeafEnv(&cfg); err != nil {
		t.Fatalf("apply %s: %v", regionEUEnvPath, err)
	}
	if err := validateLeafListenAllow(cfg.LeafListenAllow); err != nil {
		t.Fatalf("%s: %v", regionEUEnvPath, err)
	}
	if !slices.Contains(cfg.LeafListenAllow, "agents.>") ||
		slices.Contains(cfg.LeafListenAllow, "task.>") {
		t.Fatalf("%s allow-list %q is not the region link's", regionEUEnvPath,
			cfg.LeafListenAllow)
	}
	return cfg.LeafListenAllow
}

// The live cluster gains jetstream_domain=eu one node at a time on its
// existing data. Consumers can be created mid-roll, and afterwards every
// value, message and replica is where it was and $JS.eu.API answers.
func TestRehearsal_RollingDomainAdd(t *testing.T) {
	mesh := startTestRegion(t, "eu", regionOptions{mutate: asLiveMesh})
	if name := mesh.nodes[0].ClusterName(); name != liveClusterName {
		t.Fatalf("cluster name = %q, want %q", name, liveClusterName)
	}
	before := seedMesh(t, mesh)
	if _, err := mesh.connect(t, 0).Request("$JS.eu.API.INFO", nil, time.Second); err == nil {
		t.Fatal("$JS.eu.API.INFO answered before any node had the domain")
	}
	for i := range mesh.nodes {
		mesh.restartNode(t, i, func(cfg *Config) { cfg.JetStreamDomain = "eu" })
		waitRegionFormed(t, mesh)
		waitStreamsReplicated(t, mesh, (i+1)%len(mesh.nodes))
		if i == len(mesh.nodes)-1 {
			continue
		}
		consumer := fmt.Sprintf("mid_roll_%d", i)
		if _, err := mesh.jetStream(t, i, "").CreateOrUpdateConsumer(regionCtx(t),
			"WORKFLOW_HISTORY", jetstream.ConsumerConfig{Durable: consumer},
		); err != nil {
			t.Fatalf("create %s with %d of 3 nodes rolled: %v", consumer, i+1, err)
		}
	}
	after := waitStreamsReplicated(t, mesh, 2)
	for stream, count := range before {
		if after[stream] != count {
			t.Errorf("%s holds %d messages after the roll, want %d",
				stream, after[stream], count)
		}
	}
	assertCards(t, mesh.jetStream(t, 1, ""))
	// Each node answers $JS.eu.API itself (a node the roll missed has no
	// responder), and its own JetStream config names the domain.
	for i, node := range mesh.nodes {
		if config := node.JetStreamConfig(); config == nil || config.Domain != "eu" {
			t.Errorf("node %d JetStream config %+v, want domain eu", i, config)
		}
		assertDomainInfo(t, mesh.connect(t, i), "eu")
	}
}

// seedMesh provisions dagnats' streams, writes history messages and a KV
// bucket of values, and returns every stream's message count.
func seedMesh(t *testing.T, mesh *testRegion) map[string]uint64 {
	t.Helper()
	nc := mesh.connect(t, 0)
	if err := natsutil.SetupAll(nc,
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx := regionCtx(t)
	js := newJetStream(t, nc, "")
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "chat_cards", Replicas: 3,
	})
	if err != nil {
		t.Fatalf("create chat_cards: %v", err)
	}
	for i := range rehearsalCardCount {
		if _, err := kv.Put(ctx, fmt.Sprintf("card.%d", i), []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatalf("put card.%d: %v", i, err)
		}
	}
	for i := range rehearsalHistoryCount {
		subject := fmt.Sprintf("history.rehearsal.%d", i)
		if _, err := js.Publish(ctx, subject, []byte("x")); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}
	counts := waitStreamsReplicated(t, mesh, 0)
	if counts["WORKFLOW_HISTORY"] != rehearsalHistoryCount {
		t.Fatalf("WORKFLOW_HISTORY holds %d messages, want %d",
			counts["WORKFLOW_HISTORY"], rehearsalHistoryCount)
	}
	return counts
}

// waitStreamsReplicated waits, through node via, until every stream is R3
// with a leader and two current followers, and returns message counts.
func waitStreamsReplicated(t *testing.T, mesh *testRegion, via int) map[string]uint64 {
	t.Helper()
	js := mesh.jetStream(t, via, "")
	deadline := time.Now().Add(rehearsalWait)
	for {
		counts, lagging := streamReplicaState(t, js)
		if lagging == "" {
			return counts
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams not R3 and in sync within %s: %s", rehearsalWait, lagging)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// streamReplicaState lists every stream's message count, and names the
// first stream that is not R3 with a leader and two current followers.
func streamReplicaState(t *testing.T, js jetstream.JetStream) (map[string]uint64, string) {
	t.Helper()
	lister := js.ListStreams(regionCtx(t))
	counts := map[string]uint64{}
	lagging := ""
	for info := range lister.Info() {
		counts[info.Config.Name] = info.State.Msgs
		if lagging != "" {
			continue
		}
		cluster := info.Cluster
		if info.Config.Replicas != 3 || cluster == nil || cluster.Leader == "" ||
			len(cluster.Replicas) != 2 {
			lagging = fmt.Sprintf("%s: replicas %d, cluster %+v",
				info.Config.Name, info.Config.Replicas, cluster)
			continue
		}
		for _, peer := range cluster.Replicas {
			if !peer.Current || peer.Offline {
				lagging = fmt.Sprintf("%s: peer %s current=%t offline=%t",
					info.Config.Name, peer.Name, peer.Current, peer.Offline)
			}
		}
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Sprintf("list streams: %v", err)
	}
	if len(counts) == 0 {
		return nil, "no streams listed"
	}
	return counts, lagging
}

// assertCards checks chat_cards holds every value seedMesh wrote.
func assertCards(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ctx := regionCtx(t)
	kv, err := js.KeyValue(ctx, "chat_cards")
	if err != nil {
		t.Fatalf("open chat_cards: %v", err)
	}
	for i := range rehearsalCardCount {
		entry, err := kv.Get(ctx, fmt.Sprintf("card.%d", i))
		if err != nil {
			t.Fatalf("get card.%d: %v", i, err)
		}
		if want := fmt.Sprint("v", i); string(entry.Value()) != want {
			t.Fatalf("card.%d = %q, want %q", i, entry.Value(), want)
		}
	}
}

// assertDomainInfo requests $JS.<domain>.API.INFO and checks the answer
// names the domain.
func assertDomainInfo(t *testing.T, nc *nats.Conn, domain string) {
	t.Helper()
	msg, err := nc.Request("$JS."+domain+".API.INFO", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("$JS.%s.API.INFO: %v", domain, err)
	}
	var info struct {
		Domain string          `json:"domain"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("decode $JS.%s.API.INFO: %v", domain, err)
	}
	if info.Domain != domain || info.Error != nil {
		t.Fatalf("$JS.%s.API.INFO = %s, want domain %s", domain, msg.Data, domain)
	}
}

// startRehearsalPair starts eu shaped like the live cluster after the
// roll (cluster dagnats-mesh, domain eu, no region) with the runbook's
// allow-list on its leaf listeners, and us with region=us, each us node
// holding one link that fails over across every eu listener.
func startRehearsalPair(t *testing.T) (*testRegion, *testRegion) {
	t.Helper()
	allow := loadRegionAllow(t)
	eu := startTestRegion(t, "eu", regionOptions{listen: true, mutate: func(cfg *Config) {
		asLiveMesh(cfg)
		cfg.JetStreamDomain, cfg.LeafListenAllow = "eu", allow
	}})
	us := startTestRegion(t, "us", regionOptions{dial: eu.leafPorts, mutate: regionKeyOnly})
	waitRegionLinked(t, us, 1)
	if eu.nodes[0].ClusterName() != liveClusterName || us.nodes[0].ClusterName() != "us" {
		t.Fatalf("cluster names %q and %q, want %q and us",
			eu.nodes[0].ClusterName(), us.nodes[0].ClusterName(), liveClusterName)
	}
	return eu, us
}

// A us client watching eu's chat_cards by domain sees an eu put, and a
// later one after the eu node holding its node's link stops.
func TestRehearsal_USWatchesEUBucketThroughFailover(t *testing.T) {
	eu, us := startRehearsalPair(t)
	ctx := regionCtx(t)
	if _, err := eu.jetStream(t, 0, "").CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "chat_cards", Replicas: 3,
	}); err != nil {
		t.Fatalf("create chat_cards in eu: %v", err)
	}
	hub := linkedHubNode(t, eu, "us-0")
	putter, err := eu.jetStream(t, (hub+1)%len(eu.nodes), "").KeyValue(ctx, "chat_cards")
	if err != nil {
		t.Fatalf("open chat_cards in eu: %v", err)
	}
	remote, err := us.jetStream(t, 0, "eu").KeyValue(ctx, "chat_cards")
	if err != nil {
		t.Fatalf("open eu chat_cards from us by domain: %v", err)
	}
	// The watch lives as long as the test: its context ends the watch.
	watchCtx, cancel := context.WithTimeout(context.Background(), 3*rehearsalWait)
	t.Cleanup(cancel)
	watcher, err := remote.WatchAll(watchCtx, jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("watch eu chat_cards from us: %v", err)
	}
	t.Cleanup(func() { _ = watcher.Stop() })
	putCard(t, putter, "card.before")
	waitWatched(t, watcher, "card.before", rehearsalWait)

	eu.nodes[hub].Shutdown()
	eu.nodes[hub].WaitForShutdown()
	stopped := time.Now()
	waitRelinked(t, eu, us, hub)
	relinked := time.Since(stopped)
	putCard(t, putter, "card.after")
	put := time.Since(stopped)
	waitWatched(t, watcher, "card.after", watchFailoverWait)
	t.Logf("after stopping eu node %d: relinked %s, put %s, watched %s",
		hub, relinked, put, time.Since(stopped))
	if relinked := linkedHubNode(t, eu, "us-0"); relinked == hub {
		t.Fatalf("us-0 is linked to the stopped eu node %d", hub)
	}
}

// putCard puts key=key in eu, retrying while a stopped leader re-elects.
func putCard(t *testing.T, kv jetstream.KeyValue, key string) {
	t.Helper()
	deadline := time.Now().Add(rehearsalWait)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := kv.Put(ctx, key, []byte(key))
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("put %s in eu within %s: %v", key, rehearsalWait, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitWatched waits for watcher to deliver key, skipping other entries.
func waitWatched(t *testing.T, watcher jetstream.KeyWatcher, key string, wait time.Duration) {
	t.Helper()
	timeout := time.After(wait)
	for {
		select {
		case entry := <-watcher.Updates():
			if entry != nil && entry.Key() == key && string(entry.Value()) == key {
				return
			}
		case <-timeout:
			t.Fatalf("us watcher did not see %s within %s", key, wait)
		}
	}
}

// waitRelinked waits until us-0 holds a link to an eu node other than
// stopped, counted on both sides.
func waitRelinked(t *testing.T, eu, us *testRegion, stopped int) {
	t.Helper()
	deadline := time.Now().Add(testRegionLinkWait)
	for {
		hub := linkedHubNodeOrNone(eu, "us-0")
		if hub >= 0 && hub != stopped && us.nodes[0].NumLeafNodes() == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("us-0 did not relink to eu within %s", testRegionLinkWait)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The eu agents responder answers both of notify's dispatch shapes
// (notify internal/agents/runtime/mesh/mesh.go and synadia.go): an A2A
// request carrying reply_to, acked on the NATS reply then answered with
// status frames and a final frame on reply_to; and a Synadia v0.3 prompt,
// answered on the NATS reply alone.
const (
	a2aReplyToHeader = "reply_to"
	agentsSubject    = "agents.prompt.cc.test"
)

// synadiaHeaders are the headers a Synadia v0.3 prompt carries.
var synadiaHeaders = []string{
	"Notify-Context-Id", "Notify-Message-Id", "Notify-Subscriber-Id",
}

// a2aFrames are what the responder publishes on reply_to, in order.
var a2aFrames = []string{"status:working", "status:thinking", "final:hello"}

// startAgentsResponder subscribes eu node 1 to agentsSubject.
func startAgentsResponder(t *testing.T, eu *testRegion) {
	t.Helper()
	responder := eu.connect(t, 1)
	if _, err := responder.Subscribe(agentsSubject, func(msg *nats.Msg) {
		replyTo := msg.Header.Get(a2aReplyToHeader)
		if replyTo == "" {
			answerSynadia(t, msg)
			return
		}
		if err := msg.Respond([]byte("ack")); err != nil {
			t.Errorf("ack a2a request: %v", err)
		}
		for _, frame := range a2aFrames {
			if err := responder.Publish(replyTo, []byte(frame)); err != nil {
				t.Errorf("publish %s on %s: %v", frame, replyTo, err)
			}
		}
	}); err != nil {
		t.Fatalf("subscribe %s in eu: %v", agentsSubject, err)
	}
	if err := responder.Flush(); err != nil {
		t.Fatalf("flush responder: %v", err)
	}
}

// answerSynadia replies to a Synadia prompt with its prompt echoed, or
// with an error naming the first missing header.
func answerSynadia(t *testing.T, msg *nats.Msg) {
	reply := []byte(`{"error":"no prompt"}`)
	var body struct {
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal(msg.Data, &body); err == nil && body.Prompt != "" {
		reply = []byte(`{"answer":"` + body.Prompt + `"}`)
	}
	for _, header := range synadiaHeaders {
		if msg.Header.Get(header) == "" {
			reply = []byte(`{"error":"missing ` + header + `"}`)
		}
	}
	if err := msg.Respond(reply); err != nil {
		t.Errorf("answer synadia prompt: %v", err)
	}
}

// A2A dispatch from us: the request carries reply_to=a2a.reply.<token>
// and a _INBOX.notify NATS reply. The us client gets the ack on its inbox
// and every frame on reply_to, subscribed before it sends.
func TestRehearsal_A2ADispatchFromUS(t *testing.T) {
	eu, us := startRehearsalPair(t)
	startAgentsResponder(t, eu)
	notify := connectOrFail(t, us.nodes[2].ClientURL(), nats.CustomInboxPrefix("_INBOX.notify"))
	replyTo := "a2a.reply." + nuid.Next()
	frames, err := notify.SubscribeSync(replyTo)
	if err != nil {
		t.Fatalf("subscribe %s in us: %v", replyTo, err)
	}
	if err := notify.Flush(); err != nil {
		t.Fatalf("flush in us: %v", err)
	}
	waitInterest(t, eu.nodes[1], replyTo)
	waitInterest(t, us.nodes[2], agentsSubject)
	request := &nats.Msg{
		Subject: agentsSubject, Data: []byte(`{"prompt":"hello"}`),
		Header: nats.Header{a2aReplyToHeader: []string{replyTo}},
	}
	ack, err := notify.RequestMsg(request, 5*time.Second)
	if err != nil || string(ack.Data) != "ack" ||
		!strings.HasPrefix(ack.Subject, "_INBOX.notify.") {
		t.Fatalf("a2a request from us: reply %+v, err %v; want ack on _INBOX.notify", ack, err)
	}
	for _, want := range a2aFrames {
		frame, err := frames.NextMsg(5 * time.Second)
		if err != nil || string(frame.Data) != want {
			t.Fatalf("%s in us: %+v, err %v; want %s", replyTo, frame, err, want)
		}
	}
	if extra, err := frames.NextMsg(crossWait); err == nil {
		t.Fatalf("%s delivered an extra frame %q", replyTo, extra.Data)
	}
}

// Synadia v0.3 from us: a plain request with notify's headers and a
// {"prompt"} body is answered on the standard inbox. The same request
// without the headers is refused by the responder, so the headers crossed.
func TestRehearsal_SynadiaPromptFromUS(t *testing.T) {
	eu, us := startRehearsalPair(t)
	startAgentsResponder(t, eu)
	nc := us.connect(t, 0)
	waitInterest(t, us.nodes[0], agentsSubject)
	request := &nats.Msg{
		Subject: agentsSubject, Data: []byte(`{"prompt":"hello"}`), Header: nats.Header{},
	}
	for _, header := range synadiaHeaders {
		request.Header.Set(header, "id-"+nuid.Next())
	}
	reply, err := nc.RequestMsg(request, 5*time.Second)
	if err != nil || string(reply.Data) != `{"answer":"hello"}` ||
		!strings.HasPrefix(reply.Subject, "_INBOX.") {
		t.Fatalf("synadia prompt from us: reply %+v, err %v; want the answer on _INBOX",
			reply, err)
	}
	bare := &nats.Msg{Subject: agentsSubject, Data: request.Data}
	reply, err = nc.RequestMsg(bare, 5*time.Second)
	if err != nil || !strings.Contains(string(reply.Data), "missing Notify-") {
		t.Fatalf("headerless prompt from us: reply %+v, err %v; want a missing-header error",
			reply, err)
	}
}

// waitInterest waits until node's account has interest in subject, so a
// publish there is not lost to propagation across the link.
func waitInterest(t *testing.T, node *natsserver.Server, subject string) {
	t.Helper()
	deadline := time.Now().Add(testRegionLinkWait)
	for !node.GlobalAccount().SubscriptionInterest(subject) {
		if time.Now().After(deadline) {
			t.Fatalf("no interest in %s within %s", subject, testRegionLinkWait)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Discovery from us as notify sends it: $SRV.INFO.agents with the reply
// subject a2a.reply.info.<token>, answered by eu's agents service. The
// same probe for another eu service gets no answer: its $SRV subjects are
// not in the allow-list.
func TestRehearsal_AgentsServiceDiscoveryFromUS(t *testing.T) {
	eu, us := startRehearsalPair(t)
	for _, name := range []string{"agents", "billing"} {
		svc, err := micro.AddService(eu.connect(t, 1), micro.Config{Name: name, Version: "1.0.0"})
		if err != nil {
			t.Fatalf("add %s service in eu: %v", name, err)
		}
		t.Cleanup(func() { _ = svc.Stop() })
	}
	nc := us.connect(t, 0)
	waitInterest(t, us.nodes[0], "$SRV.INFO.agents")
	msg, err := discover(t, eu, nc, "$SRV.INFO.agents", 5*time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.agents from us: %v", err)
	}
	var info micro.Info
	if err := json.Unmarshal(msg.Data, &info); err != nil || info.Name != "agents" {
		t.Fatalf("$SRV.INFO.agents from us = %s (err %v), want the agents service",
			msg.Data, err)
	}
	if _, err := eu.connect(t, 2).Request("$SRV.INFO.billing", nil, 5*time.Second); err != nil {
		t.Fatalf("$SRV.INFO.billing in eu: %v", err)
	}
	if msg, err := discover(t, eu, nc, "$SRV.INFO.billing", crossWait); err == nil {
		t.Fatalf("$SRV.INFO.billing crossed to us: %s", msg.Data)
	}
}

// discover publishes subject from nc with the reply subject
// a2a.reply.info.<token>, subscribed first and its interest seen in eu,
// and waits up to wait for the answer.
func discover(
	t *testing.T, eu *testRegion, nc *nats.Conn, subject string, wait time.Duration,
) (*nats.Msg, error) {
	t.Helper()
	reply := "a2a.reply.info." + nuid.Next()
	answers, err := nc.SubscribeSync(reply)
	if err != nil {
		t.Fatalf("subscribe %s in us: %v", reply, err)
	}
	t.Cleanup(func() { _ = answers.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush in us: %v", err)
	}
	waitInterest(t, eu.nodes[1], reply)
	if err := nc.PublishRequest(subject, reply, nil); err != nil {
		t.Fatalf("publish %s from us: %v", subject, err)
	}
	return answers.NextMsg(wait)
}

// task.x and history.x published in one region are stored only there:
// a core subscriber in the other region misses them and its TASK_QUEUES
// and WORKFLOW_HISTORY keep only their own region's message.
func TestRehearsal_DagnatsSubjectsStayInRegion(t *testing.T) {
	eu, us := startRehearsalPair(t)
	for _, region := range []*testRegion{eu, us} {
		if err := natsutil.SetupAll(region.connect(t, 0),
			natsutil.WithCluster(natsutil.ClusterOptions{Routes: regionRoutes}),
		); err != nil {
			t.Fatalf("SetupAll in %s: %v", region.name, err)
		}
	}
	subjects := []string{"task.x", "history.x"}
	for _, subject := range subjects {
		if crossed(t, us, eu, subject) {
			t.Errorf("%s published in us reached eu", subject)
		}
		assertStored(t, us, subject, 1)
		assertStored(t, eu, subject, 0)
	}
	for _, subject := range subjects {
		if crossed(t, eu, us, subject) {
			t.Errorf("%s published in eu reached us", subject)
		}
		assertStored(t, eu, subject, 1)
		assertStored(t, us, subject, 1)
	}
}
