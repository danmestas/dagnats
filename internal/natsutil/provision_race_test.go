// Methodology: integration tests against a real three-node embedded
// cluster. Each test reproduces what two dagnats nodes booting together
// do: one node's create of an R3 resource is still waiting for its
// stream leader when a second node provisions the same resource. Both
// calls must get an answer well inside one per-attempt timeout; a lost
// reply shows up as a call that only returns once its attempt times out.
package natsutil

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// raceAnswerMax is how long either racing call may take. It is far below
// provisionAttemptTimeout, so passing it means no attempt timed out.
const raceAnswerMax = 3 * time.Second

// raceStagger is the gap between the first node's create and the second
// node's provision: long enough for the create to be proposed, short
// enough that its stream leader is not elected yet.
const raceStagger = 10 * time.Millisecond

func TestProvisionStream_RacingCreateGetsAnswer(t *testing.T) {
	jsFirst, jsSecond := startRaceCluster(t)
	cfg := jetstream.StreamConfig{
		Name: "RACE", Subjects: []string{"race.>"}, Replicas: 3,
	}
	first, second := raceProvision(t,
		func(ctx context.Context) error {
			_, err := jsFirst.CreateStream(ctx, cfg)
			return err
		},
		func(ctx context.Context) error {
			return createStreamWithRetry(ctx, jsSecond, cfg)
		},
	)
	assertAnsweredQuickly(t, "first create", first)
	assertAnsweredQuickly(t, "second provision", second)
}

func TestProvisionKeyValue_RacingCreateGetsAnswer(t *testing.T) {
	jsFirst, jsSecond := startRaceCluster(t)
	cfg := jetstream.KeyValueConfig{Bucket: "race", Replicas: 3}
	first, second := raceProvision(t,
		func(ctx context.Context) error {
			_, err := jsFirst.CreateKeyValue(ctx, cfg)
			return err
		},
		func(ctx context.Context) error {
			return createKeyValueWithRetry(ctx, jsSecond, cfg)
		},
	)
	assertAnsweredQuickly(t, "first create", first)
	assertAnsweredQuickly(t, "second provision", second)
}

// raceResult is one racing call's outcome.
type raceResult struct {
	err     error
	elapsed time.Duration
}

// raceProvision starts first, waits raceStagger, runs second, and returns
// both outcomes. Each call is bounded by a context longer than one
// provisioning attempt, so a lost reply is visible as elapsed time.
func raceProvision(
	t *testing.T, first, second func(context.Context) error,
) (raceResult, raceResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*provisionAttemptTimeout,
	)
	defer cancel()
	firstDone := make(chan raceResult, 1)
	go func() {
		start := time.Now()
		err := first(ctx)
		firstDone <- raceResult{err: err, elapsed: time.Since(start)}
	}()
	time.Sleep(raceStagger)
	start := time.Now()
	err := second(ctx)
	secondResult := raceResult{err: err, elapsed: time.Since(start)}
	return <-firstDone, secondResult
}

func assertAnsweredQuickly(t *testing.T, label string, result raceResult) {
	t.Helper()
	if result.err != nil {
		t.Errorf("%s: err=%v after %v", label, result.err, result.elapsed)
	}
	if result.elapsed > raceAnswerMax {
		t.Errorf("%s took %v, want < %v (reply lost, attempt timed out)",
			label, result.elapsed, raceAnswerMax)
	}
}

// startRaceCluster starts three routed JetStream servers, waits for a
// meta leader that can place R3 streams, and returns two JetStream
// handles on separate connections to the same node. Separate
// connections are enough: the lost reply needs only two requests.
func startRaceCluster(t *testing.T) (jetstream.JetStream, jetstream.JetStream) {
	t.Helper()
	const nodeCount = 3
	clusterPorts := make([]int, nodeCount)
	for i := range clusterPorts {
		clusterPorts[i] = reservePort(t)
	}
	servers := make([]*natsserver.Server, nodeCount)
	for i := range servers {
		servers[i] = startRaceNode(t, i, clusterPorts)
	}
	for _, server := range servers {
		if !server.ReadyForConnections(10 * time.Second) {
			t.Fatalf("server %s not ready", server.Name())
		}
	}
	jsFirst := raceJetStream(t, servers[0].ClientURL())
	jsSecond := raceJetStream(t, servers[0].ClientURL())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := WaitForClusterQuorum(ctx, jsFirst, nodeCount); err != nil {
		t.Fatalf("cluster quorum: %v", err)
	}
	waitForR3Placement(t, jsFirst)
	return jsFirst, jsSecond
}

func startRaceNode(t *testing.T, index int, clusterPorts []int) *natsserver.Server {
	t.Helper()
	var routes []*url.URL
	for peer, port := range clusterPorts {
		if peer != index {
			routes = append(routes, &url.URL{
				Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", port),
			})
		}
	}
	server, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1,
		ServerName:        fmt.Sprintf("race-%d", index),
		JetStream:         true,
		JetStreamMaxStore: 1 << 30,
		StoreDir:          t.TempDir(),
		Cluster: natsserver.ClusterOpts{
			Name: "race", Host: "127.0.0.1", Port: clusterPorts[index],
		},
		Routes: routes,
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("new server %d: %v", index, err)
	}
	server.Start()
	t.Cleanup(server.Shutdown)
	return server
}

func raceJetStream(t *testing.T, clientURL string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(clientURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

// waitForR3Placement creates a throwaway R3 stream until the meta leader
// has seen every peer, so the racing calls never see "no suitable peers".
func waitForR3Placement(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	cfg := jetstream.StreamConfig{Name: "WARMUP", Replicas: 3}
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = js.CreateOrUpdateStream(ctx, cfg)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("R3 placement never succeeded: %v", err)
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
