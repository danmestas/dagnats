// dagnatstest/sticky_restart_test.go
// Methodology: a real 3-node in-process cluster with R3 streams and a
// real Worker. The Worker's sticky consumer is located through the
// STICKY_TASKS consumer listing, the node leading it is restarted on
// the same ports and store, and a sticky task published afterwards must
// still reach the same Worker without restarting it. Lives here rather
// than in worker/ because the restartable cluster helper is in this
// package, which imports worker.
package dagnatstest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
)

// stickyRecoveryWait bounds the wait for the sticky task after the
// restart. When the node leading the consumer goes, the pull request it
// held goes with it; nats.go notices only after two missed idle
// heartbeats (15s each at the default 30s pull expiry) and pulls again
// from the new leader. 90s covers that plus leader election with room
// for a loaded runner.
const stickyRecoveryWait = 90 * time.Second

func TestStickyConsumer_SurvivesRestartOfItsNode(t *testing.T) {
	cluster := startClusterWithRetry(t, minTestClusterNodes, allocateFreePorts)
	t.Cleanup(cluster.shutdown)
	waitForRoutesMeshed(t, cluster.servers, minTestClusterNodes)
	waitForJetStreamLeader(t, cluster.servers)
	nc := connectAllNodes(t, cluster)
	confirmClusterQuorum(t, nc, minTestClusterNodes)
	routes := []string{"a", "b"} // only len matters for replica derivation
	err := natsutil.SetupAll(nc, natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	got := make(chan string, 4)
	w := worker.NewWorker(nc)
	w.Handle("probe.sticky", func(tc worker.TaskContext) error {
		got <- tc.RunID()
		return tc.Complete([]byte(`"ok"`))
	})
	w.Start()
	defer w.Stop()

	before := stickyConsumerInfo(t, js, "probe.sticky")
	if replicas := len(before.Cluster.Replicas) + 1; replicas != minTestClusterNodes {
		t.Fatalf("sticky consumer has %d replicas, want %d", replicas, minTestClusterNodes)
	}
	cluster.restartNode(t, nodeNamed(t, cluster, before.Cluster.Leader))
	confirmClusterQuorum(t, nc, minTestClusterNodes)

	after := stickyConsumerInfo(t, js, "probe.sticky")
	if after.Name != before.Name {
		t.Fatalf("sticky consumer %s replaced by %s", before.Name, after.Name)
	}
	subject := strings.TrimSuffix(after.Config.FilterSubject, ">") + "run-sticky"
	publishStickyTask(t, js, subject, "run-sticky")
	select {
	case run := <-got:
		if run != "run-sticky" {
			t.Fatalf("worker ran %s, want run-sticky", run)
		}
	case <-time.After(stickyRecoveryWait):
		t.Fatalf("sticky task not delivered within %v of the node restart", stickyRecoveryWait)
	}
}

// connectAllNodes connects with every node's URL, so the connection
// fails over when the node it is on restarts.
func connectAllNodes(t *testing.T, cluster *testCluster) *nats.Conn {
	t.Helper()
	urls := make([]string, 0, len(cluster.servers))
	for _, ns := range cluster.servers {
		urls = append(urls, ns.ClientURL())
	}
	nc, err := nats.Connect(strings.Join(urls, ","),
		nats.MaxReconnects(120), nats.ReconnectWait(250*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// stickyConsumerInfo finds the one STICKY_TASKS consumer for taskType,
// retrying while the stream's leader is being re-elected.
func stickyConsumerInfo(
	t *testing.T, js jetstream.JetStream, taskType string,
) *jetstream.ConsumerInfo {
	t.Helper()
	prefix := "sticky." + taskType + "."
	deadline := time.Now().Add(testClusterQuorumTTL)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), testClusterReadyTime)
		found, err := findConsumerByPrefix(ctx, js, prefix)
		cancel()
		if err == nil && found != nil {
			return found
		}
		lastErr = err
		time.Sleep(placementProbeInterval)
	}
	t.Fatalf("no STICKY_TASKS consumer for %s (last err: %v)", taskType, lastErr)
	return nil
}

func findConsumerByPrefix(
	ctx context.Context, js jetstream.JetStream, prefix string,
) (*jetstream.ConsumerInfo, error) {
	stream, err := js.Stream(ctx, "STICKY_TASKS")
	if err != nil {
		return nil, err
	}
	lister := stream.ListConsumers(ctx)
	for info := range lister.Info() {
		if strings.HasPrefix(info.Config.FilterSubject, prefix) && info.Cluster != nil {
			return info, nil
		}
	}
	return nil, lister.Err()
}

// nodeNamed returns the index of the node with the given server name.
func nodeNamed(t *testing.T, cluster *testCluster, name string) int {
	t.Helper()
	for i, ns := range cluster.servers {
		if ns.Name() == name {
			return i
		}
	}
	t.Fatalf("no node named %q", name)
	return -1
}

func publishStickyTask(t *testing.T, js jetstream.JetStream, subject, runID string) {
	t.Helper()
	data, err := json.Marshal(protocol.TaskPayload{
		TaskID: runID + ".s", RunID: runID, StepID: "s",
		Input: json.RawMessage(`"go"`),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testClusterReadyTime)
	defer cancel()
	if _, err := js.Publish(ctx, subject, data); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}
