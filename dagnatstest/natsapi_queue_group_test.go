// dagnatstest/natsapi_queue_group_test.go
// Methodology: the NATS control plane on a real 3-node in-process
// cluster, one NATSAPI per node on its own connection, as `dagnats
// serve` runs it. A request is published raw, the replies on its inbox
// are counted for a bounded window, and the runs it created are counted
// from the store. Each unscoped endpoint must be answered by exactly one
// node, so api.runs.start mints one run; micro's discovery ping, which
// every instance still answers, proves all three NATSAPIs are
// listening. Lives here because the cluster helper is in
// this package, which imports api.
package dagnatstest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// replyWindow is how long a request's replies are collected: every node
// answers within it, so a count taken after it is final.
const replyWindow = 1500 * time.Millisecond

func startAPICluster(t *testing.T) []rywNode {
	t.Helper()
	nodes := startRYWCluster(t)
	for _, node := range nodes {
		natsAPI := api.NewNATSAPI(node.svc, node.nc, "1.0.0")
		natsAPI.Start()
		t.Cleanup(natsAPI.Stop)
		if err := node.nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	return nodes
}

// countReplies publishes body on subject from nc and returns every reply
// that arrives within replyWindow.
func countReplies(t *testing.T, nc *nats.Conn, subject string, body []byte) [][]byte {
	t.Helper()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test cleanup
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
	var replies [][]byte
	deadline := time.Now().Add(replyWindow)
	for {
		msg, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			return replies
		}
		replies = append(replies, msg.Data)
	}
}

func TestCluster_NATSAPIRunStartIsAnsweredOnce(t *testing.T) {
	nodes := startAPICluster(t)
	wb := dag.NewWorkflow("fanout")
	wb.Task("park", "fanout.park")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := nodes[0].svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	body, err := json.Marshal(map[string]any{"workflow": def.Name})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	replies := countReplies(t, nodes[1].nc, "api.runs.start", body)
	if len(replies) != 1 {
		t.Fatalf("api.runs.start got %d replies, want 1: %q", len(replies), replies)
	}
	env, err := nodes[2].svc.ListRuns(context.Background(), api.RunsFilter{Workflow: def.Name}, 10)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	// Negative: one request minted one run, not one per node.
	if len(env.Runs) != 1 {
		t.Fatalf("api.runs.start created %d runs, want 1", len(env.Runs))
	}
}

func TestCluster_NATSAPIEndpointsAnswerOnce(t *testing.T) {
	nodes := startAPICluster(t)
	client := nodes[0].nc
	// Bodies a handler refuses fast: each reply is an error, and a fanned
	// out request is still answered by every node.
	for _, subject := range []string{
		"api.workflows.register", "api.runs.get", "api.runtimes.register",
		"api.runs.spawn", "api.runtimes.budget",
		"api.inputs.submit", "api.inputs.get", "api.inputs.withdraw",
	} {
		if got := countReplies(t, client, subject, []byte(`{}`)); len(got) != 1 {
			t.Errorf("%s got %d replies, want 1: %q", subject, len(got), got)
		}
	}
	// Positive control: micro discovery, which no queue group covers,
	// still reaches every node's NATSAPI, so each count above saw three
	// listeners.
	if got := countReplies(t, client, "$SRV.PING.dagnats-api", nil); len(got) != len(nodes) {
		t.Fatalf("$SRV.PING.dagnats-api got %d replies, want %d", len(got), len(nodes))
	}
}
