// natsapi_reflection_test.go
// Reply-subject reflection (#901 review): NATS checks no permission on a
// reply subject, and dagnats answers as root, so a request whose reply-to
// names a "$KV..." key, a "$JS.API..." call or another program's inbox
// would have root write it. Every program-scoped endpoint family
// (scoped routes, the event and journal relay, the stores) must drop
// such a request unanswered, and still answer one whose reply is under
// the program's own inbox.
// Methodology: real embedded NATS, orchestrator and NATSAPI; requests are
// published as root with a forged Reply, and the targets are checked
// directly: a KV key, a stream's message count, a subscription.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestProgramEndpointsDoNotReflectReplies(t *testing.T) {
	nc := startScopedAPI(t)
	runID, _ := dispatchedStep(t, nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	victim, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "VICTIM", Subjects: []string{"victim.>"},
	})
	if err != nil {
		t.Fatalf("victim stream: %v", err)
	}
	if _, err := js.Publish(ctx, "victim.x", []byte("keep")); err != nil {
		t.Fatalf("victim publish: %v", err)
	}
	foreign, err := nc.SubscribeSync("_INBOX.artifact.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	targets := []string{
		"$KV.programs.zzz", "$JS.API.STREAM.PURGE.VICTIM", "_INBOX.artifact.reply1",
	}
	requests := []string{
		"api.p.notify.runs.get", "api.p.notify.events", "api.p.notify.checkpoint",
		"api.p.notify.journal.view", "api.p.notify.workflows.register",
	}
	for _, subject := range requests {
		for _, target := range targets {
			msg := &nats.Msg{Subject: subject, Reply: target, Data: []byte(runID)}
			if err := nc.PublishMsg(msg); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := foreign.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatal("a reply was delivered to another program's inbox")
	}
	programs, err := js.KeyValue(ctx, "programs")
	if err == nil {
		if _, err := programs.Get(ctx, "zzz"); err == nil {
			t.Fatal("a reply was written as a programs record")
		}
	}
	info, err := victim.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("victim stream after reflected purges: %+v, %v", info, err)
	}
	// Positive: the same request with the program's own inbox is answered.
	if _, err := programConn(t, nc, "notify").Request(
		"api.p.notify.runs.get", []byte(runID), 2*time.Second); err != nil {
		t.Fatalf("a request with the program's own inbox: %v", err)
	}
}
