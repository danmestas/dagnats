// journal/consumer_leak_test.go
// Every journal read opens one ordered consumer (natsutil.OpenConsumer)
// and must release it when the read ends: nats-server caps a stream at
// 1000 consumers, and an ordered consumer's server-side inactivity
// threshold is minutes, so a leak per read exhausts AGENT_JOURNAL after
// a few hundred runs and every claim then fails with error 10026.
//
// Methodology: one embedded server, the public Open/Read/Check surface
// and the history reader. The consumer count of the stream is read from
// stream Info before and after each read; it must return to baseline
// every time, on the success path and on a read that fails. Every wait
// is bounded.
package journal

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

const leakReads = 50

func consumerCount(t *testing.T, c client, stream string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	s, err := c.js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("Stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Info %s: %v", stream, err)
	}
	return info.State.Consumers
}

func TestConsumerLeak_OpenAndReadReleaseTheirConsumer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	baseline := consumerCount(t, c, natsutil.AgentJournalStream)
	for i := 1; i <= leakReads; i++ {
		runID := "leakrun" + strconv.Itoa(i)
		open(t, c, task(runID, 0, 1, 1))
		_ = countRecords(t, c, runID)
		if got := consumerCount(t, c, natsutil.AgentJournalStream); got != baseline {
			t.Fatalf("after %d open+read pairs: %d consumers, want baseline %d", i, got, baseline)
		}
	}
	if baseline != 0 {
		t.Errorf("baseline = %d consumers, want 0 on a fresh stream", baseline)
	}
}

func TestConsumerLeak_FailedReadReleasesItsConsumer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	open(t, c, task("failrun", 0, 1, 1))
	for i := 0; i < leakReads; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
		boom := context.Canceled
		err := fetchUpTo(ctx, c.js, natsutil.AgentJournalStream,
			natsutil.JournalSubject("failrun", "s"), 1, 10,
			func(uint64, jetstream.Msg, *jetstream.MsgMetadata) error { return boom })
		cancel()
		if err == nil {
			t.Fatal("fetchUpTo: want the visit error")
		}
	}
	if got := consumerCount(t, c, natsutil.AgentJournalStream); got != 0 {
		t.Fatalf("after %d failed reads: %d consumers, want 0", leakReads, got)
	}
}

func TestConsumerLeak_HistoryReadReleasesItsConsumer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	evt := protocol.NewStepEvent(protocol.EventStepStarted, "hrun", "s", nil)
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	ack, err := c.js.Publish(ctx, "history.hrun", data)
	if err != nil {
		t.Fatalf("publish history: %v", err)
	}
	baseline := consumerCount(t, c, historyStream)
	for i := 1; i <= leakReads; i++ {
		out := map[string][]protocol.Event{}
		if err := readHistoryEvents(ctx, c.js, "history.hrun", ack.Sequence, out); err != nil {
			t.Fatalf("readHistoryEvents: %v", err)
		}
		if len(out["s"]) != 1 {
			t.Fatalf("history events = %d, want 1", len(out["s"]))
		}
		if got := consumerCount(t, c, historyStream); got != baseline {
			t.Fatalf("after %d history reads: %d consumers, want baseline %d", i, got, baseline)
		}
	}
}
