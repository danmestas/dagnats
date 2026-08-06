// internal/api/fetch_messages_gap_test.go
// Regression guard for #609: fetchMessages — the shared drain loop behind
// both DLQ listing (listDeadLettersInner) and run event-history reads
// (listRunEventsInner) — must return every currently-stored message up to
// limit even when delivery stalls briefly between messages, and must still
// return promptly for a genuinely short stream.
//
// Methodology: the core reproduction drives fetchMessages through a fake
// messageDrain that scripts delivery gaps (a NextMsg timeout while messages
// are still pending) interleaved with real messages. Pre-#609 the loop
// broke on the first such gap and returned a truncated prefix; the fix keeps
// draining while the consumer reports pending messages. A fake seam is used
// because a >5ms inter-message gap is not reproducible against a real
// embedded server: loopback delivers a static backlog in microseconds and
// nats-server does not enforce a push consumer's RateLimit here, so the flake
// only appears under real CPU starvation. The remaining tests use a real
// server to pin the prompt-return bound and the two call sites' wiring.
package api

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// drainStep is one scripted event for fakeDrain: a message to deliver, or a
// timeout (delivery gap) when msg is nil.
type drainStep struct {
	msg *nats.Msg
}

// fakeDrain replays a scripted sequence of deliveries and gaps against the
// messageDrain contract. NumPending is derived from the messages still ahead
// of the cursor, so a gap step reports the remaining backlog as pending —
// exactly the signal the fix relies on to keep waiting.
type fakeDrain struct {
	steps       []drainStep
	pos         int
	infoErr     error // when set, ConsumerInfo fails (drain must give up)
	infoCalls   int
	timeoutSeen int
}

func (f *fakeDrain) NextMsg(_ time.Duration) (*nats.Msg, error) {
	if f.pos < len(f.steps) {
		step := f.steps[f.pos]
		f.pos++
		if step.msg == nil {
			f.timeoutSeen++
			return nil, nats.ErrTimeout
		}
		return step.msg, nil
	}
	f.timeoutSeen++
	return nil, nats.ErrTimeout
}

func (f *fakeDrain) ConsumerInfo() (*nats.ConsumerInfo, error) {
	f.infoCalls++
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	pending := 0
	for i := f.pos; i < len(f.steps); i++ {
		if f.steps[i].msg != nil {
			pending++
		}
	}
	return &nats.ConsumerInfo{NumPending: uint64(pending)}, nil
}

// scriptWithGaps builds a script of n messages with a delivery gap inserted
// before every message after the first, so the old break-on-first-timeout
// loop would return only the leading message.
func scriptWithGaps(n int) []drainStep {
	steps := make([]drainStep, 0, 2*n)
	for i := 0; i < n; i++ {
		if i > 0 {
			steps = append(steps, drainStep{msg: nil})
		}
		steps = append(steps, drainStep{msg: &nats.Msg{
			Subject: "dead.fake." + strconv.Itoa(i),
			Data:    []byte("m" + strconv.Itoa(i)),
		}})
	}
	return steps
}

// TestFetchMessagesToleratesDeliveryGaps is the mutation-proof #609
// reproduction: with a delivery gap before every message after the first,
// the drain must still collect the whole backlog rather than bail on the
// first gap. This is the exact stopping-condition defect both call sites
// (DLQ list and run event-history) shared through fetchMessages.
func TestFetchMessagesToleratesDeliveryGaps(t *testing.T) {
	const total = 15
	drain := &fakeDrain{steps: scriptWithGaps(total)}

	got := fetchMessages(drain, total+10, time.Now().Add(30*time.Second))

	if len(got) != total {
		t.Fatalf("drained %d messages, want all %d (early-exit on a "+
			"delivery gap is the #609 bug)", len(got), total)
	}
	if drain.timeoutSeen == 0 {
		t.Fatal("script exercised no delivery gaps; test is not " +
			"reproducing the failure mode")
	}
}

// TestFetchMessagesStopsWhenNothingPending pins the acceptance bound that the
// fix must not turn the early-exit bug into a full-deadline hang: once the
// consumer reports nothing pending and no straggler arrives, the drain must
// stop promptly rather than keep polling until the deadline.
func TestFetchMessagesStopsWhenNothingPending(t *testing.T) {
	drain := &fakeDrain{steps: []drainStep{
		{msg: &nats.Msg{Subject: "dead.fake.0", Data: []byte("m0")}},
		{msg: &nats.Msg{Subject: "dead.fake.1", Data: []byte("m1")}},
		{msg: &nats.Msg{Subject: "dead.fake.2", Data: []byte("m2")}},
	}}

	got := fetchMessages(drain, 100, time.Now().Add(30*time.Second))

	if len(got) != 3 {
		t.Fatalf("drained %d, want 3", len(got))
	}
	// One end-of-stream timeout triggers a single ConsumerInfo check plus a
	// grace poll; the drain must not loop the pending check indefinitely.
	if drain.infoCalls != 1 {
		t.Fatalf("ConsumerInfo called %d times, want 1 (no busy-loop "+
			"once pending is exhausted)", drain.infoCalls)
	}
}

// TestFetchMessagesGivesUpWhenConsumerInfoFails guards the defensive path: if
// pending cannot be determined, the drain returns what it has instead of
// spinning against the deadline.
func TestFetchMessagesGivesUpWhenConsumerInfoFails(t *testing.T) {
	// One delivered message, then a sustained gap (two consecutive
	// timeouts) while pending is unknowable: the drain does a single
	// grace poll and, finding nothing, gives up rather than spinning.
	drain := &fakeDrain{
		steps: []drainStep{
			{msg: &nats.Msg{Subject: "dead.fake.0", Data: []byte("m0")}},
			{msg: nil},
			{msg: nil},
			{msg: &nats.Msg{Subject: "dead.fake.1", Data: []byte("m1")}},
		},
		infoErr: errors.New("consumer info unavailable"),
	}

	got := fetchMessages(drain, 100, time.Now().Add(30*time.Second))

	if len(got) != 1 {
		t.Fatalf("drained %d, want 1 (give up on the gap when pending "+
			"is unknowable and no straggler arrives)", len(got))
	}
	if drain.infoCalls != 1 {
		t.Fatalf("ConsumerInfo called %d times, want 1", drain.infoCalls)
	}
}

// TestFetchMessagesReturnsPromptlyForShortStream is the real-NATS bound: a
// backlog smaller than limit, with no more messages ever coming, must return
// quickly rather than block for the full deadline.
func TestFetchMessagesReturnsPromptlyForShortStream(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	const seeded = 3
	seedRawMessages(t, nc, "dead.short-seed.", seeded)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("dead.>")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	start := time.Now()
	got := fetchMessages(sub, 100, deadline)
	elapsed := time.Since(start)

	if len(got) != seeded {
		t.Fatalf("drained %d messages, want %d", len(got), seeded)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("short stream took %s to return; must not block for "+
			"the full deadline", elapsed)
	}
}

// TestListDeadLettersReturnsAllSeeded is the DLQ call-site wiring guard:
// with a static backlog, the public ListDeadLetters must surface every entry
// up to limit through the fixed primitive.
func TestListDeadLettersReturnsAllSeeded(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	const seeded = 40
	seedRawMessages(t, nc, "dead.list-seed.", seeded)

	svc := NewService(nc)
	views, err := svc.ListDeadLetters(t.Context(), seeded+50)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(views) != seeded {
		t.Fatalf("ListDeadLetters returned %d, want all %d",
			len(views), seeded)
	}
	if len(views) == 0 {
		t.Fatal("ListDeadLetters returned nothing for a seeded stream")
	}
}

// TestListRunEventsReturnsAllSeeded is the run event-history call-site wiring
// guard: with a static history backlog, ListRunEvents must return every
// event, not a truncated prefix.
func TestListRunEventsReturnsAllSeeded(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	const runID = "list-history-run"
	const seeded = 40
	seedHistoryEvents(t, nc, runID, seeded)

	svc := NewService(nc)
	events, err := svc.ListRunEvents(t.Context(), runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	if len(events) != seeded {
		t.Fatalf("ListRunEvents returned %d, want all %d",
			len(events), seeded)
	}
	if len(events) == 0 {
		t.Fatal("ListRunEvents returned nothing for a seeded run")
	}
}

// seedRawMessages publishes n synchronous JetStream messages on
// subjectPrefix+<i>, blocking until each is stored.
func seedRawMessages(
	t *testing.T, nc *nats.Conn, subjectPrefix string, n int,
) {
	t.Helper()
	if subjectPrefix == "" {
		t.Fatal("seedRawMessages: subjectPrefix must not be empty")
	}
	if n <= 0 {
		t.Fatal("seedRawMessages: n must be positive")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("seedRawMessages: JetStream: %v", err)
	}
	for i := 0; i < n; i++ {
		subject := subjectPrefix + strconv.Itoa(i)
		if _, err := js.Publish(subject, []byte("seed")); err != nil {
			t.Fatalf("seedRawMessages: publish %d: %v", i, err)
		}
	}
}

// seedHistoryEvents publishes n synchronous workflow history events on
// history.<runID>, blocking until each is stored.
func seedHistoryEvents(
	t *testing.T, nc *nats.Conn, runID string, n int,
) {
	t.Helper()
	if runID == "" {
		t.Fatal("seedHistoryEvents: runID must not be empty")
	}
	if n <= 0 {
		t.Fatal("seedHistoryEvents: n must be positive")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("seedHistoryEvents: JetStream: %v", err)
	}
	pad := strings.Repeat("y", 32)
	for i := 0; i < n; i++ {
		evt := protocol.Event{
			Type:      protocol.EventStepCompleted,
			RunID:     runID,
			StepID:    "step-" + strconv.Itoa(i),
			Timestamp: time.Now().UTC(),
			Payload:   []byte(`"` + pad + `"`),
		}
		data, err := evt.Marshal()
		if err != nil {
			t.Fatalf("seedHistoryEvents: marshal %d: %v", i, err)
		}
		if _, err := js.Publish("history."+runID, data,
			nats.MsgId("gap-evt-"+strconv.Itoa(i))); err != nil {
			t.Fatalf("seedHistoryEvents: publish %d: %v", i, err)
		}
	}
}
