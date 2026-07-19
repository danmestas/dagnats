// pollconsumer_test.go
// Pins the bridge poll path to ONE durable consumer per task type,
// shared with the native worker (issue #532).
//
// Methodology: real embedded NATS server, real bridge, real HTTP
// roundtrip. TASK_QUEUES is a WorkQueuePolicy stream, so JetStream
// permits exactly one consumer per overlapping filter subject; every
// assertion here is about that constraint. Consumer topology is read
// back off the live stream via ListConsumers rather than inferred
// from behaviour. All waits are bounded; the store budget is capped
// (precedent: bridge/dispatchspan_test.go) so the default 10 GiB
// JetStream reservation cannot fail stream creation on a small host.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// pollConsumerFetchTimeoutMs bounds every poll in this file. Long
// enough for a local server roundtrip, short enough that a hung poll
// fails the test instead of the suite deadline.
const pollConsumerFetchTimeoutMs = 2000

// newPollConsumerBridge stands up a budgeted NATS server plus a
// bridge and its HTTP test server, returning the jetstream handle used
// to publish fixtures, the bridge itself (for ackMap assertions) and
// the server URL.
func newPollConsumerBridge(
	t *testing.T,
) (jetstream.JetStream, *Bridge, *httptest.Server) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)
	return js, b, ts
}

// preCreateDurable installs a consumer on TASK_QUEUES ahead of any poll,
// standing in for a native worker that got there first.
func preCreateDurable(
	t *testing.T, js jetstream.JetStream,
	name, filter string, ackWait time.Duration,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	_, err := js.CreateOrUpdateConsumer(
		ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
			Durable:       name,
			Name:          name,
			FilterSubject: filter,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckWait:       ackWait,
			MaxDeliver:    -1,
		},
	)
	if err != nil {
		t.Fatalf("pre-create durable %q on %q: %v", name, filter, err)
	}
}

// publishTaskFixture puts one task message on task.<taskType>.<runID>.
func publishTaskFixture(
	t *testing.T, js jetstream.JetStream, taskType, runID string,
) {
	t.Helper()
	if taskType == "" {
		t.Fatalf("publishTaskFixture: taskType must not be empty")
	}
	if runID == "" {
		t.Fatalf("publishTaskFixture: runID must not be empty")
	}
	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-" + runID,
		Input:  json.RawMessage(`{"x":1}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	_, err = js.Publish(
		context.Background(), "task."+taskType+"."+runID, data,
	)
	if err != nil {
		t.Fatalf("publish task: %v", err)
	}
}

// postPollRaw issues a poll with a literal body and returns the HTTP
// status and response body, so error paths can be asserted rather
// than swallowed by postPoll's t.Fatalf on non-200.
func postPollRaw(
	t *testing.T, ts *httptest.Server, body string,
) (int, string) {
	t.Helper()
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("poll request failed: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read poll body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// consumersOnFilter returns the consumer configs on TASK_QUEUES whose
// FilterSubject equals want. Bounded by the stream's consumer count.
func consumersOnFilter(
	t *testing.T, js jetstream.JetStream, want string,
) []jetstream.ConsumerConfig {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	found := make([]jetstream.ConsumerConfig, 0, 4)
	lister := stream.ListConsumers(ctx)
	for info := range lister.Info() {
		if info.Config.FilterSubject == want {
			found = append(found, info.Config)
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ListConsumers: %v", err)
	}
	return found
}

// TestSequentialPollsSameTypeDeliver is the primary #532 guard: two
// polls for the same task type in a row must both deliver. Today the
// second poll's consumer create is rejected (err 10100, filtered
// consumer not unique on a workqueue stream) and the bare `return nil`
// reports that as "no work" — silent starvation.
func TestSequentialPollsSameTypeDeliver(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	publishTaskFixture(t, js, "seq", "seq-1")
	publishTaskFixture(t, js, "seq", "seq-2")

	first := postPoll(t, ts, "seq", 1, pollConsumerFetchTimeoutMs)
	if len(first) != 1 {
		t.Fatalf("first poll: got %d tasks, want 1", len(first))
	}
	resolveTask(t, ts.URL, first[0].TaskID, `{
		"action":"complete",
		"output":{"ok":true}
	}`)

	second := postPoll(t, ts, "seq", 1, pollConsumerFetchTimeoutMs)
	if len(second) != 1 {
		t.Fatalf(
			"second poll starved: got %d tasks, want 1", len(second),
		)
	}
	if second[0].TaskID == first[0].TaskID {
		t.Fatalf(
			"second poll redelivered %q instead of the next task",
			second[0].TaskID,
		)
	}
}

// adoptedAckWait is deliberately not defaultAckWait: an override the
// bridge would clobber is the whole point of the adoption test.
const adoptedAckWait = 90 * time.Second

// TestBridgePollAdoptsWorkerDurable pins lookup-before-create. A
// native worker's durable — possibly carrying a WithAckWait override
// — must be adopted as-is, never reconfigured by a polling bridge.
func TestBridgePollAdoptsWorkerDurable(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	preCreateDurable(
		t, js, "workers-adopt", "task.adopt.>", adoptedAckWait,
	)

	publishTaskFixture(t, js, "adopt", "adopt-1")
	tasks := postPoll(t, ts, "adopt", 1, pollConsumerFetchTimeoutMs)
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}

	cons, err := js.Consumer(ctx, "TASK_QUEUES", "workers-adopt")
	if err != nil {
		t.Fatalf("Consumer after poll: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.AckWait != adoptedAckWait {
		t.Fatalf(
			"bridge clobbered AckWait: got %v, want %v",
			info.Config.AckWait, adoptedAckWait,
		)
	}
	// Asserting only AckWait would pass even if the bridge adopted a
	// durable serving a different subject entirely — the exact blind
	// spot that let the send.email/send-email collision through.
	if info.Config.FilterSubject != "task.adopt.>" {
		t.Fatalf(
			"adopted consumer serves %q, want %q",
			info.Config.FilterSubject, "task.adopt.>",
		)
	}
}

// TestPollRejectsNameCollidingDurable is the finding-1 guard.
// sanitizeConsumerName collapses '.' to '-', so "send.email" and
// "send-email" both name the durable "workers-send-email" while their
// filters stay distinct (task.send.email.> vs task.send-email.>).
// Adopting purely by name therefore hands a poller another type's work.
// The bridge must refuse and say why, naming both filters.
func TestPollRejectsNameCollidingDurable(t *testing.T) {
	js, b, ts := newPollConsumerBridge(t)
	preCreateDurable(
		t, js, "workers-send-email", "task.send-email.>", adoptedAckWait,
	)
	// Work exists ONLY for the hyphenated type. A bridge that adopts by
	// name will happily serve it to a send.email poller.
	publishTaskFixture(t, js, "send-email", "hyphen-1")

	body := fmt.Sprintf(
		`{"task_types":["send.email"],"max_tasks":1,"timeout_ms":%d}`,
		pollConsumerFetchTimeoutMs,
	)
	status, respBody := postPollRaw(t, ts, body)
	if status == http.StatusOK {
		t.Fatalf(
			"poll served a wrong-type task instead of erroring: body=%q",
			respBody,
		)
	}
	for _, want := range []string{
		"task.send.email.>", "task.send-email.>", "workers-send-email",
	} {
		if !strings.Contains(respBody, want) {
			t.Fatalf("error body omits %q: %q", want, respBody)
		}
	}
	// Negative space: nothing was dispatched, so nothing may be pinned
	// in the ackMap awaiting a resolve that will never come.
	if got := b.ackMap.Count(); got != 0 {
		t.Fatalf("ackMap holds %d tasks after a rejected poll, want 0",
			got)
	}
}

// TestPollDegradesPerTaskType is the finding-2 guard. One unusable task
// type must not destroy the results of a healthy one: the healthy task
// is already dispatched (step.started published, message parked in the
// ackMap) by the time the later type fails, and discarding it strands
// that message unacked until AckWait expires while the run shows a
// started attempt nobody is running.
func TestPollDegradesPerTaskType(t *testing.T) {
	js, b, ts := newPollConsumerBridge(t)
	// A grouped durable makes "grp" unpollable: the bridge's ungrouped
	// filter task.grp.> overlaps it, and the work-queue stream rejects
	// the second consumer (err 10100).
	preCreateDurable(
		t, js, "workers-grp-alpha", "task.grp.alpha.>", adoptedAckWait,
	)
	publishTaskFixture(t, js, "healthy", "healthy-1")

	body := fmt.Sprintf(
		`{"task_types":["healthy","grp"],"max_tasks":2,"timeout_ms":%d}`,
		pollConsumerFetchTimeoutMs,
	)
	status, respBody := postPollRaw(t, ts, body)
	if status != http.StatusOK {
		t.Fatalf(
			"healthy type lost to a sibling's fault: status %d, body %q",
			status, respBody,
		)
	}
	var tasks []pollResponse
	if err := json.Unmarshal([]byte(respBody), &tasks); err != nil {
		t.Fatalf("decode poll body %q: %v", respBody, err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1 (the healthy type's)", len(tasks))
	}
	if tasks[0].RunID != "healthy-1" {
		t.Fatalf("served run %q, want %q", tasks[0].RunID, "healthy-1")
	}
	// The ackMap must mirror exactly what was handed out — no ghost
	// entry for the type that never produced a task.
	if got := b.ackMap.Count(); got != 1 {
		t.Fatalf("ackMap holds %d tasks, want 1", got)
	}
}

// TestPollFailsWhenEveryTypeFails pins the other side of per-type
// degradation: partial success returns 200, but a poll where nothing
// could be fetched AND something faulted is a topology error, not "no
// work". Reporting it as an empty array is what hid #532.
func TestPollFailsWhenEveryTypeFails(t *testing.T) {
	js, b, ts := newPollConsumerBridge(t)
	preCreateDurable(
		t, js, "workers-grp-alpha", "task.grp.alpha.>", adoptedAckWait,
	)

	body := fmt.Sprintf(
		`{"task_types":["grp"],"max_tasks":1,"timeout_ms":%d}`,
		pollConsumerFetchTimeoutMs,
	)
	status, respBody := postPollRaw(t, ts, body)
	if status != http.StatusInternalServerError {
		t.Fatalf("got status %d, want 500 (body %q)", status, respBody)
	}
	if !strings.Contains(respBody, "task.grp.>") {
		t.Fatalf("error body does not name the colliding filter: %q",
			respBody)
	}
	if got := b.ackMap.Count(); got != 0 {
		t.Fatalf("ackMap holds %d tasks after a failed poll, want 0", got)
	}
}

// TestPollUsesDurableNotEphemeral pins churn elimination: after two
// polls the filter must carry exactly one consumer, and it must be
// the canonical durable the native worker would create.
func TestPollUsesDurableNotEphemeral(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	publishTaskFixture(t, js, "churn", "churn-1")

	for i := 0; i < 2; i++ {
		postPoll(t, ts, "churn", 1, pollConsumerFetchTimeoutMs)
	}

	got := consumersOnFilter(t, js, "task.churn.>")
	if len(got) != 1 {
		t.Fatalf(
			"got %d consumers on task.churn.>, want exactly 1", len(got),
		)
	}
	if got[0].Durable != "workers-churn" {
		t.Fatalf(
			"consumer Durable = %q, want %q (ephemeral churn persists)",
			got[0].Durable, "workers-churn",
		)
	}
	if got[0].AckWait != 5*time.Minute {
		t.Fatalf(
			"consumer AckWait = %v, want 5m to match the native worker",
			got[0].AckWait,
		)
	}
}

// TestGroupedCollisionIsLoud is the regression guard for the whole
// silent-failure class. Grouped polling over the bridge is not
// supported; a WorkQueuePolicy uniqueness rejection must surface as a
// descriptive error, never as HTTP 200 with an empty task list.
func TestGroupedCollisionIsLoud(t *testing.T) {
	js, _, ts := newPollConsumerBridge(t)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	_, err := js.CreateOrUpdateConsumer(
		ctx, "TASK_QUEUES", jetstream.ConsumerConfig{
			Durable:       "workers-grp-alpha",
			Name:          "workers-grp-alpha",
			FilterSubject: "task.grp.alpha.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckWait:       adoptedAckWait,
			MaxDeliver:    -1,
		},
	)
	if err != nil {
		t.Fatalf("pre-create grouped durable: %v", err)
	}

	body := fmt.Sprintf(
		`{"task_types":["grp"],"max_tasks":1,"timeout_ms":%d}`,
		pollConsumerFetchTimeoutMs,
	)
	status, respBody := postPollRaw(t, ts, body)
	if status == http.StatusOK {
		t.Fatalf(
			"grouped collision reported as success: body=%q", respBody,
		)
	}
	if !strings.Contains(respBody, "task.grp.>") {
		t.Fatalf(
			"error body does not name the colliding filter: %q",
			respBody,
		)
	}
}

// TestPollRejectsInvalidTaskType covers the validation that replaces
// the ephemerals' self-cleaning behaviour: a durable created for a
// typo'd type never self-deletes, so the typo must be rejected at the
// door. Positive space: a dotted namespace type stays legal.
func TestPollRejectsInvalidTaskType(t *testing.T) {
	_, _, ts := newPollConsumerBridge(t)

	bad := []string{"", "has space", "wild*card", "greedy>", ".lead",
		"trail.", "double..dot"}
	for _, taskType := range bad {
		body := fmt.Sprintf(
			`{"task_types":["%s"],"max_tasks":1,"timeout_ms":100}`,
			taskType,
		)
		status, respBody := postPollRaw(t, ts, body)
		if status != http.StatusBadRequest {
			t.Fatalf(
				"task_type %q: got status %d, want 400 (body %q)",
				taskType, status, respBody,
			)
		}
	}

	body := fmt.Sprintf(
		`{"task_types":["send.email"],"max_tasks":1,"timeout_ms":%d}`,
		100,
	)
	status, respBody := postPollRaw(t, ts, body)
	if status != http.StatusOK {
		t.Fatalf(
			"dotted task type rejected: status %d, body %q",
			status, respBody,
		)
	}
}

// TestAdoptConsumerContract pins the helper the finding-4 TOCTOU branch
// relies on. That branch fires only when a native worker wins a race
// between our lookup and our create, which no black-box HTTP test can
// schedule deterministically — so the contract it depends on is
// asserted directly: absent reports an errors.Is-able
// ErrConsumerNotFound, a differing-config-but-right-filter durable is
// adopted, and a name-collision durable is refused.
func TestAdoptConsumerContract(t *testing.T) {
	js, b, _ := newPollConsumerBridge(t)
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	_, err := b.adoptConsumer(ctx, "workers-absent", "task.absent.>")
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("absent consumer: got %v, want ErrConsumerNotFound", err)
	}

	// AckWait differs from ours — exactly the shape that makes
	// CreateConsumer answer ErrConsumerExists instead of succeeding.
	preCreateDurable(
		t, js, "workers-toctou", "task.toctou.>", adoptedAckWait,
	)
	cons, err := b.adoptConsumer(ctx, "workers-toctou", "task.toctou.>")
	if err != nil {
		t.Fatalf("adopt matching durable: %v", err)
	}
	if got := cons.CachedInfo().Config.AckWait; got != adoptedAckWait {
		t.Fatalf("adopted AckWait = %v, want %v", got, adoptedAckWait)
	}

	preCreateDurable(
		t, js, "workers-send-email", "task.send-email.>", adoptedAckWait,
	)
	_, err = b.adoptConsumer(
		ctx, "workers-send-email", "task.send.email.>",
	)
	if err == nil {
		t.Fatal("adopted a durable serving a different filter")
	}
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("mismatch must not masquerade as not-found: %v", err)
	}
}
