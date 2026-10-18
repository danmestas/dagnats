// api/natsapi_lease_forward_test.go
// Methodology: real embedded NATS + SetupAll + a started NATSAPI. The
// "owners" are plain NATS subscribers on the documented inbox subject
// that apply the documented fence rule (reject when not holding, or when
// the fence predates the tenure's acquire revision), exactly as a
// downstream replica would. Every check goes over the wire: forward
// reaches the owner with the right fence, each failure mode returns its
// explicit kind within the caller's bound, ownership moves are followed,
// and a stale fence is refused by the owner rather than processed.
package api

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/lease"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// forwardWireReply mirrors the api.leases.forward reply.
type forwardWireReply struct {
	Key      string `json:"key"`
	Holder   string `json:"holder"`
	Revision uint64 `json:"revision"`
	Payload  []byte `json:"payload"`
	Error    string `json:"error"`
	Kind     string `json:"kind"`
}

// testOwner is a downstream replica's inbox: it answers forwarded
// requests for holder, applying the documented fence rule against the
// tenure-start revision it holds in tenureStart (0 = not holding).
type testOwner struct {
	holder      string
	tenureStart atomic.Uint64
	lastFence   atomic.Uint64
	processed   atomic.Int64
	rejected    atomic.Int64
	onRequest   func(*nats.Msg) // optional hook run before the fence check
	silent      bool            // never reply: a hung owner
}

func newOwner(holder string, tenure uint64) *testOwner {
	owner := &testOwner{holder: holder}
	owner.tenureStart.Store(tenure)
	return owner
}

func startOwner(t *testing.T, nc *nats.Conn, holder string, tenure uint64) *testOwner {
	t.Helper()
	return newOwner(holder, tenure).start(t, nc)
}

// start subscribes the owner's inbox. Set hooks before calling it: the
// callback goroutine reads them without synchronization.
func (o *testOwner) start(t *testing.T, nc *nats.Conn) *testOwner {
	t.Helper()
	subject := lease.InboxSubjectPrefix + "." + o.holder + ".>"
	sub, err := nc.Subscribe(subject, o.handle)
	if err != nil {
		t.Fatalf("owner subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil && err != nats.ErrConnectionClosed {
			t.Errorf("owner unsubscribe: %v", err)
		}
	})
	return o
}

// handle applies the owner-side rule from wire-protocol.md.
func (o *testOwner) handle(msg *nats.Msg) {
	if o.onRequest != nil {
		o.onRequest(msg)
	}
	if o.silent {
		return
	}
	fence, err := strconv.ParseUint(msg.Header.Get(lease.FenceHeader), 10, 64)
	tenure := o.tenureStart.Load()
	if err != nil || tenure == 0 || fence < tenure {
		o.rejected.Add(1)
		reject := nats.NewMsg(msg.Reply)
		reject.Header.Set(lease.RejectHeader, lease.RejectStale)
		_ = msg.RespondMsg(reject)
		return
	}
	o.lastFence.Store(fence)
	o.processed.Add(1)
	_ = msg.Respond(append([]byte(o.holder+":"), msg.Data...))
}

func forwardCall(t *testing.T, nc *nats.Conn, body any) forwardWireReply {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request("api.leases.forward", data, 10*time.Second)
	if err != nil {
		t.Fatalf("api.leases.forward: %v", err)
	}
	var reply forwardWireReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("forward: unmarshal %q: %v", msg.Data, err)
	}
	return reply
}

func acquireFor(t *testing.T, nc *nats.Conn, key, holder string) uint64 {
	t.Helper()
	got := leaseCall(t, nc, "api.leases.acquire",
		map[string]any{"key": key, "holder": holder, "ttl_ms": 10000})
	if !got.Granted || got.Revision == 0 {
		t.Fatalf("acquire %s for %s = %+v, want granted", key, holder, got)
	}
	return got.Revision
}

func releaseFor(t *testing.T, nc *nats.Conn, key, holder string, revision uint64) {
	t.Helper()
	got := leaseCall(t, nc, "api.leases.release",
		map[string]any{"key": key, "holder": holder, "revision": revision})
	if got.Error != "" {
		t.Fatalf("release %s = %+v, want success", key, got)
	}
}

func forwardBody(key, payload string, timeoutMs int) map[string]any {
	return map[string]any{"key": key, "payload": []byte(payload), "timeout_ms": timeoutMs}
}

func TestLeaseForwardReachesOwnerWithFence(t *testing.T) {
	nc := startLeaseAPI(t)
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	owner := startOwner(t, nc, "notify-a", revision)

	got := forwardCall(t, nc, forwardBody("shard-1", "hello", 2000))
	if got.Error != "" || got.Holder != "notify-a" || got.Revision != revision {
		t.Fatalf("forward = %+v, want notify-a at %d", got, revision)
	}
	if string(got.Payload) != "notify-a:hello" {
		t.Fatalf("payload = %q, want the owner's reply", got.Payload)
	}
	if owner.lastFence.Load() != revision || owner.processed.Load() != 1 {
		t.Fatalf("owner saw fence %d (%d processed), want %d once",
			owner.lastFence.Load(), owner.processed.Load(), revision)
	}
}

func TestLeaseForwardWithoutOwnerIsNoOwner(t *testing.T) {
	nc := startLeaseAPI(t)
	never := forwardCall(t, nc, forwardBody("shard-1", "x", 1000))
	if never.Kind != leaseKindNoOwner || len(never.Payload) != 0 {
		t.Fatalf("forward to never-held key = %+v, want kind no_owner", never)
	}
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	releaseFor(t, nc, "shard-1", "notify-a", revision)
	released := forwardCall(t, nc, forwardBody("shard-1", "x", 1000))
	if released.Kind != leaseKindNoOwner || released.Holder != "" {
		t.Fatalf("forward after release = %+v, want kind no_owner", released)
	}
}

// A holder whose process died leaves a live lease and no inbox
// subscriber. The forward must fail fast and say the request was not
// delivered, not wait out timeout_ms.
func TestLeaseForwardOwnerGoneIsUnavailable(t *testing.T) {
	nc := startLeaseAPI(t)
	acquireFor(t, nc, "shard-1", "notify-a")

	start := time.Now()
	got := forwardCall(t, nc, forwardBody("shard-1", "x", 5000))
	elapsed := time.Since(start)
	if got.Kind != leaseKindOwnerUnavailable || got.Holder != "notify-a" {
		t.Fatalf("forward to dead owner = %+v, want owner_unavailable naming it", got)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("no-responders took %v, want well under timeout_ms", elapsed)
	}
}

func TestLeaseForwardHungOwnerTimesOutWithinBound(t *testing.T) {
	nc := startLeaseAPI(t)
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	owner := newOwner("notify-a", revision)
	owner.silent = true
	owner.start(t, nc)

	start := time.Now()
	got := forwardCall(t, nc, forwardBody("shard-1", "x", 300))
	elapsed := time.Since(start)
	if got.Kind != leaseKindOwnerTimeout || got.Holder != "notify-a" {
		t.Fatalf("forward to hung owner = %+v, want owner_timeout naming it", got)
	}
	if elapsed < 300*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("hung owner returned after %v, want about timeout_ms", elapsed)
	}
}

func TestLeaseForwardFollowsOwnershipMove(t *testing.T) {
	nc := startLeaseAPI(t)
	first := acquireFor(t, nc, "shard-1", "notify-a")
	ownerA := startOwner(t, nc, "notify-a", first)
	if got := forwardCall(t, nc, forwardBody("shard-1", "1", 2000)); got.Holder != "notify-a" {
		t.Fatalf("first forward = %+v, want notify-a", got)
	}
	releaseFor(t, nc, "shard-1", "notify-a", first)
	ownerA.tenureStart.Store(0)
	second := acquireFor(t, nc, "shard-1", "notify-b")
	ownerB := startOwner(t, nc, "notify-b", second)

	got := forwardCall(t, nc, forwardBody("shard-1", "2", 2000))
	if got.Holder != "notify-b" || got.Revision != second || got.Revision <= first {
		t.Fatalf("forward after move = %+v, want notify-b at %d", got, second)
	}
	if ownerA.processed.Load() != 1 || ownerB.processed.Load() != 1 {
		t.Fatalf("processed a=%d b=%d, want 1 each",
			ownerA.processed.Load(), ownerB.processed.Load())
	}
}

// The owner rule over the direct (convention) path: a sender holding a
// cached fence from the owner's previous tenure is refused.
func TestLeaseOwnerRejectsStaleFence(t *testing.T) {
	nc := startLeaseAPI(t)
	old := acquireFor(t, nc, "shard-1", "notify-a")
	releaseFor(t, nc, "shard-1", "notify-a", old)
	tenure := acquireFor(t, nc, "shard-1", "notify-a")
	owner := startOwner(t, nc, "notify-a", tenure)

	msg := nats.NewMsg(lease.InboxSubject("notify-a", "shard-1"))
	msg.Header.Set(lease.FenceHeader, strconv.FormatUint(old, 10))
	msg.Data = []byte("late")
	resp, err := nc.RequestMsg(msg, 2*time.Second)
	if err != nil {
		t.Fatalf("direct request: %v", err)
	}
	if resp.Header.Get(lease.RejectHeader) != lease.RejectStale {
		t.Fatalf("stale fence %d (tenure %d) answered %q, want rejection",
			old, tenure, resp.Data)
	}
	if owner.processed.Load() != 0 || owner.rejected.Load() != 1 {
		t.Fatalf("processed=%d rejected=%d, want 0 and 1",
			owner.processed.Load(), owner.rejected.Load())
	}
}

// An owner that has stopped acting as owner refuses; KV still names it,
// so re-reading finds nobody new and forward reports the request as
// undelivered instead of looping.
func TestLeaseForwardRefusedByOwnerIsUnavailable(t *testing.T) {
	nc := startLeaseAPI(t)
	acquireFor(t, nc, "shard-1", "notify-a")
	owner := startOwner(t, nc, "notify-a", 0)

	got := forwardCall(t, nc, forwardBody("shard-1", "x", 2000))
	if got.Kind != leaseKindOwnerUnavailable || len(got.Payload) != 0 {
		t.Fatalf("forward to refusing owner = %+v, want owner_unavailable", got)
	}
	if owner.processed.Load() != 0 || owner.rejected.Load() != 1 {
		t.Fatalf("processed=%d rejected=%d, want 0 and 1 (no retry to same fence)",
			owner.processed.Load(), owner.rejected.Load())
	}
}

// A handoff in flight: A hands the key to B while A's copy of the
// request is on the wire, then refuses it. Forward re-reads the owner
// and delivers to B -- the caller sees one success, not an error.
func TestLeaseForwardRetriesAfterHandoff(t *testing.T) {
	nc := startLeaseAPI(t)
	first := acquireFor(t, nc, "shard-1", "notify-a")
	ownerB := startOwner(t, nc, "notify-b", 0)
	ownerA := newOwner("notify-a", first)
	ownerA.onRequest = func(*nats.Msg) {
		ownerA.tenureStart.Store(0)
		handoff(t, nc, first, ownerB)
	}
	ownerA.start(t, nc)

	got := forwardCall(t, nc, forwardBody("shard-1", "x", 3000))
	if got.Error != "" || got.Holder != "notify-b" || got.Revision <= first {
		t.Fatalf("forward across handoff = %+v, want served by notify-b", got)
	}
	if ownerA.processed.Load() != 0 || ownerB.processed.Load() != 1 {
		t.Fatalf("processed a=%d b=%d, want 0 and 1",
			ownerA.processed.Load(), ownerB.processed.Load())
	}
}

// handoff releases A's lease and acquires it for B from inside A's
// subscription callback, so it reports failures without t.Fatal.
func handoff(t *testing.T, nc *nats.Conn, revision uint64, ownerB *testOwner) {
	calls := []struct {
		subject string
		body    string
	}{
		{"api.leases.release", `{"key":"shard-1","holder":"notify-a","revision":` +
			strconv.FormatUint(revision, 10) + `}`},
		{"api.leases.acquire", `{"key":"shard-1","holder":"notify-b","ttl_ms":10000}`},
	}
	for _, call := range calls {
		msg, err := nc.Request(call.subject, []byte(call.body), 2*time.Second)
		if err != nil {
			t.Errorf("handoff %s: %v", call.subject, err)
			return
		}
		var reply leaseReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.Error != "" {
			t.Errorf("handoff %s = %s (%v)", call.subject, msg.Data, err)
			return
		}
		ownerB.tenureStart.Store(reply.Revision)
	}
}

func TestLeaseForwardRejectsBadInput(t *testing.T) {
	nc := startLeaseAPI(t)
	msg, err := nc.Request("api.leases.forward", []byte("{not json"), 5*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var garbled forwardWireReply
	if err := json.Unmarshal(msg.Data, &garbled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if garbled.Kind != leaseKindTransport {
		t.Fatalf("malformed body = %+v, want kind transport", garbled)
	}
	bad := []map[string]any{
		forwardBody("a.b", "x", 1000),
		forwardBody("", "x", 1000),
		forwardBody("shard-1", "x", 0),
		forwardBody("shard-1", "x", 99),
		forwardBody("shard-1", "x", 30001),
	}
	for _, body := range bad {
		got := forwardCall(t, nc, body)
		if got.Kind != leaseKindInvalid || got.Error == "" {
			t.Errorf("forward %v = %+v, want kind invalid", body, got)
		}
	}
}

func deleteLeasesStream(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteStream(ctx, "KV_"+natsutil.LeasesBucket); err != nil {
		t.Fatalf("delete leases stream: %v", err)
	}
}

func TestLeaseForwardWithoutBucketReportsUnsupported(t *testing.T) {
	nc := startLeaseAPI(t)
	deleteLeasesStream(t, nc)
	got := forwardCall(t, nc, forwardBody("shard-1", "x", 1000))
	if got.Kind != leaseKindUnsupported || got.Holder != "" {
		t.Fatalf("forward without bucket = %+v, want kind unsupported", got)
	}
}
