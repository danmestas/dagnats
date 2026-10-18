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
	"errors"
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
	replyBytes  int             // >0: reply with this many bytes instead of an echo
	t           *testing.T      // set by start; callbacks report through it
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
	o.t = t
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
		o.respondMsg(msg, reject)
		return
	}
	o.lastFence.Store(fence)
	o.processed.Add(1)
	if o.replyBytes > 0 {
		o.respond(msg, make([]byte, o.replyBytes))
		return
	}
	o.respond(msg, append([]byte(o.holder+":"), msg.Data...))
}

// respond replies with data, failing the test on any error but a closed
// connection (a callback can outlive the test's server).
func (o *testOwner) respond(msg *nats.Msg, data []byte) {
	reply := nats.NewMsg(msg.Reply)
	reply.Data = data
	o.respondMsg(msg, reply)
}

func (o *testOwner) respondMsg(msg, reply *nats.Msg) {
	if err := msg.RespondMsg(reply); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		o.t.Errorf("owner %s respond: %v", o.holder, err)
	}
}

func unsubscribe(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		t.Errorf("unsubscribe: %v", err)
	}
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

// A hung owner on one key must not delay a forward to another key: each
// forward runs on its own goroutine, not the endpoint's single one.
func TestLeaseForwardHungOwnerDoesNotBlockOthers(t *testing.T) {
	nc := startLeaseAPI(t)
	hungRevision := acquireFor(t, nc, "shard-1", "notify-a")
	hung := newOwner("notify-a", hungRevision)
	hung.silent = true
	hung.start(t, nc)
	startOwner(t, nc, "notify-b", acquireFor(t, nc, "shard-2", "notify-b"))

	data, err := json.Marshal(forwardBody("shard-1", "x", 3000))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	first := make(chan forwardWireReply, 1)
	go func() {
		var reply forwardWireReply
		msg, err := nc.Request("api.leases.forward", data, 10*time.Second)
		if err != nil {
			t.Errorf("hung forward request: %v", err)
		} else if err := json.Unmarshal(msg.Data, &reply); err != nil {
			t.Errorf("hung forward: unmarshal %q: %v", msg.Data, err)
		}
		first <- reply
	}()
	time.Sleep(100 * time.Millisecond) // let the hung forward reach the owner
	start := time.Now()
	got := forwardCall(t, nc, forwardBody("shard-2", "y", 2000))
	if got.Holder != "notify-b" || got.Error != "" {
		t.Fatalf("second forward = %+v, want served by notify-b", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("second forward took %v behind a hung owner, want well under 1s", elapsed)
	}
	if hungReply := <-first; hungReply.Kind != leaseKindOwnerTimeout {
		t.Fatalf("hung forward = %+v, want owner_timeout", hungReply)
	}
}

// With forwardInFlightMax forwards parked on a hung owner, the next one
// is refused at once with busy: it reached nobody.
func TestLeaseForwardPastCapIsBusy(t *testing.T) {
	nc := startLeaseAPI(t)
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	hung := newOwner("notify-a", revision)
	hung.silent = true
	hung.start(t, nc)

	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { unsubscribe(t, sub) })
	parked, err := json.Marshal(forwardBody("shard-1", "x", 2000))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for i := 0; i < forwardInFlightMax; i++ {
		if err := nc.PublishRequest("api.leases.forward", inbox, parked); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	got := forwardCall(t, nc, forwardBody("shard-1", "y", 1000))
	if got.Kind != leaseKindBusy || got.Holder != "" {
		t.Fatalf("forward %d = %+v, want kind busy", forwardInFlightMax+1, got)
	}
	if early, err := sub.NextMsg(10 * time.Millisecond); err == nil {
		t.Fatalf("a parked forward replied early: %s", early.Data)
	}
	// Every parked forward times out and must hand its slot back.
	for i := 0; i < forwardInFlightMax; i++ {
		if _, err := sub.NextMsg(10 * time.Second); err != nil {
			t.Fatalf("parked forward %d never replied: %v", i, err)
		}
	}
	after := forwardCall(t, nc, forwardBody("shard-1", "z", 200))
	if after.Kind != leaseKindOwnerTimeout || after.Holder != "notify-a" {
		t.Fatalf("forward after the parked ones finished = %+v, "+
			"want owner_timeout (slots released), not busy", after)
	}
}

// The owner processed the request, but its reply cannot fit the
// dagnats hop once base64-encoded: the caller must hear that, not time out.
func TestLeaseForwardOversizedReplyIsReplyTooLarge(t *testing.T) {
	nc := startLeaseAPI(t)
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	owner := newOwner("notify-a", revision)
	owner.replyBytes = int(nc.MaxPayload()) * 9 / 10
	owner.start(t, nc)

	got := forwardCall(t, nc, forwardBody("shard-1", "x", 2000))
	if got.Kind != leaseKindReplyTooLarge || len(got.Payload) != 0 {
		t.Fatalf("oversized reply = kind %q (%d bytes), want reply_too_large",
			got.Kind, len(got.Payload))
	}
	if owner.processed.Load() != 1 || got.Holder != "notify-a" {
		t.Fatalf("processed=%d holder=%q, want 1 and notify-a",
			owner.processed.Load(), got.Holder)
	}
}

// A record that names no valid holder (corrupt, or written around the
// API) fails the call; it must not crash the forward goroutine.
func TestLeaseForwardCorruptHolderIsInternal(t *testing.T) {
	nc := startLeaseAPI(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record := []byte(`{"holder":"","acquired_at":"2026-10-18T12:00:00Z","ttl_ms":10000}`)
	if _, err := js.Publish(ctx, "$KV."+natsutil.LeasesBucket+".shard-1", record); err != nil {
		t.Fatalf("write corrupt record: %v", err)
	}
	got := forwardCall(t, nc, forwardBody("shard-1", "x", 1000))
	if got.Kind != leaseKindInternal || got.Holder != "" {
		t.Fatalf("forward on corrupt record = %+v, want kind internal", got)
	}
	again := forwardCall(t, nc, forwardBody("shard-1", "x", 1000))
	if again.Kind != leaseKindInternal {
		t.Fatalf("second forward = %+v, want the instance still serving", again)
	}
}

// Stop cancels in-flight forwards instead of leaving them parked on a
// hung owner for up to timeout_ms.
func TestLeaseForwardStopCancelsInFlight(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	natsAPI := NewNATSAPI(NewService(nc), nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	revision := acquireFor(t, nc, "shard-1", "notify-a")
	hung := newOwner("notify-a", revision)
	hung.silent = true
	hung.start(t, nc)

	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { unsubscribe(t, sub) })
	data, err := json.Marshal(forwardBody("shard-1", "x", 20000))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := nc.PublishRequest("api.leases.forward", inbox, data); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the forward park on the owner
	natsAPI.Stop()
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no reply within 3s of Stop (timeout_ms is 20s): %v", err)
	}
	var reply forwardWireReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.Kind != leaseKindInternal {
		t.Fatalf("reply after Stop = %s (%v), want kind internal", msg.Data, err)
	}
}
