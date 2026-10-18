// lease/lease_test.go
// Methodology: every test runs the Store against its own real embedded
// NATS server provisioned by natsutil.SetupAll, so TTL expiry, delete
// markers, CAS rejections and the server-side republish of changes are
// the real server's behavior, not a fake's. Each test asserts the
// positive outcome and the negative space (the contender denied, the
// stale fence rejected, the event that must carry the right kind).
// Waits are bounded by explicit timeouts.
package lease

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newTestStore(t *testing.T) (*Store, *nats.Conn) {
	t.Helper()
	s, nc := natsutil.StartTestServer(t)
	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
	})
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	return NewStore(nc), nc
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestAcquireContentionGrantsOneHolder(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := testContext(t)

	first, granted, err := store.Acquire(ctx, "leader", "node-a", 10*time.Second)
	if err != nil || !granted {
		t.Fatalf("first acquire: granted=%v err=%v", granted, err)
	}
	if first.Revision == 0 || first.Holder != "node-a" {
		t.Fatalf("first acquire lease = %+v, want node-a with a revision", first)
	}

	denied, granted, err := store.Acquire(ctx, "leader", "node-b", 10*time.Second)
	if err != nil {
		t.Fatalf("contended acquire must not error: %v", err)
	}
	if granted {
		t.Fatal("node-b must not be granted a lease node-a holds")
	}
	if denied.Holder != "node-a" || denied.Revision != first.Revision {
		t.Fatalf("denied reply = %+v, want current holder node-a", denied)
	}

	again, granted, err := store.Acquire(ctx, "leader", "node-a", 10*time.Second)
	if err != nil || !granted {
		t.Fatalf("re-acquire by holder: granted=%v err=%v", granted, err)
	}
	if again.Revision != first.Revision {
		t.Fatalf("re-acquire must be idempotent: rev %d, want %d",
			again.Revision, first.Revision)
	}
}

func TestRenewWithStaleRevisionIsLost(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := testContext(t)

	held, _, err := store.Acquire(ctx, "shard-1", "node-a", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	renewed, err := store.Renew(ctx, "shard-1", "node-a", held.Revision, 20*time.Second)
	if err != nil {
		t.Fatalf("renew with current revision: %v", err)
	}
	if renewed.Revision <= held.Revision || renewed.TTL != 20*time.Second {
		t.Fatalf("renewed = %+v, want a newer revision and the new TTL", renewed)
	}
	if !renewed.AcquiredAt.Equal(held.AcquiredAt) {
		t.Fatal("renew must preserve acquired_at")
	}

	_, err = store.Renew(ctx, "shard-1", "node-a", held.Revision, 10*time.Second)
	if !errors.Is(err, ErrLost) {
		t.Fatalf("renew with stale revision: err=%v, want ErrLost", err)
	}
	_, err = store.Renew(ctx, "shard-1", "node-b", renewed.Revision, 10*time.Second)
	if !errors.Is(err, ErrLost) {
		t.Fatalf("renew by non-holder: err=%v, want ErrLost", err)
	}
	_, err = store.Renew(ctx, "never-held", "node-a", 1, 10*time.Second)
	if !errors.Is(err, ErrLost) {
		t.Fatalf("renew of a missing key: err=%v, want ErrLost", err)
	}
}

func TestExpiryFreesKeyAndEmitsExpiredEvent(t *testing.T) {
	store, nc := newTestStore(t)
	ctx := testContext(t)
	events := subscribeChanges(t, nc, "leader")

	held, _, err := store.Acquire(ctx, "leader", "node-a", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	acquired := nextEvent(t, events)
	if acquired.Header.Get(EventHeader) != EventAcquired {
		t.Fatalf("first event header = %v, want acquired", acquired.Header)
	}
	if acquired.Header.Get("Nats-Sequence") != strconv.FormatUint(held.Revision, 10) {
		t.Fatalf("event revision %q, want %d",
			acquired.Header.Get("Nats-Sequence"), held.Revision)
	}

	expired := nextEvent(t, events)
	if expired.Header.Get(EventHeader) != "" {
		t.Fatalf("expiry is server-written, got dagnats header %v", expired.Header)
	}
	if expired.Header.Get(MarkerReasonHeader) != "MaxAge" {
		t.Fatalf("expired event header = %v, want Nats-Marker-Reason", expired.Header)
	}

	_, held2, err := store.Owner(ctx, "leader")
	if err != nil || held2 {
		t.Fatalf("owner after expiry: held=%v err=%v, want not held", held2, err)
	}
	next, granted, err := store.Acquire(ctx, "leader", "node-b", 10*time.Second)
	if err != nil || !granted {
		t.Fatalf("acquire after expiry: granted=%v err=%v", granted, err)
	}
	if next.Revision <= held.Revision {
		t.Fatalf("fence must advance across holders: %d <= %d",
			next.Revision, held.Revision)
	}
}

func TestReleaseRequiresCurrentFence(t *testing.T) {
	store, nc := newTestStore(t)
	ctx := testContext(t)
	events := subscribeChanges(t, nc, "shard-2")

	held, _, err := store.Acquire(ctx, "shard-2", "node-a", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	nextEvent(t, events) // acquired
	if err := store.Release(ctx, "shard-2", "node-a", held.Revision+7); !errors.Is(err, ErrLost) {
		t.Fatalf("release with wrong revision: err=%v, want ErrLost", err)
	}
	if err := store.Release(ctx, "shard-2", "node-b", held.Revision); !errors.Is(err, ErrLost) {
		t.Fatalf("release by non-holder: err=%v, want ErrLost", err)
	}
	if err := store.Release(ctx, "shard-2", "node-a", held.Revision); err != nil {
		t.Fatalf("release with current fence: %v", err)
	}
	released := nextEvent(t, events)
	if released.Header.Get(EventHeader) != EventReleased {
		t.Fatalf("release event header = %v, want released", released.Header)
	}

	_, isHeld, err := store.Owner(ctx, "shard-2")
	if err != nil || isHeld {
		t.Fatalf("owner after release: held=%v err=%v", isHeld, err)
	}
	if err := store.Release(ctx, "shard-2", "node-a", held.Revision); !errors.Is(err, ErrLost) {
		t.Fatalf("double release: err=%v, want ErrLost", err)
	}
	_, granted, err := store.Acquire(ctx, "shard-2", "node-b", 10*time.Second)
	if err != nil || !granted {
		t.Fatalf("acquire after release: granted=%v err=%v", granted, err)
	}
}

func TestOwnerReportsHolderAndExpiry(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := testContext(t)

	_, isHeld, err := store.Owner(ctx, "leader")
	if err != nil || isHeld {
		t.Fatalf("owner of unheld key: held=%v err=%v", isHeld, err)
	}
	before := time.Now()
	held, _, err := store.Acquire(ctx, "leader", "node-a", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	owner, isHeld, err := store.Owner(ctx, "leader")
	if err != nil || !isHeld {
		t.Fatalf("owner of held key: held=%v err=%v", isHeld, err)
	}
	if owner.Holder != "node-a" || owner.Revision != held.Revision {
		t.Fatalf("owner = %+v, want node-a at %d", owner, held.Revision)
	}
	window := owner.ExpiresAt.Sub(before)
	if window < 25*time.Second || window > 35*time.Second {
		t.Fatalf("expires_at %v is not ~30s after acquire", window)
	}
}

func TestBadInputIsRejectedNotPanicked(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := testContext(t)
	cases := []struct {
		name, key, holder string
		ttl               time.Duration
	}{
		{"empty key", "", "node-a", 10 * time.Second},
		{"dotted key", "shard.1", "node-a", 10 * time.Second},
		{"wildcard key", "shard*", "node-a", 10 * time.Second},
		{"long key", strings.Repeat("k", KeyLengthMax+1), "node-a", 10 * time.Second},
		{"empty holder", "leader", "", 10 * time.Second},
		{"holder with space", "leader", "node a", 10 * time.Second},
		{"long holder", "leader", strings.Repeat("h", HolderLengthMax+1), time.Minute},
		{"zero ttl", "leader", "node-a", 0},
		{"ttl below min", "leader", "node-a", 500 * time.Millisecond},
		{"ttl above max", "leader", "node-a", TTLMax + time.Second},
		{"fractional ttl", "leader", "node-a", 1500 * time.Millisecond},
	}
	for _, c := range cases {
		_, granted, err := store.Acquire(ctx, c.key, c.holder, c.ttl)
		if !errors.Is(err, ErrInvalid) || granted {
			t.Errorf("%s: granted=%v err=%v, want ErrInvalid", c.name, granted, err)
		}
	}
	_, err := store.Renew(ctx, "leader", "node-a", 0, 10*time.Second)
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("renew without revision: err=%v, want ErrInvalid", err)
	}
	if err := store.Release(ctx, "leader", "node-a", 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("release without revision: err=%v, want ErrInvalid", err)
	}
	if _, _, err := store.Owner(ctx, "a.b"); !errors.Is(err, ErrInvalid) {
		t.Errorf("owner of invalid key: err=%v, want ErrInvalid", err)
	}
	_, granted, err := store.Acquire(ctx, strings.Repeat("k", KeyLengthMax), "node-a", TTLMax)
	if err != nil || !granted {
		t.Errorf("boundary key/ttl must be accepted: granted=%v err=%v", granted, err)
	}
}

func TestConcurrentAcquireGrantsExactlyOne(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := testContext(t)
	const contenders = 8
	type result struct {
		granted bool
		err     error
	}
	results := make(chan result, contenders)
	for i := 0; i < contenders; i++ {
		holder := "node-" + strconv.Itoa(i)
		go func() {
			_, granted, err := store.Acquire(ctx, "leader", holder, 10*time.Second)
			results <- result{granted, err}
		}()
	}
	grantedCount := 0
	for i := 0; i < contenders; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("contender errored: %v", r.err)
		}
		if r.granted {
			grantedCount++
		}
	}
	if grantedCount != 1 {
		t.Fatalf("%d contenders granted, want exactly 1", grantedCount)
	}
	owner, held, err := store.Owner(ctx, "leader")
	if err != nil || !held || owner.Holder == "" {
		t.Fatalf("owner after race: %+v held=%v err=%v", owner, held, err)
	}
}

// A direct get may be answered by any replica, so a lagging one could
// re-grant an expired lease. A deterministic lag is not reproducible in
// a test, so this pins the routing instead: snoop both JetStream API
// subjects and require every read to take the leader-answered one.
func TestReadsAreLeaderRoutedNotDirect(t *testing.T) {
	store, nc := newTestStore(t)
	ctx := testContext(t)
	direct := make(chan *nats.Msg, 64)
	leader := make(chan *nats.Msg, 64)
	for subject, ch := range map[string]chan *nats.Msg{
		"$JS.API.DIRECT.GET." + streamName + ".>": direct,
		"$JS.API.DIRECT.GET." + streamName:        direct,
		msgGetSubject:                             leader,
	} {
		sub, err := nc.ChanSubscribe(subject, ch)
		if err != nil {
			t.Fatalf("snoop %s: %v", subject, err)
		}
		t.Cleanup(func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("unsubscribe: %v", err)
			}
		})
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	held, _, err := store.Acquire(ctx, "leader", "node-a", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := store.Acquire(ctx, "leader", "node-a", 10*time.Second); err != nil {
		t.Fatalf("idempotent acquire: %v", err)
	}
	if _, _, err := store.Owner(ctx, "leader"); err != nil {
		t.Fatalf("owner: %v", err)
	}
	if _, err := store.Renew(ctx, "leader", "node-a", held.Revision, 10*time.Second); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(direct) != 0 {
		t.Fatalf("%d reads used a direct get; all must be leader-routed", len(direct))
	}
	if len(leader) < 4 {
		t.Fatalf("%d leader-routed reads, want one per call (4)", len(leader))
	}
}

func TestMissingBucketReportsUnsupported(t *testing.T) {
	store, nc := newTestStore(t)
	ctx := testContext(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := js.DeleteStream(ctx, streamName); err != nil {
		t.Fatalf("delete leases stream: %v", err)
	}
	_, granted, err := store.Acquire(ctx, "leader", "node-a", 10*time.Second)
	if !errors.Is(err, ErrUnsupported) || granted {
		t.Fatalf("acquire without bucket: granted=%v err=%v, want ErrUnsupported",
			granted, err)
	}
	if _, _, err := store.Owner(ctx, "leader"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("owner without bucket: err=%v, want ErrUnsupported", err)
	}
}

func subscribeChanges(t *testing.T, nc *nats.Conn, key string) chan *nats.Msg {
	t.Helper()
	events := make(chan *nats.Msg, 16)
	sub, err := nc.ChanSubscribe(ChangedSubject(key), events)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	})
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return events
}

func nextEvent(t *testing.T, events chan *nats.Msg) *nats.Msg {
	t.Helper()
	select {
	case msg := <-events:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("no lease event within 10s")
		return nil
	}
}
