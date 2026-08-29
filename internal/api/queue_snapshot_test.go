// api/queue_snapshot_test.go
// Tests for the periodic event.queue.snapshot publisher (#632):
// interval parsing (env var validation) and the publisher goroutine's
// change-suppression + heartbeat behavior against a real embedded NATS
// server.
//
// Methodology: interval parsing is a pure table test. The publisher
// tests start an embedded NATS server, subscribe an ephemeral consumer
// to event.queue.snapshot, run the publisher for a bounded window, and
// count messages received.
package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func TestParseQueueSnapshotInterval(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{"default on empty", "", queueSnapshotIntervalDefault, false},
		{"min boundary", "1s", queueSnapshotIntervalMin, false},
		{"max boundary", "5m", queueSnapshotIntervalMax, false},
		{"garbage", "not-a-duration", 0, true},
		{"below min", "500ms", 0, true},
		{"above max", "6m", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseQueueSnapshotInterval(tc.val)
			if tc.wantErr {
				// Positive: invalid input is a hard error, not a
				// silent clamp.
				if err == nil {
					t.Fatalf("ParseQueueSnapshotInterval(%q) = %v, nil, want error",
						tc.val, got)
				}
				return
			}
			// Negative: valid input must not error and must resolve
			// to exactly the expected duration.
			if err != nil {
				t.Fatalf("ParseQueueSnapshotInterval(%q) error = %v", tc.val, err)
			}
			if got != tc.want {
				t.Fatalf("ParseQueueSnapshotInterval(%q) = %v, want %v",
					tc.val, got, tc.want)
			}
		})
	}
}

// drainQueueSnapshots subscribes to event.queue.snapshot for window
// and returns every decoded message received. Bounded by window so a
// broken publisher fails the test instead of hanging it.
func drainQueueSnapshots(
	t *testing.T, js jetstream.JetStream, window time.Duration,
) []protocol.QueueSnapshot {
	t.Helper()
	ctx := t.Context()
	cons, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		FilterSubject: "event.queue.snapshot",
		AckPolicy:     jetstream.AckNonePolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	var out []protocol.QueueSnapshot
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		msgs, err := cons.Fetch(10, jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			continue
		}
		for msg := range msgs.Messages() {
			var snap protocol.QueueSnapshot
			if err := json.Unmarshal(msg.Data(), &snap); err != nil {
				t.Fatalf("unmarshal snapshot: %v", err)
			}
			out = append(out, snap)
		}
	}
	return out
}

func TestQueueSnapshotPublisherSuppressesUnchangedState(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.Publish(t.Context(), "task.build", []byte("{}")); err != nil {
		t.Fatalf("publish task.build: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)

	// tick is pinned to queueSnapshotIntervalMin -- the shortest legal
	// snapshot-check cadence -- so this test both exercises the real
	// production floor and keeps runtime bounded. The clock is frozen
	// so "same queue state across 3 ticks" means exactly that -- the
	// real, always-growing oldest_wait_ms is held constant rather than
	// crossing a rounded-second boundary on every tick and registering
	// as a spurious change.
	const tick = queueSnapshotIntervalMin
	frozen := time.Now()
	stop, err := StartQueueSnapshotPublisher(
		t.Context(), tp, js, tick, nil,
		withQueueSnapshotHeartbeatInterval(10*time.Minute),
		withQueueSnapshotNow(func() time.Time { return frozen }),
	)
	if err != nil {
		t.Fatalf("StartQueueSnapshotPublisher: %v", err)
	}
	defer stop()

	// Positive: three ticks of unchanged state collapse to exactly one
	// published snapshot.
	got := drainQueueSnapshots(t, js, tick*7/2)
	if len(got) != 1 {
		t.Fatalf("len(snapshots) = %d, want 1 (unchanged state suppressed)", len(got))
	}

	// Negative: enqueuing a new task-type subject changes the state,
	// which must produce a second publish.
	if _, err := js.Publish(t.Context(), "task.test", []byte("{}")); err != nil {
		t.Fatalf("publish task.test: %v", err)
	}
	got = drainQueueSnapshots(t, js, tick*3/2)
	if len(got) != 1 {
		t.Fatalf("len(snapshots after change) = %d, want 1 more publish", len(got))
	}
}

func TestQueueSnapshotPublisherHeartbeat(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)

	// A snapshot interval at the legal ceiling isolates the heartbeat
	// within the test window: no change-triggered publish can fire in
	// time, so any message observed must be a heartbeat.
	stop, err := StartQueueSnapshotPublisher(
		t.Context(), tp, js, queueSnapshotIntervalMax, nil,
		withQueueSnapshotHeartbeatInterval(40*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("StartQueueSnapshotPublisher: %v", err)
	}
	defer stop()

	// Positive: the heartbeat fires on its own cadence with no state
	// change at all (empty queue throughout).
	got := drainQueueSnapshots(t, js, 250*time.Millisecond)
	if len(got) < 2 {
		t.Fatalf("len(heartbeats) = %d, want >= 2", len(got))
	}
	// Negative: every heartbeat still reports the real (empty) state,
	// not a stale/garbage payload.
	for _, snap := range got {
		if len(snap.Groups) != 0 {
			t.Fatalf("heartbeat groups = %+v, want empty", snap.Groups)
		}
	}
}

func TestStartQueueSnapshotPublisherPanicsOnNilArgs(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)

	// Positive: nil tp panics.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil tp")
			}
		}()
		_, _ = StartQueueSnapshotPublisher(t.Context(), nil, js, time.Second, nil)
	}()

	// Negative: nil js panics too.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil js")
			}
		}()
		_, _ = StartQueueSnapshotPublisher(t.Context(), tp, nil, time.Second, nil)
	}()
}

// TestHashQueueSnapshotIncludesTruncated proves the change-suppression
// hash reacts to a Truncated flip even when Groups is byte-for-byte
// identical (review round 1, #649): otherwise a fleet-sizing consumer
// watching event.queue.snapshot could see `truncated` go stale for up
// to the 60s heartbeat after subject cardinality crosses 256.
func TestHashQueueSnapshotIncludesTruncated(t *testing.T) {
	groups := []protocol.QueueGroup{{TaskType: "build", Pending: 3}}
	notTruncated := protocol.QueueSnapshot{Groups: groups, Truncated: false}
	truncated := protocol.QueueSnapshot{Groups: groups, Truncated: true}

	// Positive: identical groups, only Truncated differs -> hashes differ.
	if hashQueueSnapshot(notTruncated) == hashQueueSnapshot(truncated) {
		t.Fatal("hash unchanged when Truncated flipped false->true, want different hash")
	}
	// Negative: two snapshots with the same Truncated value and groups
	// hash identically (no spurious difference introduced).
	again := protocol.QueueSnapshot{Groups: groups, Truncated: false}
	if hashQueueSnapshot(notTruncated) != hashQueueSnapshot(again) {
		t.Fatal("hash differs for two snapshots with identical Groups/Truncated")
	}
}
