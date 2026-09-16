// audit_kv_test.go
// Tests for listAuditKeys / listAuditEventsInner's #698 migration to
// natsutil.ListKeys: a key deleted (TTL expiry, in production) between
// listing and Get must not resurrect into the log, a non-NotFound Get
// error must fail the call rather than shorten it silently, and
// enumeration must still come back in chronological order even though
// the underlying helper's subject-state map is unordered. Methodology:
// real embedded NATS server + real console_audit bucket (auditkv.NewKV),
// since the ordering claim depends on the actual key format
// (auditkv.KeyFor) sorting correctly, not a stub.
package console

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// newAuditTestKV starts an embedded NATS server, opens the
// console_audit bucket, and returns js + kv for direct manipulation.
func newAuditTestKV(t *testing.T) (jetstream.JetStream, jetstream.KeyValue) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := auditkv.NewKV(ctx, js)
	if err != nil {
		t.Fatalf("auditkv.NewKV: %v", err)
	}
	return js, kv
}

// TestListAuditEvents_SkipsDeletedKey asserts that an audit key
// deleted directly from the bucket -- standing in for the TTL expiry
// that normally removes an entry between listing and Get -- is not
// resurrected into the log, and a live, untouched entry survives.
func TestListAuditEvents_SkipsDeletedKey(t *testing.T) {
	js, kv := newAuditTestKV(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t0 := time.Now().UTC().Add(-time.Minute)
	t1 := time.Now().UTC()
	if err := auditkv.Emit(ctx, kv, slog.Default(), auditkv.AuditEvent{
		Time: t0, Actor: "alice", Action: "dlq.retry",
		Target: "gone", Outcome: "success",
	}); err != nil {
		t.Fatalf("Emit gone: %v", err)
	}
	if err := auditkv.Emit(ctx, kv, slog.Default(), auditkv.AuditEvent{
		Time: t1, Actor: "bob", Action: "dlq.retry",
		Target: "kept", Outcome: "success",
	}); err != nil {
		t.Fatalf("Emit kept: %v", err)
	}

	lister, err := kv.ListKeys(ctx)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	var goneKey string
	for k := range lister.Keys() {
		entry, err := kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var evt auditkv.AuditEvent
		if err := json.Unmarshal(entry.Value(), &evt); err == nil &&
			evt.Target == "gone" {
			goneKey = k
		}
	}
	if goneKey == "" {
		t.Fatal("could not locate the 'gone' event's key")
	}
	if err := kv.Delete(ctx, goneKey); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	events, err := listAuditEventsInner(ctx, js, kv, 100)
	if err != nil {
		t.Fatalf("listAuditEventsInner: %v", err)
	}
	for _, e := range events {
		if e.Target == "gone" {
			t.Fatalf("listAuditEventsInner resurrected deleted event: %+v", events)
		}
	}
	found := false
	for _, e := range events {
		if e.Target == "kept" {
			found = true
		}
	}
	if !found {
		t.Fatalf("listAuditEventsInner dropped live event, got %+v", events)
	}
}

// TestListAuditEvents_NewestFirst pins that events come back in
// newest-first order even though natsutil.ListKeys' underlying
// enumeration (a stream subject-state map) has no inherent order --
// listAuditKeys' explicit sort.Strings on the timestamp-prefixed key
// is what recovers chronological order.
func TestListAuditEvents_NewestFirst(t *testing.T) {
	js, kv := newAuditTestKV(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	base := time.Now().UTC().Add(-time.Hour)
	targets := []string{"t0", "t1", "t2", "t3", "t4"}
	for i, target := range targets {
		if err := auditkv.Emit(ctx, kv, slog.Default(), auditkv.AuditEvent{
			Time:   base.Add(time.Duration(i) * time.Second),
			Actor:  "alice",
			Action: "dlq.retry",
			Target: target, Outcome: "success",
		}); err != nil {
			t.Fatalf("Emit %s: %v", target, err)
		}
	}

	events, err := listAuditEventsInner(ctx, js, kv, 100)
	if err != nil {
		t.Fatalf("listAuditEventsInner: %v", err)
	}
	if len(events) != len(targets) {
		t.Fatalf("len(events) = %d, want %d", len(events), len(targets))
	}
	// Positive: newest (t4) first, oldest (t0) last.
	for i, want := range []string{"t4", "t3", "t2", "t1", "t0"} {
		if events[i].Target != want {
			t.Fatalf("events[%d].Target = %q, want %q (order = %v)",
				i, events[i].Target, want, targetsOf(events))
		}
	}
}

// stubAuditStream reports a fixed subject set for a deterministic
// Get-error propagation test.
type stubAuditStream struct {
	jetstream.Stream
	subjects map[string]uint64
}

func (s stubAuditStream) Info(
	_ context.Context, _ ...jetstream.StreamInfoOpt,
) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{
		State: jetstream.StreamState{Subjects: s.subjects},
	}, nil
}

// errAuditGetBroke stands in for a transient KV Get failure that is
// not a missing key.
var errAuditGetBroke = errors.New("kv get broke")

// stubAuditKV serves Bucket() and fails every Get with a non-NotFound
// error.
type stubAuditKV struct {
	jetstream.KeyValue
}

func (stubAuditKV) Bucket() string { return auditkv.Bucket }

func (stubAuditKV) Get(
	_ context.Context, _ string,
) (jetstream.KeyValueEntry, error) {
	return nil, errAuditGetBroke
}

// stubAuditJetStream serves just enough of jetstream.JetStream for
// listAuditEventsInner's Stream(ctx, "KV_"+bucket) call.
type stubAuditJetStream struct {
	jetstream.JetStream
	stream jetstream.Stream
}

func (s stubAuditJetStream) Stream(
	_ context.Context, _ string,
) (jetstream.Stream, error) {
	return s.stream, nil
}

// TestListAuditEvents_PropagatesGetError pins that a Get failure which
// is NOT ErrKeyNotFound fails the call instead of being swallowed --
// the exact hazard natsutil.ListKeys' doc comment calls out, since it
// (unlike the prior kv.ListKeys-based enumeration on this path) also
// surfaces TTL delete-marker subjects.
func TestListAuditEvents_PropagatesGetError(t *testing.T) {
	stream := stubAuditStream{subjects: map[string]uint64{
		"$KV." + auditkv.Bucket + ".k1": 1,
	}}
	js := stubAuditJetStream{stream: stream}
	kv := stubAuditKV{}

	_, err := listAuditEventsInner(context.Background(), js, kv, 10)
	if !errors.Is(err, errAuditGetBroke) {
		t.Fatalf("listAuditEventsInner error = %v, want %v", err, errAuditGetBroke)
	}
}

// targetsOf extracts Target for a failure message.
func targetsOf(events []AuditEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Target
	}
	return out
}
