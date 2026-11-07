// natsutil/retention_test.go
// Tests for JetStream retention: history stores are size-bounded ring
// buffers. Data retention is the consumer's call, so dagnats never ages
// history out on a timer: every history stream carries MaxAge 0, a
// proportional MaxBytes ceiling and DiscardOld, so the oldest message falls
// off only when the stream is full. AGENT_JOURNAL has no MaxAge either but
// is not a ring: it refuses appends when full (DiscardNew), because a
// journal record must never be lost. Operational limits whose expiry is part
// of correct behaviour (dedup windows, idempotency TTLs, liveness TTLs,
// sticky routing) are unchanged, and TASK_QUEUES / SLEEP_TIMERS still carry
// no age bound (aging them would drop live work).
// Methodology: an embedded NATS server per test; provisioning runs against
// it and the live configs are asserted. The upgrade test pre-creates every
// history stream with the age limits older releases shipped, re-provisions
// through SetupAll, and asserts the update path cleared them.
// Bounded 5-second timeout on all operations.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// historyStreams are the stores that roll by size alone. AGENT_JOURNAL is
// history too but never rolls: see assertJournalNeverDrops.
var historyStreams = []string{
	"WORKFLOW_HISTORY", "EVENTS", "DEAD_LETTERS", "TELEMETRY",
	"TRIGGER_HISTORY",
}

// legacyHistoryMaxAge is the age limit each history stream shipped with
// before the ring-buffer model.
var legacyHistoryMaxAge = map[string]time.Duration{
	"WORKFLOW_HISTORY": 30 * 24 * time.Hour,
	"EVENTS":           14 * 24 * time.Hour,
	"DEAD_LETTERS":     30 * 24 * time.Hour,
	"TELEMETRY":        7 * 24 * time.Hour,
	"TRIGGER_HISTORY":  30 * 24 * time.Hour,
	AgentJournalStream: 30 * 24 * time.Hour,
}

// liveStreamConfig returns the named stream's config as the server holds it.
func liveStreamConfig(
	t *testing.T, js jetstream.JetStream, name string,
) jetstream.StreamConfig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("Stream(%s): %v", name, err)
	}
	return stream.CachedInfo().Config
}

// assertJournalNeverDrops fails unless the journal has no age limit, a
// byte ceiling, and DiscardNew: a full journal refuses appends rather
// than losing its oldest records.
func assertJournalNeverDrops(t *testing.T, cfg jetstream.StreamConfig) {
	t.Helper()
	if cfg.MaxAge != 0 {
		t.Errorf("AGENT_JOURNAL MaxAge = %v, want 0", cfg.MaxAge)
	}
	if cfg.MaxBytes <= 0 {
		t.Errorf("AGENT_JOURNAL MaxBytes = %d, want a ceiling", cfg.MaxBytes)
	}
	if cfg.Discard != jetstream.DiscardNew {
		t.Errorf("AGENT_JOURNAL Discard = %v, want DiscardNew", cfg.Discard)
	}
}

// assertRingBuffer fails unless cfg rolls by size alone.
func assertRingBuffer(t *testing.T, cfg jetstream.StreamConfig) {
	t.Helper()
	if cfg.MaxAge != 0 {
		t.Errorf("%s MaxAge = %v, want 0 (history rolls by size)",
			cfg.Name, cfg.MaxAge)
	}
	if cfg.MaxBytes <= 0 {
		t.Errorf("%s MaxBytes = %d, want a positive ceiling",
			cfg.Name, cfg.MaxBytes)
	}
	if cfg.Discard != jetstream.DiscardOld {
		t.Errorf("%s Discard = %v, want DiscardOld", cfg.Name, cfg.Discard)
	}
}

func TestHistoryStreamsAreRingBuffers(t *testing.T) {
	const budget = testServerMaxStoreBytes
	cfgs := streamConfigsWithBudget(t, budget)
	for _, name := range historyStreams {
		cfg, ok := cfgs[name]
		if !ok {
			t.Fatalf("%s not provisioned", name)
		}
		assertRingBuffer(t, cfg)
		want := proportionalMaxBytes(budget, fileStreamFractions[name])
		if cfg.MaxBytes != want {
			t.Errorf("%s MaxBytes = %d, want %d", name, cfg.MaxBytes, want)
		}
	}
	assertJournalNeverDrops(t, cfgs[AgentJournalStream])
}

// TestFullJournalRefusesAppend: once AGENT_JOURNAL reaches its ceiling a
// new append is refused and the first record is still there.
func TestFullJournalRefusesAppend(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, agentJournalStreamConfig(1, 100<<10))
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	record := make([]byte, 512) // the ceiling is 4% of 100 KiB, ~4 KiB
	var refused error
	for i := 0; i < 100 && refused == nil; i++ {
		_, refused = js.Publish(ctx, JournalSubject("run-1", "step"), record)
	}
	if refused == nil {
		t.Fatal("100 appends of 512 B into a ~4 KiB journal were all accepted")
	}
	first, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("first record lost after the journal filled: %v", err)
	}
	if len(first.Data) != len(record) {
		t.Fatalf("first record = %d bytes, want %d", len(first.Data), len(record))
	}
}

// TestUpgradeClearsHistoryMaxAge is the upgrade guard: streams created by
// an older release keep their age limit unless the update path rewrites
// them, so re-provisioning must land MaxAge 0 on EXISTING streams.
func TestUpgradeClearsHistoryMaxAge(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subjects := map[string]string{
		"WORKFLOW_HISTORY": "history.>", "EVENTS": "event.>",
		"DEAD_LETTERS": "dead.>", "TELEMETRY": "telemetry.>",
		"TRIGGER_HISTORY": "trigger.fire.>", AgentJournalStream: "journal.>",
	}
	for name, maxAge := range legacyHistoryMaxAge {
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: name, Subjects: []string{subjects[name]},
			Storage: jetstream.FileStorage, MaxAge: maxAge,
		})
		if err != nil {
			t.Fatalf("pre-create %s: %v", name, err)
		}
		if got := liveStreamConfig(t, js, name).MaxAge; got != maxAge {
			t.Fatalf("precondition: %s MaxAge = %v, want %v", name, got, maxAge)
		}
	}

	err = SetupAll(nc, WithStoreBudget(testServerMaxStoreBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	for _, name := range historyStreams {
		assertRingBuffer(t, liveStreamConfig(t, js, name))
	}
	assertJournalNeverDrops(t, liveStreamConfig(t, js, AgentJournalStream))
	assertOperationalLimitsKept(t, js)
}

// assertOperationalLimitsKept pins limits whose expiry is behaviour, not
// retention: re-provisioning must not touch them.
func assertOperationalLimitsKept(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ttls := map[string]time.Duration{
		"idempotency_keys": 24 * time.Hour,
		"http_idempotency": time.Hour,
		"workers":          60 * time.Second,
		"checkpoints":      checkpointsTTL,
	}
	for bucket, want := range ttls {
		got := liveStreamConfig(t, js, "KV_"+bucket).MaxAge
		if got != want {
			t.Errorf("KV %s TTL = %v, want %v (operational)", bucket, got, want)
		}
	}
	if got := liveStreamConfig(t, js, "STICKY_TASKS").MaxAge; got != 30*time.Minute {
		t.Errorf("STICKY_TASKS MaxAge = %v, want 30m (operational)", got)
	}
	cfg := liveStreamConfig(t, js, "DEAD_LETTERS")
	if cfg.Duplicates != 24*time.Hour {
		t.Errorf("DEAD_LETTERS dedup = %v, want 24h (operational)", cfg.Duplicates)
	}
}

// TestTaskQueuesNoAgeBound is the hard negative-space guard: a MaxAge on a
// work-queue stream silently deletes un-acked (live) tasks. The age bound is
// forbidden.
func TestTaskQueuesNoAgeBound(t *testing.T) {
	cfgs := streamConfigsWithBudget(t, testServerMaxStoreBytes)
	cfg := cfgs["TASK_QUEUES"]
	if cfg.MaxAge != 0 {
		t.Fatalf("TASK_QUEUES MaxAge = %v, want 0 (un-acked work is live)",
			cfg.MaxAge)
	}
	if cfg.Retention != jetstream.WorkQueuePolicy {
		t.Fatalf("TASK_QUEUES retention = %v, want work-queue", cfg.Retention)
	}
}

// TestSleepTimersNoAgeBound guards the other forbidden age bound: SLEEP_TIMERS
// holds pending future sleep messages; aging would drop un-fired timers.
func TestSleepTimersNoAgeBound(t *testing.T) {
	cfgs := streamConfigsWithBudget(t, testServerMaxStoreBytes)
	cfg := cfgs["SLEEP_TIMERS"]
	if cfg.MaxAge != 0 {
		t.Fatalf("SLEEP_TIMERS MaxAge = %v, want 0 (pending timers are live)",
			cfg.MaxAge)
	}
	if cfg.MaxBytes <= 0 {
		t.Fatalf("SLEEP_TIMERS MaxBytes = %d, want a ceiling", cfg.MaxBytes)
	}
}
