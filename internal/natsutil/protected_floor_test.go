// internal/natsutil/protected_floor_test.go
// The journal's protected floor is stream metadata on AGENT_JOURNAL:
// dagnats.protected_floor (a decimal sequence) and
// dagnats.protected_floor_at (RFC3339). Writing it changes nothing
// else about the stream; a changed floor is written at most once a
// minute, an unchanged one every 5 minutes so its time never reaches
// the archiver's 10-minute staleness; and provisioning on restart
// keeps it (and any other metadata it did not set) instead of wiping it.
//
// Methodology: an embedded server per case. The stream's messages and
// config are read back around every write; the keys are asserted by
// their literal wire names. The write decision is also checked as a
// pure function over a table of published values and clock readings.
package natsutil

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func journalStream(t *testing.T, js jetstream.JetStream) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, AgentJournalStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return stream.CachedInfo()
}

func publishFloor(t *testing.T, js jetstream.JetStream, floor uint64, now time.Time) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wrote, err := PublishProtectedFloor(ctx, js, floor, now)
	if err != nil {
		t.Fatalf("PublishProtectedFloor(%d): %v", floor, err)
	}
	return wrote
}

func TestProtectedFloorMetadataSurvivesReprovision(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := SetupStreams(js, 1, testServerMaxStoreBytes); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := js.Publish(ctx, JournalSubject("r", "s"), []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	before := journalStream(t, js)
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if !publishFloor(t, js, 2, now) {
		t.Fatal("first publish did not write")
	}
	after := journalStream(t, js)
	if got := after.Config.Metadata["dagnats.protected_floor"]; got != "2" {
		t.Fatalf("dagnats.protected_floor = %q, want \"2\"", got)
	}
	if got := after.Config.Metadata["dagnats.protected_floor_at"]; got != "2026-11-08T12:00:00Z" {
		t.Fatalf("dagnats.protected_floor_at = %q, want 2026-11-08T12:00:00Z", got)
	}
	assertStreamUntouched(t, before, after)
	if err := SetupStreams(js, 1, testServerMaxStoreBytes); err != nil {
		t.Fatalf("SetupStreams again: %v", err)
	}
	restarted := journalStream(t, js)
	if got := restarted.Config.Metadata[MetaProtectedFloor]; got != "2" {
		t.Fatalf("after re-provision floor = %q, want \"2\" (metadata %v)",
			got, restarted.Config.Metadata)
	}
	assertStreamUntouched(t, before, restarted)
}

func assertStreamUntouched(t *testing.T, before, after *jetstream.StreamInfo) {
	t.Helper()
	if after.State.Msgs != before.State.Msgs || after.State.LastSeq != before.State.LastSeq {
		t.Fatalf("messages moved: %d/%d -> %d/%d", before.State.Msgs, before.State.LastSeq,
			after.State.Msgs, after.State.LastSeq)
	}
	if after.Config.MaxBytes != before.Config.MaxBytes ||
		after.Config.Discard != before.Config.Discard ||
		after.Config.Replicas != before.Config.Replicas ||
		after.Config.Subjects[0] != before.Config.Subjects[0] {
		t.Fatalf("config changed: %+v -> %+v", before.Config, after.Config)
	}
}

func TestPublishProtectedFloorWritesOnlyWhenItShould(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := SetupStreams(js, 1, testServerMaxStoreBytes); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	steps := []struct {
		name  string
		floor uint64
		at    time.Duration
		wrote bool
		want  uint64
	}{
		{"first", 10, 0, true, 10},
		{"unchanged", 10, 30 * time.Second, false, 10},
		{"raised within a minute", 20, 40 * time.Second, false, 10},
		{"raised after a minute", 20, 61 * time.Second, true, 20},
		{"lowered within a minute", 15, 90 * time.Second, false, 20},
		{"lowered after a minute", 15, 2*time.Minute + 2*time.Second, true, 15},
		{"unchanged before refresh", 15, 7*time.Minute + time.Second, false, 15},
		{"unchanged refreshed", 15, 7*time.Minute + 2*time.Second, true, 15},
		{"unchanged again before refresh", 15, 12*time.Minute + time.Second, false, 15},
		{"unchanged refreshed again", 15, 12*time.Minute + 2*time.Second, true, 15},
	}
	// The clock is injected: each step publishes at start+at, no sleep.
	var lastWrite time.Time
	for _, s := range steps {
		now := start.Add(s.at)
		if got := publishFloor(t, js, s.floor, now); got != s.wrote {
			t.Fatalf("%s: wrote = %v, want %v", s.name, got, s.wrote)
		}
		if s.wrote {
			lastWrite = now
		}
		meta := journalStream(t, js).Config.Metadata
		if meta[MetaProtectedFloor] != strconv.FormatUint(s.want, 10) {
			t.Fatalf("%s: floor = %q, want %d", s.name, meta[MetaProtectedFloor], s.want)
		}
		if want := lastWrite.Format(time.RFC3339); meta[MetaProtectedFloorAt] != want {
			t.Fatalf("%s: floor_at = %q, want %s", s.name, meta[MetaProtectedFloorAt], want)
		}
	}
}

func TestMergeKeptMetadata(t *testing.T) {
	existing := map[string]string{
		MetaProtectedFloor: "9", "operator.note": "keep", "_nats.req.level": "1", "owned": "old",
	}
	got := mergeKeptMetadata(map[string]string{"owned": "new"}, existing)
	if got[MetaProtectedFloor] != "9" || got["operator.note"] != "keep" {
		t.Fatalf("lost existing keys: %v", got)
	}
	if got["owned"] != "new" {
		t.Fatalf("config's own key must win: %v", got)
	}
	if _, ok := got["_nats.req.level"]; ok {
		t.Fatalf("server-reserved key must be left to the server: %v", got)
	}
	if mergeKeptMetadata(nil, nil) != nil {
		t.Fatal("nothing to keep must stay nil")
	}
}

func published(floor, at string) map[string]string {
	return map[string]string{MetaProtectedFloor: floor, MetaProtectedFloorAt: at}
}

func TestFloorWriteDueRepairsUnusableValues(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-10 * time.Second).Format(time.RFC3339)
	future := now.Add(time.Hour).Format(time.RFC3339)
	cases := []struct {
		name      string
		published map[string]string
		want      bool
	}{
		{"missing", nil, true},
		{"malformed floor", published("x", fresh), true},
		{"malformed at", published("5", "soon"), true},
		{"future at", published("5", future), true},
		{"fresh and unchanged", published("5", fresh), false},
		{"fresh and changed", published("4", fresh), false},
	}
	for _, c := range cases {
		if got := floorWriteDue(c.published, 5, now); got != c.want {
			t.Errorf("%s: floorWriteDue = %v, want %v", c.name, got, c.want)
		}
	}
}

// On an R3 journal provisioning takes the create-then-update path;
// the floor must survive it there too.
func TestProtectedFloorSurvivesReprovisionOnR3(t *testing.T) {
	js, _ := startRaceCluster(t)
	cfg := agentJournalStreamConfig(3, testServerMaxStoreBytes)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := createStreamWithRetry(ctx, js, cfg); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if _, err := js.Publish(ctx, JournalSubject("r", "s"), []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	before := journalStream(t, js)
	if !publishFloor(t, js, 1, time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)) {
		t.Fatal("first publish did not write")
	}
	if err := createStreamWithRetry(ctx, js, cfg); err != nil {
		t.Fatalf("re-provision: %v", err)
	}
	after := journalStream(t, js)
	if got := after.Config.Metadata[MetaProtectedFloor]; got != "1" {
		t.Fatalf("after R3 re-provision floor = %q, want \"1\" (%v)", got, after.Config.Metadata)
	}
	assertStreamUntouched(t, before, after)
}
