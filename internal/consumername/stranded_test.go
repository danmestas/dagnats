// internal/consumername/stranded_test.go
// Methodology: real embedded NATS server per test (JetStream StreamInfo
// subject-filter counts are a server behavior, not something a fake can
// stand in for). Seed a message directly on a legacy-shaped subject
// (bypassing any consumer), then assert CheckStrandedSubjects reports it
// with the right count and never errors merely because messages are
// pending. Bounded 5s context timeout on every call.
package consumername

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCheckStrandedSubjects_ReportsLegacyGroupedMessages(t *testing.T) {
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
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Seed two messages on the pre-#674 ">"-wildcard legacy shape and one
	// on the post-#674-pre-#704 "*"-anchored legacy shape — no consumer
	// for either will ever exist post-#704, so both must be reported.
	legacySubjectA := "task.render.gpu.deadbeefdeadbeefdeadbeefdeadbeef1"
	legacySubjectB := "task.render.gpu.deadbeefdeadbeefdeadbeefdeadbeef2"
	for _, subj := range []string{legacySubjectA, legacySubjectA, legacySubjectB} {
		if _, err := js.Publish(ctx, subj, []byte("stranded")); err != nil {
			t.Fatalf("Publish(%q): %v", subj, err)
		}
	}

	stranded, err := CheckStrandedSubjects(ctx, stream, []GroupPair{
		{Task: "render", Group: "gpu"},
	})
	if err != nil {
		t.Fatalf("CheckStrandedSubjects: %v", err)
	}
	if len(stranded) != 2 {
		t.Fatalf("stranded = %+v, want exactly 2 subjects", stranded)
	}
	total := uint64(0)
	for _, s := range stranded {
		if s.Task != "render" || s.Group != "gpu" {
			t.Fatalf("StrandedSubject = %+v, want Task/Group render/gpu", s)
		}
		total += s.Count
	}
	if total != 3 {
		t.Fatalf("total pending = %d, want 3", total)
	}
}

// TestCheckStrandedSubjects_SilentOnCleanStream is the negative proof: a
// pair with no pending legacy messages (the steady-state, fully-upgraded
// case) must report zero stranded subjects and no error — a check that
// only ever finds problems is not trustworthy as a detector.
func TestCheckStrandedSubjects_SilentOnCleanStream(t *testing.T) {
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
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	stranded, err := CheckStrandedSubjects(ctx, stream, []GroupPair{
		{Task: "render", Group: "gpu"},
	})
	if err != nil {
		t.Fatalf("CheckStrandedSubjects: %v", err)
	}
	if len(stranded) != 0 {
		t.Fatalf("stranded = %+v, want none on a clean stream", stranded)
	}
}

// TestCheckStrandedSubjects_SkipsUngroupedPairs proves ungrouped pairs
// (Group == "") are never queried — #704 left ungrouped subjects
// byte-identical, so there is nothing legacy about them.
func TestCheckStrandedSubjects_SkipsUngroupedPairs(t *testing.T) {
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
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := js.Publish(
		ctx, "task.render.deadbeefdeadbeefdeadbeefdeadbeef", []byte("x"),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stranded, err := CheckStrandedSubjects(ctx, stream, []GroupPair{
		{Task: "render", Group: ""},
	})
	if err != nil {
		t.Fatalf("CheckStrandedSubjects: %v", err)
	}
	if len(stranded) != 0 {
		t.Fatalf("stranded = %+v, want none for an ungrouped pair", stranded)
	}
}
