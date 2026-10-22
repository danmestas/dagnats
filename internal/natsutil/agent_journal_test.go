// internal/natsutil/agent_journal_test.go
// AGENT_JOURNAL provisioning (ADR-026): the per-step journal stream
// exists after SetupStreams with the retention of WORKFLOW_HISTORY and
// with direct get disabled, so a tail read is answered by the stream
// leader rather than a replica that may lag it.
//
// Methodology: provision against an embedded server and read the live
// stream config back; every field the journal protocol depends on is
// asserted, with the negative space (no direct get, no work-queue
// retention) alongside the positive.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAgentJournalStreamProvisioned(t *testing.T) {
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
	stream, err := js.Stream(ctx, AgentJournalStream)
	if err != nil {
		t.Fatalf("Stream(%s): %v", AgentJournalStream, err)
	}
	cfg := stream.CachedInfo().Config
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "journal.>" {
		t.Errorf("subjects = %v, want [journal.>]", cfg.Subjects)
	}
	if cfg.MaxAge != historyMaxAge {
		t.Errorf("MaxAge = %v, want %v (as WORKFLOW_HISTORY)", cfg.MaxAge, historyMaxAge)
	}
	if cfg.Retention != jetstream.LimitsPolicy || cfg.Storage != jetstream.FileStorage {
		t.Errorf("retention/storage = %v/%v, want limits/file", cfg.Retention, cfg.Storage)
	}
	if cfg.AllowDirect {
		t.Error("AllowDirect = true; tail reads must come from the leader")
	}
	if cfg.Duplicates <= 0 {
		t.Error("Duplicates window unset; appends rely on a stable Nats-Msg-Id")
	}
	if cfg.MaxBytes <= 0 {
		t.Errorf("MaxBytes = %d, want a proportional ceiling", cfg.MaxBytes)
	}
}

func TestJournalSubjectShape(t *testing.T) {
	got := JournalSubject("run-1", "step a")
	if got != "journal.run-1.step_a" {
		t.Errorf("JournalSubject = %q, want journal.run-1.step_a", got)
	}
	if JournalSubject("run-1", "s") == JournalSubject("run-1", "t") {
		t.Error("distinct steps must map to distinct subjects")
	}
}
