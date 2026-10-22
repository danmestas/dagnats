// internal/natsutil/agent_journal.go
// AGENT_JOURNAL (ADR-026): one append-only subject per step,
// journal.{runID}.{stepID}, that an agent step's SDK writes fenced
// records to. Provisioning lives here with the other streams so the
// replica count, retention and byte ceiling come from one sweep.
package natsutil

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// AgentJournalStream is the stream's name.
const AgentJournalStream = "AGENT_JOURNAL"

// agentJournalDuplicates is the dedup window for journal appends. An
// append whose acknowledgement was lost is resent with the same
// Nats-Msg-Id; inside this window the server answers with the stored
// sequence instead of rejecting the stale expected sequence. Outside it
// the writer re-reads the tail (journal.Journal) and finds its record.
const agentJournalDuplicates = 2 * time.Minute

// agentJournalStreamConfig is the journal's config. AllowDirect is left
// off on purpose: a direct get may be answered by a replica that lags
// the leader, and the claim protocol needs the true tail. Without it,
// GetLastMsgForSubject goes through the JetStream API, which only the
// stream leader serves.
func agentJournalStreamConfig(replicas int, maxStoreBytes int64) jetstream.StreamConfig {
	if replicas < 1 {
		panic("agentJournalStreamConfig: replicas must be positive")
	}
	cfg := jetstream.StreamConfig{
		Name:       AgentJournalStream,
		Subjects:   []string{"journal.>"},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		MaxAge:     historyMaxAge,
		Duplicates: agentJournalDuplicates,
		MaxBytes: proportionalMaxBytes(
			maxStoreBytes, fractionAgentJournal,
		),
		Replicas:    replicas,
		AllowDirect: false,
	}
	if cfg.AllowDirect {
		panic("agentJournalStreamConfig: direct get must stay off")
	}
	return cfg
}

// JournalSubject is the one place the journal subject shape is
// assembled: journal.{runID}.{stepID}, with the step sanitised the way
// LogSubject sanitises it.
func JournalSubject(runID, stepID string) string {
	if runID == "" {
		panic("JournalSubject: runID must not be empty")
	}
	if strings.ContainsAny(runID, ". \t*>") {
		panic("JournalSubject: runID must not contain NATS subject metacharacters")
	}
	return "journal." + runID + "." + SubjectToken(stepID)
}
