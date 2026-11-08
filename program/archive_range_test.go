// program/archive_range_test.go
// The archive range protocol's access control (Endless JetStream): only
// dagnats itself asks an archive for records, so no program may publish
// under "lode.stream.>" whatever its manifest (its name, protocols or
// Archives); a program with an effective archive grant on stream S may
// subscribe "lode.stream.<domain>.<S>.range" in the server's domain and
// answer on the requester's inbox; a program without one may subscribe
// to no range subject.
// Methodology: PermissionsGranted as a pure function over a spread of
// manifests, with a NATS wildcard matcher deciding what each permission
// set admits; then a live server where "lode" (granted) answers a
// root request and "nogrant" is refused both directions, with the
// server's violation log as evidence.
package program_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

// rangeManifests are manifests whose grants could reach "lode.>": a
// program named lode (own "lode.>"), one speaking a protocol under
// lode.stream, and one archiving AGENT_JOURNAL.
func rangeManifests() []program.Manifest {
	plain := validManifest()
	lode := program.Manifest{Name: "lode", Archives: []string{"AGENT_JOURNAL", "TELEMETRY"}}
	speaker := program.Manifest{Name: "speaker"}
	speaker.Protocols = []string{"lode.stream", "lode.stream.eu.AGENT_JOURNAL.range"}
	archiver := validManifest()
	archiver.Archives = []string{"AGENT_JOURNAL"}
	return []program.Manifest{plain, lode, speaker, archiver}
}

func TestPermissions_NoProgramPublishesArchiveRangeRequests(t *testing.T) {
	subjects := []string{
		"lode.stream.eu.AGENT_JOURNAL.range", "lode.stream.default.AGENT_JOURNAL.range",
		"lode.stream.eu.TELEMETRY.range", "lode.stream.x",
	}
	for _, m := range rangeManifests() {
		for _, domain := range []string{"", "eu"} {
			perms := program.PermissionsGranted(m, domain, []string{"AGENT_JOURNAL", "TELEMETRY"})
			for _, subject := range subjects {
				if allowedBy(perms.Publish, subject) {
					t.Errorf("%s (domain %q) may publish %s", m.Name, domain, subject)
				}
			}
			if !allowedBy(perms.Publish, "_INBOX.abc.1") {
				t.Errorf("%s may not publish replies on _INBOX.>", m.Name)
			}
		}
	}
	if allowedBy(program.PermissionsFor(rangeManifests()[1]).Publish, "lode.stream.eu.S.range") {
		t.Error("PermissionsFor grants publish under lode.stream.>")
	}
}

func TestPermissions_ArchiveGrantSubscribesItsRangeSubjectOnly(t *testing.T) {
	m := validManifest()
	m.Archives = []string{"AGENT_JOURNAL", "WORKFLOW_HISTORY"}
	perms := program.PermissionsGranted(m, "eu", []string{"AGENT_JOURNAL"})
	if !allowedBy(perms.Subscribe, "lode.stream.eu.AGENT_JOURNAL.range") {
		t.Error("granted archiver cannot subscribe its range subject")
	}
	for _, subject := range []string{
		"lode.stream.eu.WORKFLOW_HISTORY.range", // declared, not granted
		"lode.stream.us.AGENT_JOURNAL.range",    // another domain
		"lode.stream.default.AGENT_JOURNAL.range",
	} {
		if allowedBy(perms.Subscribe, subject) {
			t.Errorf("granted archiver may subscribe %s", subject)
		}
	}
	bare := program.PermissionsGranted(m, "", []string{"AGENT_JOURNAL"})
	if !allowedBy(bare.Subscribe, "lode.stream.default.AGENT_JOURNAL.range") {
		t.Error("with no domain the range subject uses the default token")
	}
	for _, other := range rangeManifests() {
		perms := program.PermissionsGranted(other, "eu", nil)
		if allowedBy(perms.Subscribe, "lode.stream.eu.AGENT_JOURNAL.range") {
			t.Errorf("%s without a grant may subscribe a range subject", other.Name)
		}
	}
}

func TestPermissions_ArchiverAnswersRootRangeRequests(t *testing.T) {
	f := archiveFixture(t)
	subject := dagnatsext.ArchiveRangeSubject("", archiveStream)
	lode := f.dialProgram(t, "lode")
	sub, err := lode.Subscribe(subject, func(m *nats.Msg) {
		if err := m.Respond([]byte(`{"messages":[],"done":true}`)); err != nil {
			t.Errorf("respond: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	}()
	if err := lode.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	reply, err := f.admin.Request(subject, []byte(`{}`), 5*time.Second)
	if err != nil || !strings.Contains(string(reply.Data), `"done":true`) {
		t.Fatalf("root range request = %v, %v; want lode's reply", reply, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := lode.RequestWithContext(ctx, subject, []byte(`{}`)); err == nil {
		t.Error("lode's own range request was delivered")
	}
	nogrant := f.dialProgram(t, "nogrant")
	if _, err := nogrant.SubscribeSync(subject); err != nil {
		t.Fatalf("subscribe call: %v", err)
	}
	if err := nogrant.Flush(); err != nil {
		t.Logf("flush after denied subscribe: %v", err)
	}
	for _, kind := range []string{"Publish Violation", "Subscription Violation"} {
		if !waitFor(func() bool { return violationOf(f, kind, subject) }) {
			t.Errorf("no %s logged for %q", kind, subject)
		}
	}
}

func violationOf(f *fixture, kind, subject string) bool {
	for _, line := range f.violations.snapshot() {
		if strings.Contains(line, kind) && strings.Contains(line, subject) {
			return true
		}
	}
	return false
}
