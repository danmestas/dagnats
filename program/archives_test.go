// program/archives_test.go
// Tests for Manifest.Archives: the streams a program asks to archive,
// which the operator must also grant (PermissionsGranted).
// Methodology: three layers. Validate rejects bad declarations row by
// row. PermissionsGranted is checked as a pure function: the exact
// archive subjects appear only for a stream both declared and granted.
// Then a program drains, reads another consumer's floor and purges a
// real stream as an NKey user with those permissions, while a program
// without the grant is denied each step and the violations are logged.
package program_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

func TestValidate_RejectsBadArchives(t *testing.T) {
	tooMany := make([]string, 33)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("S%d", i)
	}
	cases := []struct {
		name     string
		archives []string
		want     string
	}{
		{"too many", tooMany, "archives"},
		{"empty", []string{""}, "archive stream"},
		{"dot", []string{"A.B"}, "archive stream"},
		{"wildcard", []string{"A*"}, "archive stream"},
		{"gt", []string{">"}, "archive stream"},
		{"space", []string{"A B"}, "archive stream"},
		{"too long", []string{strings.Repeat("A", 129)}, "archive stream"},
		{"duplicate", []string{"AGENT_JOURNAL", "AGENT_JOURNAL"}, "twice"},
	}
	for _, c := range cases {
		m := validManifest()
		m.Archives = c.archives
		err := program.Validate(m)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Validate = %v, want error containing %q", c.name, err, c.want)
		}
	}
	m := validManifest()
	m.Archives = []string{"AGENT_JOURNAL", "KV_toy_state", "WORKFLOW_HISTORY"}
	if err := program.Validate(m); err != nil {
		t.Fatalf("Validate(valid archives) = %v, want nil", err)
	}
}

// archiveSubjects is the literal list a granted stream S yields for
// program toy: its consumer is "toy_archive".
func archiveSubjects(stream string) []string {
	return []string{
		"$JS.API.STREAM.INFO." + stream,
		"$JS.API.STREAM.PURGE." + stream,
		"$JS.API.CONSUMER.LIST." + stream,
		"$JS.API.CONSUMER.NAMES." + stream,
		"$JS.API.CONSUMER.INFO." + stream + ".*",
		"$JS.API.CONSUMER.CREATE." + stream + ".toy_archive",
		"$JS.API.CONSUMER.CREATE." + stream + ".toy_archive.>",
		"$JS.API.CONSUMER.MSG.NEXT." + stream + ".toy_archive",
		"$JS.API.CONSUMER.DELETE." + stream + ".toy_archive",
	}
}

func TestPermissionsGranted_ArchiveIsDeclaredAndGranted(t *testing.T) {
	if got := program.ArchiveConsumer("toy"); got != "toy_archive" {
		t.Fatalf("ArchiveConsumer(toy) = %q, want toy_archive", got)
	}
	m := validManifest()
	m.Archives = []string{"AGENT_JOURNAL", "WORKFLOW_HISTORY"}
	granted := []string{"AGENT_JOURNAL", "RUN_SNAPSHOTS"}
	if got := program.EffectiveArchives(m, granted); !slices.Equal(got, []string{"AGENT_JOURNAL"}) {
		t.Fatalf("EffectiveArchives = %v, want [AGENT_JOURNAL]", got)
	}
	base := program.PermissionsFor(m)
	perms := program.PermissionsGranted(m, granted)
	for _, s := range archiveSubjects("AGENT_JOURNAL") {
		if !slices.Contains(perms.Publish.Allow, s) {
			t.Errorf("declared+granted: %q missing", s)
		}
	}
	// Declared but ungranted, granted but undeclared: none of their subjects.
	for _, stream := range []string{"WORKFLOW_HISTORY", "RUN_SNAPSHOTS"} {
		for _, s := range perms.Publish.Allow {
			if strings.Contains(s, stream) {
				t.Errorf("%s not effective, yet %q granted", stream, s)
			}
		}
	}
	// Nothing else changed: exactly the nine subjects were added, and no
	// publish on the stream's subjects nor any ack.
	added := len(perms.Publish.Allow) - len(base.Publish.Allow)
	if added != len(archiveSubjects("AGENT_JOURNAL")) {
		t.Errorf("added %d subjects, want %d", added, len(archiveSubjects("AGENT_JOURNAL")))
	}
	for _, s := range perms.Publish.Allow {
		if strings.HasPrefix(s, "$JS.ACK.AGENT_JOURNAL") || strings.HasPrefix(s, "journal.") {
			t.Errorf("archive grant widened to %q", s)
		}
	}
	if !slices.Equal(perms.Subscribe.Allow, base.Subscribe.Allow) {
		t.Error("archive grant changed subscribe permissions")
	}
	// Without a grant, PermissionsFor and PermissionsGranted agree.
	if !slices.Equal(program.PermissionsGranted(m, nil).Publish.Allow, base.Publish.Allow) {
		t.Error("PermissionsGranted(m, nil) differs from PermissionsFor(m)")
	}
}

const archiveStream = "ARCH_TEST"

// archiveFixture starts a server where "lode" holds an effective grant
// on ARCH_TEST and "nogrant" declares it without one. ARCH_TEST holds
// five messages and a durable "other" that has acked two of them.
func archiveFixture(t *testing.T) *fixture {
	t.Helper()
	lode := program.Manifest{Name: "lode", Archives: []string{archiveStream}}
	nogrant := program.Manifest{Name: "nogrant", Archives: []string{archiveStream}}
	f := startFixtureGranted(t, map[string][]string{"lode": {archiveStream}}, lode, nogrant)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: archiveStream, Subjects: []string{"arch.>"}, MaxBytes: 1 << 20,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for i := range 5 {
		if _, err := js.Publish(ctx, "arch.event", []byte{byte('a' + i)}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	other, err := js.CreateConsumer(ctx, archiveStream, jetstream.ConsumerConfig{
		Durable: "other", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	batch, err := other.Fetch(2, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch other: %v", err)
	}
	for msg := range batch.Messages() {
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("ack other: %v", err)
		}
	}
	return f
}

func TestPermissions_ArchiveGrantDrainsAndPurges(t *testing.T) {
	f := archiveFixture(t)
	js, err := jetstream.New(f.dialProgram(t, "lode"))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, archiveStream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	// The floor of a consumer lode does not own is readable.
	info, err := stream.Consumer(ctx, "other")
	if err != nil {
		t.Fatalf("other's consumer info: %v", err)
	}
	if info.CachedInfo().AckFloor.Stream != 2 {
		t.Fatalf("other's ack floor = %d, want 2", info.CachedInfo().AckFloor.Stream)
	}
	names := stream.ConsumerNames(ctx)
	for range names.Name() {
	}
	if err := names.Err(); err != nil {
		t.Fatalf("consumer names: %v", err)
	}
	drainArchive(ctx, t, f, js, stream)
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(3)); err != nil {
		t.Fatalf("purge up to 3: %v", err)
	}
	after, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info after purge: %v", err)
	}
	if after.State.FirstSeq != 3 || after.State.Msgs != 3 {
		t.Fatalf("after purge first=%d msgs=%d, want 3 and 3",
			after.State.FirstSeq, after.State.Msgs)
	}
	assertArchiveLimits(t, f, js)
}

// drainArchive is one drain step: a named ephemeral consumer with no
// acks, a fetched batch, and the consumer deleted. An ack is denied.
func drainArchive(
	ctx context.Context, t *testing.T, f *fixture, js jetstream.JetStream,
	stream jetstream.Stream,
) {
	t.Helper()
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: program.ArchiveConsumer("lode"), AckPolicy: jetstream.AckNonePolicy,
		InactiveThreshold: time.Minute,
	})
	if err != nil {
		t.Fatalf("create archive consumer: %v", err)
	}
	batch, err := consumer.Fetch(3, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var got []byte
	var last jetstream.Msg
	for msg := range batch.Messages() {
		got = append(got, msg.Data()...)
		last = msg
	}
	if string(got) != "abc" || last == nil {
		t.Fatalf("drained %q, want abc", got)
	}
	if err := last.Ack(); err != nil {
		t.Fatalf("ack publish: %v", err)
	}
	if err := js.Conn().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !waitFor(func() bool { return violationWithPrefix(f, "$JS.ACK."+archiveStream+".") }) {
		t.Error("ack was not denied: want no ACK grant on an archived stream")
	}
	if err := stream.DeleteConsumer(ctx, program.ArchiveConsumer("lode")); err != nil {
		t.Fatalf("delete archive consumer: %v", err)
	}
}

// assertArchiveLimits: lode's grant stops at its own consumer name, the
// legacy durable create and the stream's subjects.
func assertArchiveLimits(t *testing.T, f *fixture, js jetstream.JetStream) {
	t.Helper()
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	if _, err := js.CreateOrUpdateConsumer(short(), archiveStream, jetstream.ConsumerConfig{
		Name: "lode_other", AckPolicy: jetstream.AckNonePolicy,
	}); err == nil {
		t.Error("consumer with an unpinned name created, want denial")
	}
	nc := js.Conn()
	durable := "$JS.API.CONSUMER.DURABLE.CREATE." + archiveStream + ".lode_archive"
	if _, err := nc.Request(durable, []byte(`{}`), time.Second); err == nil {
		t.Error("legacy durable create answered, want denial")
	}
	if err := nc.Publish("arch.event", []byte("forged")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, subj := range []string{
		"$JS.API.CONSUMER.CREATE." + archiveStream + ".lode_other", durable, "arch.event",
	} {
		if !waitFor(func() bool { return f.violations.contains(subj) }) {
			t.Errorf("no violation logged for %q", subj)
		}
	}
}

func TestPermissions_ArchiveDeclaredWithoutGrantIsDenied(t *testing.T) {
	f := archiveFixture(t)
	js, err := jetstream.New(f.dialProgram(t, "nogrant"))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	if _, err := js.Stream(short(), archiveStream); err == nil {
		t.Error("stream info answered without a grant")
	}
	if _, err := js.Consumer(short(), archiveStream, "other"); err == nil {
		t.Error("consumer info answered without a grant")
	}
	if _, err := js.CreateOrUpdateConsumer(short(), archiveStream, jetstream.ConsumerConfig{
		Name: program.ArchiveConsumer("nogrant"), AckPolicy: jetstream.AckNonePolicy,
	}); err == nil {
		t.Error("archive consumer created without a grant")
	}
	nc := js.Conn()
	purge := "$JS.API.STREAM.PURGE." + archiveStream
	if _, err := nc.Request(purge, []byte(`{"seq":5}`), time.Second); err == nil {
		t.Error("purge answered without a grant")
	}
	for _, subj := range []string{
		"$JS.API.STREAM.INFO." + archiveStream,
		"$JS.API.CONSUMER.INFO." + archiveStream + ".other",
		"$JS.API.CONSUMER.CREATE." + archiveStream + ".nogrant_archive",
		purge,
	} {
		if !waitFor(func() bool { return f.violations.contains(subj) }) {
			t.Errorf("no violation logged for %q", subj)
		}
	}
	assertStreamUntouched(t, f)
}

func assertStreamUntouched(t *testing.T, f *fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, archiveStream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	state := stream.CachedInfo().State
	if state.Msgs != 5 || state.Consumers != 1 {
		t.Fatalf("stream msgs=%d consumers=%d, want 5 and 1", state.Msgs, state.Consumers)
	}
}

func violationWithPrefix(f *fixture, prefix string) bool {
	for _, line := range f.violations.snapshot() {
		if strings.Contains(line, `"`+prefix) {
			return true
		}
	}
	return false
}
