// update_test.go
// Tests for re-registering a program with a changed manifest: a new
// Version alone is admitted for free, additions are admitted and
// provisioned, destructive changes are refused with what they would
// destroy, growth is rechecked against the budget without charging the
// program for itself, concurrent updates from two registrars settle by
// compare-and-set, and a migrating program's existing streams count once.
// Methodology: real embedded NATS per test, the Registrar driven
// directly, provisioning checked against the live streams and buckets.
package programs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

func (f *fixture) record(t *testing.T, name string) Record {
	t.Helper()
	record, found, err := f.registrar.lookup(context.Background(), name)
	if err != nil || !found {
		t.Fatalf("lookup %s: found=%v err=%v", name, found, err)
	}
	return record
}

func (f *fixture) mustRegister(t *testing.T, req program.RegisterRequest) program.Grant {
	t.Helper()
	grant, err := f.register(t, req)
	if err != nil {
		t.Fatalf("Register %s: %v", req.Manifest.Name, err)
	}
	return grant
}

func TestUpdate_VersionOnlyIsAdmittedForFree(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 2})
	req := request(t, "notify", plaintext)
	grant := f.mustRegister(t, req)

	rebuilt := req
	rebuilt.Manifest.Version = "1.0.1"
	again := f.mustRegister(t, rebuilt)
	if again.StoreBudgetBytes != grant.StoreBudgetBytes || again.Replicas != grant.Replicas {
		t.Fatalf("rebuild grant = %+v, want the stored %+v", again, grant)
	}
	if got := f.record(t, "notify").Manifest.Version; got != "1.0.1" {
		t.Fatalf("stored Version = %q, want 1.0.1", got)
	}
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("UsesLeft = %d, want 1: a rebuild spends no use", got)
	}
}

func TestUpdate_AdditionsAreAdmittedAndProvisioned(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	f.mustRegister(t, req)

	grown := req
	grown.Manifest.TaskTypes = append(slices.Clone(req.Manifest.TaskTypes), "notify.send")
	grown.Manifest.Workflows = []string{"notify.flow"}
	grown.Manifest.Streams = append(slices.Clone(req.Manifest.Streams), program.StreamSpec{
		Name: "NOTIFY_OUTBOX", Subjects: []string{"notify.outbox.>"}, MaxBytesFloor: 1 << 20,
	})
	grown.Manifest.Buckets = append(slices.Clone(req.Manifest.Buckets),
		program.BucketSpec{Name: "notify_seen", MaxBytes: 1 << 20})
	grant := f.mustRegister(t, grown)

	if grant.StreamMaxBytes["NOTIFY_OUTBOX"] != 1<<20 || grant.StoreBudgetBytes != 4<<20 {
		t.Fatalf("grant = %+v, want the new stream sized and 4 MiB in all", grant)
	}
	record := f.record(t, "notify")
	if !slices.Contains(record.Manifest.TaskTypes, "notify.send") ||
		record.Grant.StoreBudgetBytes != grant.StoreBudgetBytes {
		t.Fatalf("record not updated: %+v", record)
	}
	if !strings.Contains(fmt.Sprint(record.Permissions), "notify.send") {
		t.Fatalf("permissions do not cover the new task type: %+v", record.Permissions)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.js.Stream(ctx, "NOTIFY_OUTBOX"); err != nil {
		t.Fatalf("new stream not provisioned: %v", err)
	}
	if _, err := f.js.KeyValue(ctx, "notify_seen"); err != nil {
		t.Fatalf("new bucket not provisioned: %v", err)
	}
}

func TestUpdate_DestructiveChangesAreRefused(t *testing.T) {
	cases := []struct {
		name   string
		change func(m *program.Manifest)
		detail string
	}{
		{"remove stream", func(m *program.Manifest) { m.Streams = nil },
			`stream "NOTIFY_EVENTS" is no longer declared`},
		{"remove bucket", func(m *program.Manifest) { m.Buckets = nil },
			`bucket "notify_state" is no longer declared`},
		{"change subjects", func(m *program.Manifest) {
			m.Streams[0].Subjects = []string{"notify.other.>"}
		}, `stream "NOTIFY_EVENTS" subjects change`},
		{"shrink below held", func(m *program.Manifest) {
			m.Streams[0].MaxBytesFloor = 4 << 10
		}, `stream "NOTIFY_EVENTS" would be capped at 4096 bytes but holds`},
		{"lower MaxAge", func(m *program.Manifest) { m.Streams[0].MaxAge = time.Hour },
			`stream "NOTIFY_EVENTS" MaxAge drops`},
		{"lower History", func(m *program.Manifest) { m.Buckets[0].History = 0 },
			`bucket "notify_state" History drops from 4 to 1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Config{Open: true})
			req := request(t, "notify", "")
			req.Manifest.Buckets[0].History = 4
			f.mustRegister(t, req)
			fillStream(t, f.js, "notify.events.x", 16<<10)

			changed := req
			changed.Manifest = cloneManifest(req.Manifest)
			tc.change(&changed.Manifest)
			_, err := f.register(t, changed)
			wantRefused(t, err, program.RefusedManifestChanged)
			if !strings.Contains(err.Error(), tc.detail) ||
				!strings.Contains(err.Error(), "an operator") {
				t.Fatalf("refusal %q does not say %q and that it needs an operator",
					err, tc.detail)
			}
			if !sameManifest(f.record(t, "notify").Manifest, req.Manifest) {
				t.Fatal("a refused update changed the stored manifest")
			}
		})
	}
}

func TestUpdate_GrowthIsRecheckedAgainstOtherPrograms(t *testing.T) {
	f := newFixture(t, Config{Open: true, MaxStoreBytes: 6 << 20})
	f.mustRegister(t, request(t, "alpha", ""))
	req := request(t, "notify", "")
	f.mustRegister(t, req)
	// alpha holds 2 MiB and notify 2 MiB of the 6 MiB. Growing notify to
	// 4 MiB fits only when its own old Grant is not counted against it.
	grown := req
	grown.Manifest = cloneManifest(req.Manifest)
	grown.Manifest.Buckets[0].MaxBytes = 3 << 20
	if grant := f.mustRegister(t, grown); grant.StoreBudgetBytes != 4<<20 {
		t.Fatalf("grown grant = %d bytes, want 4 MiB", grant.StoreBudgetBytes)
	}
	tooBig := grown
	tooBig.Manifest = cloneManifest(grown.Manifest)
	tooBig.Manifest.Buckets[0].MaxBytes = 4 << 20
	_, err := f.register(t, tooBig)
	wantRefused(t, err, program.RefusedBudgetExceeded)
	if got := f.record(t, "notify").Grant.StoreBudgetBytes; got != 4<<20 {
		t.Fatalf("refused growth left a %d-byte grant, want the 4 MiB one", got)
	}
}

// TestUpdate_ConcurrentUpdatesSettleByCAS: eight updates from two
// registrars each add their own stream to the same base. The first to
// commit wins; every other one, judged against the record as it now
// stands, drops the winner's stream and must be refused. A write that
// skipped the compare-and-set would judge some against the stale base
// and admit more than one, orphaning a stream.
func TestUpdate_ConcurrentUpdatesSettleByCAS(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	peer, err := NewRegistrar(ctx, f.js, f.keys, f.registrar.cfg)
	if err != nil {
		t.Fatalf("second registrar: %v", err)
	}
	req := request(t, "notify", "")
	f.mustRegister(t, req)
	const updates = 8
	submitted := make([]program.Manifest, updates)
	errs := make([]error, updates)
	var wg sync.WaitGroup
	for i := range updates {
		submitted[i] = cloneManifest(req.Manifest)
		submitted[i].Streams = append(submitted[i].Streams, program.StreamSpec{
			Name:     fmt.Sprintf("NOTIFY_T%d", i),
			Subjects: []string{fmt.Sprintf("notify.t%d.>", i)}, MaxBytesFloor: 1 << 20,
		})
		registrar := []*Registrar{f.registrar, peer}[i%2]
		update := req
		update.Manifest = submitted[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = registrar.Register(ctx, update)
		}()
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner >= 0 {
				t.Fatalf("updates %d and %d were both admitted", winner, i)
			}
			winner = i
			continue
		}
		wantRefused(t, err, program.RefusedManifestChanged)
	}
	if winner < 0 {
		t.Fatal("no update was admitted")
	}
	if !sameManifest(f.record(t, "notify").Manifest, submitted[winner]) {
		t.Fatal("the stored manifest is not the admitted update's")
	}
}

func TestRegister_MigratingStreamsCountOnce(t *testing.T) {
	f := newFixture(t, Config{Open: true, MaxStoreBytes: 2 << 20})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// notify made its stream before it was a program. Its stream and
	// bucket are 1 MiB each, so it fits the 2 MiB only if that stream
	// is counted once, through the Grant.
	_, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "NOTIFY_EVENTS", Subjects: []string{"notify.events.>"}, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("create pre-existing stream: %v", err)
	}
	if grant := f.mustRegister(t, request(t, "notify", "")); grant.StoreBudgetBytes != 2<<20 {
		t.Fatalf("grant = %d bytes, want 2 MiB", grant.StoreBudgetBytes)
	}
	// Negative space: another program still sees the stream as notify's,
	// so nothing is left for it.
	_, err = f.register(t, request(t, "alpha", ""))
	wantRefused(t, err, program.RefusedBudgetExceeded)
}

func TestRegister_MigratingStreamWithOtherSubjectsIsRefused(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "NOTIFY_EVENTS", Subjects: []string{"notify.legacy.>"}, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("create pre-existing stream: %v", err)
	}
	_, err = f.register(t, request(t, "notify", ""))
	wantRefused(t, err, program.RefusedManifestChanged)
	if _, found, _ := f.registrar.lookup(ctx, "notify"); found {
		t.Fatal("a refused registration was stored")
	}
}

// fillStream publishes size bytes of payload to subject.
func fillStream(t *testing.T, js jetstream.JetStream, subject string, size int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := make([]byte, 1<<10)
	for range size / len(payload) {
		if _, err := js.Publish(ctx, subject, payload); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}
}

func cloneManifest(m program.Manifest) program.Manifest {
	m.TaskTypes = slices.Clone(m.TaskTypes)
	m.Workflows = slices.Clone(m.Workflows)
	m.Streams = slices.Clone(m.Streams)
	for i := range m.Streams {
		m.Streams[i].Subjects = slices.Clone(m.Streams[i].Subjects)
	}
	m.Buckets = slices.Clone(m.Buckets)
	return m
}

// TestUpdate_NewStreamFirstDoesNotHideALaterChange: a stream that does
// not exist yet is skipped, not the end of the live checks, so a
// destructive change to a later stream or bucket is still refused.
func TestUpdate_NewStreamFirstDoesNotHideALaterChange(t *testing.T) {
	newFirst := program.StreamSpec{
		Name: "NOTIFY_AAA", Subjects: []string{"notify.aaa.>"}, MaxBytesFloor: 1 << 20,
	}
	cases := []struct {
		name   string
		change func(m *program.Manifest)
		detail string
	}{
		{"later stream subjects", func(m *program.Manifest) {
			m.Streams[1].Subjects = []string{"notify.other.>"}
		}, `stream "NOTIFY_EVENTS" subjects change`},
		{"later stream shrink", func(m *program.Manifest) {
			m.Streams[1].MaxBytesFloor = 4 << 10
		}, `stream "NOTIFY_EVENTS" would be capped at 4096 bytes`},
		{"bucket after streams", func(m *program.Manifest) {
			m.Buckets[0].MaxBytes = 4 << 10
		}, `bucket "notify_state" would be capped at 4096 bytes`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Config{Open: true})
			req := request(t, "notify", "")
			f.mustRegister(t, req)
			fillStream(t, f.js, "notify.events.x", 16<<10)
			fillBucket(t, f.js, "notify_state", 16<<10)

			changed := req
			changed.Manifest = cloneManifest(req.Manifest)
			changed.Manifest.Streams = append([]program.StreamSpec{newFirst},
				changed.Manifest.Streams...)
			tc.change(&changed.Manifest)
			_, err := f.register(t, changed)
			wantRefused(t, err, program.RefusedManifestChanged)
			if !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("refusal %q does not say %q", err, tc.detail)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := f.js.Stream(ctx, "NOTIFY_AAA"); err == nil {
				t.Fatal("a refused update provisioned its new stream")
			}
		})
	}
}

// TestUpdate_RechecksTheAdmittingJoinKey: a change other than Version is
// refused once the key that admitted the program could not admit it
// now. A Version-only rebuild is not a change to what was admitted and
// still goes through.
func TestUpdate_RechecksTheAdmittingJoinKey(t *testing.T) {
	grow := func(m program.Manifest) program.Manifest {
		m = cloneManifest(m)
		m.TaskTypes = append(m.TaskTypes, "notify.more")
		return m
	}
	t.Run("revoked", func(t *testing.T) {
		f := newFixture(t, Config{Open: true})
		key, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 1})
		req := request(t, "notify", plaintext)
		f.mustRegister(t, req)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.keys.Revoke(ctx, key.ID); err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		changed := req
		changed.Manifest = grow(req.Manifest)
		_, err := f.register(t, changed)
		wantRefused(t, err, program.RefusedJoinKeyInvalid)
		rebuilt := req
		rebuilt.Manifest = cloneManifest(req.Manifest)
		rebuilt.Manifest.Version = "1.0.1"
		f.mustRegister(t, rebuilt)
	})
	t.Run("expired", func(t *testing.T) {
		f := newFixture(t, Config{Open: true})
		expires := time.Now().Add(500 * time.Millisecond)
		_, plaintext := f.createKey(t, joinkey.Spec{Label: "k", ExpiresAt: &expires})
		req := request(t, "notify", plaintext)
		f.mustRegister(t, req)
		time.Sleep(time.Until(expires) + 50*time.Millisecond)
		changed := req
		changed.Manifest = grow(req.Manifest)
		_, err := f.register(t, changed)
		wantRefused(t, err, program.RefusedJoinKeyInvalid)
	})
	t.Run("protocol outside glob", func(t *testing.T) {
		f := newFixture(t, Config{Open: true})
		_, plaintext := f.createKey(t, joinkey.Spec{Label: "k", NameGlob: "notify*"})
		req := request(t, "notify", plaintext)
		f.mustRegister(t, req)
		changed := req
		changed.Manifest = grow(req.Manifest)
		changed.Manifest.Protocols = []string{"tool.exec"}
		_, err := f.register(t, changed)
		wantRefused(t, err, program.RefusedProtocolNotAllowed)
		if slices.Contains(f.record(t, "notify").Manifest.Protocols, "tool.exec") {
			t.Fatal("a refused protocol was stored")
		}
	})
}

// TestRegister_AdoptingKeepsOperatorSettings: an operator's stream is
// adopted with the fields the manifest does not own left as they were,
// and a manifest that would start expiring its data is refused.
func TestRegister_AdoptingKeepsOperatorSettings(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "NOTIFY_EVENTS", Subjects: []string{"notify.events.>"}, MaxBytes: 1 << 20,
		Discard: jetstream.DiscardNew, MaxMsgs: 5000, DenyDelete: true,
	})
	if err != nil {
		t.Fatalf("create operator stream: %v", err)
	}
	expiring := request(t, "notify", "")
	expiring.Manifest.Streams[0].MaxAge = time.Minute
	_, err = f.register(t, expiring)
	wantRefused(t, err, program.RefusedManifestChanged)
	if !strings.Contains(err.Error(), `MaxAge drops from 0s to 1m0s`) {
		t.Fatalf("refusal %q does not name the MaxAge change", err)
	}

	f.mustRegister(t, request(t, "notify", ""))
	stream, err := f.js.Stream(ctx, "NOTIFY_EVENTS")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	live := stream.CachedInfo().Config
	if live.Discard != jetstream.DiscardNew || live.MaxMsgs != 5000 || !live.DenyDelete {
		t.Fatalf("adoption reset operator settings: discard=%v maxMsgs=%d denyDelete=%v",
			live.Discard, live.MaxMsgs, live.DenyDelete)
	}
}

// TestUpdate_PartialProvisionRestoresStreams: an update that grows one
// stream and adds one that cannot be created is undone whole: the grown
// stream gets its old config back and the record its old manifest, so
// the two still agree.
func TestUpdate_PartialProvisionRestoresStreams(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	f.mustRegister(t, req)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "SQUAT", Subjects: []string{"notify.outbox.>"},
	})
	if err != nil {
		t.Fatalf("create squatting stream: %v", err)
	}
	changed := req
	changed.Manifest = cloneManifest(req.Manifest)
	changed.Manifest.Streams[0].MaxBytesFloor = 2 << 20
	changed.Manifest.Streams = append(changed.Manifest.Streams, program.StreamSpec{
		Name: "NOTIFY_OUTBOX", Subjects: []string{"notify.outbox.>"}, MaxBytesFloor: 1 << 20,
	})
	_, err = f.register(t, changed)
	var refusal *program.ErrRefused
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("update err = %v, want a provisioning failure", err)
	}
	stream, err := f.js.Stream(ctx, "NOTIFY_EVENTS")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if got := stream.CachedInfo().Config.MaxBytes; got != 1<<20 {
		t.Fatalf("NOTIFY_EVENTS MaxBytes = %d after the failed update, want 1 MiB", got)
	}
	if !sameManifest(f.record(t, "notify").Manifest, req.Manifest) {
		t.Fatal("the record kept the manifest whose provisioning failed")
	}
}

// fillBucket writes size bytes of values to bucket.
func fillBucket(t *testing.T, js jetstream.JetStream, bucket string, size int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		t.Fatalf("bucket %s: %v", bucket, err)
	}
	value := make([]byte, 1<<10)
	for i := range size / len(value) {
		if _, err := kv.Put(ctx, fmt.Sprintf("k%d", i), value); err != nil {
			t.Fatalf("put %s: %v", bucket, err)
		}
	}
}

// TestUpdate_RollBackLeavesANewerUpdatesStreams forces the interleaving
// a rollback must survive: A's update U1 commits and fails to provision,
// then, before A rolls back, B (another node) commits and provisions U2.
// A's record compare-and-set then loses, so A must leave the streams as
// U2 configured them rather than writing the configs from before U1
// over them.
func TestUpdate_RollBackLeavesANewerUpdatesStreams(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	peer, err := NewRegistrar(ctx, f.js, f.keys, f.registrar.cfg)
	if err != nil {
		t.Fatalf("second registrar: %v", err)
	}
	req := request(t, "notify", "")
	f.mustRegister(t, req)
	if _, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "SQUAT", Subjects: []string{"notify.outbox.>"},
	}); err != nil {
		t.Fatalf("create squatting stream: %v", err)
	}
	u1 := req
	u1.Manifest = cloneManifest(req.Manifest)
	u1.Manifest.Streams[0].MaxBytesFloor = 2 << 20
	u1.Manifest.Streams = append(u1.Manifest.Streams, program.StreamSpec{
		Name: "NOTIFY_OUTBOX", Subjects: []string{"notify.outbox.>"}, MaxBytesFloor: 1 << 20,
	})
	u2 := u1
	u2.Manifest = cloneManifest(u1.Manifest)
	u2.Manifest.Streams[0].MaxBytesFloor = 3 << 20
	u2.Manifest.Buckets[0].History = 5
	f.registrar.beforeRollBack = func() {
		if err := f.js.DeleteStream(ctx, "SQUAT"); err != nil {
			t.Errorf("delete squatting stream: %v", err)
		}
		if _, err := peer.Register(ctx, u2); err != nil {
			t.Errorf("B's update in A's rollback window: %v", err)
		}
	}
	_, err = f.register(t, u1)
	var refusal *program.ErrRefused
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("A's update err = %v, want its provisioning failure", err)
	}
	if !sameManifest(f.record(t, "notify").Manifest, u2.Manifest) {
		t.Fatal("the record is not B's update")
	}
	events, err := f.js.Stream(ctx, "NOTIFY_EVENTS")
	if err != nil {
		t.Fatalf("NOTIFY_EVENTS: %v", err)
	}
	if got := events.CachedInfo().Config.MaxBytes; got != 3<<20 {
		t.Fatalf("NOTIFY_EVENTS MaxBytes = %d, want B's 3 MiB", got)
	}
	bucket, err := f.js.Stream(ctx, "KV_notify_state")
	if err != nil {
		t.Fatalf("KV_notify_state: %v", err)
	}
	if got := bucket.CachedInfo().Config.MaxMsgsPerSubject; got != 5 {
		t.Fatalf("notify_state History = %d, want B's 5", got)
	}
}
