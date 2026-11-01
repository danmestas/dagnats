// register_test.go
// Tests for program registration: the open-mode happy path, first come
// first served with idempotent same-key re-registration, every join-key
// refusal, refunds on every failure after the use is reserved, exactly
// UsesMax admissions under concurrency, budget and version refusals, and
// the required-auth-mode refusal.
// Methodology: real embedded NATS per test, a real joinkey.Store, and
// the Registrar driven directly (the NATS endpoint has its own test).
package programs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fixture struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	keys      *joinkey.Store
	registrar *Registrar
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys, err := joinkey.Open(ctx, js)
	if err != nil {
		t.Fatalf("joinkey.Open: %v", err)
	}
	t.Cleanup(keys.Close)
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	// A test's MaxStoreBytes is the room it leaves for programs, on top
	// of what SetupAll's own streams already reserve.
	if cfg.MaxStoreBytes == 0 {
		cfg.MaxStoreBytes = natstest.MaxStoreBytes
	} else {
		cfg.MaxStoreBytes += engineReserved(t, ctx, js)
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "0.0.40"
	}
	registrar, err := NewRegistrar(ctx, js, keys, cfg)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	return &fixture{nc: nc, js: js, keys: keys, registrar: registrar}
}

func engineReserved(t *testing.T, ctx context.Context, js jetstream.JetStream) int64 {
	t.Helper()
	var reserved int64
	lister := js.ListStreams(ctx)
	for info := range lister.Info() {
		if info.Config.MaxBytes > 0 {
			reserved += info.Config.MaxBytes
		}
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list streams: %v", err)
	}
	return reserved
}

// manifest sizes its stream by the floor alone, so budget arithmetic in
// the tests is exact.
func manifest(name string) program.Manifest {
	upper := strings.ToUpper(name)
	return program.Manifest{
		Name:      name,
		Version:   "1.0.0",
		Dagnats:   ">=0.0.35 <0.1.0",
		TaskTypes: []string{name + ".work"},
		Streams: []program.StreamSpec{{
			Name: upper + "_EVENTS", Subjects: []string{name + ".events.>"},
			MaxBytesFloor: 1 << 20,
		}},
		Buckets: []program.BucketSpec{{Name: name + "_state", MaxBytes: 1 << 20}},
	}
}

func publicKey(t *testing.T) string {
	t.Helper()
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	return key.PublicKey()
}

func request(t *testing.T, name, joinKey string) program.RegisterRequest {
	t.Helper()
	return program.RegisterRequest{
		JoinKey: joinKey, Manifest: manifest(name), PublicKey: publicKey(t),
		Instance: program.Instance{Hostname: "test", Pid: 1},
	}
}

func (f *fixture) register(t *testing.T, req program.RegisterRequest) (program.Grant, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.registrar.Register(ctx, req)
}

func (f *fixture) createKey(t *testing.T, spec joinkey.Spec) (joinkey.JoinKey, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key, plaintext, err := f.keys.Create(ctx, spec, "tester")
	if err != nil {
		t.Fatalf("Create join key: %v", err)
	}
	return key, plaintext
}

func (f *fixture) usesLeft(t *testing.T, id string) int {
	t.Helper()
	for _, key := range f.keys.List() {
		if key.ID == id {
			return key.UsesLeft
		}
	}
	t.Fatalf("join key %s not listed", id)
	return 0
}

func wantRefused(t *testing.T, err error, code string) {
	t.Helper()
	var refused *program.ErrRefused
	if !errors.As(err, &refused) || refused.Code != code {
		t.Fatalf("err = %v, want refusal %q", err, code)
	}
	if refused.Detail == "" {
		t.Fatalf("refusal %q has no detail", code)
	}
}

func TestRegister_OpenModeNeedsNoKey(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	req.Manifest.Streams[0].MaxBytesFraction = 0.01
	grant, err := f.register(t, req)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	budget := natstest.MaxStoreBytes
	wantStream := int64(float64(budget) * 0.01)
	if grant.Replicas != 1 || grant.StreamMaxBytes["NOTIFY_EVENTS"] != wantStream ||
		grant.StoreBudgetBytes != wantStream+1<<20 || grant.ServerVersion != "0.0.40" {
		t.Fatalf("grant = %+v", grant)
	}
	record, ok, err := f.registrar.lookup(context.Background(), "notify")
	if err != nil || !ok {
		t.Fatalf("lookup: %v %v", ok, err)
	}
	if record.PublicKey != req.PublicKey || record.JoinKeyID != "" ||
		record.RegisteredAt.IsZero() || len(record.Permissions.Publish.Allow) == 0 {
		t.Fatalf("record = %+v", record)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.js.Stream(ctx, "NOTIFY_EVENTS"); err != nil {
		t.Fatalf("stream not provisioned: %v", err)
	}
	if _, err := f.js.KeyValue(ctx, "notify_state"); err != nil {
		t.Fatalf("bucket not provisioned: %v", err)
	}
}

func TestRegister_FirstComeFirstServed(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 5})
	first := request(t, "notify", plaintext)
	grant, err := f.register(t, first)
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}
	again, err := f.register(t, first)
	if err != nil || again.StoreBudgetBytes != grant.StoreBudgetBytes {
		t.Fatalf("same-key re-register = %+v, %v; want the stored grant", again, err)
	}
	_, err = f.register(t, request(t, "notify", plaintext))
	wantRefused(t, err, program.RefusedNameTaken)
	if got := f.usesLeft(t, key.ID); got != 4 {
		t.Fatalf("UsesLeft = %d, want 4: only the first registration spends", got)
	}
	changed := first
	changed.Manifest.TaskTypes = append(changed.Manifest.TaskTypes, "notify.more")
	_, err = f.register(t, changed)
	wantRefused(t, err, program.RefusedManifestChanged)
	record, _, err := f.registrar.lookup(context.Background(), "notify")
	if err != nil || record.JoinKeyID != key.ID {
		t.Fatalf("record join key = %q, %v; want %q", record.JoinKeyID, err, key.ID)
	}
}

func TestRegister_JoinKeyRefusals(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	_, scoped := f.createKey(t, joinkey.Spec{Label: "scoped", NameGlob: "artifact*"})
	revokedKey, revoked := f.createKey(t, joinkey.Spec{Label: "revoked"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.keys.Revoke(ctx, revokedKey.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	usedKey, used := f.createKey(t, joinkey.Spec{Label: "once", UsesMax: 1})
	if err := f.keys.ConsumeUse(ctx, usedKey.ID); err != nil {
		t.Fatalf("ConsumeUse: %v", err)
	}
	cases := []struct{ name, key, code string }{
		{"notify", "djk_bogus_secret", program.RefusedJoinKeyInvalid},
		{"notify", scoped, program.RefusedJoinKeyInvalid},
		{"notify", revoked, program.RefusedJoinKeyInvalid},
		{"notify", used, program.RefusedJoinKeyExhausted},
	}
	for _, tc := range cases {
		_, err := f.register(t, request(t, tc.name, tc.key))
		wantRefused(t, err, tc.code)
	}
	if _, err := f.register(t, request(t, "artifactstore", scoped)); err != nil {
		t.Fatalf("glob-covered name refused: %v", err)
	}
}

func TestRegister_RefundsOnLaterFailure(t *testing.T) {
	f := newFixture(t, Config{Open: true, MaxStoreBytes: 4 << 20})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "once", UsesMax: 1})
	tooOld := request(t, "notify", plaintext)
	tooOld.Manifest.Dagnats = ">=9.0.0"
	_, err := f.register(t, tooOld)
	wantRefused(t, err, program.RefusedVersionIncompatible)
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("version refusal spent a use: UsesLeft = %d", got)
	}
	huge := request(t, "notify", plaintext)
	huge.Manifest.Buckets[0].MaxBytes = 64 << 20
	_, err = f.register(t, huge)
	wantRefused(t, err, program.RefusedBudgetExceeded)
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("budget refusal spent a use: UsesLeft = %d", got)
	}
	if _, err := f.register(t, request(t, "notify", plaintext)); err != nil {
		t.Fatalf("Register after refunds: %v", err)
	}
	if got := f.usesLeft(t, key.ID); got != 0 {
		t.Fatalf("UsesLeft = %d, want 0", got)
	}
}

func TestRegister_BudgetExceededNamesTheNumbers(t *testing.T) {
	f := newFixture(t, Config{Open: true, MaxStoreBytes: 4 << 20})
	if _, err := f.register(t, request(t, "alpha", "")); err != nil {
		t.Fatalf("alpha: %v", err)
	}
	// alpha holds 2 MiB of the 4 MiB left for programs; beta's 4 MiB
	// does not fit in the 2 MiB that remain.
	beta := request(t, "beta", "")
	beta.Manifest.Buckets[0].MaxBytes = 3 << 20
	_, err := f.register(t, beta)
	wantRefused(t, err, program.RefusedBudgetExceeded)
	if !strings.Contains(err.Error(), "needs "+fmt.Sprint(4<<20)+" bytes; "+
		fmt.Sprint(2<<20)+" of the") {
		t.Fatalf("budget refusal %q does not name what was needed and free", err)
	}
	if _, ok, _ := f.registrar.lookup(context.Background(), "beta"); ok {
		t.Fatal("refused program was stored")
	}
}

func TestRegister_VersionRange(t *testing.T) {
	f := newFixture(t, Config{Open: true, ServerVersion: "0.0.40"})
	ranges := map[string]string{
		"":                 "",
		">=0.0.35 <0.1.0":  "",
		"=0.0.40":          "",
		"v0.0.40":          "",
		">0.0.40":          program.RefusedVersionIncompatible,
		"<0.0.40":          program.RefusedVersionIncompatible,
		">=banana":         program.RefusedVersionIncompatible,
		">=0.0.35 <=0.0.3": program.RefusedVersionIncompatible,
	}
	index := 0
	for expr, code := range ranges {
		index++
		req := request(t, fmt.Sprintf("prog%d", index), "")
		req.Manifest.Dagnats = expr
		_, err := f.register(t, req)
		if code == "" && err != nil {
			t.Errorf("range %q refused: %v", expr, err)
		}
		if code != "" {
			var refused *program.ErrRefused
			if !errors.As(err, &refused) || refused.Code != code {
				t.Errorf("range %q err = %v, want %s", expr, err, code)
			}
		}
	}
	dev := newFixture(t, Config{Open: true, ServerVersion: "dev"})
	req := request(t, "notify", "")
	req.Manifest.Dagnats = ">=9.0.0"
	if _, err := dev.register(t, req); err != nil {
		t.Fatalf("dev server refused a range: %v", err)
	}
}

func TestRegister_InvalidRequests(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	bad := request(t, "Notify", "")
	_, err := f.register(t, bad)
	wantRefused(t, err, program.RefusedNameInvalid)
	noKey := request(t, "notify", "")
	noKey.PublicKey = "not-a-key"
	_, err = f.register(t, noKey)
	wantRefused(t, err, program.RefusedRequestInvalid)
}

func TestRegister_RequiredAuthModeRefused(t *testing.T) {
	f := newFixture(t, Config{Open: false})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 2})
	_, err := f.register(t, request(t, "notify", plaintext))
	wantRefused(t, err, program.RefusedAuthModeUnsupported)
	if got := f.usesLeft(t, key.ID); got != 2 {
		t.Fatalf("auth-mode refusal spent a use: UsesLeft = %d", got)
	}
	if _, ok, _ := f.registrar.lookup(context.Background(), "notify"); ok {
		t.Fatal("refused program was stored")
	}
}

// TestRegister_ExactlyUsesMaxUnderConcurrency races more distinct
// programs than the key has uses. The reservation precedes the commit,
// so no interleaving admits more than UsesMax.
func TestRegister_ExactlyUsesMaxUnderConcurrency(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	const usesMax, contenders = 3, 12
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "race", UsesMax: usesMax})
	requests := make([]program.RegisterRequest, contenders)
	for i := range requests {
		requests[i] = request(t, fmt.Sprintf("race%d", i), plaintext)
	}
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(req program.RegisterRequest) {
			defer wg.Done()
			_, err := f.register(t, req)
			errs <- err
		}(req)
	}
	wg.Wait()
	close(errs)
	admitted, exhausted := 0, 0
	for err := range errs {
		var refused *program.ErrRefused
		switch {
		case err == nil:
			admitted++
		case errors.As(err, &refused) && refused.Code == program.RefusedJoinKeyExhausted:
			exhausted++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if admitted != usesMax || exhausted != contenders-usesMax {
		t.Fatalf("admitted=%d exhausted=%d, want %d and %d",
			admitted, exhausted, usesMax, contenders-usesMax)
	}
	if got := f.usesLeft(t, key.ID); got != 0 {
		t.Fatalf("UsesLeft = %d, want 0", got)
	}
}

// TestRegister_ProtocolClash refuses a protocol root that lands on a
// registered program's subjects, and a program name that lands under a
// protocol another program already declared. notify and notifyx share
// a byte prefix but no subject, so they do not clash.
func TestRegister_ProtocolClash(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	if _, err := f.register(t, request(t, "notify", "")); err != nil {
		t.Fatalf("notify: %v", err)
	}
	for _, protocol := range []string{"notify", "notify.events"} {
		evil := request(t, "evil", "")
		evil.Manifest.Protocols = []string{protocol}
		_, err := f.register(t, evil)
		wantRefused(t, err, program.RefusedProtocolClash)
	}
	squatter := request(t, "squatter", "")
	squatter.Manifest.Protocols = []string{"victim.rpc"}
	if _, err := f.register(t, squatter); err != nil {
		t.Fatalf("squatter: %v", err)
	}
	_, err := f.register(t, request(t, "victim", ""))
	wantRefused(t, err, program.RefusedProtocolClash)

	neighbour := request(t, "neighbour", "")
	neighbour.Manifest.Protocols = []string{"notifyx.ping"}
	if _, err := f.register(t, neighbour); err != nil {
		t.Fatalf("notifyx protocol beside notify refused: %v", err)
	}
	if _, err := f.register(t, request(t, "victimx", "")); err != nil {
		t.Fatalf("victimx beside the victim protocol refused: %v", err)
	}
	shared := request(t, "other", "")
	shared.Manifest.Protocols = []string{"victim.rpc"}
	_, err = f.register(t, shared)
	if err != nil {
		t.Fatalf("a protocol two programs share was refused: %v", err)
	}
}

func TestRegister_HugeSizesAreRefusedNotOverflowed(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	const huge = int64(^uint64(0) >> 1) // math.MaxInt64
	cases := []func(*program.Manifest){
		func(m *program.Manifest) { m.Streams[0].MaxBytesFloor = huge },
		func(m *program.Manifest) { m.Buckets[0].MaxBytes = huge },
		func(m *program.Manifest) {
			m.Streams[0].MaxBytesFloor = huge / 2
			m.Buckets[0].MaxBytes = huge/2 + 2
		},
	}
	for i, mutate := range cases {
		req := request(t, fmt.Sprintf("huge%d", i), "")
		mutate(&req.Manifest)
		_, err := f.register(t, req)
		wantRefused(t, err, program.RefusedBudgetExceeded)
	}
}

func TestRegister_NameTakenRevealsNothing(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "k"})
	if _, err := f.register(t, request(t, "notify", plaintext)); err != nil {
		t.Fatalf("notify: %v", err)
	}
	_, err := f.register(t, request(t, "notify", ""))
	wantRefused(t, err, program.RefusedNameTaken)
	if strings.Contains(err.Error(), key.ID) || strings.Contains(err.Error(), "20") {
		t.Fatalf("name_taken detail %q reveals the owner", err)
	}
}

// TestRegister_ProvisionFailureRollsBack makes stream creation fail
// with a pre-existing stream that already captures the program's
// subjects. The record is removed and the use refunded, so the name and
// the key are both free again.
func TestRegister_ProvisionFailureRollsBack(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := f.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "SQUAT", Subjects: []string{"notify.events.>"},
	})
	if err != nil {
		t.Fatalf("create squatting stream: %v", err)
	}
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "once", UsesMax: 1})
	_, err = f.register(t, request(t, "notify", plaintext))
	var refusal *program.ErrRefused
	if err == nil || errors.As(err, &refusal) {
		t.Fatalf("Register err = %v, want a provisioning failure", err)
	}
	if _, ok, _ := f.registrar.lookup(ctx, "notify"); ok {
		t.Fatal("a record survived its failed provisioning")
	}
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("UsesLeft = %d, want 1 after the refund", got)
	}
}

// TestRegister_NamesKeyLimitsProtocols: a key restricted to artifact*
// may not declare a protocol rooted outside artifact*, which would
// otherwise lock those names out through protocol_clash. The refusal
// spends no use; unrestricted keys and keyless open mode are unchanged.
func TestRegister_NamesKeyLimitsProtocols(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	scopedKey, scoped := f.createKey(t, joinkey.Spec{Label: "s", NameGlob: "artifact*", UsesMax: 3})
	outside := request(t, "artifactstore", scoped)
	outside.Manifest.Protocols = []string{"notify.rpc"}
	_, err := f.register(t, outside)
	wantRefused(t, err, program.RefusedProtocolNotAllowed)
	if got := f.usesLeft(t, scopedKey.ID); got != 3 {
		t.Fatalf("refusal spent a use: UsesLeft = %d", got)
	}
	inside := request(t, "artifactstore", scoped)
	inside.Manifest.Protocols = []string{"artifacts.fetch"}
	if _, err := f.register(t, inside); err != nil {
		t.Fatalf("protocol inside the glob refused: %v", err)
	}
	_, open := f.createKey(t, joinkey.Spec{Label: "open"})
	unrestricted := request(t, "lode", open)
	unrestricted.Manifest.Protocols = []string{"notify.rpc"}
	if _, err := f.register(t, unrestricted); err != nil {
		t.Fatalf("unrestricted key refused a protocol: %v", err)
	}
	keyless := request(t, "relay", "")
	keyless.Manifest.Protocols = []string{"tool.exec"}
	if _, err := f.register(t, keyless); err != nil {
		t.Fatalf("keyless open-mode protocol refused: %v", err)
	}
}

func TestRegister_ZeroSizedStreamIsRequestInvalid(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	req.Manifest.Streams[0].MaxBytesFloor = 0
	req.Manifest.Streams[0].MaxBytesFraction = 1e-18
	_, err := f.register(t, req)
	wantRefused(t, err, program.RefusedRequestInvalid)
	if _, ok, _ := f.registrar.lookup(context.Background(), "notify"); ok {
		t.Fatal("a zero-sized program was stored")
	}
}
