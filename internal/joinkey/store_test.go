// store_test.go
// Tests for the joinkey Store: create/verify round trip, each typed
// Verify refusal, immediate revocation through the watched cache,
// once-only plaintext, and ConsumeUse's CAS bound under concurrency.
// Methodology: real embedded NATS per test (the Store is a KV wrapper
// with no pure seam worth faking), bounded contexts on every call, and
// an injected clock for expiry so no test sleeps on wall time.
package joinkey

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func newTestJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

func openTestStore(t *testing.T, js jetstream.JetStream) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return openTestStore(t, newTestJetStream(t))
}

func mustCreate(t *testing.T, store *Store, spec Spec) (JoinKey, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key, plaintext, err := store.Create(ctx, spec, "tester")
	if err != nil {
		t.Fatalf("Create(%+v): %v", spec, err)
	}
	return key, plaintext
}

func TestCreateAndVerify(t *testing.T) {
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "deploys"})
	if !strings.HasPrefix(plaintext, keyPrefix+"_"+created.ID+"_") {
		t.Fatalf("plaintext %q does not carry prefix and id %q", plaintext, created.ID)
	}
	if created.SecretHash != nil {
		t.Fatal("Create returned the secret hash")
	}
	got, err := store.Verify(plaintext, "notify")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.ID != created.ID || got.Label != "deploys" || got.CreatedBy != "tester" {
		t.Fatalf("Verify = %+v, want the created key", got)
	}
	if got.SecretHash != nil {
		t.Fatal("Verify leaked the secret hash")
	}
	if got.UsesMax != 0 || got.ExpiresAt != nil || got.NameGlob != "" {
		t.Fatalf("default key is not unbounded: %+v", got)
	}
}

func TestVerifyWrongSecret(t *testing.T) {
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "deploys"})
	other, _ := mustCreate(t, store, Spec{Label: "other"})
	secretPart := plaintext[strings.LastIndex(plaintext, "_")+1:]
	cases := map[string]string{
		"empty":           "",
		"garbage":         "not-a-join-key",
		"wrong secret":    keyPrefix + "_" + created.ID + "_" + "AAAA",
		"unknown id":      keyPrefix + "_nosuchid_" + secretPart,
		"secret of other": keyPrefix + "_" + other.ID + "_" + secretPart,
		"wrong prefix":    "dgn_" + created.ID + "_" + secretPart,
	}
	for name, candidate := range cases {
		if _, err := store.Verify(candidate, "notify"); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Verify err = %v, want ErrInvalid", name, err)
		}
	}
	if _, err := store.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("the real key stopped verifying: %v", err)
	}
}

// TestRevokeIsImmediateThroughCache revokes on one Store and verifies
// on a second Store over the same bucket, so the refusal can only come
// from the watch updating the second Store's cache.
func TestRevokeIsImmediateThroughCache(t *testing.T) {
	js := newTestJetStream(t)
	admin := openTestStore(t, js)
	verifier := openTestStore(t, js)
	created, plaintext := mustCreate(t, admin, Spec{Label: "deploys"})
	waitFor(t, func() bool {
		_, err := verifier.Verify(plaintext, "notify")
		return err == nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.Revoke(ctx, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := admin.Verify(plaintext, "notify"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revoking store: Verify err = %v, want ErrRevoked", err)
	}
	waitFor(t, func() bool {
		_, err := verifier.Verify(plaintext, "notify")
		return errors.Is(err, ErrRevoked)
	})
	listed := admin.List()
	if len(listed) != 1 || listed[0].RevokedAt == nil {
		t.Fatalf("List after revoke = %+v, want one revoked record", listed)
	}
	if err := admin.Revoke(ctx, "nosuchid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Revoke unknown err = %v, want ErrNotFound", err)
	}
}

func TestVerifyExpired(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	expires := now.Add(time.Hour)
	_, plaintext := mustCreate(t, store, Spec{Label: "short", ExpiresAt: &expires})
	if _, err := store.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("Verify before expiry: %v", err)
	}
	store.now = func() time.Time { return expires }
	if _, err := store.Verify(plaintext, "notify"); !errors.Is(err, ErrExpired) {
		t.Fatalf("Verify at expiry err = %v, want ErrExpired", err)
	}
	past := now.Add(-time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := store.Create(ctx, Spec{Label: "stale", ExpiresAt: &past}, "t"); err == nil {
		t.Fatal("Create accepted an expiry in the past")
	}
}

func TestVerifyNameGlob(t *testing.T) {
	store := newTestStore(t)
	_, exact := mustCreate(t, store, Spec{Label: "exact", NameGlob: "notify"})
	_, family := mustCreate(t, store, Spec{Label: "family", NameGlob: "artifact*"})
	allowed := map[string]string{exact: "notify", family: "artifactstore"}
	for plaintext, name := range allowed {
		if _, err := store.Verify(plaintext, name); err != nil {
			t.Errorf("Verify(%q): %v", name, err)
		}
	}
	refused := map[string]string{exact: "notifyx", family: "notify"}
	for plaintext, name := range refused {
		if _, err := store.Verify(plaintext, name); !errors.Is(err, ErrNameNotAllowed) {
			t.Errorf("Verify(%q) err = %v, want ErrNameNotAllowed", name, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, glob := range []string{"[a-z]*", "Notify", "a/b", strings.Repeat("a", 65)} {
		if _, _, err := store.Create(ctx, Spec{Label: "bad", NameGlob: glob}, "t"); err == nil {
			t.Errorf("Create accepted glob %q", glob)
		}
	}
}

func TestUsesExhausted(t *testing.T) {
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "twice", UsesMax: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, err := store.Verify(plaintext, "notify"); err != nil {
			t.Fatalf("Verify use %d: %v", i, err)
		}
		if err := store.ConsumeUse(ctx, created.ID); err != nil {
			t.Fatalf("ConsumeUse %d: %v", i, err)
		}
	}
	if _, err := store.Verify(plaintext, "notify"); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Verify after uses err = %v, want ErrExhausted", err)
	}
	if err := store.ConsumeUse(ctx, created.ID); !errors.Is(err, ErrExhausted) {
		t.Fatalf("ConsumeUse past max err = %v, want ErrExhausted", err)
	}
	if err := store.ConsumeUse(ctx, "nosuchid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ConsumeUse unknown err = %v, want ErrNotFound", err)
	}
}

// TestConsumeUseConcurrent races more consumers than uses across two
// Stores, so the bound can only hold through the KV revision CAS, not
// through either Store's cache.
func TestConsumeUseConcurrent(t *testing.T) {
	js := newTestJetStream(t)
	stores := []*Store{openTestStore(t, js), openTestStore(t, js)}
	const usesMax, consumerCount = 5, 24
	created, _ := mustCreate(t, stores[0], Spec{Label: "race", UsesMax: usesMax})
	waitFor(t, func() bool {
		_, ok := stores[1].lookup(created.ID)
		return ok
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	results := make(chan error, consumerCount)
	var wg sync.WaitGroup
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			results <- store.ConsumeUse(ctx, created.ID)
		}(stores[i%2])
	}
	wg.Wait()
	close(results)
	succeeded, exhausted := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrExhausted):
			exhausted++
		default:
			t.Errorf("ConsumeUse: unexpected error %v", err)
		}
	}
	if succeeded != usesMax || exhausted != consumerCount-usesMax {
		t.Fatalf("succeeded=%d exhausted=%d, want %d and %d",
			succeeded, exhausted, usesMax, consumerCount-usesMax)
	}
	entry, err := js.KeyValue(ctx, BucketName)
	if err != nil {
		t.Fatalf("bind bucket: %v", err)
	}
	stored, err := fetch(ctx, entry, created.ID)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if stored.UsesLeft != 0 {
		t.Fatalf("UsesLeft = %d, want 0", stored.UsesLeft)
	}
}

func TestUnboundedConsumeUseIsFree(t *testing.T) {
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "forever"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := store.ConsumeUse(ctx, created.ID); err != nil {
			t.Fatalf("ConsumeUse %d: %v", i, err)
		}
	}
	if _, err := store.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("unbounded key refused after uses: %v", err)
	}
}

// TestPlaintextNeverListedOrLogged captures every slog record emitted
// across the key's whole lifecycle and checks the secret half is in
// none of them, nor in List.
func TestPlaintextNeverListedOrLogged(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs,
		&slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "secretive", UsesMax: 3})
	secretPart := plaintext[strings.LastIndex(plaintext, "_")+1:]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if err := store.ConsumeUse(ctx, created.ID); err != nil {
		t.Fatalf("ConsumeUse: %v", err)
	}
	if err := store.Revoke(ctx, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	listed := store.List()
	if len(listed) != 1 || listed[0].SecretHash != nil {
		t.Fatalf("List = %+v, want one record without a hash", listed)
	}
	for _, key := range listed {
		if strings.Contains(key.Label+key.ID+key.NameGlob, secretPart) {
			t.Fatal("List carries the secret")
		}
	}
	if strings.Contains(logs.String(), secretPart) {
		t.Fatalf("logs carry the secret:\n%s", logs.String())
	}
}

func TestCreateBounds(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bad := []Spec{
		{Label: ""},
		{Label: strings.Repeat("l", LabelLengthMax+1)},
		{Label: "negative", UsesMax: -1},
		{Label: "huge", UsesMax: UsesMaxLimit + 1},
	}
	for _, spec := range bad {
		if _, _, err := store.Create(ctx, spec, "t"); err == nil {
			t.Errorf("Create accepted %+v", spec)
		}
	}
	if got := store.List(); len(got) != 0 {
		t.Fatalf("refused creates left records: %+v", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// fetch reads id's record straight from KV.
func fetch(ctx context.Context, kv jetstream.KeyValue, id string) (JoinKey, error) {
	if kv == nil {
		panic("joinkey.fetch: kv must not be nil")
	}
	entry, err := kv.Get(ctx, id)
	if err != nil {
		return JoinKey{}, err
	}
	return decode(entry)
}

// TestConsumeUseRefusesRevokedOnStaleCache stops store B's watch, so
// B's cache still shows the key live after A revokes it. Verify on B
// passes (stale), and ConsumeUse on B must still refuse, because it
// reads KV.
func TestConsumeUseRefusesRevokedOnStaleCache(t *testing.T) {
	js := newTestJetStream(t)
	storeA := openTestStore(t, js)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	storeB, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	created, plaintext := mustCreate(t, storeA, Spec{Label: "stale", UsesMax: 3})
	waitFor(t, func() bool {
		_, ok := storeB.lookup(created.ID)
		return ok
	})
	storeB.Close() // stops only the watch; B's KV handle still works
	if err := storeA.Revoke(ctx, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := storeB.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("precondition: B's cache should be stale, Verify err = %v", err)
	}
	if err := storeB.ConsumeUse(ctx, created.ID); !errors.Is(err, ErrRevoked) {
		t.Fatalf("ConsumeUse on stale store err = %v, want ErrRevoked", err)
	}
	stored, err := fetch(ctx, storeA.kv, created.ID)
	if err != nil || stored.UsesLeft != 3 {
		t.Fatalf("revoked key spent a use: %+v, %v", stored, err)
	}
}

func TestConsumeUseRefusesExpired(t *testing.T) {
	store := newTestStore(t)
	expires := time.Now().UTC().Add(time.Hour)
	created, _ := mustCreate(t, store, Spec{Label: "e", UsesMax: 2, ExpiresAt: &expires})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.ConsumeUse(ctx, created.ID); err != nil {
		t.Fatalf("ConsumeUse before expiry: %v", err)
	}
	store.now = func() time.Time { return expires }
	if err := store.ConsumeUse(ctx, created.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("ConsumeUse at expiry err = %v, want ErrExpired", err)
	}
}

func TestRefundUse(t *testing.T) {
	store := newTestStore(t)
	created, plaintext := mustCreate(t, store, Spec{Label: "refund", UsesMax: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.ConsumeUse(ctx, created.ID); err != nil {
		t.Fatalf("ConsumeUse: %v", err)
	}
	if _, err := store.Verify(plaintext, "notify"); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Verify after use err = %v, want ErrExhausted", err)
	}
	if err := store.RefundUse(ctx, created.ID); err != nil {
		t.Fatalf("RefundUse: %v", err)
	}
	if _, err := store.Verify(plaintext, "notify"); err != nil {
		t.Fatalf("Verify after refund: %v", err)
	}
	// A refund with nothing spent must not raise UsesLeft past UsesMax.
	if err := store.RefundUse(ctx, created.ID); err != nil {
		t.Fatalf("second RefundUse: %v", err)
	}
	stored, err := fetch(ctx, store.kv, created.ID)
	if err != nil || stored.UsesLeft != 1 {
		t.Fatalf("UsesLeft after double refund = %+v, %v; want 1", stored, err)
	}
}

// alwaysConflictKV loses every compare-and-set, standing in for a
// writer that keeps winning the revision race.
type alwaysConflictKV struct {
	jetstream.KeyValue
	updates int
}

func (kv *alwaysConflictKV) Update(
	_ context.Context, _ string, _ []byte, _ uint64,
) (uint64, error) {
	kv.updates++
	return 0, jetstream.ErrKeyRevisionMismatch
}

func TestMutateContentionIsTypedAndBounded(t *testing.T) {
	store := newTestStore(t)
	created, _ := mustCreate(t, store, Spec{Label: "busy", UsesMax: 3})
	store.Close() // the watch goroutine reads s.kv; stop it before swapping
	conflicting := &alwaysConflictKV{KeyValue: store.kv}
	store.kv = conflicting
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := store.ConsumeUse(ctx, created.ID)
	if !errors.Is(err, ErrContention) {
		t.Fatalf("ConsumeUse err = %v, want ErrContention", err)
	}
	if conflicting.updates != casAttemptsMax {
		t.Fatalf("Update attempts = %d, want %d", conflicting.updates, casAttemptsMax)
	}
	stored, err := fetch(ctx, conflicting.KeyValue, created.ID)
	if err != nil || stored.UsesLeft != 3 {
		t.Fatalf("contended ConsumeUse wrote: %+v, %v", stored, err)
	}
}

// TestCheck: Check vouches for a key that admitted a program, for a
// change to that program's registration. It refuses a missing, revoked
// or expired key and a name or protocol root outside the glob, but not
// an exhausted key, whose last use was the program's own.
func TestCheck(t *testing.T) {
	store := newTestStore(t)
	key, plaintext := mustCreate(t, store,
		Spec{Label: "once", NameGlob: "notify*", UsesMax: 1})
	verified, err := store.Verify(plaintext, "notify")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.ConsumeUse(ctx, verified.ID); err != nil {
		t.Fatalf("ConsumeUse: %v", err)
	}
	if err := store.Check(key.ID, "notify", []string{"notifyx.rpc"}); err != nil {
		t.Fatalf("Check on the exhausted admitting key: %v", err)
	}
	refusals := []struct {
		id, name  string
		protocols []string
		want      error
	}{
		{"nosuchid", "notify", nil, ErrNotFound},
		{key.ID, "other", nil, ErrNameNotAllowed},
		{key.ID, "notify", []string{"tool.exec"}, ErrProtocolNotAllowed},
	}
	for _, refusal := range refusals {
		err := store.Check(refusal.id, refusal.name, refusal.protocols)
		if !errors.Is(err, refusal.want) {
			t.Errorf("Check(%s, %s, %v) = %v, want %v",
				refusal.id, refusal.name, refusal.protocols, err, refusal.want)
		}
	}
	expires := time.Now().UTC().Add(time.Hour)
	expiring, _ := mustCreate(t, store, Spec{Label: "dated", ExpiresAt: &expires})
	store.now = func() time.Time { return expires }
	if err := store.Check(expiring.ID, "notify", nil); !errors.Is(err, ErrExpired) {
		t.Errorf("Check at expiry = %v, want ErrExpired", err)
	}
	if err := store.Revoke(ctx, key.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if err := store.Check(key.ID, "notify", nil); !errors.Is(err, ErrRevoked) {
		t.Errorf("Check after revoke = %v, want ErrRevoked", err)
	}
}
