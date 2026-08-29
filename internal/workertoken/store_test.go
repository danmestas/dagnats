// store_test.go
// Unit tests for the workertoken Store: bearer parsing strictness,
// mint/authorize round trip, revocation, and the Mint-time bounds.
// Methodology: real embedded NATS (workertoken.Store is a thin KV
// wrapper — there is no pure-Go seam worth faking), one Store per test,
// bounded context timeouts on every call.
package workertoken

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
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
	store, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestAuthorizeParseStrictness(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, bearer, err := store.Mint(ctx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	cases := []struct {
		name   string
		bearer string
	}{
		{"missing prefix", strings.TrimPrefix(bearer, "dgn_")},
		{"wrong segment count", "dgn_onlyoneseg"},
		{"empty secret", bearer[:strings.LastIndex(bearer, "_")+1]},
		{"garbage", "not-a-bearer-at-all"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Positive: malformed bearers are rejected.
			if _, err := store.Authorize(tc.bearer); err == nil {
				t.Fatalf("Authorize(%q) = nil error, want error", tc.bearer)
			}
		})
	}
	// Negative: the well-formed bearer from Mint must still authorize,
	// proving the malformed cases above failed on shape, not on state.
	if _, err := store.Authorize(bearer); err != nil {
		t.Fatalf("Authorize(valid bearer) = %v, want nil", err)
	}
}

func TestMintAuthorizeRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, bearer, err := store.Mint(ctx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	claims, err := store.Authorize(bearer)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Positive: claims carry the minted token's identity and scope.
	if claims.TokenID != id {
		t.Fatalf("TokenID = %q, want %q", claims.TokenID, id)
	}
	if claims.Admin {
		t.Fatalf("claims.Admin = true, want false for a minted token")
	}
	// Negative: a minted worker token is never treated as admin.
	if len(claims.TaskTypePrefixes) != 1 || claims.TaskTypePrefixes[0] != "echo" {
		t.Fatalf("TaskTypePrefixes = %v, want [echo]", claims.TaskTypePrefixes)
	}
}

func TestAuthorizeWrongSecretSameErrorAsUnknownID(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, bearer, err := store.Mint(ctx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	wrongSecret := "dgn_" + id + "_totallywrongsecretvalue"
	unknownID := "dgn_0000000000000000000000000000ffff_" +
		strings.SplitN(bearer, "_", 3)[2]

	_, wrongSecretErr := store.Authorize(wrongSecret)
	_, unknownIDErr := store.Authorize(unknownID)
	// Positive: both are rejected.
	if wrongSecretErr == nil || unknownIDErr == nil {
		t.Fatalf("expected both to error: wrongSecret=%v unknownID=%v",
			wrongSecretErr, unknownIDErr)
	}
	// Negative: the error text must not distinguish the two cases —
	// otherwise an attacker can enumerate valid token IDs.
	if wrongSecretErr.Error() != unknownIDErr.Error() {
		t.Fatalf("error texts differ: wrongSecret=%q unknownID=%q",
			wrongSecretErr.Error(), unknownIDErr.Error())
	}
}

func TestRevokedTokenRejected(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, bearer, err := store.Mint(ctx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if _, err := store.Authorize(bearer); err != nil {
		t.Fatalf("Authorize before revoke: %v", err)
	}
	if err := store.Revoke(ctx, id); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	// Positive: a revoked token is rejected.
	if _, err := store.Authorize(bearer); err == nil {
		t.Fatalf("Authorize(revoked) = nil error, want error")
	}
	// Negative: Revoke on an unknown ID errors rather than silently
	// succeeding.
	if err := store.Revoke(ctx, "does-not-exist"); err == nil {
		t.Fatalf("Revoke(unknown) = nil error, want error")
	}
}

func TestMintPrefixBounds(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tooMany := make([]string, PrefixesCountMax+1)
	for i := range tooMany {
		tooMany[i] = "p"
	}
	// Positive: exceeding PrefixesCountMax is rejected.
	if _, _, err := store.Mint(ctx, "worker-a", tooMany, "tester"); err == nil {
		t.Fatalf("Mint with %d prefixes = nil error, want error", len(tooMany))
	}

	tooLong := strings.Repeat("p", PrefixLengthMax+1)
	// Negative-of-that: within-bounds count but an over-long single
	// prefix is rejected too.
	if _, _, err := store.Mint(
		ctx, "worker-a", []string{tooLong}, "tester",
	); err == nil {
		t.Fatalf("Mint with over-long prefix = nil error, want error")
	}
}

// TestMintRejectsInvalidPrefixCharset pins the fix for mintable
// prefixes that can never match anything: validateTaskType already
// rejects '*', '>', whitespace, and leading/trailing '.' bytes on
// poll's task_type entries, so a prefix using any of those could never
// be satisfied by a real poll -- minting one is an operator error
// worth a 400 rather than a silently dead scope.
func TestMintRejectsInvalidPrefixCharset(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cases := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{"wildcard star", "build*", true},
		{"wildcard gt", "build>", true},
		{"whitespace", "build deploy", true},
		{"leading dot", ".build", true},
		{"trailing dot", "build.", true},
		{"valid segment", "build", false},
		{"valid multi-segment", "build.deploy", false},
		{"valid with dash and underscore", "build-a_b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := store.Mint(ctx, "worker", []string{tc.prefix}, "tester")
			// Positive: invalid-charset prefixes are rejected.
			if tc.wantErr && err == nil {
				t.Fatalf("Mint(%q) = nil error, want error", tc.prefix)
			}
			// Negative: valid prefixes are still accepted.
			if !tc.wantErr && err != nil {
				t.Fatalf("Mint(%q) = %v, want nil error", tc.prefix, err)
			}
		})
	}
}

func TestMintEmptyPrefixesMeansNoTaskTypes(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, bearer, err := store.Mint(ctx, "worker-a", nil, "tester")
	if err != nil {
		t.Fatalf("Mint with empty prefixes: %v", err)
	}
	claims, err := store.Authorize(bearer)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	// Positive: no prefixes at all authorizes nothing.
	if claims.AllowsTaskType("anything") {
		t.Fatalf("AllowsTaskType(anything) = true, want false for empty prefixes")
	}
	// Negative: this is not the same as "no scoping" — Admin is still
	// false, so a caller cannot mistake it for the unscoped admin path.
	if claims.Admin {
		t.Fatalf("claims.Admin = true, want false")
	}
}

func TestMintTokensCountMaxEnforced(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TokensCountMax exhaustion test in -short mode")
	}
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for i := 0; i < TokensCountMax; i++ {
		if _, _, err := store.Mint(
			ctx, "worker", []string{"t"}, "tester",
		); err != nil {
			t.Fatalf("Mint %d: %v", i, err)
		}
	}
	// Positive: the mint one past the cap is refused.
	if _, _, err := store.Mint(
		ctx, "one-too-many", []string{"t"}, "tester",
	); err == nil {
		t.Fatalf("Mint at cap+1 = nil error, want error")
	}
	// Negative: the store still holds exactly TokensCountMax tokens,
	// not a partial cap+1'th record.
	toks, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(toks) != TokensCountMax {
		t.Fatalf("len(List()) = %d, want %d", len(toks), TokensCountMax)
	}
}

func TestListNeverContainsSecretHash(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := store.Mint(ctx, "worker-a", []string{"echo"}, "tester"); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	toks, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Positive: at least the one minted token comes back.
	if len(toks) != 1 {
		t.Fatalf("len(List()) = %d, want 1", len(toks))
	}
	// Negative: SecretHash must never leave the store via List.
	if toks[0].SecretHash != nil {
		t.Fatalf("List()[0].SecretHash = %v, want nil", toks[0].SecretHash)
	}
}

// TestAuthorizeEmptyBearerReturnsError pins the fix for a reachable
// panic: a non-HTTP caller (or a future transport) can hand Authorize
// an empty string, and that must be an ordinary error, not a crash.
// The nil-receiver / uninitialized-store panic stays a panic --
// that's a genuine programmer error, not caller input.
func TestAuthorizeEmptyBearerReturnsError(t *testing.T) {
	store := newTestStore(t)
	// Positive: empty bearer returns an error, does not panic.
	_, err := store.Authorize("")
	if err == nil {
		t.Fatal("Authorize(\"\") = nil error, want error")
	}
	// Negative: it's the same opaque error as any other malformed
	// bearer, not a distinct "empty" message that would leak shape.
	_, garbageErr := store.Authorize("not-a-bearer")
	if err.Error() != garbageErr.Error() {
		t.Fatalf("error texts differ: empty=%q garbage=%q",
			err.Error(), garbageErr.Error())
	}
}

// TestRevokeFallsBackToKVOnCacheMiss pins the fix for watch-lag false
// negatives: if a token was just minted by another Store instance and
// this Store's watch hasn't caught up yet, Revoke must not report 404
// without first checking the KV bucket directly.
func TestRevokeFallsBackToKVOnCacheMiss(t *testing.T) {
	storeA := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, _, err := storeA.Mint(ctx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	// storeA's own cache has the id immediately (Mint updates it
	// synchronously) -- Revoke on the SAME instance already covers the
	// warm-cache path. To exercise the cold-cache path, clear the
	// local cache entry directly, simulating a Store whose watch has
	// not yet replayed this id.
	storeA.mu.Lock()
	delete(storeA.tokens, id)
	storeA.mu.Unlock()

	// Positive: Revoke still succeeds via a KV fallback read, not a
	// false 404 from the empty cache.
	if err := storeA.Revoke(ctx, id); err != nil {
		t.Fatalf("Revoke after cache eviction: %v", err)
	}
	// Negative: revoking a truly unknown id (absent from KV too)
	// still 404s.
	if err := storeA.Revoke(ctx, "genuinely-unknown-id"); err == nil {
		t.Fatal("Revoke(unknown) = nil error, want error")
	}
}

// TestRevokedRecordsAreBounded pins the fix for unbounded bucket
// growth: revoked tokens are audit records, but a long-running
// mint/revoke loop must not grow the bucket forever. Once revoked
// records exceed TokensCountMax, the oldest (by RevokedAt) are pruned.
func TestRevokedRecordsAreBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping revoked-records bound test in -short mode")
	}
	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	ids := make([]string, 0, TokensCountMax+5)
	for i := 0; i < TokensCountMax+5; i++ {
		id, _, err := store.Mint(ctx, "worker", []string{"t"}, "tester")
		if err != nil {
			t.Fatalf("Mint %d: %v", i, err)
		}
		if err := store.Revoke(ctx, id); err != nil {
			t.Fatalf("Revoke %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	toks, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Positive: revoked records never exceed TokensCountMax.
	if len(toks) > TokensCountMax {
		t.Fatalf("len(List()) = %d, want <= %d", len(toks), TokensCountMax)
	}
	// Negative: the earliest-revoked ids are the ones pruned, not an
	// arbitrary subset.
	if len(toks) == TokensCountMax {
		found := false
		for _, tok := range toks {
			if tok.ID == ids[0] {
				found = true
			}
		}
		if found {
			t.Fatal("earliest-revoked id survived pruning, want it evicted")
		}
	}
}
