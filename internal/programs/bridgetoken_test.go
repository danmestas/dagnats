// bridgetoken_test.go
// Bridge tokens (#885, D5): a program whose manifest sets BridgeToken
// gets an HTTP-bridge worker token scoped to its task types, delivered
// only in the reply to its confirmation, which arrives on its own
// connection. Every attach mints a fresh token and revokes the last, and
// every live token is the one its record names.
// Methodology: real embedded NATS with Serve answering; registration
// goes over a _JOIN-prefixed connection and confirmation over one with
// the program's inbox prefix, as program.Run dials them. Assertions are
// on the raw reply JSON, so the wire keys are literal, and on the real
// workertoken.Store's Authorize. Failure branches inject a failing
// Revoke or record write through the registrar's seams and then check
// which of every minted bearer still authorizes.
package programs

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// recordingTokens is the real store with every minted token noted and a
// Revoke that can be made to fail.
type recordingTokens struct {
	*workertoken.Store
	minted    []mintedToken
	revokeErr func(id string) error
}

type mintedToken struct{ id, bearer string }

func (r *recordingTokens) Mint(
	ctx context.Context, label string, prefixes, groups []string, createdBy string,
) (string, string, error) {
	id, bearer, err := r.Store.Mint(ctx, label, prefixes, groups, createdBy)
	if err == nil {
		r.minted = append(r.minted, mintedToken{id, bearer})
	}
	return id, bearer, err
}

func (r *recordingTokens) Revoke(ctx context.Context, id string) error {
	if r.revokeErr != nil {
		if err := r.revokeErr(id); err != nil {
			return err
		}
	}
	return r.Store.Revoke(ctx, id)
}

type bridgeFixture struct {
	*fixture
	tokens  *recordingTokens
	req     program.RegisterRequest
	keyText string
}

func newBridgeFixture(t *testing.T) *bridgeFixture {
	t.Helper()
	f := registrationOnlyFixture(t, Config{Open: false})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := workertoken.Open(ctx, f.js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(store.Close)
	tokens := &recordingTokens{Store: store}
	f.registrar.tokens = tokens
	_, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 100})
	req := request(t, "notify", plaintext)
	req.Manifest.BridgeToken = true
	return &bridgeFixture{fixture: f, tokens: tokens, req: req, keyText: plaintext}
}

// ask sends req to subject on a connection with inboxPrefix and
// returns the raw reply.
func (b *bridgeFixture) ask(
	t *testing.T, inboxPrefix, subject string, req program.RegisterRequest,
) []byte {
	t.Helper()
	nc, err := nats.Connect(b.nc.ConnectedUrl(), nats.CustomInboxPrefix(inboxPrefix))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request(subject, body, 10*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}
	return msg.Data
}

func (b *bridgeFixture) registerRaw(t *testing.T) []byte {
	t.Helper()
	return b.ask(t, program.JoinInboxPrefix, program.RegisterSubject, b.req)
}

func (b *bridgeFixture) confirmRaw(t *testing.T) []byte {
	t.Helper()
	return b.ask(t, program.InboxPrefix(b.req.Manifest.Name),
		program.ConfirmSubject(b.req.PublicKey), b.req)
}

func (b *bridgeFixture) confirmReply(t *testing.T) program.RegisterReply {
	t.Helper()
	raw := b.confirmRaw(t)
	var reply program.RegisterReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("decode confirm reply %s: %v", raw, err)
	}
	if reply.Grant == nil || reply.Refused != nil || reply.Error != "" {
		t.Fatalf("confirm reply %s: want a grant", raw)
	}
	return reply
}

// attach registers and confirms once, directly, and returns the first
// token's ID, so a test can then break the second attach.
func (b *bridgeFixture) attach(t *testing.T) string {
	t.Helper()
	b.registerRaw(t)
	b.confirmReply(t) // the first attach mints
	return b.record(t, "notify").BridgeTokenID
}

func (b *bridgeFixture) reattach(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := b.registrar.issueBridgeToken(ctx, b.req.Manifest)
	return err
}

// requireOnlyNamedLive fails unless every minted bearer except the one
// the record names is rejected; named says whether the named one must
// authorize (false when its token was revoked too).
func (b *bridgeFixture) requireOnlyNamedLive(t *testing.T, named bool) {
	t.Helper()
	id := b.record(t, "notify").BridgeTokenID
	if len(b.tokens.minted) == 0 {
		t.Fatal("no token was minted")
	}
	for _, minted := range b.tokens.minted {
		_, err := b.tokens.Authorize(minted.bearer)
		live := err == nil
		switch {
		case minted.id != id && live:
			t.Fatalf("token %s is live but its record names %q", minted.id, id)
		case minted.id == id && live != named:
			t.Fatalf("the token the record names (%s): live=%v, want %v", id, live, named)
		}
	}
}

func TestBridgeToken_ConfirmReplyCarriesScopedBearer(t *testing.T) {
	b := newBridgeFixture(t)
	if raw := b.registerRaw(t); strings.Contains(string(raw), "bridge_token") {
		t.Fatalf("a register reply carries a token: %s", raw)
	}
	reply := b.confirmReply(t)
	claims, err := b.tokens.Authorize(reply.BridgeToken)
	if err != nil {
		t.Fatalf("Authorize(confirm bearer): %v", err)
	}
	if !slices.Equal(claims.TaskTypePrefixes, b.req.Manifest.TaskTypes) {
		t.Fatalf("prefixes %v, want %v", claims.TaskTypePrefixes, b.req.Manifest.TaskTypes)
	}
	if !claims.AllowsTaskType("notify.work") || claims.AllowsTaskType("other.work") {
		t.Fatal("token must allow the program's task type and nothing else")
	}
	record := b.record(t, "notify")
	if record.BridgeTokenID != claims.TokenID {
		t.Fatalf("record names token %q, want %q", record.BridgeTokenID, claims.TokenID)
	}
	stored, err := json.Marshal(record)
	if err != nil || strings.Contains(string(stored), reply.BridgeToken) {
		t.Fatalf("the bearer must never be stored (err %v)", err)
	}
}

func TestBridgeToken_EveryAttachRotatesTheToken(t *testing.T) {
	b := newBridgeFixture(t)
	b.registerRaw(t)
	first := b.confirmReply(t).BridgeToken
	second := b.confirmReply(t).BridgeToken
	if first == "" || second == "" || first == second {
		t.Fatalf("want two distinct bearers, got %q and %q", first, second)
	}
	if _, err := b.tokens.Authorize(first); err == nil {
		t.Fatal("the previous attach's token still authorizes")
	}
	if _, err := b.tokens.Authorize(second); err != nil {
		t.Fatalf("the current token does not authorize: %v", err)
	}
}

func TestBridgeToken_RegisterReplyNeverCarriesOne(t *testing.T) {
	b := newBridgeFixture(t)
	first := b.registerRaw(t)
	b.confirmReply(t)
	again := b.registerRaw(t)
	for _, raw := range [][]byte{first, again} {
		if strings.Contains(string(raw), "bridge_token") {
			t.Fatalf("a register reply carries a token: %s", raw)
		}
	}
	var reply program.RegisterReply
	if err := json.Unmarshal(again, &reply); err != nil || reply.Grant == nil {
		t.Fatalf("a confirmed program's register reply %s (%v) should hold its grant", again, err)
	}
}

func TestBridgeToken_NonBridgeProgramGetsNone(t *testing.T) {
	b := newBridgeFixture(t)
	b.req.Manifest.BridgeToken = false
	b.registerRaw(t)
	raw := b.confirmRaw(t)
	if !strings.Contains(string(raw), `"grant"`) || strings.Contains(string(raw), "bridge_token") {
		t.Fatalf("a non-bridge confirm reply should hold a grant and no token: %s", raw)
	}
	if id := b.record(t, "notify").BridgeTokenID; id != "" || len(b.tokens.minted) != 0 {
		t.Fatalf("a non-bridge program has token %q, %d minted", id, len(b.tokens.minted))
	}
}

func TestBridgeToken_RefusedWithoutATokenStore(t *testing.T) {
	f := newFixture(t, Config{Open: false})
	req := request(t, "notify", "")
	req.Manifest.BridgeToken = true
	requireBridgeRefusal(t, f, req)
	if _, found, _ := f.registrar.lookup(context.Background(), "notify"); found {
		t.Fatal("a refused program was stored")
	}
}

// In open mode anonymous clients are root and may publish any confirm
// subject, so a confirm proves no possession of the key and a token
// would go to whoever asked.
func TestBridgeToken_RefusedInOpenMode(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	store, err := workertoken.Open(context.Background(), f.js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(store.Close)
	f.registrar.tokens = store
	req := request(t, "notify", "")
	req.Manifest.BridgeToken = true
	requireBridgeRefusal(t, f, req)
	req.Manifest.BridgeToken = false
	if _, err := registerOnly(t, f, req); err != nil {
		t.Fatalf("a program without a bridge token registers in open mode: %v", err)
	}
}

func requireBridgeRefusal(t *testing.T, f *fixture, req program.RegisterRequest) {
	t.Helper()
	_, err := registerOnly(t, f, req)
	var refusal *program.ErrRefused
	if !errors.As(err, &refusal) || refusal.Code != program.RefusedBridgeTokenUnavailable {
		t.Fatalf("Register err = %v, want %s", err, program.RefusedBridgeTokenUnavailable)
	}
}

func TestBridgeToken_FailedRevokeOfThePreviousKeepsItNamedAndLive(t *testing.T) {
	b := newBridgeFixture(t)
	previous := b.attach(t)
	b.tokens.revokeErr = func(id string) error {
		if id == previous {
			return errors.New("injected revoke failure")
		}
		return nil
	}
	if err := b.reattach(t); err == nil {
		t.Fatal("a failed revoke of the previous token was not reported")
	}
	if id := b.record(t, "notify").BridgeTokenID; id != previous {
		t.Fatalf("record names %q, want the still-live previous %q", id, previous)
	}
	b.requireOnlyNamedLive(t, true)
}

func TestBridgeToken_FailedRecordWriteLeavesNoLiveToken(t *testing.T) {
	cases := map[string]error{
		"timeout":  context.DeadlineExceeded,
		"mismatch": jetstream.ErrKeyRevisionMismatch,
	}
	for name, writeErr := range cases {
		t.Run(name, func(t *testing.T) {
			b := newBridgeFixture(t)
			previous := b.attach(t)
			b.registrar.writeBridgeRecord = func(
				context.Context, Record, uint64,
			) (uint64, error) {
				return 0, writeErr
			}
			if err := b.reattach(t); err == nil {
				t.Fatal("a failed record write was not reported")
			}
			if id := b.record(t, "notify").BridgeTokenID; id != previous {
				t.Fatalf("record names %q, want %q", id, previous)
			}
			// The previous token was revoked first and the new one after the
			// lost write, so nothing is live.
			b.requireOnlyNamedLive(t, false)
			if len(b.tokens.minted) < 2 {
				t.Fatalf("%d tokens minted, want the new one too", len(b.tokens.minted))
			}
		})
	}
}

func TestBridgeToken_TurningItOffRevokesAndClears(t *testing.T) {
	b := newBridgeFixture(t)
	previous := b.attach(t)
	bearer := b.tokens.minted[0].bearer
	b.req.Manifest.BridgeToken = false
	raw := b.confirmRaw(t)
	if strings.Contains(string(raw), "bridge_token") || !strings.Contains(string(raw), `"grant"`) {
		t.Fatalf("a confirm that turns the token off: %s", raw)
	}
	if _, err := b.tokens.Authorize(bearer); err == nil {
		t.Fatalf("token %s still authorizes after the manifest dropped BridgeToken", previous)
	}
	if id := b.record(t, "notify").BridgeTokenID; id != "" {
		t.Fatalf("record still names token %q", id)
	}
}

func TestBridgeToken_LimitsMatchTheTokenStore(t *testing.T) {
	if program.BridgeTokenTaskTypesMax != workertoken.PrefixesCountMax {
		t.Fatalf("program allows %d task types, workertoken %d",
			program.BridgeTokenTaskTypesMax, workertoken.PrefixesCountMax)
	}
	if program.BridgeTokenTaskTypeBytesMax != workertoken.PrefixLengthMax {
		t.Fatalf("program allows %d bytes, workertoken %d",
			program.BridgeTokenTaskTypeBytesMax, workertoken.PrefixLengthMax)
	}
}

func TestBridgeToken_RunRotatesAndHandsTheHostTheBearer(t *testing.T) {
	b := newBridgeFixture(t)
	serveFixtureOn(t, b.fixture)
	opts := program.Options{
		URL: b.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "notify.seed"),
		JoinKey: b.keyText,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var bearers []string
	for run := 0; run < 2; run++ {
		prog := &countingProgram{manifest: b.req.Manifest}
		running, err := program.Run(ctx, prog, opts)
		if err != nil {
			t.Fatalf("Run %d: %v", run, err)
		}
		bearers = append(bearers, prog.bridgeToken.Load().(string))
		if err := running.Stop(ctx); err != nil {
			t.Fatalf("Stop %d: %v", run, err)
		}
	}
	if _, err := b.tokens.Authorize(bearers[0]); err == nil {
		t.Fatal("a restart left the earlier token live")
	}
	claims, err := b.tokens.Authorize(bearers[1])
	if err != nil || !claims.AllowsTaskType("notify.work") {
		t.Fatalf("the restarted program's token: claims %v, err %v", claims, err)
	}
}
