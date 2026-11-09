// bridgetoken_test.go
// Bridge tokens (#885, D5): a program whose manifest sets BridgeToken
// gets an HTTP-bridge worker token scoped to its task types, delivered
// only in the reply to its confirmation, which arrives on its own
// connection. Every attach mints a fresh token and revokes the last.
// Methodology: real embedded NATS with Serve answering; registration
// goes over a _JOIN-prefixed connection and confirmation over one with
// the program's inbox prefix, as program.Run dials them. Assertions are
// on the raw reply JSON, so the wire keys are literal, and on the real
// workertoken.Store's Authorize.
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

	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

type bridgeFixture struct {
	*fixture
	tokens *workertoken.Store
	req    program.RegisterRequest
}

func newBridgeFixture(t *testing.T) *bridgeFixture {
	t.Helper()
	f := registrationOnlyFixture(t, Config{Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tokens, err := workertoken.Open(ctx, f.js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(tokens.Close)
	f.registrar.cfg.Tokens = tokens
	req := request(t, "notify", "")
	req.Manifest.BridgeToken = true
	return &bridgeFixture{fixture: f, tokens: tokens, req: req}
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
	if id := b.record(t, "notify").BridgeTokenID; id != "" {
		t.Fatalf("a non-bridge record names token %q", id)
	}
}

func TestBridgeToken_RemovingTheRecordRevokesTheToken(t *testing.T) {
	b := newBridgeFixture(t)
	now := time.Now()
	b.registrar.cfg.Now = func() time.Time { return now }
	b.registrar.cfg.PendingTTL = time.Second
	if _, err := registerOnly(t, b.fixture, b.req); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx := context.Background()
	prefixes := []string{"notify.work"}
	id, bearer, err := b.tokens.Mint(ctx, "program:notify", prefixes, nil, "programs")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	entry, record, err := b.registrar.lookupEntry(ctx, "notify")
	if err != nil {
		t.Fatalf("lookupEntry: %v", err)
	}
	record.BridgeTokenID = id
	if _, err := b.registrar.writeRecord(ctx, record, entry.Revision); err != nil {
		t.Fatalf("writeRecord: %v", err)
	}
	now = now.Add(time.Minute)
	if err := b.registrar.ExpirePending(ctx); err != nil {
		t.Fatalf("ExpirePending: %v", err)
	}
	if _, found, _ := b.registrar.lookup(ctx, "notify"); found {
		t.Fatal("the expired record is still stored")
	}
	if _, err := b.tokens.Authorize(bearer); err == nil {
		t.Fatal("a removed program's token still authorizes")
	}
}

func TestBridgeToken_RefusedWithoutATokenStore(t *testing.T) {
	f := newFixture(t, Config{Open: true})
	req := request(t, "notify", "")
	req.Manifest.BridgeToken = true
	_, err := registerOnly(t, f, req)
	var refusal *program.ErrRefused
	if !errors.As(err, &refusal) || refusal.Code != program.RefusedBridgeTokenUnavailable {
		t.Fatalf("Register err = %v, want %s", err, program.RefusedBridgeTokenUnavailable)
	}
	if _, found, _ := f.registrar.lookup(context.Background(), "notify"); found {
		t.Fatal("a refused program was stored")
	}
}

func TestBridgeToken_RunRotatesAndHandsTheHostTheBearer(t *testing.T) {
	b := newBridgeFixture(t)
	serveFixtureOn(t, b.fixture)
	opts := program.Options{
		URL: b.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "notify.seed"),
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
