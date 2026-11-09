package programs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// Bridge tokens (#885, D5). A program whose manifest sets BridgeToken is
// minted an HTTP-bridge worker token scoped to its task types, so a
// bridge worker has one identity across NATS and HTTP. Every confirmed
// attach mints a new token and revokes the previous one: nothing is kept
// on the program's side, a restart rotates the token, and there is one
// live token per program (a second instance revokes the first's).
//
// The bearer is returned only to the caller of Confirm, whose connection
// logged in with the program's key. It is never stored: the record keeps
// the token's ID, which is enough to revoke it. Register never issues
// one, because its anonymous caller proves only a join key. In open mode
// anonymous clients are root and can publish any confirm subject, so a
// confirm proves nothing there and a BridgeToken manifest is refused.

// bridgeTokenCreatedBy is the CreatedBy an operator sees on the token.
const bridgeTokenCreatedBy = "programs"

// bridgeTokenAttemptsMax bounds the retries of the record's
// compare-and-set when another node rewrites it between read and write.
const bridgeTokenAttemptsMax = 5

// bridgeTokenCleanupTimeout bounds a revoke that must run even when the
// request's own context is already done.
const bridgeTokenCleanupTimeout = 10 * time.Second

// tokenMinter is the part of workertoken.Store the registrar uses; tests
// replace it to fail a mint or revoke.
type tokenMinter interface {
	Mint(ctx context.Context, label string, prefixes, workerGroups []string,
		createdBy string) (id, bearer string, err error)
	Revoke(ctx context.Context, id string) error
}

// checkBridgeToken refuses a BridgeToken manifest this server cannot
// safely serve: with no token store to mint from, or in open mode.
func (r *Registrar) checkBridgeToken(m program.Manifest) error {
	if m.Name == "" {
		panic("programs.checkBridgeToken: manifest must name its program")
	}
	if r == nil {
		panic("programs.checkBridgeToken: r is nil")
	}
	if !m.BridgeToken {
		return nil
	}
	if r.tokens == nil {
		return refused(program.RefusedBridgeTokenUnavailable,
			"program %q asks for a bridge token and this dagnats has no worker token store",
			m.Name)
	}
	if r.cfg.Open {
		return refused(program.RefusedBridgeTokenUnavailable,
			"program %q asks for a bridge token, but this dagnats needs no client login "+
				"(open mode), so a confirmation does not prove the program holds its key",
			m.Name)
	}
	return nil
}

// issueBridgeToken rotates m's program's bridge token and returns the new
// bearer ("" when the manifest sets no BridgeToken). m is the manifest
// Confirm has just matched against the record, so the scope is the
// validated request's, not the stored record's.
//
// The invariant is that every live token is one its record names. The
// order keeps it through any failure: mint the new token N; revoke the
// previous P (already gone counts as revoked); only then write the record
// to name N. If the revoke fails, N is revoked and the record still names
// P, live. If the write fails, even by a timeout that in fact committed,
// N is revoked, and P is already revoked, so at worst the record names a
// dead token and the next attach's revoke of it is a no-op.
func (r *Registrar) issueBridgeToken(
	ctx context.Context, m program.Manifest,
) (string, error) {
	if ctx == nil || m.Name == "" {
		panic("programs.issueBridgeToken: ctx and the manifest's name must be set")
	}
	for attempt := 1; attempt <= bridgeTokenAttemptsMax; attempt++ {
		bearer, err := r.rotateBridgeToken(ctx, m)
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		return bearer, err
	}
	return "", fmt.Errorf("issue bridge token for %q: its record changed on every one of "+
		"%d attempts", m.Name, bridgeTokenAttemptsMax)
}

// rotateBridgeToken is one attempt of issueBridgeToken. A returned
// jetstream.ErrKeyRevisionMismatch means the record changed since it was
// read; the new token is already revoked and the caller reads again.
func (r *Registrar) rotateBridgeToken(
	ctx context.Context, m program.Manifest,
) (bearer string, err error) {
	entry, record, err := r.lookupEntry(ctx, m.Name)
	if err != nil {
		return "", err
	}
	if record.Pending {
		return "", fmt.Errorf("program %q is pending; confirm before a token", m.Name)
	}
	if !m.BridgeToken && record.BridgeTokenID == "" {
		return "", nil
	}
	id, bearer, err := r.mint(ctx, m)
	if err != nil {
		return "", err
	}
	if err := r.revokePrevious(ctx, record.BridgeTokenID); err != nil {
		r.revokeToken(id, m.Name)
		return "", fmt.Errorf("program %q: revoke its previous bridge token: %w", m.Name, err)
	}
	record.BridgeTokenID = id
	write := r.writeRecord
	if r.writeBridgeRecord != nil {
		write = r.writeBridgeRecord
	}
	if _, err := write(ctx, record, entry.Revision); err != nil {
		r.revokeToken(id, m.Name)
		return "", err
	}
	return bearer, nil
}

// mint mints m's token, scoped to its TaskTypes exactly as declared; it
// returns nothing for a manifest that sets no BridgeToken.
func (r *Registrar) mint(ctx context.Context, m program.Manifest) (id, bearer string, err error) {
	if r.tokens == nil {
		panic("programs.mint: no token store; checkBridgeToken refuses before this")
	}
	if m.BridgeToken && len(m.TaskTypes) == 0 {
		panic("programs.mint: Validate requires task types for a bridge token")
	}
	if !m.BridgeToken {
		return "", "", nil
	}
	id, bearer, err = r.tokens.Mint(ctx, "program:"+m.Name, m.TaskTypes, nil,
		bridgeTokenCreatedBy)
	if err != nil {
		return "", "", fmt.Errorf("mint bridge token for %q: %w", m.Name, err)
	}
	return id, bearer, nil
}

// revokePrevious revokes the token a record named before this attach. A
// token already gone is not a failure.
func (r *Registrar) revokePrevious(ctx context.Context, id string) error {
	if r == nil || ctx == nil {
		panic("programs.revokePrevious: r and ctx must be set")
	}
	if r.tokens == nil {
		panic("programs.revokePrevious: no token store")
	}
	if id == "" {
		return nil
	}
	err := r.tokens.Revoke(ctx, id)
	if errors.Is(err, workertoken.ErrTokenNotFound) {
		return nil
	}
	return err
}

// revokeToken revokes id on its own context, for the new token a failed
// step leaves behind; a failure is logged, since the caller's own error
// is what it needs and the program was never handed this token. An empty
// id (a manifest that mints nothing) is a no-op.
func (r *Registrar) revokeToken(id, name string) {
	if r == nil || name == "" {
		panic("programs.revokeToken: r and name must be set")
	}
	if r.tokens == nil {
		panic("programs.revokeToken: no token store")
	}
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), bridgeTokenCleanupTimeout)
	defer cancel()
	err := r.tokens.Revoke(ctx, id)
	if err != nil && !errors.Is(err, workertoken.ErrTokenNotFound) {
		slog.Error("programs: bridge token not revoked",
			"token_id", id, "program", name, "error", err)
	}
}
