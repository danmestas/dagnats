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
// one, because its anonymous caller proves only a join key.

// bridgeTokenCreatedBy is the CreatedBy an operator sees on the token.
const bridgeTokenCreatedBy = "programs"

// bridgeTokenAttemptsMax bounds the retries of the record's
// compare-and-set when another node rewrites it between read and write.
const bridgeTokenAttemptsMax = 5

// bridgeTokenCleanupTimeout bounds a revoke that must run even when the
// request's own context is already done.
const bridgeTokenCleanupTimeout = 10 * time.Second

// issueBridgeToken mints name's new bridge token, records its ID, then
// revokes the previous one, and returns the bearer ("" when the manifest
// sets no BridgeToken). The order matters: if the record write loses, the
// new token is revoked so none is left live and unreferenced; if the
// revoke of the old one fails, the record already names the new token.
func (r *Registrar) issueBridgeToken(ctx context.Context, name string) (string, error) {
	if ctx == nil || name == "" {
		panic("programs.issueBridgeToken: ctx and name must be set")
	}
	for attempt := 1; attempt <= bridgeTokenAttemptsMax; attempt++ {
		entry, record, err := r.lookupEntry(ctx, name)
		if err != nil {
			return "", err
		}
		if record.Pending {
			return "", fmt.Errorf("program %q is pending; confirm before a token", name)
		}
		if !record.Manifest.BridgeToken && record.BridgeTokenID == "" {
			return "", nil
		}
		bearer, id, err := r.mintFor(ctx, record)
		if err != nil {
			return "", err
		}
		previous := record.BridgeTokenID
		record.BridgeTokenID = id
		_, err = r.writeRecord(ctx, record, entry.Revision)
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			r.revokeToken(id, name)
			continue
		}
		if err != nil {
			r.revokeToken(id, name)
			return "", err
		}
		if err := r.revokePrevious(ctx, previous); err != nil {
			return "", fmt.Errorf("program %q: revoke its previous bridge token: %w", name, err)
		}
		return bearer, nil
	}
	return "", fmt.Errorf("issue bridge token for %q: its record changed on every one of "+
		"%d attempts", name, bridgeTokenAttemptsMax)
}

// mintFor mints record's token, or for a manifest that no longer sets
// BridgeToken mints nothing and returns an empty bearer and ID, so the
// caller clears the record's reference and revokes the stale token.
func (r *Registrar) mintFor(ctx context.Context, record Record) (bearer, id string, err error) {
	if record.Manifest.Name == "" {
		panic("programs.mintFor: record must name its program")
	}
	if !record.Manifest.BridgeToken {
		return "", "", nil
	}
	if r.cfg.Tokens == nil {
		return "", "", refused(program.RefusedBridgeTokenUnavailable,
			"this dagnats has no worker token store")
	}
	id, bearer, err = r.cfg.Tokens.Mint(ctx, "program:"+record.Manifest.Name,
		record.Manifest.TaskTypes, nil, bridgeTokenCreatedBy)
	if err != nil {
		return "", "", fmt.Errorf("mint bridge token for %q: %w", record.Manifest.Name, err)
	}
	return bearer, id, nil
}

// revokePrevious revokes the token a record named before this attach. A
// token already gone is not a failure.
func (r *Registrar) revokePrevious(ctx context.Context, id string) error {
	if id == "" || r.cfg.Tokens == nil {
		return nil
	}
	err := r.cfg.Tokens.Revoke(ctx, id)
	if errors.Is(err, workertoken.ErrTokenNotFound) {
		return nil
	}
	return err
}

// revokeToken revokes id on its own context, for a token a failed write
// or a removed record leaves behind; a failure is logged, since the
// caller's own outcome is what it needs, and the token is one the
// program was never handed.
func (r *Registrar) revokeToken(id, name string) {
	if id == "" || name == "" {
		panic("programs.revokeToken: id and name must be set")
	}
	if r.cfg.Tokens == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), bridgeTokenCleanupTimeout)
	defer cancel()
	err := r.cfg.Tokens.Revoke(ctx, id)
	if err != nil && !errors.Is(err, workertoken.ErrTokenNotFound) {
		slog.Error("programs: bridge token not revoked",
			"token_id", id, "program", name, "error", err)
	}
}
