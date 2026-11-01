package programs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// Proof of possession (#885). A program registers over an anonymous
// connection, so a request only claims a public key. dagnats does not
// trust the claim until the program logs in with the key, which is
// NATS's own NKey challenge (the server signs a nonce check against the
// public key), and confirms on api.programs.confirm.<its key>: a subject
// only that key's NATS user may publish.
//
//   - A new registration is stored pending for PendingTTL, provisions
//     nothing, and its NATS user may publish nothing but its confirm
//     subject, so a squatter who knows another program's public key
//     reserves nothing it can use; its reservation expires, and a join key
//     cannot re-register the same name for a PendingTTL after that.
//   - A manifest change of a registered program is not applied at
//     register; the reply asks for confirmation, and the update runs on
//     confirm, so knowing a program's public key is not enough to widen
//     or re-provision it.
//
// In open mode anonymous NATS clients are root and can publish any
// confirm subject; open mode trusts every client, so this proves
// nothing there.

// PendingTTL is how long a registration waits for its confirmation
// before it is removed and its join-key use refunded (Config.PendingTTL
// overrides it).
const PendingTTL = 60 * time.Second

func (r *Registrar) pendingTTL() time.Duration {
	if r.cfg.PendingTTL > 0 {
		return r.cfg.PendingTTL
	}
	return PendingTTL
}

// expirySweepInterval paces RunExpiry; a pending record lives at most
// PendingTTL plus this.
const expirySweepInterval = 10 * time.Second

// errConfirmRequired: the request changes a registered program's
// manifest; the program must confirm with its key to apply it.
var errConfirmRequired = errors.New("program change must be confirmed with its key")

// UserPermissions is the NATS permission set a record's NKey user gets:
// for a pending registration only its confirm subject (and replies on
// its own inbox), and for a confirmed one PermissionsFor its manifest
// plus its confirm subject, so it can confirm a later manifest change.
func UserPermissions(record Record) program.Permissions {
	if record.PublicKey == "" || record.Manifest.Name == "" {
		panic("programs.UserPermissions: record must name its key and program")
	}
	confirm := program.ConfirmSubject(record.PublicKey)
	inbox := program.InboxPrefix(record.Manifest.Name) + ".>"
	if record.Pending {
		return program.Permissions{
			Publish:   program.SubjectPermission{Allow: []string{confirm}},
			Subscribe: program.SubjectPermission{Allow: []string{inbox}},
		}
	}
	perms := record.Permissions
	perms.Publish.Allow = append(append([]string(nil), perms.Publish.Allow...), confirm)
	return perms
}

// expired reports a pending record past its window.
func (r Record) expired(now time.Time) bool {
	return r.Pending && !now.Before(r.PendingUntil)
}

// Confirm completes a registration for the program whose key is
// publicKey, on behalf of a connection that logged in with that key. It
// promotes a pending record to its full permissions, or applies a
// manifest change (update.go), or, for an unchanged confirmed program,
// returns its Grant.
func (r *Registrar) Confirm(
	ctx context.Context, publicKey string, req program.RegisterRequest,
) (program.Grant, error) {
	if ctx == nil || publicKey == "" {
		panic("programs.Confirm: ctx and publicKey must be set")
	}
	if req.PublicKey != publicKey {
		return program.Grant{}, refused(program.RefusedRequestInvalid,
			"the request's public key is not the one this connection proved")
	}
	if err := r.checkRequest(req); err != nil {
		return program.Grant{}, err
	}
	entry, record, err := r.lookupEntry(ctx, req.Manifest.Name)
	if err != nil {
		return program.Grant{}, err
	}
	if record.PublicKey != publicKey {
		return program.Grant{}, refused(program.RefusedNameTaken,
			"program %q is taken", req.Manifest.Name)
	}
	if record.Pending {
		return r.promote(ctx, entry.Revision(), record, req)
	}
	if sameManifest(record.Manifest, req.Manifest) {
		return record.Grant, nil
	}
	grant, err := r.update(ctx, req)
	if err != nil {
		return program.Grant{}, err
	}
	return grant, r.bindStored(ctx, req.Manifest.Name)
}

// promote turns a pending record into a registered one at revision and
// makes its full NATS user live on this node.
func (r *Registrar) promote(
	ctx context.Context, revision uint64, record Record, req program.RegisterRequest,
) (program.Grant, error) {
	if !record.Pending || revision == 0 {
		panic("programs.promote: only a pending record at a read revision")
	}
	if record.expired(time.Now()) {
		return program.Grant{}, refused(program.RefusedConfirmExpired,
			"program %q was not confirmed within %s; register again",
			record.Manifest.Name, r.pendingTTL())
	}
	if !sameManifest(record.Manifest, req.Manifest) {
		return program.Grant{}, refused(program.RefusedManifestChanged,
			"program %q is pending with a different manifest; confirm that one "+
				"or wait %s for it to expire", record.Manifest.Name, r.pendingTTL())
	}
	// Provisioned only now, by a holder of the key: a pending record owns
	// no streams or buckets, so one that expires leaks none.
	lock := r.programLock(record.Manifest.Name)
	lock.Lock()
	defer lock.Unlock()
	before, err := r.provision(ctx, record.Manifest, record.Grant)
	if err != nil {
		// Undo the registration at once, refunding its use, so the program
		// can register again rather than wait out the pending window.
		cause := fmt.Errorf("provision program %q: %w", record.Manifest.Name, err)
		err = r.rollBack(record.Manifest.Name, revision, before, cause)
		if record.JoinKeyID != "" {
			r.refund(record.JoinKeyID, record.Manifest.Name)
		}
		return program.Grant{}, err
	}
	record.Pending, record.PendingUntil = false, time.Time{}
	if _, err := r.writeRecord(ctx, record, revision); err != nil {
		return program.Grant{}, errors.Join(err, r.restoreConfigs(before))
	}
	if r.cfg.Bind != nil {
		if err := r.cfg.Bind(record); err != nil {
			return program.Grant{}, fmt.Errorf("bind program %q's NATS user: %w",
				record.Manifest.Name, err)
		}
	}
	return record.Grant, nil
}

// bindStored makes the stored record's NATS user live on this node,
// after an update rewrote it.
func (r *Registrar) bindStored(ctx context.Context, name string) error {
	if r.cfg.Bind == nil {
		return nil
	}
	record, found, err := r.lookup(ctx, name)
	if err != nil || !found {
		return errors.Join(err, fmt.Errorf("program %q: no record to bind", name))
	}
	return r.cfg.Bind(record)
}

// expireIfStale removes a pending record past its window, at the
// revision read, and refunds its join-key use. It reports whether the
// name is free now.
func (r *Registrar) expireIfStale(ctx context.Context, name string) (bool, error) {
	if name == "" {
		panic("programs.expireIfStale: name must not be empty")
	}
	entry, record, err := r.lookupEntry(ctx, name)
	if err != nil || !record.expired(time.Now()) {
		return false, nil
	}
	err = r.kv.Delete(ctx, name, jetstream.LastRevision(entry.Revision()))
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("expire program %q: %w", name, err)
	}
	if record.JoinKeyID != "" {
		r.refund(record.JoinKeyID, name)
		r.expired.note(record.JoinKeyID, name)
	}
	slog.Info("programs: unconfirmed registration expired",
		"program", name, "join_key_id", record.JoinKeyID)
	return true, nil
}

// ExpirePending removes every pending registration past its window.
func (r *Registrar) ExpirePending(ctx context.Context) error {
	if ctx == nil {
		panic("programs.ExpirePending: ctx must not be nil")
	}
	records, err := r.records(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	var errs []error
	for _, record := range records {
		if !record.expired(now) {
			continue
		}
		if _, err := r.expireIfStale(ctx, record.Manifest.Name); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// RunExpiry sweeps expired pending registrations until ctx is done.
func (r *Registrar) RunExpiry(ctx context.Context) {
	if ctx == nil {
		panic("programs.RunExpiry: ctx must not be nil")
	}
	tick := time.NewTicker(expirySweepInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			sweepCtx, cancel := context.WithTimeout(ctx, expirySweepInterval)
			if err := r.ExpirePending(sweepCtx); err != nil {
				slog.Warn("programs: pending expiry sweep failed", "error", err)
			}
			cancel()
		}
	}
}

// expiredRegistrationsMax bounds the (join key, name) pairs remembered;
// the oldest is forgotten first.
const expiredRegistrationsMax = 1024

// expiredRegistrations rate-limits re-registration after an expiry, per
// join key and name, on this node: without it a single-use key could
// register, let it expire (refunding the use) and register again
// forever, re-squatting the name. Per node, like the budget lock.
type expiredRegistrations struct {
	mu   sync.Mutex
	when map[string]time.Time
}

func expiredKey(keyID, name string) string { return keyID + "\x00" + name }

func (e *expiredRegistrations) note(keyID, name string) {
	if keyID == "" || name == "" {
		panic("expiredRegistrations.note: key and name must be set")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.when == nil {
		e.when = make(map[string]time.Time)
	}
	if len(e.when) >= expiredRegistrationsMax {
		oldest, oldestAt := "", time.Time{}
		for k, at := range e.when {
			if oldest == "" || at.Before(oldestAt) {
				oldest, oldestAt = k, at
			}
		}
		delete(e.when, oldest)
	}
	e.when[expiredKey(keyID, name)] = time.Now()
}

// recent reports whether keyID's registration of name expired within
// window.
func (e *expiredRegistrations) recent(keyID, name string, window time.Duration) bool {
	if keyID == "" || name == "" {
		panic("expiredRegistrations.recent: key and name must be set")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	at, ok := e.when[expiredKey(keyID, name)]
	return ok && time.Since(at) < window
}
