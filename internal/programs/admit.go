package programs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// admit checks the version range, claims the name and provisions the
// program's streams and buckets. committed reports whether this call's
// registration now stands, so the caller refunds the reserved use
// exactly when it does not.
func (r *Registrar) admit(
	ctx context.Context, req program.RegisterRequest, keyID string,
) (grant program.Grant, committed bool, err error) {
	m := req.Manifest
	if err := checkVersion(m.Dagnats, r.cfg.ServerVersion); err != nil {
		return program.Grant{}, false, err
	}
	// Held through provisioning, so an update that commits right after
	// this Create provisions after it, not before.
	lock := r.programLock(m.Name)
	lock.Lock()
	defer lock.Unlock()
	grant, revision, err := r.reserveName(ctx, req, keyID)
	if err != nil || revision == 0 {
		// revision 0: a concurrent registration of the same program won
		// the Create; err is its stored Grant's outcome.
		return grant, false, err
	}
	if err := r.provision(ctx, m, grant); err != nil {
		return program.Grant{}, false, r.rollBack(m.Name, revision, err)
	}
	return grant, true, nil
}

// rollBack removes the record a failed provisioning left, but only at
// the revision this call created: a record rewritten since then belongs
// to a later registration and must survive.
func (r *Registrar) rollBack(name string, revision uint64, cause error) error {
	if name == "" || revision == 0 {
		panic("programs.rollBack: name and revision must be set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := fmt.Errorf("provision program %q: %w", name, cause)
	deleteErr := r.kv.Delete(ctx, name, jetstream.LastRevision(revision))
	if deleteErr != nil && !errors.Is(deleteErr, jetstream.ErrKeyRevisionMismatch) {
		err = errors.Join(err, fmt.Errorf("roll back program %q: %w", name, deleteErr))
	}
	return err
}

// reserveName checks the manifest against every registered program
// (protocol clashes, the store budget, the program count) and writes the
// record with KV Create, all under admitMu. It returns the record's
// revision, or 0 when another registration of the name got there first,
// in which case that registration's outcome is returned.
func (r *Registrar) reserveName(
	ctx context.Context, req program.RegisterRequest, keyID string,
) (grant program.Grant, revision uint64, err error) {
	if req.Manifest.Name == "" {
		panic("programs.reserveName: name was validated")
	}
	r.admitMu.Lock()
	defer r.admitMu.Unlock()
	records, err := r.records(ctx)
	if err != nil {
		return program.Grant{}, 0, err
	}
	if err := checkProtocols(req.Manifest, records); err != nil {
		return program.Grant{}, 0, err
	}
	grant, err = r.grantFor(ctx, req.Manifest, records)
	if err != nil {
		return program.Grant{}, 0, err
	}
	if err := r.checkLive(ctx, req.Manifest, grant); err != nil {
		return program.Grant{}, 0, err
	}
	data, err := json.Marshal(Record{
		Manifest: req.Manifest, PublicKey: req.PublicKey,
		Permissions: program.PermissionsFor(req.Manifest), Grant: grant,
		JoinKeyID: keyID, RegisteredAt: time.Now().UTC(),
	})
	if err != nil {
		return program.Grant{}, 0, fmt.Errorf("encode program record: %w", err)
	}
	revision, err = r.kv.Create(ctx, req.Manifest.Name, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return r.lostCreate(ctx, req)
	}
	if err != nil {
		return program.Grant{}, 0, fmt.Errorf("store program %q: %w", req.Manifest.Name, err)
	}
	return grant, revision, nil
}

// lostCreate settles a registration whose Create lost to a concurrent
// one, still under admitMu, so it cannot re-admit here: a changed
// manifest is refused and its next registration is an update.
func (r *Registrar) lostCreate(
	ctx context.Context, req program.RegisterRequest,
) (program.Grant, uint64, error) {
	record, found, err := r.lookup(ctx, req.Manifest.Name)
	if err != nil || !found {
		return program.Grant{}, 0, errors.Join(err,
			fmt.Errorf("program %q: Create lost but no record found", req.Manifest.Name))
	}
	grant, changed, err := settle(record, req)
	if changed {
		return program.Grant{}, 0, refused(program.RefusedManifestChanged,
			"program %q was registered at the same moment with a different "+
				"manifest; register again to update it", req.Manifest.Name)
	}
	return grant, 0, err
}
