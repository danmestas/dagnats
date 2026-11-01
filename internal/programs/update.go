package programs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
)

// updateAttemptsMax bounds the reads of a record another node keeps
// rewriting between this node's read and its compare-and-set.
const updateAttemptsMax = 4

// pendingUpdate is a committed record change still to be provisioned.
type pendingUpdate struct {
	grant     program.Grant
	previous  []byte
	revision  uint64
	provision bool
}

// update re-admits a registered program whose key matches and whose
// manifest changed. A rebuild that only changes Version is recorded and
// gets the stored Grant. Anything else is checked against the other
// programs as a new registration would be, refused when it would
// destroy data the program already holds, then recorded with a
// compare-and-set on the record's revision and provisioned. It spends no
// join-key use: the program was admitted already.
func (r *Registrar) update(
	ctx context.Context, req program.RegisterRequest,
) (program.Grant, error) {
	name := req.Manifest.Name
	if name == "" {
		panic("programs.update: name was validated")
	}
	if err := checkVersion(req.Manifest.Dagnats, r.cfg.ServerVersion); err != nil {
		return program.Grant{}, err
	}
	for attempt := 1; attempt <= updateAttemptsMax; attempt++ {
		pending, err := r.commitUpdate(ctx, req)
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		if err != nil || !pending.provision {
			return pending.grant, err
		}
		if err := program.Provision(ctx, r.js, req.Manifest, pending.grant); err != nil {
			return program.Grant{}, r.rollBackUpdate(name, pending, err)
		}
		return pending.grant, nil
	}
	return program.Grant{}, fmt.Errorf("update program %q: its record changed on every one "+
		"of %d attempts", name, updateAttemptsMax)
}

// commitUpdate makes the checks and the record write under admitMu. An
// ErrKeyRevisionMismatch means another node rewrote the record since it
// was read; the caller reads it again.
func (r *Registrar) commitUpdate(
	ctx context.Context, req program.RegisterRequest,
) (pendingUpdate, error) {
	r.admitMu.Lock()
	defer r.admitMu.Unlock()
	entry, record, err := r.lookupEntry(ctx, req.Manifest.Name)
	if err != nil {
		return pendingUpdate{}, err
	}
	grant, changed, err := settle(record, req)
	if err != nil || !changed {
		return pendingUpdate{grant: grant}, err
	}
	pending := pendingUpdate{grant: record.Grant, previous: entry.Value()}
	if !versionOnly(record.Manifest, req.Manifest) {
		pending.grant, err = r.readmit(ctx, record.Manifest, req.Manifest)
		if err != nil {
			return pendingUpdate{}, err
		}
		record.Permissions = program.PermissionsFor(req.Manifest)
		pending.provision = true
	}
	record.Manifest, record.Grant = req.Manifest, pending.grant
	pending.revision, err = r.writeRecord(ctx, record, entry.Revision())
	if err != nil {
		return pendingUpdate{}, err
	}
	return pending, nil
}

// readmit runs a changed manifest through admission against every
// program but itself, and refuses changes that would destroy what the
// program holds.
func (r *Registrar) readmit(
	ctx context.Context, stored, changed program.Manifest,
) (program.Grant, error) {
	if stored.Name != changed.Name {
		panic("programs.readmit: an update keeps the program's name")
	}
	records, err := r.records(ctx)
	if err != nil {
		return program.Grant{}, err
	}
	others := slices.DeleteFunc(records, func(record Record) bool {
		return record.Manifest.Name == changed.Name
	})
	if err := checkProtocols(changed, others); err != nil {
		return program.Grant{}, err
	}
	if err := checkRemovals(stored, changed); err != nil {
		return program.Grant{}, err
	}
	grant, err := r.grantFor(ctx, changed, others)
	if err != nil {
		return program.Grant{}, err
	}
	if err := r.checkLive(ctx, changed, grant); err != nil {
		return program.Grant{}, err
	}
	return grant, nil
}

// lookupEntry reads a record with its KV entry, for a compare-and-set.
func (r *Registrar) lookupEntry(
	ctx context.Context, name string,
) (jetstream.KeyValueEntry, Record, error) {
	entry, err := r.kv.Get(ctx, name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, Record{}, refused(program.RefusedManifestChanged,
			"program %q was removed while this registration ran; register again", name)
	}
	if err != nil {
		return nil, Record{}, fmt.Errorf("read program %q: %w", name, err)
	}
	var record Record
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return nil, Record{}, fmt.Errorf("decode program %q: %w", name, err)
	}
	return entry, record, nil
}

// writeRecord replaces the record if it is still at revision.
func (r *Registrar) writeRecord(
	ctx context.Context, record Record, revision uint64,
) (uint64, error) {
	if revision == 0 {
		panic("programs.writeRecord: an update needs the read revision")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("encode program record: %w", err)
	}
	written, err := r.kv.Update(ctx, record.Manifest.Name, data, revision)
	if err != nil && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, fmt.Errorf("store program %q: %w", record.Manifest.Name, err)
	}
	return written, err
}

// rollBackUpdate restores the record an update replaced when its
// provisioning failed, but only at the revision the update wrote, so a
// later registration's record survives. Streams the update created are
// left; the program's next registration declares them again.
func (r *Registrar) rollBackUpdate(name string, pending pendingUpdate, cause error) error {
	if name == "" || pending.revision == 0 || len(pending.previous) == 0 {
		panic("programs.rollBackUpdate: name, revision and previous record must be set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := fmt.Errorf("provision program %q update: %w", name, cause)
	_, restoreErr := r.kv.Update(ctx, name, pending.previous, pending.revision)
	if restoreErr != nil && !errors.Is(restoreErr, jetstream.ErrKeyRevisionMismatch) {
		err = errors.Join(err, fmt.Errorf("restore program %q: %w", name, restoreErr))
	}
	return err
}

// versionOnly reports whether two manifests differ in Version alone,
// which is informational and never re-admitted.
func versionOnly(stored, changed program.Manifest) bool {
	stored.Version, changed.Version = "", ""
	return sameManifest(stored, changed)
}
