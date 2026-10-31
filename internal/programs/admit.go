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

// admit checks the version range and the budget, stores the record and
// provisions the program's streams and buckets. committed reports
// whether this call's registration now stands, so the caller refunds
// the reserved use exactly when it does not.
func (r *Registrar) admit(
	ctx context.Context, req program.RegisterRequest, keyID string,
) (grant program.Grant, committed bool, err error) {
	m := req.Manifest
	if err := checkVersion(m.Dagnats, r.cfg.ServerVersion); err != nil {
		return program.Grant{}, false, err
	}
	grant, created, err := r.reserveName(ctx, req, keyID)
	if err != nil || !created {
		// !created: a concurrent registration of the same program won
		// the Create; err is its stored Grant's outcome.
		return grant, false, err
	}
	if err := program.Provision(ctx, r.js, m, grant); err != nil {
		if deleteErr := r.kv.Delete(context.Background(), m.Name); deleteErr != nil {
			err = errors.Join(err, fmt.Errorf("roll back program %q: %w", m.Name, deleteErr))
		}
		return program.Grant{}, false, fmt.Errorf("provision program %q: %w", m.Name, err)
	}
	return grant, true, nil
}

// reserveName sizes the Grant against the budget and writes the record
// with KV Create, under admitMu. created is false when another
// registration of the name got there first; its outcome is returned.
func (r *Registrar) reserveName(
	ctx context.Context, req program.RegisterRequest, keyID string,
) (grant program.Grant, created bool, err error) {
	if req.Manifest.Name == "" {
		panic("programs.reserveName: name was validated")
	}
	r.admitMu.Lock()
	defer r.admitMu.Unlock()
	grant, err = r.grantFor(ctx, req.Manifest)
	if err != nil {
		return program.Grant{}, false, err
	}
	record := Record{
		Manifest: req.Manifest, PublicKey: req.PublicKey,
		Permissions: program.PermissionsFor(req.Manifest), Grant: grant,
		JoinKeyID: keyID, RegisteredAt: time.Now().UTC(),
	}
	data, err := json.Marshal(record)
	if err != nil {
		return program.Grant{}, false, fmt.Errorf("encode program record: %w", err)
	}
	_, err = r.kv.Create(ctx, req.Manifest.Name, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		grant, _, err = r.existing(ctx, req)
		return grant, false, err
	}
	if err != nil {
		return program.Grant{}, false, fmt.Errorf("store program %q: %w", req.Manifest.Name, err)
	}
	return grant, true, nil
}

// grantFor sizes m's streams from the store budget and refuses it when
// they and its buckets do not fit in what is free. A stream gets its
// fraction of the budget or its floor, whichever is larger.
func (r *Registrar) grantFor(ctx context.Context, m program.Manifest) (program.Grant, error) {
	grant := program.Grant{
		Replicas:       r.cfg.Replicas,
		StreamMaxBytes: make(map[string]int64, len(m.Streams)),
		ServerVersion:  r.cfg.ServerVersion,
	}
	for _, s := range m.Streams {
		size := max(s.MaxBytesFloor, int64(float64(r.cfg.MaxStoreBytes)*s.MaxBytesFraction))
		grant.StreamMaxBytes[s.Name] = size
		grant.StoreBudgetBytes += size
	}
	for _, b := range m.Buckets {
		grant.StoreBudgetBytes += b.MaxBytes
	}
	reserved, count, err := r.reservedBytes(ctx)
	if err != nil {
		return program.Grant{}, err
	}
	if count >= ProgramsCountMax {
		return program.Grant{}, refused(program.RefusedBudgetExceeded,
			"this dagnats already has %d programs, its limit", count)
	}
	free := r.cfg.MaxStoreBytes - reserved
	if grant.StoreBudgetBytes > free {
		return program.Grant{}, refused(program.RefusedBudgetExceeded,
			"program %q needs %d bytes; %d of the %d-byte store budget is free "+
				"(%d reserved)", m.Name, grant.StoreBudgetBytes, max(free, 0),
			r.cfg.MaxStoreBytes, reserved)
	}
	return grant, nil
}

// reservedBytes is what the store budget already holds: every
// registered program's Grant, plus the MaxBytes of every stream no
// program owns (dagnats' own and any an operator made). A program's
// streams are counted through its Grant, not their configs, so one
// registered but not yet provisioned is still counted.
func (r *Registrar) reservedBytes(ctx context.Context) (reserved int64, count int, err error) {
	records, err := r.records(ctx)
	if err != nil {
		return 0, 0, err
	}
	owned := make(map[string]bool)
	for _, record := range records {
		reserved += record.Grant.StoreBudgetBytes
		for _, s := range record.Manifest.Streams {
			owned[s.Name] = true
		}
		for _, b := range record.Manifest.Buckets {
			owned["KV_"+b.Name] = true
		}
	}
	lister := r.js.ListStreams(ctx)
	for info := range lister.Info() {
		if info == nil || owned[info.Config.Name] || info.Config.MaxBytes <= 0 {
			continue
		}
		reserved += info.Config.MaxBytes
	}
	if err := lister.Err(); err != nil {
		return 0, 0, fmt.Errorf("list streams for the store budget: %w", err)
	}
	return reserved, len(records), nil
}
