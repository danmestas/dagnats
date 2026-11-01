package programs

import (
	"context"
	"fmt"
	"math"

	"github.com/danmestas/dagnats/program"
)

// grantFor sizes m's streams from the store budget and refuses it when
// they and its buckets do not fit in what is free. A stream gets its
// fraction of the budget or its floor, whichever is larger. Every size
// is capped at the whole budget and every sum is checked, so a manifest
// with sizes near MaxInt64 is refused rather than wrapped negative.
func (r *Registrar) grantFor(
	ctx context.Context, m program.Manifest, records []Record,
) (program.Grant, error) {
	if len(records) >= ProgramsCountMax {
		return program.Grant{}, refused(program.RefusedBudgetExceeded,
			"this dagnats already has %d programs, its limit", len(records))
	}
	grant, err := r.sizeGrant(m)
	if err != nil {
		return program.Grant{}, err
	}
	reserved, err := r.reservedBytes(ctx, records)
	if err != nil {
		return program.Grant{}, err
	}
	free := r.cfg.MaxStoreBytes - min(reserved, r.cfg.MaxStoreBytes)
	if grant.StoreBudgetBytes > free {
		return program.Grant{}, refused(program.RefusedBudgetExceeded,
			"program %q needs %d bytes; %d of the %d-byte store budget is free "+
				"(%d reserved)", m.Name, grant.StoreBudgetBytes, free,
			r.cfg.MaxStoreBytes, reserved)
	}
	return grant, nil
}

// sizeGrant resolves m's sizes, refusing any one larger than the whole
// store budget.
func (r *Registrar) sizeGrant(m program.Manifest) (program.Grant, error) {
	budget := r.cfg.MaxStoreBytes
	grant := program.Grant{
		Replicas:       r.cfg.Replicas,
		StreamMaxBytes: make(map[string]int64, len(m.Streams)),
		ServerVersion:  r.cfg.ServerVersion,
	}
	tooBig := func(kind, name string, size int64) error {
		return refused(program.RefusedBudgetExceeded,
			"%s %q asks for %d bytes, more than the whole %d-byte store budget",
			kind, name, size, budget)
	}
	for _, s := range m.Streams {
		size := max(s.MaxBytesFloor, int64(float64(budget)*s.MaxBytesFraction))
		if size > budget {
			return program.Grant{}, tooBig("stream", s.Name, size)
		}
		if size <= 0 {
			return program.Grant{}, refused(program.RefusedRequestInvalid,
				"stream %q sizes to 0 bytes: its fraction of the %d-byte budget "+
					"rounds down and it has no floor", s.Name, budget)
		}
		grant.StreamMaxBytes[s.Name] = size
		grant.StoreBudgetBytes = addSaturating(grant.StoreBudgetBytes, size)
	}
	for _, b := range m.Buckets {
		if b.MaxBytes > budget {
			return program.Grant{}, tooBig("bucket", b.Name, b.MaxBytes)
		}
		grant.StoreBudgetBytes = addSaturating(grant.StoreBudgetBytes, b.MaxBytes)
	}
	return grant, nil
}

// addSaturating adds two non-negative sizes, pinning at MaxInt64 instead
// of wrapping. A pinned total is always over any real budget.
func addSaturating(a, b int64) int64 {
	if a < 0 || b < 0 {
		panic(fmt.Sprintf("programs.addSaturating: negative size %d + %d", a, b))
	}
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// reservedBytes is what the store budget already holds: every
// registered program's Grant, plus the MaxBytes of every stream no
// program owns (dagnats' own and any an operator made). A program's
// streams are counted through its Grant, not their configs, so one
// registered but not yet provisioned is still counted.
func (r *Registrar) reservedBytes(ctx context.Context, records []Record) (int64, error) {
	var reserved int64
	owned := make(map[string]bool)
	for _, record := range records {
		reserved = addSaturating(reserved, max(record.Grant.StoreBudgetBytes, 0))
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
		reserved = addSaturating(reserved, info.Config.MaxBytes)
	}
	if err := lister.Err(); err != nil {
		return 0, fmt.Errorf("list streams for the store budget: %w", err)
	}
	return reserved, nil
}
