package programs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/program"
)

// widening is what a changed manifest reaches beyond its own prefix that
// the stored one did not: new protocols and new reads of another
// program's buckets. Everything else a manifest declares sits under the
// program's own name, which its NKey already owns.
type widening struct {
	protocols []string
	reads     []string
}

func widened(stored, changed program.Manifest) widening {
	if stored.Name != changed.Name {
		panic("programs.widened: an update keeps the program's name")
	}
	var w widening
	for _, protocol := range changed.Protocols {
		if !slices.Contains(stored.Protocols, protocol) {
			w.protocols = append(w.protocols, protocol)
		}
	}
	for _, read := range changed.Reads {
		if !slices.Contains(stored.Reads, read) {
			w.reads = append(w.reads, read)
		}
	}
	if len(w.protocols) > len(changed.Protocols) || len(w.reads) > len(changed.Reads) {
		panic("programs.widened: more widening than the manifest declares")
	}
	return w
}

func (w widening) none() bool { return len(w.protocols) == 0 && len(w.reads) == 0 }

func (w widening) String() string {
	var parts []string
	if len(w.protocols) > 0 {
		parts = append(parts, "protocols "+strings.Join(w.protocols, ", "))
	}
	if len(w.reads) > 0 {
		parts = append(parts, "reads "+strings.Join(w.reads, ", "))
	}
	return strings.Join(parts, " and ")
}

// reserveWidening admits an update's widening against the join key in
// its request and spends one use, returning the key's ID for a refund,
// or "" when nothing widened or an open server takes a keyless request
// (as it would a keyless first registration). The key must be usable
// and its names must cover the program, every new protocol's root and
// every new read's owner.
func (r *Registrar) reserveWidening(
	ctx context.Context, req program.RegisterRequest, w widening,
) (string, error) {
	if w.none() || (req.JoinKey == "" && r.cfg.Open) {
		return "", nil
	}
	name := req.Manifest.Name
	if req.JoinKey == "" {
		return "", refusedWidening(name, w, "the request carries no join key")
	}
	key, err := r.keys.Verify(req.JoinKey, name)
	if err == nil {
		err = covers(key, w)
	}
	if err == nil {
		err = r.keys.ConsumeUse(ctx, key.ID)
	}
	switch {
	case err == nil:
		return key.ID, nil
	case errors.Is(err, joinkey.ErrInvalid), errors.Is(err, joinkey.ErrRevoked),
		errors.Is(err, joinkey.ErrExpired), errors.Is(err, joinkey.ErrNameNotAllowed),
		errors.Is(err, joinkey.ErrExhausted), errors.Is(err, joinkey.ErrProtocolNotAllowed):
		return "", refusedWidening(name, w, err.Error())
	}
	return "", fmt.Errorf("reserve join key use for program %q: %w", name, err)
}

// covers reports, as joinkey.ErrProtocolNotAllowed, the first new
// protocol root or read owner key's names do not cover.
func covers(key joinkey.JoinKey, w widening) error {
	if key.ID == "" {
		panic("programs.covers: key must be verified")
	}
	if protocol, outside := key.ProtocolOutside(w.protocols); outside {
		return fmt.Errorf("%w: protocol %q is outside names %q",
			joinkey.ErrProtocolNotAllowed, protocol, key.NameGlob)
	}
	for _, read := range w.reads {
		owner, _, found := strings.Cut(read, "_")
		if !found {
			panic("programs.covers: reads were validated as <program>_<rest>")
		}
		if !key.AllowsName(owner) {
			return fmt.Errorf("%w: read %q's owner %q is outside names %q",
				joinkey.ErrProtocolNotAllowed, read, owner, key.NameGlob)
		}
	}
	return nil
}

func refusedWidening(name string, w widening, why string) error {
	return refused(program.RefusedJoinKeyRequiredToWiden,
		"program %q adds %s, outside its own prefix: present a join key whose names "+
			"cover them (%s)", name, w, why)
}
