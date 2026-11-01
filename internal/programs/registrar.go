// Package programs admits programs to a dagnats: the server side of
// program.Run's registration (#885). A Registrar checks a
// RegisterRequest, spends a join-key use, sizes the program's streams
// and buckets against the store budget, records the program in the
// programs KV bucket, provisions its resources, and returns the Grant.
//
// The order is fixed by one invariant: a use-limited join key never
// admits more than UsesMax programs. The use is reserved by
// joinkey.ConsumeUse (a KV compare-and-set) before anything is
// committed, and refunded if any later step refuses or fails.
//
// Name reservation is a KV Create, so the first registration of a name
// wins. Re-registering a name with the same public key and manifest
// returns the stored Grant and spends nothing, which is what a
// restarting program does until attach exists (D4).
//
// In open mode anonymous NATS clients are root, and that includes this
// endpoint: any client can subscribe to program.RegisterSubject or join
// the dagnats-programs queue group and read the join keys in requests,
// or answer them. Join keys only protect a server with a client login.
package programs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/kvmirror"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// BucketName is the KV bucket registered programs live in, keyed by
// program name.
const BucketName = "programs"

// ProgramsCountMax bounds the programs one dagnats admits, and so the
// scan the budget check makes over them.
const ProgramsCountMax = 256

// Config is what the server tells the Registrar about itself.
type Config struct {
	// Open is true when the server's NATS clients need no login, so
	// anonymous connections are root and a join key is optional. When
	// false (a client login is configured), registration is refused
	// with auth_mode_unsupported until programs get NKey users: an
	// admitted program could not log in.
	Open bool
	// MaxStoreBytes is the JetStream store budget streams are sized
	// from and admitted against.
	MaxStoreBytes int64
	Replicas      int
	// ServerVersion is checked against Manifest.Dagnats. "dev" (an
	// unstamped build) admits every range.
	ServerVersion string
}

// Record is one registered program, as stored.
type Record struct {
	Manifest     program.Manifest    `json:"manifest"`
	PublicKey    string              `json:"public_key"`
	Permissions  program.Permissions `json:"permissions"`
	Grant        program.Grant       `json:"grant"`
	JoinKeyID    string              `json:"join_key_id,omitempty"`
	RegisteredAt time.Time           `json:"registered_at"`
}

// Registrar admits programs. Safe for concurrent use.
type Registrar struct {
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	stream jetstream.Stream
	keys   *joinkey.Store
	cfg    Config
	// admitMu serializes the clash, budget and count checks with the
	// record write, so two registrations on this node cannot both claim
	// the last free bytes. It is per node: in an N-node cluster,
	// registrations served at the same moment by different nodes can
	// overshoot the budget by up to N-1 Grants, and ProgramsCountMax by
	// up to N-1 programs. The protocol_clash check is per node in the
	// same way: two nodes can each admit one side of a clash at the same
	// moment. Join-key uses are exact across nodes.
	admitMu sync.Mutex
}

// NewRegistrar binds the programs bucket, creating it when absent.
func NewRegistrar(
	ctx context.Context, js jetstream.JetStream, keys *joinkey.Store, cfg Config,
) (*Registrar, error) {
	if js == nil || keys == nil {
		panic("programs.NewRegistrar: js and keys must not be nil")
	}
	if cfg.MaxStoreBytes <= 0 || cfg.Replicas < 1 {
		panic("programs.NewRegistrar: MaxStoreBytes and Replicas must be positive")
	}
	kv, stream, err := kvmirror.Bind(ctx, js, BucketName)
	if err != nil {
		return nil, err
	}
	return &Registrar{js: js, kv: kv, stream: stream, keys: keys, cfg: cfg}, nil
}

// Register admits req's program and returns its Grant, or an
// *program.ErrRefused. Any other error is a server fault.
func (r *Registrar) Register(
	ctx context.Context, req program.RegisterRequest,
) (program.Grant, error) {
	if ctx == nil {
		panic("programs.Register: ctx must not be nil")
	}
	if r.kv == nil {
		panic("programs.Register: registrar not constructed")
	}
	if err := r.checkRequest(req); err != nil {
		return program.Grant{}, err
	}
	if grant, settled, err := r.existing(ctx, req); settled {
		return grant, err
	}
	keyID, err := r.reserveUse(ctx, req)
	if err != nil {
		return program.Grant{}, err
	}
	grant, committed, err := r.admit(ctx, req, keyID)
	if !committed && keyID != "" {
		r.refund(keyID, req.Manifest.Name)
	}
	return grant, err
}

func refused(code, format string, args ...any) error {
	return &program.ErrRefused{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// checkRequest refuses what can be judged from the request alone.
func (r *Registrar) checkRequest(req program.RegisterRequest) error {
	if err := program.ValidateName(req.Manifest.Name); err != nil {
		return refused(program.RefusedNameInvalid, "%v", err)
	}
	if err := program.Validate(req.Manifest); err != nil {
		return refused(program.RefusedRequestInvalid, "%v", err)
	}
	if !nkeys.IsValidPublicUserKey(req.PublicKey) {
		return refused(program.RefusedRequestInvalid, "public key is not an NKey user key")
	}
	if !r.cfg.Open {
		return refused(program.RefusedAuthModeUnsupported,
			"this dagnats requires a NATS login, and program NKey users are "+
				"not supported yet; run it with no client login to register programs")
	}
	return nil
}

// existing settles a request for a name already registered: the same
// key and manifest get the stored Grant; anything else is refused.
// settled is false when the name is free.
func (r *Registrar) existing(
	ctx context.Context, req program.RegisterRequest,
) (grant program.Grant, settled bool, err error) {
	record, found, err := r.lookup(ctx, req.Manifest.Name)
	if err != nil {
		return program.Grant{}, true, err
	}
	if !found {
		return program.Grant{}, false, nil
	}
	if record.PublicKey != req.PublicKey {
		return program.Grant{}, true, refused(program.RefusedNameTaken,
			"program %q is taken", req.Manifest.Name)
	}
	if !sameManifest(record.Manifest, req.Manifest) {
		return program.Grant{}, true, refused(program.RefusedManifestChanged,
			"program %q is registered with a different manifest; manifest changes "+
				"are not admitted yet", req.Manifest.Name)
	}
	return record.Grant, true, nil
}

// sameManifest compares by JSON encoding, the form the stored record
// went through, so nil and empty slices do not differ.
func sameManifest(a, b program.Manifest) bool {
	encodedA, errA := json.Marshal(a)
	encodedB, errB := json.Marshal(b)
	if errA != nil || errB != nil {
		panic("programs.sameManifest: a Manifest always encodes")
	}
	return bytes.Equal(encodedA, encodedB)
}

// reserveUse verifies the join key and spends one use, returning its
// ID, or "" when an open-mode request presents no key.
func (r *Registrar) reserveUse(ctx context.Context, req program.RegisterRequest) (string, error) {
	if req.JoinKey == "" && r.cfg.Open {
		return "", nil
	}
	key, err := r.keys.Verify(req.JoinKey, req.Manifest.Name)
	if err == nil {
		if refusal := protocolsOutsideGlob(key, req.Manifest.Protocols); refusal != nil {
			return "", refusal
		}
		err = r.keys.ConsumeUse(ctx, key.ID)
	}
	switch {
	case err == nil:
		return key.ID, nil
	case errors.Is(err, joinkey.ErrExhausted):
		return "", refused(program.RefusedJoinKeyExhausted, "%v", err)
	case errors.Is(err, joinkey.ErrInvalid), errors.Is(err, joinkey.ErrRevoked),
		errors.Is(err, joinkey.ErrExpired), errors.Is(err, joinkey.ErrNameNotAllowed):
		return "", refused(program.RefusedJoinKeyInvalid, "%v", err)
	}
	return "", fmt.Errorf("reserve join key use: %w", err)
}

// protocolsOutsideGlob refuses a protocol whose root a --names key does
// not cover. A protocol claims its root for protocol_clash, so a key
// limited to artifact* that could declare "notify.rpc" would lock the
// name notify out. Checked before the use is spent.
func protocolsOutsideGlob(key joinkey.JoinKey, protocols []string) error {
	if key.ID == "" {
		panic("programs.protocolsOutsideGlob: key must be verified")
	}
	for _, protocol := range protocols {
		protocolRoot, _, _ := strings.Cut(protocol, ".")
		if !key.AllowsName(protocolRoot) {
			return refused(program.RefusedProtocolNotAllowed,
				"protocol %q: root %q is outside the join key's names %q",
				protocol, protocolRoot, key.NameGlob)
		}
	}
	return nil
}

// refund returns a reserved use. It runs on its own context so a
// cancelled request still refunds; a failed refund is logged, because
// the registration's own outcome is what the caller needs.
func (r *Registrar) refund(keyID, name string) {
	if keyID == "" || name == "" {
		panic("programs.refund: keyID and name must not be empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.keys.RefundUse(ctx, keyID); err != nil {
		slog.Error("programs: join key use not refunded",
			"join_key_id", keyID, "program", name, "error", err)
	}
}

// lookup reads one record straight from KV.
func (r *Registrar) lookup(ctx context.Context, name string) (Record, bool, error) {
	entry, err := r.kv.Get(ctx, name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("read program %q: %w", name, err)
	}
	var record Record
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return Record{}, false, fmt.Errorf("decode program %q: %w", name, err)
	}
	return record, true, nil
}

// records returns every registered program, bounded by
// ProgramsCountMax.
func (r *Registrar) records(ctx context.Context) ([]Record, error) {
	names, err := natsutil.ListKeys(ctx, r.stream, BucketName)
	if err != nil {
		return nil, err
	}
	if len(names) > ProgramsCountMax {
		names = names[:ProgramsCountMax]
	}
	out := make([]Record, 0, len(names))
	for _, name := range names {
		record, found, err := r.lookup(ctx, name)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, record)
		}
	}
	return out, nil
}
