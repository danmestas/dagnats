package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/danmestas/dagnats/internal/programs"
	"github.com/danmestas/dagnats/program"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// The built-in users every dagnats NATS server has. Their passwords are
// random per start and never handed out: they are only ever reached as
// the NoAuthUser, by a client that presents no credentials.
const (
	// rootUser is anonymous in open mode (no nats_user): unrestricted,
	// exactly what anonymous was before programs had NKey users.
	rootUser = "_dagnats_root"
	// joinUser is anonymous in required mode: it may only ask to
	// register a program and read the reply.
	joinUser = "_dagnats_join"
)

// applyAuth sets opts' client authorization at start: the operator's
// nats_user login (unrestricted) when configured, and the built-in
// anonymous user. It replaces any single Username/Password, which
// nats-server refuses alongside a users list. Program NKey users are
// added later by userSet; AlwaysEnableNonce lets a program sign in with
// its NKey whatever the current set.
func applyAuth(opts *natsserver.Options, login, password string) {
	if opts == nil {
		panic("applyAuth: opts must not be nil")
	}
	if login != "" && password == "" {
		panic("applyAuth: a login needs its resolved password")
	}
	opts.Username, opts.Password = "", ""
	opts.AlwaysEnableNonce = true
	anonymous := &natsserver.User{Username: rootUser, Password: randomPassword()}
	if login != "" {
		anonymous = &natsserver.User{
			Username: joinUser, Password: randomPassword(), Permissions: joinPermissions(),
		}
	}
	opts.Users = []*natsserver.User{anonymous}
	if login != "" {
		opts.Users = append(opts.Users, &natsserver.User{Username: login, Password: password})
	}
	opts.NoAuthUser = anonymous.Username
	opts.Nkeys = programUsers(nil)
}

// placeholderNkey is a user key whose seed was discarded at birth, so
// nobody can sign as it, denied everything. It keeps the NKey user set
// non-empty: with no NKey users at all, nats-server ignores a presented
// NKey and authenticates the client as the anonymous user instead, so an
// unknown or deleted program key would get in whenever no program is
// registered.
var placeholderNkey = func() string {
	pair, err := nkeys.CreateUser()
	if err != nil {
		panic("placeholderNkey: " + err.Error())
	}
	public, err := pair.PublicKey()
	if err != nil {
		panic("placeholderNkey: " + err.Error())
	}
	pair.Wipe()
	return public
}()

// programUsers is one NKey user per registered program, with
// PermissionsFor its manifest, in a stable order, plus the placeholder.
func programUsers(records []programs.Record) []*natsserver.NkeyUser {
	sorted := slices.Clone(records)
	slices.SortFunc(sorted, func(a, b programs.Record) int {
		return strings.Compare(a.Manifest.Name, b.Manifest.Name)
	})
	users := make([]*natsserver.NkeyUser, 0, len(sorted)+1)
	users = append(users, &natsserver.NkeyUser{Nkey: placeholderNkey, Permissions: denyAll()})
	for _, record := range sorted {
		users = append(users, &natsserver.NkeyUser{
			Nkey:        record.PublicKey,
			Permissions: serverPermissions(programs.UserPermissions(record)),
		})
	}
	return users
}

// joinPermissions is all an anonymous client may do in required mode:
// request registration over the _JOIN inbox program.Run dials with.
func joinPermissions() *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: []string{program.RegisterSubject}},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{program.JoinInboxPrefix + ".>"}},
	}
}

func denyAll() *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Deny: []string{">"}},
		Subscribe: &natsserver.SubjectPermission{Deny: []string{">"}},
	}
}

// serverPermissions converts a program.Permissions, which program keeps
// free of nats-server, into the server's form.
func serverPermissions(p program.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish: &natsserver.SubjectPermission{
			Allow: slices.Clone(p.Publish.Allow), Deny: slices.Clone(p.Publish.Deny),
		},
		Subscribe: &natsserver.SubjectPermission{
			Allow: slices.Clone(p.Subscribe.Allow), Deny: slices.Clone(p.Subscribe.Deny),
		},
	}
}

func randomPassword() string {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		panic("randomPassword: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(raw)
}

// liveOptions remembers the *Options each embedded server was started
// with. nats-server exposes no getter for them, and a reload must start
// from the server's own options, not from cfg: a #688 store-shrink
// recovery runs the server at a budget cfg does not hold.
var liveOptions sync.Map // *natsserver.Server -> *natsserver.Options

func rememberOptions(ns *natsserver.Server, opts *natsserver.Options) {
	if ns == nil || opts == nil {
		panic("rememberOptions: ns and opts must not be nil")
	}
	liveOptions.Store(ns, opts)
}

func forgetOptions(ns *natsserver.Server) {
	if ns == nil {
		panic("forgetOptions: ns must not be nil")
	}
	liveOptions.Delete(ns)
}

// userSet holds one server's program NKey users and reloads them into
// it. Each change reloads the whole set, serialized, so the server always
// ends on the latest one. A reload re-authorizes every connected client:
// a program whose key is unchanged stays connected; a removed or
// replaced key is disconnected.
type userSet struct {
	ns *natsserver.Server

	mu      sync.Mutex
	records map[string]programs.Record // program name -> record, as the server has it
	// dirty: the last reload failed, so the server may lag records.
	dirty bool
}

func newUserSet(ns *natsserver.Server) *userSet {
	if ns == nil {
		panic("newUserSet: ns must not be nil")
	}
	if _, ok := liveOptions.Load(ns); !ok {
		panic("newUserSet: server was not started through tryStartNATS")
	}
	return &userSet{ns: ns, records: make(map[string]programs.Record)}
}

// put records (or, with deleted, drops) one program and reloads when
// the user set changed.
func (u *userSet) put(name string, record programs.Record, deleted bool) error {
	if name == "" {
		panic("userSet.put: name must not be empty")
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	current, exists := u.records[name]
	if deleted && !exists {
		return nil
	}
	if !deleted && exists && !u.dirty && sameUser(current, record) {
		return nil
	}
	// The change is committed only once the server took it: a failed
	// reload leaves the set as the server has it, marked dirty, so the
	// next change (or a replay of this one) reloads in full again
	// instead of being skipped as already applied.
	next := maps.Clone(u.records)
	if deleted {
		delete(next, name)
	} else {
		next[name] = record
	}
	if err := u.reloadLocked(next); err != nil {
		u.dirty = true
		return err
	}
	u.records, u.dirty = next, false
	return nil
}

// sameUser reports whether two records give the same NATS user, so a
// watch replay of a write this node already applied costs no reload.
func sameUser(a, b programs.Record) bool {
	if a.PublicKey != b.PublicKey {
		return false
	}
	pa, pb := programs.UserPermissions(a), programs.UserPermissions(b)
	return slices.Equal(pa.Publish.Allow, pb.Publish.Allow) &&
		slices.Equal(pa.Publish.Deny, pb.Publish.Deny) &&
		slices.Equal(pa.Subscribe.Allow, pb.Subscribe.Allow) &&
		slices.Equal(pa.Subscribe.Deny, pb.Subscribe.Deny)
}

func (u *userSet) reloadLocked(set map[string]programs.Record) error {
	if set == nil {
		panic("userSet.reloadLocked: set must not be nil")
	}
	value, ok := liveOptions.Load(u.ns)
	if !ok {
		return fmt.Errorf("reload NATS users: server options unknown")
	}
	next := value.(*natsserver.Options).Clone()
	records := make([]programs.Record, 0, len(set))
	for _, record := range set {
		records = append(records, record)
	}
	next.Nkeys = programUsers(records)
	if err := u.ns.ReloadOptions(next); err != nil {
		return fmt.Errorf("reload NATS users: %w", err)
	}
	liveOptions.Store(u.ns, next)
	return nil
}
