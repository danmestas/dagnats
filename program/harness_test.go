// program/harness_test.go
// Shared fixture for the permission tests: an embedded nats-server whose
// anonymous user is an unrestricted admin (the engine's seat) and whose
// programs log in as NKey users carrying exactly the permissions
// program.PermissionsFor generates.
// Methodology: helpers only. Every permission violation the server logs
// or sends to a client is recorded, so a test can assert on the precise
// set of denials rather than infer them from timeouts.
package program_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/program"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// violationLog collects every "Violation" line the server logs. It
// implements natsserver.Logger.
type violationLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *violationLog) record(format string, v ...any) {
	line := fmt.Sprintf(format, v...)
	if !strings.Contains(line, "Violation") {
		return
	}
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
}

func (l *violationLog) Noticef(format string, v ...any) { l.record(format, v...) }
func (l *violationLog) Warnf(format string, v ...any)   { l.record(format, v...) }
func (l *violationLog) Fatalf(format string, v ...any)  { l.record(format, v...) }
func (l *violationLog) Errorf(format string, v ...any)  { l.record(format, v...) }
func (l *violationLog) Debugf(format string, v ...any)  {}
func (l *violationLog) Tracef(format string, v ...any)  {}

func (l *violationLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// contains reports whether any recorded violation mentions subject.
func (l *violationLog) contains(subject string) bool {
	for _, line := range l.snapshot() {
		if strings.Contains(line, `"`+subject+`"`) {
			return true
		}
	}
	return false
}

// fixture is one embedded server plus the admin connection the engine
// side of a test uses.
type fixture struct {
	server     *natsserver.Server
	admin      *nats.Conn
	violations *violationLog
	keys       map[string]program.Key
}

// serverPermissions is the conversion D3 owns in server; the test keeps
// its own copy so program never links nats-server.
func serverPermissions(p program.Permissions) *natsserver.Permissions {
	out := &natsserver.Permissions{
		Publish: &natsserver.SubjectPermission{
			Allow: p.Publish.Allow, Deny: p.Publish.Deny,
		},
		Subscribe: &natsserver.SubjectPermission{
			Allow: p.Subscribe.Allow, Deny: p.Subscribe.Deny,
		},
	}
	return out
}

// startFixture starts a server with one NKey user per manifest and the
// dagnats streams and buckets provisioned by the admin connection.
func startFixture(t *testing.T, manifests ...program.Manifest) *fixture {
	t.Helper()
	f := &fixture{violations: &violationLog{}, keys: map[string]program.Key{}}
	opts := &natsserver.Options{
		Host:              "127.0.0.1",
		Port:              -1,
		JetStream:         true,
		JetStreamMaxStore: natstest.MaxStoreBytes + natstest.MaxStoreHeadroomBytes,
		StoreDir:          t.TempDir(),
		Users:             []*natsserver.User{{Username: "admin", Password: "admin"}},
		NoAuthUser:        "admin",
	}
	for _, m := range manifests {
		key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), m.Name+".seed"))
		if err != nil {
			t.Fatalf("LoadOrCreateKey(%s): %v", m.Name, err)
		}
		f.keys[m.Name] = key
		opts.Nkeys = append(opts.Nkeys, &natsserver.NkeyUser{
			Nkey:        key.PublicKey(),
			Permissions: serverPermissions(program.PermissionsFor(m)),
		})
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.SetLoggerV2(f.violations, false, false, false)
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready after 10s")
	}
	f.server = ns
	f.admin = dial(t, ns.ClientURL())
	if err := natsutil.SetupAll(
		f.admin, natsutil.WithStoreBudget(natstest.MaxStoreBytes),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	return f
}

// dialProgram connects as the named program's NKey user with its
// confined inbox prefix. Client-side permission errors are recorded too.
func (f *fixture) dialProgram(t *testing.T, name string) *nats.Conn {
	t.Helper()
	key, ok := f.keys[name]
	if !ok {
		t.Fatalf("no key for program %q", name)
	}
	return dial(t, f.server.ClientURL(),
		key.ConnectOption(),
		nats.CustomInboxPrefix(program.InboxPrefix(name)),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			f.violations.record("client %s: %v", name, err)
		}),
	)
}

func dial(t *testing.T, url string, opts ...nats.Option) *nats.Conn {
	t.Helper()
	opts = append(opts, nats.Timeout(15*time.Second))
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// subjectMatches applies NATS wildcard rules: "*" matches one token and
// a trailing ">" matches one or more.
func subjectMatches(pattern, subject string) bool {
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")
	for i, p := range pt {
		if p == ">" {
			return len(st) > i
		}
		if i >= len(st) {
			return false
		}
		if p != "*" && p != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}

func allowedBy(sp program.SubjectPermission, subject string) bool {
	for _, d := range sp.Deny {
		if subjectMatches(d, subject) {
			return false
		}
	}
	for _, a := range sp.Allow {
		if subjectMatches(a, subject) {
			return true
		}
	}
	return false
}
