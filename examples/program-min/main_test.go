// examples/program-min/main_test.go
// Acceptance test for the program contract: min serves one task over a
// permission-restricted NKey user with a confined inbox.
// Methodology: an embedded nats-server whose anonymous user is the
// engine's unrestricted seat and whose only other user is min's key,
// carrying exactly program.PermissionsFor(min). A real orchestrator
// dispatches a run; min must complete it with no permission violation
// logged, and a stranger on min's credentials cannot widen its inbox.
package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/program"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type violations struct {
	mu    sync.Mutex
	lines []string
}

func (v *violations) log(format string, args ...any) {
	if line := fmt.Sprintf(format, args...); strings.Contains(line, "Violation") {
		v.mu.Lock()
		v.lines = append(v.lines, line)
		v.mu.Unlock()
	}
}

func (v *violations) Noticef(f string, a ...any) { v.log(f, a...) }
func (v *violations) Warnf(f string, a ...any)   { v.log(f, a...) }
func (v *violations) Fatalf(f string, a ...any)  { v.log(f, a...) }
func (v *violations) Errorf(f string, a ...any)  { v.log(f, a...) }
func (v *violations) Debugf(string, ...any)      {}
func (v *violations) Tracef(string, ...any)      {}

func (v *violations) snapshot() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.lines...)
}

func startServer(t *testing.T, key program.Key, log *violations) *natsserver.Server {
	t.Helper()
	perms := program.PermissionsFor(minProgram{}.Manifest())
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		JetStreamMaxStore: natstest.MaxStoreBytes + natstest.MaxStoreHeadroomBytes,
		Users:             []*natsserver.User{{Username: "engine", Password: "engine"}},
		NoAuthUser:        "engine",
		Nkeys: []*natsserver.NkeyUser{{Nkey: key.PublicKey(), Permissions: &natsserver.Permissions{
			Publish:   &natsserver.SubjectPermission{Allow: perms.Publish.Allow},
			Subscribe: &natsserver.SubjectPermission{Allow: perms.Subscribe.Allow},
		}}},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.SetLoggerV2(log, false, false, false)
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	return ns
}

func TestProgramMinServesTaskAsRestrictedUser(t *testing.T) {
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "min.seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	log := &violations{}
	ns := startServer(t, key, log)
	engineConn, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("engine connect: %v", err)
	}
	t.Cleanup(engineConn.Close)
	budget := natsutil.WithStoreBudget(natstest.MaxStoreBytes)
	if err := natsutil.SetupAll(engineConn, budget); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(engineConn)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)

	svc := api.NewService(engineConn)
	// A program's step events and journal records go through the engine
	// relay that NATSAPI serves (#885).
	natsAPI := api.NewNATSAPI(svc, engineConn, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	stop, err := serve(context.Background(), ns.ClientURL(), key)
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	wb := dag.NewWorkflow("min.upper")
	wb.Task("upper", "min.upper")
	dagnatstest.Workflow(t, svc, wb)
	run := dagnatstest.RunAndWait(t, svc, "min.upper", []byte(`"hello"`), 20*time.Second)
	// Positive: the restricted program completed the task.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %s, want completed (steps %+v)", run.Status, run.Steps)
	}
	if out := string(run.Steps["upper"].Output); out != `"HELLO"` {
		t.Fatalf("output = %s, want \"HELLO\"", out)
	}
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// Negative: serving it took nothing the manifest did not grant.
	if v := log.snapshot(); len(v) > 0 {
		t.Fatalf("permission violations:\n%s", strings.Join(v, "\n"))
	}
	assertInboxConfined(t, ns, key, log)
}

// assertInboxConfined dials as min without the custom prefix: the
// server refuses the default "_INBOX.>" reply subscription.
func assertInboxConfined(t *testing.T, ns *natsserver.Server, key program.Key, log *violations) {
	t.Helper()
	nc, err := nats.Connect(ns.ClientURL(), key.ConnectOption())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	_, err = nc.Request("api.p.min.ping", nil, 500*time.Millisecond)
	// Negative: a request without the confined inbox gets no reply.
	if err == nil {
		t.Fatal("request on the default inbox succeeded")
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(log.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	// Positive: the refusal is a subscribe violation on _INBOX.
	if v := log.snapshot(); len(v) == 0 || !strings.Contains(strings.Join(v, "\n"), "_INBOX.") {
		t.Fatalf("violations = %v, want a refused _INBOX subscription", v)
	}
}
