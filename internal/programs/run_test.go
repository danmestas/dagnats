// run_test.go
// End-to-end tests for program.Run against the registration endpoint:
// a program registers over an anonymous _JOIN connection, attaches on
// its own inbox prefix, serves a task, and on restart (seed on disk)
// gets the same Grant without spending a join-key use; a refusal comes
// back as a typed *program.ErrRefused before anything is created; Stop
// makes no control-plane request.
// Methodology: real embedded NATS with the endpoint served by Serve, a
// minimal Program, and a subscription on api.> to observe every
// control-plane request the program makes.
package programs

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/program"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

type countingProgram struct {
	manifest program.Manifest
	handled  atomic.Int32
	stops    atomic.Int32
	// conn is the connection Start was handed.
	conn atomic.Pointer[nats.Conn]
}

func (p *countingProgram) Manifest() program.Manifest { return p.manifest }

func (p *countingProgram) Start(_ context.Context, h program.Host) error {
	if h.Conn == nil || h.Handle == nil {
		panic("countingProgram.Start: host must be complete")
	}
	p.conn.Store(h.Conn)
	h.Handle(p.manifest.Name+".work", func(tc worker.TaskContext) error {
		p.handled.Add(1)
		return tc.Complete(tc.Input())
	})
	return nil
}

func (p *countingProgram) Stop(context.Context) error {
	p.stops.Add(1)
	return nil
}

func serveFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	f := newFixture(t, cfg)
	service, err := Serve(f.nc, f.registrar)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Errorf("service.Stop: %v", err)
		}
	})
	return f
}

func TestRun_RegistersThenReRegistersOnRestart(t *testing.T) {
	f := serveFixture(t, Config{Open: true})
	key, plaintext := f.createKey(t, joinkey.Spec{Label: "k", UsesMax: 2})
	opts := program.Options{
		URL:      f.nc.ConnectedUrl(),
		SeedPath: filepath.Join(t.TempDir(), "notify.seed"),
		JoinKey:  plaintext,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for restart := 0; restart < 2; restart++ {
		prog := &countingProgram{manifest: manifest("notify")}
		running, err := program.Run(ctx, prog, opts)
		if err != nil {
			t.Fatalf("Run %d: %v", restart, err)
		}
		controlPlane := watchControlPlane(t, f.nc)
		if err := running.Stop(ctx); err != nil {
			t.Fatalf("stop %d: %v", restart, err)
		}
		if got := controlPlane.Load(); got != 0 {
			t.Fatalf("stop made %d control-plane requests, want 0", got)
		}
		if prog.stops.Load() != 1 {
			t.Fatalf("Stop calls = %d, want 1", prog.stops.Load())
		}
	}
	if got := f.usesLeft(t, key.ID); got != 1 {
		t.Fatalf("UsesLeft = %d, want 1: a restart spends no use", got)
	}
}

func TestRun_RefusalIsTypedBeforeAnythingIsCreated(t *testing.T) {
	f := serveFixture(t, Config{Open: false})
	prog := &countingProgram{manifest: manifest("notify")}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := program.Run(ctx, prog, program.Options{
		URL: f.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "seed"),
	})
	var refusal *program.ErrRefused
	if !errors.As(err, &refusal) || refusal.Code != program.RefusedAuthModeUnsupported {
		t.Fatalf("Run err = %v, want auth_mode_unsupported", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("refusal took %v, want under a second", elapsed)
	}
	if _, err := f.js.Stream(ctx, "NOTIFY_EVENTS"); err == nil {
		t.Fatal("a refused program's stream was created")
	}
}

func TestRun_ServesTasks(t *testing.T) {
	f := serveFixture(t, Config{Open: true})
	prog := &countingProgram{manifest: manifest("notify")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	running, err := program.Run(ctx, prog, program.Options{
		URL: f.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "seed"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		if err := running.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
	if prog.handled.Load() != 0 {
		t.Fatal("handled a task before any was sent")
	}
	orch := engine.NewOrchestrator(f.nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	svc := api.NewService(f.nc)
	wb := dag.NewWorkflow("notify.send")
	wb.Task("work", "notify.work")
	dagnatstest.Workflow(t, svc, wb)
	run := dagnatstest.RunAndWait(t, svc, "notify.send", []byte(`"hi"`), 20*time.Second)
	if run.Status != dag.RunStatusCompleted || prog.handled.Load() != 1 {
		t.Fatalf("run %s, handled %d; want completed and 1", run.Status, prog.handled.Load())
	}
}

// TestRun_CallerOptionsCannotOverrideIsolation passes options that try
// to take the shared inbox and cap reconnects; Run's own win, and the
// caller's ClosedHandler is chained, not dropped.
func TestRun_CallerOptionsCannotOverrideIsolation(t *testing.T) {
	f := serveFixture(t, Config{Open: true})
	prog := &countingProgram{manifest: manifest("notify")}
	callerClosed := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	running, err := program.Run(ctx, prog, program.Options{
		URL: f.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "seed"),
		NATSOptions: []nats.Option{
			nats.CustomInboxPrefix("_INBOX"), nats.MaxReconnects(3),
			nats.ClosedHandler(func(*nats.Conn) { close(callerClosed) }),
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	conn := prog.conn.Load()
	if conn.Opts.InboxPrefix != program.InboxPrefix("notify") {
		t.Fatalf("inbox prefix = %q, want %q", conn.Opts.InboxPrefix, program.InboxPrefix("notify"))
	}
	opts := conn.Opts
	if opts.MaxReconnect != -1 || opts.ReconnectWait <= 0 || opts.ReconnectJitter <= 0 {
		t.Fatalf("reconnect policy = max %d wait %v jitter %v, want forever with jitter",
			opts.MaxReconnect, opts.ReconnectWait, opts.ReconnectJitter)
	}
	if err := running.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-callerClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("the caller's ClosedHandler never ran")
	}
	if err := waitDone(t, running); err != nil {
		t.Fatalf("Done after Stop = %v, want nil", err)
	}
}

// TestRun_DoneFiresWhenTheConnectionClosesForGood closes the program's
// connection behind Run's back, as a reconnect limit or a fatal auth
// error would; Done must report it so the host can exit.
func TestRun_DoneFiresWhenTheConnectionClosesForGood(t *testing.T) {
	f := serveFixture(t, Config{Open: true})
	prog := &countingProgram{manifest: manifest("notify")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	running, err := program.Run(ctx, prog, program.Options{
		URL: f.nc.ConnectedUrl(), SeedPath: filepath.Join(t.TempDir(), "seed"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	select {
	case err := <-running.Done():
		t.Fatalf("Done fired while connected: %v", err)
	default:
	}
	prog.conn.Load().Close()
	if err := waitDone(t, running); err == nil {
		t.Fatal("Done reported nil for a close nobody asked for")
	}
	if err := running.Stop(ctx); err != nil {
		t.Fatalf("Stop after a close: %v", err)
	}
}

func waitDone(t *testing.T, running *program.Running) error {
	t.Helper()
	select {
	case err := <-running.Done():
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Done never fired")
	}
	return nil
}

// watchControlPlane counts requests on api.> from now on.
func watchControlPlane(t *testing.T, nc *nats.Conn) *atomic.Int32 {
	t.Helper()
	var count atomic.Int32
	sub, err := nc.Subscribe("api.>", func(*nats.Msg) { count.Add(1) })
	if err != nil {
		t.Fatalf("subscribe api.>: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	})
	return &count
}
