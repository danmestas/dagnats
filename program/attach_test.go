// program/attach_test.go
// Tests for Attach, the one path that calls a program's Start.
// Methodology: a real embedded server with the program's restricted
// user and a literal Grant (D2's attach handler does not exist yet).
// Counters on the toy program show how often Start and Stop ran; runs
// through the engine show the worker Attach starts actually serves.
package program_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/program"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func expectPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		// Negative: the call did not return normally.
		if r == nil {
			t.Fatalf("no panic, want one mentioning %q", want)
		}
		// Positive: the panic names the broken contract.
		if !strings.Contains(strings.ToLower(asString(r)), want) {
			t.Fatalf("panic %q, want it to mention %q", r, want)
		}
	}()
	fn()
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if e, ok := v.(error); ok {
		return e.Error()
	}
	return ""
}

func TestAttach_PanicsWithoutInboxPrefix(t *testing.T) {
	m := validManifest()
	f := startFixture(t, m)
	prog := &toyProgram{manifest: m}
	ctx := context.Background()
	bare := dial(t, f.server.ClientURL())
	expectPanic(t, "inbox", func() { _, _ = program.Attach(ctx, bare, prog, toyGrant()) })
	foreign := dial(t, f.server.ClientURL(), nats.CustomInboxPrefix("_INBOX.other"))
	expectPanic(t, "inbox", func() { _, _ = program.Attach(ctx, foreign, prog, toyGrant()) })
	// Negative: neither refused Attach reached Start.
	if n := prog.starts.Load(); n != 0 {
		t.Fatalf("Start ran %d times, want 0", n)
	}
}

func TestAttach_StartStopIdempotent(t *testing.T) {
	m := validManifest()
	f, svc := startEngineFixture(t, m)
	nc := f.dialProgram(t, m.Name)
	prog := &toyProgram{manifest: m}
	ctx := context.Background()
	for round := 1; round <= 2; round++ {
		stop, err := program.Attach(ctx, nc, prog, toyGrant())
		if err != nil {
			t.Fatalf("round %d Attach: %v", round, err)
		}
		runToyWorkflow(t, svc, dag.RunStatusCompleted)
		for i := 0; i < 2; i++ {
			if err := stop(ctx); err != nil {
				t.Fatalf("round %d stop %d: %v", round, i, err)
			}
		}
	}
	// Positive: each Attach started once and stopped once.
	if s, p := prog.starts.Load(), prog.stops.Load(); s != 2 || p != 2 {
		t.Fatalf("starts=%d stops=%d, want 2 and 2", s, p)
	}
	// Negative: a repeated stop did not reach the program again.
	if v := f.violations.snapshot(); len(v) > 0 {
		t.Fatalf("permission violations: %v", v)
	}
}

func TestAttach_HandlerPanicFailsTaskNotProcess(t *testing.T) {
	m := validManifest()
	f, svc := startEngineFixture(t, m)
	prog := &toyProgram{manifest: m, handle: func(worker.TaskContext) error {
		panic("toy bug")
	}}
	ctx := context.Background()
	stop, err := program.Attach(ctx, f.dialProgram(t, m.Name), prog, toyGrant())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Cleanup(func() { _ = stop(ctx) })
	// Positive: the panicking task fails its run.
	runToyWorkflow(t, svc, dag.RunStatusFailed)
	// Negative: the process survived to run the next one too.
	runToyWorkflow(t, svc, dag.RunStatusFailed)
}

func TestAttach_RefusesUndeclaredTaskTypeAndBadGrant(t *testing.T) {
	m := validManifest()
	f, _ := startEngineFixture(t, m)
	nc := f.dialProgram(t, m.Name)
	ctx := context.Background()
	undeclared := &toyProgram{manifest: m, handle: nil}
	undeclared.manifest.TaskTypes = []string{"toy.send.email"}
	// Negative: handling a task type the manifest does not declare is a
	// programmer error, caught before NATS would time it out.
	expectPanic(t, "declare", func() { _, _ = program.Attach(ctx, nc, undeclared, toyGrant()) })
	grant := toyGrant()
	delete(grant.StreamMaxBytes, "TOY_EVENTS")
	// Negative: a Grant that does not size a declared stream is refused.
	if _, err := program.Attach(ctx, nc, &toyProgram{manifest: m}, grant); err == nil {
		t.Fatal("Attach accepted a Grant without TOY_EVENTS")
	}
	bad := validManifest()
	bad.TaskTypes = []string{"echo"}
	// Positive: an invalid manifest is an error, not a half-started worker.
	if _, err := program.Attach(ctx, nc, &toyProgram{manifest: bad}, toyGrant()); err == nil {
		t.Fatal("Attach accepted an invalid manifest")
	}
}

// TestAttach_FailsFastWithoutAControlPlane: with no dagnats answering
// the program relay, Attach returns an error naming it instead of
// starting a worker whose every run would stall at running.
func TestAttach_FailsFastWithoutAControlPlane(t *testing.T) {
	m := validManifest()
	f := startFixture(t, m)
	prog := &toyProgram{manifest: m}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := program.Attach(ctx, f.dialProgram(t, m.Name), prog, toyGrant())
	if !errors.Is(err, program.ErrRelayUnavailable) {
		t.Fatalf("Attach err = %v, want ErrRelayUnavailable", err)
	}
	if prog.starts.Load() != 0 {
		t.Fatal("Start ran with no control plane")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Attach took %v to fail, want fast", elapsed)
	}
}

// startEngineFixture adds an orchestrator and an API service, both on the
// admin connection, to the permission fixture.
func startEngineFixture(t *testing.T, m program.Manifest) (*fixture, *api.Service) {
	t.Helper()
	f := startFixture(t, m)
	orch := engine.NewOrchestrator(f.admin)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	admin, err := jetstream.New(f.admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	// The host provisions; the program's user may not create streams.
	if err := program.Provision(context.Background(), admin, m, toyGrant()); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	svc := api.NewService(f.admin)
	// The program's step events and journal records go through the
	// engine relay NATSAPI serves.
	natsAPI := api.NewNATSAPI(svc, f.admin, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	wb := dag.NewWorkflow("toy.echo")
	send := wb.Task("send", "toy.send.email").After(wb.Task("echo", "toy.echo"))
	wb.Task("wait", "toy.wait").After(send)
	dagnatstest.Workflow(t, svc, wb)
	return f, svc
}

func runToyWorkflow(t *testing.T, svc *api.Service, want dag.RunStatus) {
	t.Helper()
	run := dagnatstest.RunAndWait(t, svc, "toy.echo", []byte(`"hi"`), 20*time.Second)
	if run.Status != want {
		t.Fatalf("run status = %s, want %s (steps %+v)", run.Status, want, run.Steps)
	}
}
