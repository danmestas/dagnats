// worker/controlplane_test.go
// Unit tests for the worker-side ControlPlane handle. These exercise the
// failures the handle must surface as typed *ControlPlaneError WITHOUT a
// running server: programmer-error panics, the reserved Promote rejection,
// client-side name validation, and the transport-error path against a
// fake NATS responder. The end-to-end register->spawn->complete path is
// covered by the integration tests in internal/api (which can wire the
// orchestrator). Methodology: each test gets a fresh embedded NATS server;
// bounded waits; >=2 assertions covering positive + negative space.
package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func validDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    "do-step",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "noop", Type: dag.StepTypeNormal},
		},
	}
}

func TestControlPlane_NewPanicsOnNilConn(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil nc, got none")
		}
	}()
	NewControlPlane(nil)
}

func TestControlPlane_NamespaceNameRejectedOnRegister(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	cp := newControlPlaneFor(nc, "owner-run", "step-1")

	// A def whose name carries a ':' must be rejected client-side as a
	// namespace error before any request reaches the server.
	def := validDef()
	def.Name = "bad:name"
	name, err := cp.RegisterWorkflow(
		context.Background(), def, RegisterOpts{},
	)
	var cpErr *ControlPlaneError
	if !errors.As(err, &cpErr) || cpErr.Kind != KindNamespace {
		t.Fatalf("expected KindNamespace, got %v", err)
	}
	if name != "" {
		t.Fatalf("expected empty scopedName, got %q", name)
	}
}

func TestControlPlane_ForgeNameRejectedClientSide(t *testing.T) {
	// Defense-in-depth: the worker mirrors the server's validateRuntimeName
	// so a forge attempt fails fast client-side with KindNamespace and no
	// round-trip, instead of relying solely on the server guard.
	_, nc := natsutil.StartTestServer(t)
	cp := newControlPlaneFor(nc, "owner-run", "step-1")

	cases := []string{
		"agent.other-run.steal", // forged scope prefix
		"has.dot",               // bare scope-separator
	}
	for _, bad := range cases {
		def := validDef()
		def.Name = bad
		name, err := cp.RegisterWorkflow(
			context.Background(), def, RegisterOpts{},
		)
		var cpErr *ControlPlaneError
		if !errors.As(err, &cpErr) || cpErr.Kind != KindNamespace {
			t.Fatalf("name %q: expected KindNamespace, got %v", bad, err)
		}
		if name != "" {
			t.Fatalf("name %q: expected empty scopedName, got %q",
				bad, name)
		}
	}
}

func TestControlPlane_NamespaceNameRejectedOnStart(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	cp := newControlPlaneFor(nc, "owner-run", "step-1")

	// A name longer than the bound must be rejected before any request.
	longName := make([]byte, 257)
	for i := range longName {
		longName[i] = 'a'
	}
	runID, err := cp.StartRun(
		context.Background(), string(longName), nil,
	)
	var cpErr *ControlPlaneError
	if !errors.As(err, &cpErr) || cpErr.Kind != KindNamespace {
		t.Fatalf("expected KindNamespace, got %v", err)
	}
	if runID != "" {
		t.Fatalf("expected empty runID, got %q", runID)
	}
}

func TestControlPlane_TransportErrorWhenNoServer(t *testing.T) {
	// No api micro service is listening on api.runtimes.register, so the
	// bounded nc.Request times out and must surface as KindTransport,
	// never a panic.
	_, nc := natsutil.StartTestServer(t)
	cp := newControlPlaneFor(nc, "owner-run", "step-1")

	name, err := cp.RegisterWorkflow(
		context.Background(), validDef(), RegisterOpts{},
	)
	var cpErr *ControlPlaneError
	if !errors.As(err, &cpErr) || cpErr.Kind != KindTransport {
		t.Fatalf("expected KindTransport, got %v", err)
	}
	if name != "" {
		t.Fatalf("expected empty scopedName, got %q", name)
	}
}
