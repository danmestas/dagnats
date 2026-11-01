// api/def_noop_write_test.go
//
// Tests that re-registering an unchanged workflow writes nothing. A
// program re-registers every workflow on each boot, and on an R3 mesh
// every KV write is a replicated round trip, so a byte-identical
// register must cost reads only. A changed def still writes its version
// and moves the pointer, including back onto an older retained version.
// Methodology: real embedded NATS server + Service; KV_workflow_defs'
// last stream sequence counts writes. Positive and negative space.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// defsLastSeq is KV_workflow_defs' last stream sequence: it moves on
// every write to the bucket, put, create or delete.
func defsLastSeq(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "KV_workflow_defs")
	if err != nil {
		t.Fatalf("stream KV_workflow_defs: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	return info.State.LastSeq
}

func defsService(t *testing.T) (*Service, jetstream.JetStream) {
	t.Helper()
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return NewService(nc), js
}

func TestRegisterWorkflowUnchangedWritesNothing(t *testing.T) {
	svc, js := defsService(t)
	def := versionedDef("noop-wf", 2)
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("first register: %v", err)
	}
	before := defsLastSeq(t, js)
	for i := 0; i < 3; i++ {
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("re-register %d: %v", i, err)
		}
	}
	// Negative: no write reached the bucket.
	if after := defsLastSeq(t, js); after != before {
		t.Fatalf("workflow_defs last seq %d -> %d, want no writes", before, after)
	}
	// Positive: the definition is still registered as it was.
	got, err := svc.GetWorkflow("noop-wf")
	if err != nil || dag.DefHash(got) != dag.DefHash(def) {
		t.Fatalf("GetWorkflow = %+v, %v; want the registered def", got, err)
	}
}

func TestRegisterWorkflowChangedStillWritesVersionAndPointer(t *testing.T) {
	svc, js := defsService(t)
	first, second := versionedDef("moving-wf", 1), versionedDef("moving-wf", 2)
	register := func(def dag.WorkflowDef) {
		t.Helper()
		if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	pointer := func() string {
		t.Helper()
		got, err := svc.GetWorkflow("moving-wf")
		if err != nil {
			t.Fatalf("GetWorkflow: %v", err)
		}
		return dag.DefHash(got)
	}
	register(first)
	before := defsLastSeq(t, js)
	register(second)
	// Positive: a new def writes its version and moves the pointer.
	if after := defsLastSeq(t, js); after != before+2 {
		t.Fatalf("workflow_defs last seq %d -> %d, want +2 (version, pointer)", before, after)
	}
	if pointer() != dag.DefHash(second) {
		t.Fatal("pointer did not move to the changed def")
	}
	before = defsLastSeq(t, js)
	register(first)
	// Negative space of the no-op: an already-retained version still
	// moves the pointer back onto it, a single write.
	if after := defsLastSeq(t, js); after != before+1 {
		t.Fatalf("workflow_defs last seq %d -> %d, want +1 (pointer)", before, after)
	}
	if pointer() != dag.DefHash(first) {
		t.Fatal("pointer did not move back to the retained def")
	}
}
