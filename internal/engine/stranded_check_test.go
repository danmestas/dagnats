// internal/engine/stranded_check_test.go
// Methodology: real embedded NATS server + real Orchestrator, same
// conventions as reconciler_test.go's captureSlog. Seed a workflow def
// with a grouped step and a message on that pair's pre-#704 legacy
// subject, then drive Orchestrator.Start() and assert the stranded
// subject is reported (count, task, group) without Start failing.
// Bounded waits via captureSlog's synchronous check (Start runs the
// check inline before returning).
package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// defKVBucket is the workflow_defs bucket name, matching the constant
// natsutil.SetupKVBuckets provisions.
const defKVBucket = "workflow_defs"

// TestCheckStrandedGroupSubjects_ReportsAndDoesNotFailStartup seeds a
// legacy grouped subject with pending messages, registers a workflow
// def whose step carries the SAME (task, group) pair, then calls
// Start() and asserts: (1) Start returns nil -- a stranded subject must
// never fail startup, (2) the log names the subject and the count.
func TestCheckStrandedGroupSubjects_ReportsAndDoesNotFailStartup(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, err := js.KeyValue(context.Background(), defKVBucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "gpu-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "train", Task: "render", WorkerGroup: "gpu",
				Type: dag.StepTypeNormal,
			},
		},
	}
	mustPutJS(t, context.Background(), defKV, wfDef.Name, mustMarshal(t, wfDef))

	// Seed a message on the pre-#674 legacy grouped subject -- the shape
	// a process from either pre-#674 or pre-#704 could have published.
	legacySubject := "task.render.gpu.deadbeefdeadbeefdeadbeefdeadbeef"
	if _, err := js.Publish(
		context.Background(), legacySubject, []byte("stranded"),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	buf, restore := captureSlog(t)
	defer restore()

	orch := NewOrchestrator(nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("Start() = %v, want nil (a stranded subject must not "+
			"fail startup)", err)
	}
	defer orch.Stop()

	logged := buf.String()
	if !strings.Contains(logged, legacySubject) {
		t.Fatalf("log = %q, want it to name the stranded subject %q",
			logged, legacySubject)
	}
	if !strings.Contains(logged, "pending=1") {
		t.Fatalf("log = %q, want the pending count", logged)
	}
	if !strings.Contains(logged, "task=render") || !strings.Contains(logged, "group=gpu") {
		t.Fatalf("log = %q, want task=render group=gpu", logged)
	}
}

// TestCheckStrandedGroupSubjects_SilentWhenClean proves the check does
// not log anything for a workflow def whose grouped pair has no pending
// legacy messages -- a check that always fires on ANY grouped def would
// be useless noise.
func TestCheckStrandedGroupSubjects_SilentWhenClean(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, err := js.KeyValue(context.Background(), defKVBucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "gpu-wf-clean", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "train", Task: "render", WorkerGroup: "gpu",
				Type: dag.StepTypeNormal,
			},
		},
	}
	mustPutJS(t, context.Background(), defKV, wfDef.Name, mustMarshal(t, wfDef))

	buf, restore := captureSlog(t)
	defer restore()

	orch := NewOrchestrator(nc)
	if err := orch.Start(); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	defer orch.Stop()

	if strings.Contains(buf.String(), "stranded grouped task subject") {
		t.Fatalf("log = %q, want no stranded-subject report on a clean "+
			"pair", buf.String())
	}
}
