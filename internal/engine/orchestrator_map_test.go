// engine/orchestrator_map_test.go
// Map-step fan-out tests for the orchestrator: fan-out over a collection,
// real workflow-name propagation, control-plane capability stripping,
// fail-fast behavior, and map-instance ID helpers. Uses real embedded NATS
// server (except the pure helper test).
// Methodology: publish events for a map step, let the orchestrator process
// them, then verify per-item child tasks and identity/capability
// propagation. Each test gets its own embedded server.

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestratorMapStepFanOut(t *testing.T) {
	// Methodology: workflow has fetch -> map -> summarize.
	// fetch returns a JSON array of 3 items.
	// map processes each item (3 instances).
	// summarize receives the collected array of results.
	// Verify: all 3 map instances complete, summarize gets [r0, r1, r2].
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "map-fanout", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:      dag.StepTypeMap,
				DependsOn: []string{"fetch"},
				Config:    dag.MarshalConfig(&dag.MapConfig{MaxItems: 10}),
			},
			{
				ID: "summarize", Task: "summarize-task",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"process"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-run-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain fetch task.
	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := fetchSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	msgs[0].Ack()

	// Complete fetch with a JSON array of 3 items.
	fetchOutput := []byte(`["item-a","item-b","item-c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-1",
		"fetch", fetchOutput)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for 3 map instance tasks to appear. Use a
	// polling loop because CI runners may be slow to
	// deliver all 3 messages in a single Fetch call.
	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	var mapMsgs []*nats.Msg
	fetchDeadline := time.After(10 * time.Second)
	for len(mapMsgs) < 3 {
		batch, fetchErr := mapSub.Fetch(
			3-len(mapMsgs),
			nats.MaxWait(2*time.Second))
		if fetchErr == nil {
			mapMsgs = append(mapMsgs, batch...)
		}
		select {
		case <-fetchDeadline:
			t.Fatalf("expected 3 map tasks, got %d",
				len(mapMsgs))
		default:
		}
	}
	// Positive: exactly 3 map instance tasks published.
	if len(mapMsgs) != 3 {
		t.Fatalf("expected 3 map tasks, got %d", len(mapMsgs))
	}

	// Complete all 3 map instances.
	for i := 0; i < 3; i++ {
		mapMsgs[i].Ack()
		instanceID := fmt.Sprintf("process.map.%d", i)
		result := []byte(fmt.Sprintf(`"result-%d"`, i))
		evt := protocol.NewStepEvent(
			protocol.EventStepCompleted, "map-run-1",
			instanceID, result)
		data, err := evt.Marshal()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		msgID := fmt.Sprintf(
			"map-run-1.%s.completed", instanceID)
		mustPublish(t, js, evt.NATSSubject(), data,
			nats.MsgId(msgID))
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for summarize task.
	sumSub, _ := js.PullSubscribe(
		"task.summarize-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	sumMsgs, err := sumSub.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch summarize-task failed: %v", err)
	}
	// Positive: summarize task receives collected array.
	if len(sumMsgs) != 1 {
		t.Fatalf("expected 1 summarize task, got %d",
			len(sumMsgs))
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(
		sumMsgs[0].Data, &payload,
	); err != nil {
		t.Fatalf("unmarshal summarize payload: %v", err)
	}
	// Verify input is the collected array.
	var collected []json.RawMessage
	if err := json.Unmarshal(
		payload.Input, &collected,
	); err != nil {
		t.Fatalf("unmarshal collected: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("collected len = %d, want 3",
			len(collected))
	}

	// Complete summarize -> workflow completes.
	sumMsgs[0].Ack()
	sumEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-1",
		"summarize", []byte(`"final"`))
	sumData, err := sumEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, sumEvt.NATSSubject(), sumData,
		nats.MsgId(sumEvt.NATSMsgID()))

	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "map-run-1")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			// Positive: workflow completed.
			if run.Steps["process"].Status !=
				dag.StepStatusCompleted {
				t.Fatalf("process = %v, want Completed",
					run.Steps["process"].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should complete after map fan-out")
}

func TestOrchestratorMapStepFanOutCarriesRealWorkflowName(t *testing.T) {
	// Methodology: #513 regression guard (C1). Same fixture as
	// TestOrchestratorMapStepFanOut (Map step with no RequiredCapabilities).
	// After fetching the 3 map instance tasks, unmarshal each into
	// protocol.TaskPayload and assert WorkflowName equals the parent run's
	// workflow definition name (wfDef.Name), never "". RED against main
	// (which forges an empty workflow name for map instances), GREEN after
	// the fix (publishMapTasks threads wfDef.Name through unmodified).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "map-fanout", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:      dag.StepTypeMap,
				DependsOn: []string{"fetch"},
				Config:    dag.MarshalConfig(&dag.MapConfig{MaxItems: 10}),
			},
			{
				ID: "summarize", Task: "summarize-task",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"process"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-run-name-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := fetchSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	msgs[0].Ack()

	fetchOutput := []byte(`["item-a","item-b","item-c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-name-1",
		"fetch", fetchOutput)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	var mapMsgs []*nats.Msg
	fetchDeadline := time.After(10 * time.Second)
	for len(mapMsgs) < 3 {
		batch, fetchErr := mapSub.Fetch(
			3-len(mapMsgs),
			nats.MaxWait(2*time.Second))
		if fetchErr == nil {
			mapMsgs = append(mapMsgs, batch...)
		}
		select {
		case <-fetchDeadline:
			t.Fatalf("expected 3 map tasks, got %d",
				len(mapMsgs))
		default:
		}
	}
	if len(mapMsgs) != 3 {
		t.Fatalf("expected 3 map tasks, got %d", len(mapMsgs))
	}

	for i, msg := range mapMsgs {
		msg.Ack()
		var payload protocol.TaskPayload
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Fatalf("unmarshal map instance %d payload: %v", i, err)
		}
		// Positive: WorkflowName equals the parent run's workflow definition name.
		if payload.WorkflowName != wfDef.Name {
			t.Fatalf(
				"map instance %d WorkflowName = %q, want %q",
				i, payload.WorkflowName, wfDef.Name,
			)
		}
		// Negative space: never the forged-empty telemetry name (the bug).
		if payload.WorkflowName == "" {
			t.Fatalf("map instance %d WorkflowName is empty, want %q", i, wfDef.Name)
		}
	}
}

func TestOrchestratorMapStepFanOutStripsControlPlaneCapabilityRegardlessOfGrant(t *testing.T) {
	// Methodology: #513 invariant-preservation guard (C2/C3/C4). Same
	// fixture, but the Map step declares RequiredCapabilities including
	// "control-plane", and the orchestrator is constructed with a
	// GrantPolicy that DOES grant the workflow's name. Verify the map
	// instances still never carry "control-plane" (the #380 deny-by-default
	// invariant for map instances holds unconditionally, even when granted),
	// that unrelated capabilities like "gpu" survive unstripped, and that
	// WorkflowName is still the real name (the #513 fix coexists with the
	// preserved #380 invariant in the same dispatch).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "map-fanout", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:                 dag.StepTypeMap,
				DependsOn:            []string{"fetch"},
				Config:               dag.MarshalConfig(&dag.MapConfig{MaxItems: 10}),
				RequiredCapabilities: []string{"control-plane", "gpu"},
			},
			{
				ID: "summarize", Task: "summarize-task",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"process"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	holder := &GrantPolicyHolder{}
	holder.Store(NewGrantPolicy([]string{"map-fanout"}, nil))
	orch := NewOrchestrator(nc, WithGrantPolicyHolder(holder))
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-run-grant-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := fetchSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	msgs[0].Ack()

	fetchOutput := []byte(`["item-a","item-b","item-c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-grant-1",
		"fetch", fetchOutput)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	var mapMsgs []*nats.Msg
	fetchDeadline := time.After(10 * time.Second)
	for len(mapMsgs) < 3 {
		batch, fetchErr := mapSub.Fetch(
			3-len(mapMsgs),
			nats.MaxWait(2*time.Second))
		if fetchErr == nil {
			mapMsgs = append(mapMsgs, batch...)
		}
		select {
		case <-fetchDeadline:
			t.Fatalf("expected 3 map tasks, got %d",
				len(mapMsgs))
		default:
		}
	}
	if len(mapMsgs) != 3 {
		t.Fatalf("expected 3 map tasks, got %d", len(mapMsgs))
	}

	for i, msg := range mapMsgs {
		msg.Ack()
		var payload protocol.TaskPayload
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Fatalf("unmarshal map instance %d payload: %v", i, err)
		}
		// Positive: control-plane is stripped even though "map-fanout" IS
		// granted -- proves #380 is not weakened by #513's name change.
		if slices.Contains(payload.RequiredCapabilities, "control-plane") {
			t.Fatalf(
				"map instance %d RequiredCapabilities = %v, must not contain control-plane",
				i, payload.RequiredCapabilities,
			)
		}
		// Negative space: unrelated capabilities survive unstripped.
		if !slices.Contains(payload.RequiredCapabilities, "gpu") {
			t.Fatalf(
				"map instance %d RequiredCapabilities = %v, want gpu preserved",
				i, payload.RequiredCapabilities,
			)
		}
		// The #513 fix: real workflow name still flows through in the same dispatch.
		if payload.WorkflowName != wfDef.Name {
			t.Fatalf(
				"map instance %d WorkflowName = %q, want %q",
				i, payload.WorkflowName, wfDef.Name,
			)
		}
	}
}

func TestOrchestratorMapStepFailFast(t *testing.T) {
	// Methodology: workflow has fetch -> map.
	// One map instance fails. Verify: map step and workflow fail.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "map-fail", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:      dag.StepTypeMap,
				DependsOn: []string{"fetch"},
				Config:    dag.MarshalConfig(&dag.MapConfig{MaxItems: 10}),
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-fail-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete fetch.
	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	fMsgs, err := fetchSub.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	fMsgs[0].Ack()

	fetchOutput := []byte(`["a","b","c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-fail-1",
		"fetch", fetchOutput)
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for map tasks.
	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	mapMsgs, err := mapSub.Fetch(
		3, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch map tasks failed: %v", err)
	}
	for _, m := range mapMsgs {
		m.Ack()
	}

	// Fail instance 1.
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "map-fail-1",
		"process.map.1", []byte(`"instance error"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, failEvt.NATSSubject(), failData,
		nats.MsgId("map-fail-1.process.map.1.failed"))

	// Verify workflow fails.
	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "map-fail-1")
		if err == nil &&
			run.Status == dag.RunStatusFailed {
			// Positive: map step is failed.
			if run.Steps["process"].Status !=
				dag.StepStatusFailed {
				t.Fatalf("process = %v, want Failed",
					run.Steps["process"].Status)
			}
			// Positive: map instance 1 is failed.
			inst := run.Steps["process"].MapInstances
			if len(inst) != 3 {
				t.Fatalf("MapInstances len = %d, want 3",
					len(inst))
			}
			if inst[1].Status != dag.StepStatusFailed {
				t.Fatalf("instance[1] = %v, want Failed",
					inst[1].Status)
			}
			// Positive: failing a map step with no on-failure
			// handler is terminal, so CompletedAt is stamped —
			// the Traces "Duration" reports an honest value.
			if run.CompletedAt == nil {
				t.Fatal("CompletedAt = nil, want non-nil on map-failed run")
			}
			// Negative: the stamp never precedes creation.
			if run.CompletedAt.Before(run.CreatedAt) {
				t.Fatalf("CompletedAt %v before CreatedAt %v",
					run.CompletedAt, run.CreatedAt)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should fail after map instance failure")
}

func TestMapInstanceIDHelpers(t *testing.T) {
	// Methodology: unit tests for map instance ID construction
	// and parsing utilities.

	// Positive: mapInstanceID constructs correct format.
	id := mapInstanceID("process", 2)
	if id != "process.map.2" {
		t.Fatalf("mapInstanceID = %q, want process.map.2", id)
	}

	// Positive: isMapInstanceID detects compound IDs.
	if !isMapInstanceID("process.map.0") {
		t.Fatal("process.map.0 should be a map instance ID")
	}

	// Negative: normal step IDs are not map instances.
	if isMapInstanceID("process") {
		t.Fatal("process should not be a map instance ID")
	}

	// Positive: parseMapInstanceID extracts base and index.
	base, idx := parseMapInstanceID("process.map.5")
	if base != "process" || idx != 5 {
		t.Fatalf("parse = (%q, %d), want (process, 5)",
			base, idx)
	}
}
