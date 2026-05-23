package configfile

// Methodology: integration tests against an embedded NATS server.
// Each test seeds workflow_defs + triggers, runs Apply, then asserts
// the KV state matches the expected outcome. Positive: the records
// the Plan promised appear; negative: KV entries the Plan did not
// touch remain unchanged.

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
)

// openForTest is an os.Open shim so test helpers stay readable.
func openForTest(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

// kvHandles boots an embedded NATS, sets up the buckets we need,
// and returns ready-to-use handles. Callers cleanup via t.Cleanup
// inside natsutil.StartTestServer.
func kvHandles(t *testing.T) KVHandles {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wfKV, err := js.KeyValue(ctx, "workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs: %v", err)
	}
	trKV, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("triggers: %v", err)
	}
	return KVHandles{WorkflowDefs: wfKV, Triggers: trKV}
}

func TestApplyAddsWorkflowAndTrigger(t *testing.T) {
	kv := kvHandles(t)
	wf := dag.WorkflowDef{
		Name: "hello",
		Steps: []dag.StepDef{
			{ID: "a", Task: "echo", Timeout: 30 * time.Second},
		},
	}
	tr := trigger.TriggerDef{
		ID: "t1", WorkflowID: "hello", Enabled: true,
		Source: SourceFilePrefix + "dagnats.yaml",
		Cron: &trigger.CronConfig{
			Expression: "* * * * *", Timezone: "UTC",
		},
	}
	plan := Plan{
		WorkflowsAdd: []dag.WorkflowDef{wf},
		TriggersAdd:  []trigger.TriggerDef{tr},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Apply(ctx, kv, plan); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Positive: both KV entries appear with the expected content.
	wfEntry, err := kv.WorkflowDefs.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("workflow_defs Get hello: %v", err)
	}
	var loadedWF dag.WorkflowDef
	if err := json.Unmarshal(wfEntry.Value(), &loadedWF); err != nil {
		t.Fatalf("unmarshal workflow: %v", err)
	}
	if loadedWF.Name != "hello" || len(loadedWF.Steps) != 1 {
		t.Fatalf("workflow load mismatch: %+v", loadedWF)
	}

	trEntry, err := kv.Triggers.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("triggers Get t1: %v", err)
	}
	var loadedTR trigger.TriggerDef
	if err := json.Unmarshal(trEntry.Value(), &loadedTR); err != nil {
		t.Fatalf("unmarshal trigger: %v", err)
	}
	if loadedTR.Source != SourceFilePrefix+"dagnats.yaml" {
		t.Fatalf("Source = %q, want file:dagnats.yaml",
			loadedTR.Source)
	}
}

func TestApplyRemovesTrigger(t *testing.T) {
	kv := kvHandles(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-seed a file-managed trigger.
	src := SourceFilePrefix + "dagnats.yaml"
	seeded := trigger.TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Source: src,
		Cron:   &trigger.CronConfig{Expression: "* * * * *"},
	}
	data, _ := json.Marshal(seeded)
	if _, err := kv.Triggers.Put(ctx, "t1", data); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	plan := Plan{TriggersRemove: []string{"t1"}}
	if err := Apply(ctx, kv, plan); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := kv.Triggers.Get(ctx, "t1"); err == nil {
		t.Fatalf("trigger t1 still present after remove")
	}
}

func TestReadCurrentReturnsOnlyFileManagedTriggers(t *testing.T) {
	kv := kvHandles(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := SourceFilePrefix + "dagnats.yaml"

	file := trigger.TriggerDef{
		ID: "file-1", WorkflowID: "wf", Enabled: true,
		Source: src,
		Cron:   &trigger.CronConfig{Expression: "* * * * *"},
	}
	kvOnly := trigger.TriggerDef{
		ID: "kv-1", WorkflowID: "wf", Enabled: true,
		// no Source → legacy / API-written record
		Cron: &trigger.CronConfig{Expression: "* * * * *"},
	}
	for _, def := range []trigger.TriggerDef{file, kvOnly} {
		data, _ := json.Marshal(def)
		if _, err := kv.Triggers.Put(ctx, def.ID, data); err != nil {
			t.Fatalf("seed Put %s: %v", def.ID, err)
		}
	}

	cur, err := ReadCurrent(ctx, kv, src)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if _, ok := cur.Triggers["file-1"]; !ok {
		t.Fatalf("file-managed trigger missing from ReadCurrent")
	}
	if _, ok := cur.Triggers["kv-1"]; ok {
		t.Fatalf("KV-managed trigger leaked into ReadCurrent")
	}
}

// e2e: file edit triggers a real watcher reload, the resulting plan
// is applied through Apply, and the new trigger appears in the
// triggers KV. End-to-end coverage of the Phase-4 promise.
func TestE2EWatcherToKVHotReload(t *testing.T) {
	kv := kvHandles(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	cfgPath := dir + "/dagnats.yaml"
	writeFile(t, cfgPath, initialYAML)

	src := SourceLabel("dagnats.yaml")

	reload := func(cfg ConfigFile) error {
		desired := DesiredState{
			Workflows: map[string]dag.WorkflowDef{},
			Triggers:  map[string]trigger.TriggerDef{},
		}
		for _, wf := range cfg.Workflows {
			desired.Workflows[wf.Name] = ToWorkflowDef(wf)
		}
		for _, tr := range cfg.Triggers {
			desired.Triggers[tr.ID] = ToTriggerDef(tr, src)
		}
		current, err := ReadCurrent(ctx, kv, src)
		if err != nil {
			return err
		}
		plan := Diff(current, desired)
		return Apply(ctx, kv, plan)
	}

	w, err := NewWatcher(cfgPath, reload, silentLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	w.SetDebounce(75 * time.Millisecond)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(w.Stop)

	// First reload — synthesise via direct call so the initial
	// state matches the file before any edit.
	if err := reload(mustLoad(t, cfgPath)); err != nil {
		t.Fatalf("initial reload: %v", err)
	}

	// Edit: flip enabled.
	writeFile(t, cfgPath, updatedYAML)

	// Wait up to 1.5s for the trigger update to propagate.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		entry, err := kv.Triggers.Get(ctx, "t1")
		if err == nil {
			var def trigger.TriggerDef
			if json.Unmarshal(entry.Value(), &def) == nil &&
				!def.Enabled {
				return // success
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("trigger t1 did not flip to disabled within 1.5s")
}

// mustLoad helper for tests that need to drive the reload directly.
func mustLoad(t *testing.T, path string) ConfigFile {
	t.Helper()
	f, err := openForTest(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	cfg, err := Load(f)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}
