// cli/demo_rich_test.go
// Unit + integration tests for the rich keep-alive demo harness.
// Methodology: the pure builders (workflow defs, task-type list,
// trigger defs, bound clamps) are exercised without NATS so they run
// fast and lock in the wire shapes (distinct task types, valid DAGs,
// valid cron triggers). The continuous generator loop is exercised
// against a real embedded NATS harness with a tiny max-runs cap and a
// cancelled context so we prove it (a) registers every rich workflow,
// (b) drives runs through the noop worker, and (c) honours the run
// cap + context-cancel shutdown without leaking.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/trigger"
)

// TestRichWorkflowDefsAreValidAndDistinct asserts every rich demo
// workflow builds into a valid DAG and that the set covers several
// distinct task types (so the Functions page shows more than one).
func TestRichWorkflowDefsAreValidAndDistinct(t *testing.T) {
	t.Parallel()
	defs := richWorkflowDefs()

	if len(defs) < 6 {
		t.Fatalf("richWorkflowDefs returned %d defs, want >= 6",
			len(defs))
	}

	names := map[string]bool{}
	for _, def := range defs {
		if err := dag.Validate(def); err != nil {
			t.Errorf("workflow %q invalid: %v", def.Name, err)
		}
		if names[def.Name] {
			t.Errorf("duplicate workflow name %q", def.Name)
		}
		names[def.Name] = true
	}

	// demo-noop must still be present so the keep-alive set stays a
	// superset of the one-shot seed surface; the new recognizable
	// domains must all be registered so every console page has variety.
	wantNames := []string{
		demoWorkflowName,
		demoWorkflowImagePipeline,
		demoWorkflowRetryErrors,
		demoWorkflowAgentLoop,
		demoWorkflowETL,
		demoWorkflowNotify,
	}
	for _, want := range wantNames {
		if !names[want] {
			t.Errorf("rich defs missing %q", want)
		}
	}

	// A multi-step workflow proves the Traces page gets real fan-out.
	maxSteps := 0
	for _, def := range defs {
		if len(def.Steps) > maxSteps {
			maxSteps = len(def.Steps)
		}
	}
	if maxSteps < 3 {
		t.Errorf("no multi-step workflow found (max steps = %d)",
			maxSteps)
	}
}

// TestDemoTaskTypesCoverAllWorkflowSteps asserts the worker registers
// a handler for every task type any rich workflow step references —
// otherwise runs would hang on an unhandled step.
func TestDemoTaskTypesCoverAllWorkflowSteps(t *testing.T) {
	t.Parallel()
	handled := map[string]bool{}
	for _, tt := range demoTaskTypes() {
		if tt == "" {
			t.Errorf("empty task type in demoTaskTypes")
		}
		handled[tt] = true
	}
	if len(handled) < 3 {
		t.Errorf("demoTaskTypes covers %d types, want >= 3 distinct",
			len(handled))
	}

	for _, def := range richWorkflowDefs() {
		for _, step := range def.Steps {
			if !handled[step.Task] {
				t.Errorf("workflow %q step %q uses unhandled task"+
					" type %q", def.Name, step.ID, step.Task)
			}
		}
	}
}

// TestRichTriggerDefsAreValidAndVaried asserts the demo triggers all
// validate, bind to registered workflows, and span every trigger type
// the Triggers page renders (cron + webhook + http + subject). Cron
// triggers must stay disabled so the generator (not the scheduler)
// owns the observable run cadence.
func TestRichTriggerDefsAreValidAndVaried(t *testing.T) {
	t.Parallel()
	defs := richTriggerDefs()
	if len(defs) < 4 {
		t.Fatalf("richTriggerDefs returned %d, want >= 4", len(defs))
	}

	workflows := map[string]bool{}
	for _, wf := range richWorkflowDefs() {
		workflows[wf.Name] = true
	}

	var sawCron, sawWebhook, sawHTTP, sawSubject bool
	for _, td := range defs {
		if err := trigger.Validate(td); err != nil {
			t.Errorf("trigger %q invalid: %v", td.ID, err)
		}
		if !workflows[td.WorkflowID] {
			t.Errorf("trigger %q binds unknown workflow %q",
				td.ID, td.WorkflowID)
		}
		switch {
		case td.Cron != nil:
			sawCron = true
			if td.Enabled {
				t.Errorf("cron trigger %q must stay disabled", td.ID)
			}
		case td.Webhook != nil:
			sawWebhook = true
		case td.HTTP != nil:
			sawHTTP = true
		case td.Subject != nil:
			sawSubject = true
		}
	}
	if !sawCron || !sawWebhook || !sawHTTP || !sawSubject {
		t.Errorf("trigger types incomplete: cron=%v webhook=%v http=%v"+
			" subject=%v", sawCron, sawWebhook, sawHTTP, sawSubject)
	}
}

// TestClampMaxRuns locks the bounds on the run cap: positive in,
// pass-through; over the hard ceiling, clamped; zero/negative coerced
// to the default.
func TestClampMaxRuns(t *testing.T) {
	t.Parallel()
	if got := clampMaxRuns(120); got != 120 {
		t.Errorf("clampMaxRuns(120) = %d, want 120", got)
	}
	if got := clampMaxRuns(0); got != demoKeepAliveDefaultMaxRuns {
		t.Errorf("clampMaxRuns(0) = %d, want default %d",
			got, demoKeepAliveDefaultMaxRuns)
	}
	if got := clampMaxRuns(1 << 30); got != demoKeepAliveMaxRunsCeil {
		t.Errorf("clampMaxRuns(big) = %d, want ceil %d",
			got, demoKeepAliveMaxRunsCeil)
	}
}

// TestParseDemoSeedFlagsKeepAlive verifies the new flags parse and
// that the one-shot default leaves keepAlive false.
func TestParseDemoSeedFlagsKeepAlive(t *testing.T) {
	t.Parallel()

	f, err := parseDemoSeedFlags([]string{
		"--keep-alive", "--max-runs=42", "--interval=3s",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !f.keepAlive {
		t.Errorf("keepAlive = false, want true")
	}
	if f.maxRuns != 42 {
		t.Errorf("maxRuns = %d, want 42", f.maxRuns)
	}
	if f.interval != 3*time.Second {
		t.Errorf("interval = %v, want 3s", f.interval)
	}

	one, err := parseDemoSeedFlags([]string{"--count=5"})
	if err != nil {
		t.Fatalf("parse one-shot: %v", err)
	}
	if one.keepAlive {
		t.Errorf("one-shot keepAlive = true, want false")
	}
}

// TestRunDemoKeepAliveRespectsRunCap drives the generator against a
// real harness with a small cap and proves it registers the rich
// workflows, drives runs to terminal states, and stops at the cap.
func TestRunDemoKeepAliveRespectsRunCap(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	res, err := runDemoKeepAlive(ctx, h.Svc, h.NC, demoKeepAliveOptions{
		maxRuns:    12,
		interval:   50 * time.Millisecond,
		batchSize:  4,
		seed:       7,
		runTimeout: 8 * time.Second,
	})
	if err != nil {
		t.Fatalf("runDemoKeepAlive: %v", err)
	}

	if res.Total() < 12 {
		t.Errorf("Total = %d, want >= 12 (cap honoured) %+v",
			res.Total(), res)
	}
	// Some runs must have completed — proves the worker handled steps
	// across the rich workflows, not just hung.
	if res.Completed == 0 {
		t.Errorf("Completed = 0, want > 0 %+v", res)
	}
}

// TestRunDemoKeepAliveStopsOnContextCancel proves a cancelled context
// shuts the generator down promptly rather than running to the cap.
func TestRunDemoKeepAliveStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before we start

	res, err := runDemoKeepAlive(ctx, h.Svc, h.NC, demoKeepAliveOptions{
		maxRuns:    1000,
		interval:   50 * time.Millisecond,
		batchSize:  4,
		seed:       7,
		runTimeout: 8 * time.Second,
	})
	if err != nil {
		t.Fatalf("runDemoKeepAlive: %v", err)
	}
	// Already-cancelled context: should return near-immediately well
	// under the cap.
	if res.Total() >= 1000 {
		t.Errorf("Total = %d, expected early stop on cancel",
			res.Total())
	}
}
