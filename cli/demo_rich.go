// cli/demo_rich.go
// Rich keep-alive demo harness for `dagnats demo seed --keep-alive`.
//
// Where the one-shot `demo seed` registers a single workflow, seeds a
// fixed batch, and exits, the keep-alive mode is built to populate a
// LIVE `dagnats serve` console with continuously FLOWING data for
// visual review:
//
//   - registers a small set of varied workflows (single-step,
//     multi-step pipeline, sometimes-failing) so the Functions and
//     Workers pages show several distinct task types;
//   - keeps the in-process noop worker RUNNING (never exits) so it
//     stays registered + heartbeating in the `workers` KV;
//   - runs a bounded generator loop that trickles new runs across the
//     workflows on an interval, so Runs / DLQ / Traces accumulate and
//     the telemetry aggregator keeps receiving samples (the thing
//     that makes the dashboard sparkcards and Metrics charts populate
//     with real time-series instead of degenerating);
//   - creates cron triggers so the Triggers page is populated.
//
// This is a dev/demo harness — non-destructive, no engine changes. It
// reuses the same outcome-driven noop handler as the one-shot path;
// the handler is a pure dispatcher keyed on the run input payload, so
// a single handler body serves every demo task type.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// Bounds for the keep-alive run cap. demoKeepAliveDefaultMaxRuns is
// the operator default; demoKeepAliveMaxRunsCeil is the hard upper
// bound enforced everywhere (flag parse + clamp) so the generator can
// never run unbounded.
const (
	demoKeepAliveDefaultMaxRuns = 300
	demoKeepAliveMaxRunsCeil    = 100000
)

// Demo task types beyond the single-step demo-noop. Kept dot-free for
// the same NATS-subject-token reason demoTaskType is (see noop_worker).
const (
	demoTaskFetchURLs   = "demo-fetch-urls"
	demoTaskFetch       = "demo-fetch"
	demoTaskBuildGalley = "demo-build-gallery"
	demoTaskFlaky       = "demo-flaky"

	demoTaskAgentPlan      = "agent-plan"
	demoTaskAgentAct       = "agent-act"
	demoTaskAgentObserve   = "agent-observe"
	demoTaskAgentSummarize = "agent-summarize"

	demoTaskETLExtract   = "etl-extract"
	demoTaskETLTransform = "etl-transform"
	demoTaskETLLoad      = "etl-load"

	demoTaskNotifyRender = "notify-render"
	demoTaskNotifyEmail  = "notify-email"
	demoTaskNotifySlack  = "notify-slack"

	// demoTaskAgentSupervise is the single gated task type: its handler
	// spawns the agent fleet via the control plane. demoTaskAgentSubtask is
	// the runtime-authored child workflow's only step, handled by the noop
	// dispatcher so each spawned child completes.
	demoTaskAgentSupervise = "agent-supervise"
	demoTaskAgentSubtask   = "agent-subtask-run"
)

// demoCapabilityControlPlane is the capability the supervise step declares
// so a granted deployment hands its handler a ControlPlane. Deny-by-default:
// a serve without a matching policy.control_plane.grant leaves the handle
// nil (the server strips the capability from the task message).
const demoCapabilityControlPlane = "control-plane"

// demoSupervisorChildCount is how many child runs each supervisor run
// spawns. A small fleet (>1) so the Agents page shows a real parent->child
// tree, not a lone run. Fixed and bounded — the demo never fans out
// unbounded.
const demoSupervisorChildCount = 3

// Rich demo workflow names. demo-noop is shared with the one-shot
// path (declared in noop_worker.go).
const (
	demoWorkflowImagePipeline = "image-pipeline"
	demoWorkflowRetryErrors   = "retry-errors"
	demoWorkflowAgentLoop     = "llm-agent-loop"
	demoWorkflowETL           = "etl-nightly"
	demoWorkflowNotify        = "notify-fanout"

	// demoWorkflowAgentSupervisor is the 7th, control-plane workflow: its
	// gated step spawns a fleet of child runs so the Agents page shows real
	// runtime trees. demoChildWorkflowName is the ephemeral child def the
	// supervisor authors at runtime (never statically registered).
	demoWorkflowAgentSupervisor = "agent-supervisor"
	demoChildWorkflowName       = "agent-subtask"
)

// demoKeepAliveOptions configures a keep-alive generator run.
type demoKeepAliveOptions struct {
	// maxRuns caps total runs started before the loop returns.
	maxRuns int
	// interval is the delay between generator batches.
	interval time.Duration
	// batchSize is how many runs to start per interval tick.
	batchSize int
	// seed seeds the outcome RNG. Zero means time-based.
	seed int64
	// runTimeout bounds how long the loop waits for in-flight runs to
	// reach a terminal state after the cap/cancel before tallying.
	runTimeout time.Duration
}

// clampMaxRuns coerces an operator-supplied cap into the allowed
// range: non-positive becomes the default, over-ceiling becomes the
// ceiling, everything else passes through.
func clampMaxRuns(n int) int {
	if n <= 0 {
		return demoKeepAliveDefaultMaxRuns
	}
	if n > demoKeepAliveMaxRunsCeil {
		return demoKeepAliveMaxRunsCeil
	}
	return n
}

// demoTaskTypes returns every task type the keep-alive worker must
// handle. Must cover every step of every workflow in
// richWorkflowDefs() or runs would hang on an unhandled step.
func demoTaskTypes() []string {
	return []string{
		demoTaskType, // demo-noop
		demoTaskFetchURLs,
		demoTaskFetch,
		demoTaskBuildGalley,
		demoTaskFlaky,
		demoTaskAgentPlan,
		demoTaskAgentAct,
		demoTaskAgentObserve,
		demoTaskAgentSummarize,
		demoTaskETLExtract,
		demoTaskETLTransform,
		demoTaskETLLoad,
		demoTaskNotifyRender,
		demoTaskNotifyEmail,
		demoTaskNotifySlack,
		demoTaskAgentSupervise,
		demoTaskAgentSubtask,
	}
}

// demoWorkerDomains partitions the demo task types across worker
// instances so the Workers page shows a realistic fleet instead of one
// row. worker.NewWorker exposes no name/ID setter (the ID is generated
// per instance) and WithGroups only narrows the subscription subject —
// which the demo runs, carrying no group token, would never match. So
// the cheap mechanism is simply distinct NewWorker instances, each
// owning a disjoint slice of task types; each gets its own generated
// ID and heartbeats independently. The union MUST equal demoTaskTypes()
// or a step would have no handler and its run would hang forever.
// The "supervisor" domain owns only the gated supervise task; its worker is
// built WITH a control plane (see buildDomainWorker) while the others are
// plain. The child task (demoTaskAgentSubtask) rides in the "agent" domain so
// spawned children complete via the noop dispatcher. The union of all
// domains MUST equal demoTaskTypes() or a step would have no handler.
func demoWorkerDomains() map[string][]string {
	return map[string][]string{
		"media": {
			demoTaskType, demoTaskFetchURLs, demoTaskFetch,
			demoTaskBuildGalley,
		},
		"agent": {
			demoTaskAgentPlan, demoTaskAgentAct, demoTaskAgentObserve,
			demoTaskAgentSummarize, demoTaskAgentSubtask,
		},
		"etl": {
			demoTaskETLExtract, demoTaskETLTransform, demoTaskETLLoad,
			demoTaskNotifyRender, demoTaskNotifyEmail, demoTaskNotifySlack,
			demoTaskFlaky,
		},
		"supervisor": {demoTaskAgentSupervise},
	}
}

// richWorkflowDefs builds the varied demo workflow set. Each panics
// on a build error because these are compile-time-fixed definitions —
// a build failure here is a programmer error, not operator input. Every
// built def is decorated with a domain-shaped input/output schema (so the
// Functions detail Contract section populates) and, for the supervisor,
// the control-plane capability on its gated step.
func richWorkflowDefs() []dag.WorkflowDef {
	builders := []*dag.WorkflowBuilder{
		buildNoopWorkflow(),
		buildImagePipeline(),
		buildRetryErrors(),
		buildAgentLoop(),
		buildETLNightly(),
		buildNotifyFanout(),
		buildAgentSupervisor(),
	}
	defs := make([]dag.WorkflowDef, 0, len(builders))
	for _, wb := range builders {
		defs = append(defs, decorateDemoDef(mustBuildWorkflow(wb)))
	}
	return defs
}

// decorateDemoDef stamps a built demo def with its input/output schema and,
// for the supervisor, the control-plane capability its gated step needs.
// Schemas are set post-Build because dag.WorkflowBuilder exposes no schema
// setter and the capability is set post-Build because the builder exposes no
// capability setter — both are pure struct fields on the resulting def.
func decorateDemoDef(def dag.WorkflowDef) dag.WorkflowDef {
	if pair, ok := demoWorkflowSchemas()[def.Name]; ok {
		def.InputSchema = pair.input
		def.OutputSchema = pair.output
	}
	if def.Name == demoWorkflowAgentSupervisor {
		if len(def.Steps) == 0 {
			panic("decorateDemoDef: supervisor has no steps")
		}
		// The supervise step declares control-plane so a granted deployment
		// hands its handler a ControlPlane to spawn the agent fleet.
		def.Steps[0].RequiredCapabilities =
			[]string{demoCapabilityControlPlane}
	}
	return def
}

// demoSchemaPair bundles a workflow's input and output JSON Schemas.
type demoSchemaPair struct {
	input  json.RawMessage
	output json.RawMessage
}

// demoWorkflowSchemas maps each rich workflow name to its input/output JSON
// Schema. The shapes mirror the payloads buildWorkflowInput produces and the
// per-step outputs the noop worker emits, so the Contract section on the
// Functions detail page reads like the real run IO an operator inspects.
func demoWorkflowSchemas() map[string]demoSchemaPair {
	return map[string]demoSchemaPair{
		demoWorkflowName: {
			json.RawMessage(schemaNoopInput),
			json.RawMessage(schemaNoopOutput),
		},
		demoWorkflowImagePipeline: {
			json.RawMessage(schemaImagePipelineInput),
			json.RawMessage(schemaImagePipelineOutput),
		},
		demoWorkflowRetryErrors: {
			json.RawMessage(schemaRetryErrorsInput),
			json.RawMessage(schemaRetryErrorsOutput),
		},
		demoWorkflowAgentLoop: {
			json.RawMessage(schemaAgentLoopInput),
			json.RawMessage(schemaAgentLoopOutput),
		},
		demoWorkflowETL: {
			json.RawMessage(schemaETLInput),
			json.RawMessage(schemaETLOutput),
		},
		demoWorkflowNotify: {
			json.RawMessage(schemaNotifyInput),
			json.RawMessage(schemaNotifyOutput),
		},
		demoWorkflowAgentSupervisor: {
			json.RawMessage(schemaSupervisorInput),
			json.RawMessage(schemaSupervisorOutput),
		},
	}
}

// buildNoopWorkflow returns the single-step demo workflow builder,
// matching ensureDemoWorkflow so the keep-alive set is a superset.
func buildNoopWorkflow() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowName)
	wb.Task("noop", demoTaskType)
	return wb
}

// buildImagePipeline returns a three-step fan-in pipeline:
// fetch-urls -> fetch -> build-gallery.
func buildImagePipeline() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowImagePipeline)
	urls := wb.Task("fetch-urls", demoTaskFetchURLs)
	fetch := wb.Task("fetch", demoTaskFetch).After(urls)
	wb.Task("build-gallery", demoTaskBuildGalley).After(fetch)
	return wb
}

// buildRetryErrors returns a single-step workflow on a task type that
// the seeder sometimes drives to failure so the DLQ / retry surfaces
// get real entries.
func buildRetryErrors() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowRetryErrors)
	wb.Task("attempt", demoTaskFlaky)
	return wb
}

// buildAgentLoop returns the linear LLM agent pipeline:
// plan -> act -> observe -> summarize.
func buildAgentLoop() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowAgentLoop)
	plan := wb.Task("plan", demoTaskAgentPlan)
	act := wb.Task("act", demoTaskAgentAct).After(plan)
	observe := wb.Task("observe", demoTaskAgentObserve).After(act)
	wb.Task("summarize", demoTaskAgentSummarize).After(observe)
	return wb
}

// buildETLNightly returns the linear ETL pipeline:
// extract -> transform -> load.
func buildETLNightly() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowETL)
	extract := wb.Task("extract", demoTaskETLExtract)
	transform := wb.Task("transform", demoTaskETLTransform).After(extract)
	wb.Task("load", demoTaskETLLoad).After(transform)
	return wb
}

// buildNotifyFanout returns a fan-out: a single render step whose
// output feeds two independent leaf steps (send-email AND send-slack)
// so the Traces page shows real parallel branches.
func buildNotifyFanout() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowNotify)
	render := wb.Task("render", demoTaskNotifyRender)
	wb.Task("send-email", demoTaskNotifyEmail).After(render)
	wb.Task("send-slack", demoTaskNotifySlack).After(render)
	return wb
}

// buildAgentSupervisor returns the single-step supervisor workflow. The
// step's control-plane capability is stamped post-Build by decorateDemoDef
// (the builder exposes no capability setter); its handler (superviseHandle)
// spawns a fleet of child runs to form the Agents-page runtime tree.
func buildAgentSupervisor() *dag.WorkflowBuilder {
	wb := dag.NewWorkflow(demoWorkflowAgentSupervisor)
	wb.Task("supervise", demoTaskAgentSupervise)
	return wb
}

// mustBuildWorkflow builds a workflow definition or panics. Used only
// for the compile-time-fixed demo defs.
func mustBuildWorkflow(wb *dag.WorkflowBuilder) dag.WorkflowDef {
	if wb == nil {
		panic("mustBuildWorkflow: wb must not be nil")
	}
	def, err := wb.Build()
	if err != nil {
		panic(fmt.Sprintf("mustBuildWorkflow %q: %v", wb.Name(), err))
	}
	return def
}

// richTriggerDefs returns the cron triggers that populate the
// Triggers page. Bound to registered demo workflows; disabled by
// default so the keep-alive generator (not the scheduler) drives the
// observable run cadence — the triggers exist to populate the page,
// not to double-drive runs.
func richTriggerDefs() []trigger.TriggerDef {
	return []trigger.TriggerDef{
		demoCronTrigger("demo-image-pipeline-hourly",
			demoWorkflowImagePipeline, "0 * * * *"),
		demoCronTrigger("demo-etl-nightly", demoWorkflowETL, "0 2 * * *"),
		demoCronTrigger("demo-noop-every-5min", demoWorkflowName,
			"*/5 * * * *"),
		// Webhook/HTTP/Subject triggers are externally driven — nothing
		// in the demo posts to them, so they never double-drive the run
		// stream. They stay enabled to look realistic on the page.
		{
			ID:         "demo-notify-webhook",
			WorkflowID: demoWorkflowNotify,
			Enabled:    true,
			Webhook:    &trigger.WebhookConfig{Path: "/hooks/notify-fanout"},
			Source:     "demo",
		},
		{
			ID:         "demo-image-http",
			WorkflowID: demoWorkflowImagePipeline,
			Enabled:    true,
			HTTP: &trigger.HTTPConfig{
				Path:         "/api/v1/image-pipeline",
				Method:       http.MethodPost,
				TimeoutMs:    30000,
				MaxBodyBytes: 1 << 20,
			},
			Source: "demo",
		},
		{
			ID:         "demo-agent-subject",
			WorkflowID: demoWorkflowAgentLoop,
			Enabled:    true,
			Subject:    &trigger.SubjectConfig{Subject: "demo.agent.requests"},
			Source:     "demo",
		},
	}
}

// demoCronTrigger builds a DISABLED demo cron trigger. Cron triggers
// stay disabled because the keep-alive generator (not the scheduler)
// drives the observable run cadence — an enabled cron would double-drive
// runs and muddy the demo stream. The trigger still populates the page.
func demoCronTrigger(id, workflowID, expr string) trigger.TriggerDef {
	if id == "" {
		panic("demoCronTrigger: id must not be empty")
	}
	if workflowID == "" {
		panic("demoCronTrigger: workflowID must not be empty")
	}
	return trigger.TriggerDef{
		ID:         id,
		WorkflowID: workflowID,
		Enabled:    false,
		Cron:       &trigger.CronConfig{Expression: expr},
		Source:     "demo",
	}
}

// runDemoKeepAliveCmd is the CLI-facing entry point. Wires Ctrl-C /
// SIGTERM to a cancellable context, then runs the generator. Panics
// on nil svc/nc — programmer error from the dispatcher.
func runDemoKeepAliveCmd(
	svc *api.Service, nc *nats.Conn, f demoSeedFlags,
) {
	if svc == nil {
		panic("runDemoKeepAliveCmd: svc must not be nil")
	}
	if nc == nil {
		panic("runDemoKeepAliveCmd: nc must not be nil")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	maxRuns := clampMaxRuns(f.maxRuns)
	interval := f.interval
	if interval <= 0 {
		interval = 3 * time.Second
	}

	fmt.Printf("Keep-alive demo: up to %d runs, batch every %s."+
		" Press Ctrl-C to stop.\n", maxRuns, interval)

	res, err := runDemoKeepAlive(ctx, svc, nc, demoKeepAliveOptions{
		maxRuns:    maxRuns,
		interval:   interval,
		batchSize:  3,
		runTimeout: 30 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}
	printDemoSeedResult(res, res.Total())
}

// runDemoKeepAlive registers the rich workflows + triggers, starts a
// long-lived noop worker, then trickles runs across the workflows on
// opts.interval until opts.maxRuns is reached or ctx is cancelled.
// After the loop stops it waits (bounded by opts.runTimeout) for
// in-flight runs to terminate and returns the tally.
func runDemoKeepAlive(
	ctx context.Context, svc *api.Service, nc *nats.Conn,
	opts demoKeepAliveOptions,
) (demoSeedResult, error) {
	if svc == nil {
		panic("runDemoKeepAlive: svc must not be nil")
	}
	if nc == nil {
		panic("runDemoKeepAlive: nc must not be nil")
	}

	if err := ensureRichWorkflows(svc); err != nil {
		return demoSeedResult{}, fmt.Errorf("register workflows: %w", err)
	}
	// Triggers only populate the Triggers page — they do not drive the
	// observable run stream. A server without the triggers KV bucket
	// (or any other trigger hiccup) must not abort the demo, so this
	// is best-effort: log and continue.
	ensureRichTriggers(ctx, svc)

	workers, err := startRichWorker(nc, svc)
	if err != nil {
		return demoSeedResult{}, fmt.Errorf("start worker: %w", err)
	}
	defer func() {
		for _, w := range workers {
			w.Stop()
		}
	}()

	runIDs := generateRuns(ctx, svc, opts)
	if len(runIDs) == 0 {
		return demoSeedResult{}, nil
	}
	return waitForTerminal(svc, runIDs, opts.runTimeout), nil
}

// ensureRichWorkflows registers every workflow in richWorkflowDefs.
// Idempotent — re-registering the same definition is a no-op.
func ensureRichWorkflows(svc *api.Service) error {
	if svc == nil {
		panic("ensureRichWorkflows: svc must not be nil")
	}
	defs := richWorkflowDefs()
	if len(defs) == 0 {
		panic("ensureRichWorkflows: no defs")
	}
	for _, def := range defs {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		err := svc.RegisterWorkflow(ctx, def)
		cancel()
		if err != nil {
			return fmt.Errorf("register %q: %w", def.Name, err)
		}
	}
	return nil
}

// ensureRichTriggers creates the demo cron triggers, best-effort.
// Failures are logged and skipped (see call site) because triggers
// only populate the Triggers page and a missing triggers KV bucket
// must not abort the run stream.
func ensureRichTriggers(ctx context.Context, svc *api.Service) {
	if ctx == nil {
		panic("ensureRichTriggers: ctx must not be nil")
	}
	if svc == nil {
		panic("ensureRichTriggers: svc must not be nil")
	}
	for _, td := range richTriggerDefs() {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := svc.CreateTrigger(cctx, td)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"demo: create trigger %q (non-fatal): %v\n", td.ID, err)
		}
	}
}

// startRichWorker builds the demo worker fleet: one worker.Worker per
// domain in demoWorkerDomains. Most handle their disjoint slice of task
// types via the shared outcome-driven noopHandle dispatcher; the gated
// supervisor domain gets a control plane and the fleet-spawning
// superviseHandle (see buildDomainWorker). Distinct instances surface as
// distinct heartbeating rows on the Workers page. The union of domains
// covers every task type, so no step is left without a handler. Caller owns
// Stop() for each.
func startRichWorker(
	nc *nats.Conn, svc *api.Service,
) ([]*worker.Worker, error) {
	if nc == nil {
		panic("startRichWorker: nc must not be nil")
	}
	if svc == nil {
		panic("startRichWorker: svc must not be nil")
	}
	domains := demoWorkerDomains()
	if len(domains) == 0 {
		panic("startRichWorker: no worker domains")
	}
	workers := make([]*worker.Worker, 0, len(domains))
	for _, taskTypes := range domains {
		w := buildDomainWorker(nc, svc, taskTypes)
		w.Start()
		workers = append(workers, w)
	}
	return workers, nil
}

// buildDomainWorker builds one fleet worker for a domain's task types. A
// domain owning the gated supervise task gets a control plane (so a granted
// deployment can spawn children) and routes that task to superviseHandle;
// every other task routes to the outcome-driven noop dispatcher. Attaching a
// control plane is harmless for the non-gated tasks — the handle is per-
// dispatch and nil unless the step both declares AND is granted the
// capability.
func buildDomainWorker(
	nc *nats.Conn, svc *api.Service, taskTypes []string,
) *worker.Worker {
	if nc == nil {
		panic("buildDomainWorker: nc must not be nil")
	}
	if svc == nil {
		panic("buildDomainWorker: svc must not be nil")
	}
	if len(taskTypes) == 0 {
		panic("buildDomainWorker: taskTypes must not be empty")
	}
	var opts []worker.WorkerOption
	if containsTask(taskTypes, demoTaskAgentSupervise) {
		opts = append(opts,
			worker.WithControlPlane(worker.NewControlPlane(nc)))
	}
	w := worker.NewWorker(nc, opts...)
	for _, taskType := range taskTypes {
		if taskType == demoTaskAgentSupervise {
			w.Handle(taskType, superviseHandle)
			continue
		}
		w.Handle(taskType, func(tc worker.TaskContext) error {
			return noopHandle(tc, svc)
		})
	}
	return w
}

// containsTask reports whether want is in taskTypes. Bounded by the slice.
func containsTask(taskTypes []string, want string) bool {
	if want == "" {
		panic("containsTask: want must not be empty")
	}
	for _, t := range taskTypes {
		if t == want {
			return true
		}
	}
	return false
}

// controlPlaneDenyWarnOnce makes the deny-by-default warning print at most
// once per process, so a serve that forgot the grant surfaces WHY the Agents
// page is empty without spamming stderr on every supervisor run.
var controlPlaneDenyWarnOnce sync.Once

// superviseHandle is the gated supervisor handler. With a granted control
// plane it authors an ephemeral child workflow and launches
// demoSupervisorChildCount child runs, forming the parent->child tree the
// console Agents page renders. Without a grant (cp == nil) it warns once and
// completes, so the run still terminates cleanly instead of hanging.
func superviseHandle(tc worker.TaskContext) error {
	if tc == nil {
		panic("superviseHandle: tc must not be nil")
	}
	cp := tc.ControlPlane()
	if cp == nil {
		controlPlaneDenyWarnOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "demo: workflow %q not granted"+
				" control-plane; Agents page stays empty. Add"+
				" policy.control_plane.grant: [%s] to the serve config.\n",
				demoWorkflowAgentSupervisor, demoWorkflowAgentSupervisor)
		})
		return tc.Complete(
			[]byte(`{"agents_spawned":0,"control_plane":"denied"}`))
	}
	childName, err := cp.RegisterWorkflow(
		tc.Context(), demoChildWorkflowDef(), worker.RegisterOpts{},
	)
	if err != nil {
		return tc.Fail(fmt.Errorf("register child workflow: %w", err))
	}
	spawned := spawnAgentChildren(tc, cp, childName)
	return tc.Complete(
		[]byte(fmt.Sprintf(`{"agents_spawned":%d}`, spawned)))
}

// spawnAgentChildren launches demoSupervisorChildCount child runs of the
// scoped child workflow, each carrying a distinct goal so the tree reads
// like a real fleet. A single failed spawn (e.g. a quota trip) is logged and
// skipped so one hiccup does not abort the whole fleet. Returns the count
// actually started. Bounded by demoSupervisorChildCount.
func spawnAgentChildren(
	tc worker.TaskContext, cp worker.ControlPlane, childName string,
) int {
	if cp == nil {
		panic("spawnAgentChildren: cp must not be nil")
	}
	if childName == "" {
		panic("spawnAgentChildren: childName must not be empty")
	}
	spawned := 0
	for i := 0; i < demoSupervisorChildCount; i++ {
		input := marshalDemoInput(demoTaskInput{
			Outcome: outcomeCompleted,
			Goal: fmt.Sprintf("subtask %d of %d",
				i+1, demoSupervisorChildCount),
		})
		if _, err := cp.StartRun(tc.Context(), childName, input); err != nil {
			fmt.Fprintf(os.Stderr, "demo: spawn child %d: %v\n", i+1, err)
			continue
		}
		spawned++
	}
	return spawned
}

// demoChildWorkflowDef is the ephemeral child workflow a supervisor authors
// at runtime. Its single step rides demoTaskAgentSubtask (handled by the noop
// dispatcher) so each spawned child reaches a terminal state and the tree is
// fully resolved on the Agents page.
func demoChildWorkflowDef() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name:    demoChildWorkflowName,
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "subtask",
				Task: demoTaskAgentSubtask,
				Type: dag.StepTypeNormal,
			},
		},
	}
}

// generateRuns is the continuous generator loop. It starts batches of
// runs across the rich workflows on opts.interval until the run cap
// is hit or ctx is cancelled, returning the started run IDs. Bounded
// by maxRuns (hard upper bound) and ctx — never unbounded.
func generateRuns(
	ctx context.Context, svc *api.Service, opts demoKeepAliveOptions,
) []string {
	if svc == nil {
		panic("generateRuns: svc must not be nil")
	}
	if opts.maxRuns <= 0 || opts.maxRuns > demoKeepAliveMaxRunsCeil {
		panic("generateRuns: maxRuns out of bounds")
	}

	seed := opts.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	batch := opts.batchSize
	if batch <= 0 {
		batch = 3
	}

	runIDs := make([]string, 0, opts.maxRuns)
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for len(runIDs) < opts.maxRuns {
		if ctx.Err() != nil {
			return runIDs
		}
		remaining := opts.maxRuns - len(runIDs)
		started := startGeneratorBatch(ctx, svc, rng, min(batch, remaining))
		runIDs = append(runIDs, started...)
		if len(runIDs) >= opts.maxRuns {
			return runIDs
		}
		select {
		case <-ctx.Done():
			return runIDs
		case <-ticker.C:
		}
	}
	return runIDs
}

// startGeneratorBatch starts up to n runs, picking a workflow + an
// outcome per run. Returns the IDs of runs that started. Errors are
// logged-and-skipped: a single failed StartRun must not stop the demo
// stream. Bounded by n.
func startGeneratorBatch(
	ctx context.Context, svc *api.Service, rng *rand.Rand, n int,
) []string {
	if svc == nil {
		panic("startGeneratorBatch: svc must not be nil")
	}
	if rng == nil {
		panic("startGeneratorBatch: rng must not be nil")
	}
	started := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			return started
		}
		name, input := pickRun(rng)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		runID, err := svc.StartRun(cctx, name, input)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "demo: start run %q: %v\n", name, err)
			continue
		}
		started = append(started, runID)
	}
	return started
}

// pickRun chooses a workflow and an outcome-encoded input payload.
// The retry-errors workflow is biased toward failure so the DLQ
// populates; the others follow the standard 70/20/10 distribution.
func pickRun(rng *rand.Rand) (string, []byte) {
	if rng == nil {
		panic("pickRun: rng must not be nil")
	}
	names := []string{
		demoWorkflowName,
		demoWorkflowImagePipeline,
		demoWorkflowRetryErrors,
		demoWorkflowAgentLoop,
		demoWorkflowETL,
		demoWorkflowNotify,
		demoWorkflowAgentSupervisor,
	}
	name := names[rng.Intn(len(names))]
	outcome := drawOutcome(rng)
	if name == demoWorkflowRetryErrors && rng.Intn(100) < 50 {
		outcome = outcomeFailed
	}
	return name, buildWorkflowInput(name, outcome)
}

// buildWorkflowInput constructs the realistic, domain-shaped input for a
// run of the named workflow, carrying the control outcome plus payload
// fields an operator would recognize on the run-detail IO tab. The
// outcome only needs to be honored at the root step: a failed/cancelled
// root short-circuits the run before downstream steps see input.
func buildWorkflowInput(name string, outcome demoOutcome) []byte {
	if name == "" {
		panic("buildWorkflowInput: name must not be empty")
	}
	if outcome == "" {
		panic("buildWorkflowInput: outcome must not be empty")
	}
	in := demoTaskInput{Outcome: outcome}
	switch name {
	case demoWorkflowImagePipeline:
		in.Album = "summer-2026"
		in.SourceURLs = []string{
			"https://cdn.example.com/a.jpg",
			"https://cdn.example.com/b.jpg",
			"https://cdn.example.com/c.jpg",
		}
		in.MaxDimensionPx = 2048
	case demoWorkflowAgentLoop:
		in.Goal = "triage failing CI on main and open a fix PR"
		in.Model = "claude-sonnet"
		in.MaxIterations = 8
	case demoWorkflowETL:
		in.Table = "analytics.events_daily"
		in.Date = "2026-06-28"
		in.BatchSize = 5000
	case demoWorkflowNotify:
		in.Template = "weekly-digest"
		in.Recipients = []string{"ops@example.com", "#alerts"}
	case demoWorkflowAgentSupervisor:
		in.Goal = "coordinate a fleet of subtask agents"
		in.Model = "claude-sonnet"
	}
	return marshalDemoInput(in)
}

// marshalDemoInput marshals a demoTaskInput. Panics on marshal failure —
// the input is a fixed-shape struct, so a failure is a programmer error,
// not operator input.
func marshalDemoInput(in demoTaskInput) []byte {
	if in.Outcome == "" {
		panic("marshalDemoInput: outcome must not be empty")
	}
	data, err := json.Marshal(in)
	if err != nil {
		panic(fmt.Sprintf("marshalDemoInput: %v", err))
	}
	return data
}
