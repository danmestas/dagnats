// cli/noop_worker.go
// Noop worker used by `dagnats demo seed` to drive seeded runs into
// terminal states. The worker registers a handler for the demo
// task type and dispatches each task to one of three outcomes
// (completed / failed / cancelled) based on the per-run input
// payload that the seed function chooses up front.
//
// Why per-run input rather than per-handler-call RNG: the seed
// function picks the full outcome plan deterministically (given the
// RNG seed) before any task fires. Encoding the planned outcome on
// the run input means the handler is a pure dispatcher — no shared
// RNG state, no concurrency-sensitive global rand draws, no test
// flake from goroutine interleaving. Distribution shape is owned by
// the seeder; the handler only enacts what the seed decided.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// demoOutcome is the final state a seeded run should reach.
type demoOutcome string

const (
	outcomeCompleted demoOutcome = "completed"
	outcomeFailed    demoOutcome = "failed"
	outcomeCancelled demoOutcome = "cancelled"
)

// demoTaskInput is the JSON payload carried on each seeded run.
// Outcome tells the noop handler which terminal state to drive to.
type demoTaskInput struct {
	Outcome demoOutcome `json:"outcome"`
}

// demoTaskType is the single task type the noop worker registers.
// Kept dot-free to match the convention in examples/ — task subjects
// embed the task type and NATS treats `.` as a token separator.
const demoTaskType = "demo-noop"

// demoWorkflowName is the workflow registered for seeded runs.
// The name is used as a NATS KV key, which forbids `.`, `>`, `*`,
// and other subject-illegal runes — plain alphanumerics + hyphen
// keeps the key valid across every storage layer.
const demoWorkflowName = "demo-noop"

// demoSeedOptions configures a demo-seed invocation.
type demoSeedOptions struct {
	// count is how many runs to seed. Must be positive.
	count int
	// includeFailed forces the distribution to include at least one
	// failed and one cancelled outcome for audit verification.
	includeFailed bool
	// seed is the RNG seed for deterministic distributions in tests.
	// Zero means "use time.Now().UnixNano()" (production default).
	seed int64
	// waitTimeout bounds how long we wait for all seeded runs to
	// reach a terminal state before giving up.
	waitTimeout time.Duration
}

// demoSeedResult counts the terminal states reached by seeded runs.
type demoSeedResult struct {
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
	Stuck     int `json:"stuck"`
}

// Total returns the sum of all terminal-state counts.
func (r demoSeedResult) Total() int {
	return r.Completed + r.Failed + r.Cancelled + r.Stuck
}

// runDemoSeed is the in-process entry point invoked by both the CLI
// command and the test suite. Registers the demo workflow, starts a
// noop worker, seeds opts.count runs with pre-decided outcomes, and
// waits for every run to reach a terminal state.
//
// Caller-supplied svc and nc must already be connected and the
// workflow runtime (orchestrator + streams + KV) must be live.
func runDemoSeed(
	svc *api.Service, nc *nats.Conn, opts demoSeedOptions,
) (demoSeedResult, error) {
	if svc == nil {
		panic("runDemoSeed: svc must not be nil")
	}
	if nc == nil {
		panic("runDemoSeed: nc must not be nil")
	}
	if opts.count <= 0 {
		panic("runDemoSeed: count must be positive")
	}
	if opts.count > 1000 {
		panic("runDemoSeed: count exceeds upper bound (1000)")
	}
	if opts.waitTimeout <= 0 {
		panic("runDemoSeed: waitTimeout must be positive")
	}

	plan := planDemoOutcomes(opts.count, opts.includeFailed, opts.seed)

	w, err := startNoopWorker(nc, svc)
	if err != nil {
		return demoSeedResult{},
			fmt.Errorf("start noop worker: %w", err)
	}
	defer w.Stop()

	if err := ensureDemoWorkflow(svc); err != nil {
		return demoSeedResult{},
			fmt.Errorf("register demo workflow: %w", err)
	}

	runIDs, err := startSeedRuns(svc, plan)
	if err != nil {
		return demoSeedResult{},
			fmt.Errorf("start runs: %w", err)
	}

	return waitForTerminal(svc, runIDs, opts.waitTimeout), nil
}

// planDemoOutcomes generates the outcome list for `count` runs using
// a deterministic local RNG (no global state). The default 70/20/10
// distribution may be overridden when includeFailed is set: the
// final plan is guaranteed to contain at least one failed and one
// cancelled outcome regardless of how the RNG draws come out.
func planDemoOutcomes(
	count int, includeFailed bool, seed int64,
) []demoOutcome {
	if count <= 0 {
		panic("planDemoOutcomes: count must be positive")
	}
	if count > 1000 {
		panic("planDemoOutcomes: count exceeds upper bound (1000)")
	}

	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	r := rand.New(rand.NewSource(seed))

	plan := make([]demoOutcome, count)
	for i := 0; i < count; i++ {
		plan[i] = drawOutcome(r)
	}

	if !includeFailed {
		return plan
	}

	// Forcing diversity: rewrite indices 0 and 1 to guarantee at
	// least one failed and one cancelled regardless of the RNG
	// draws. Done deterministically (first two slots, not random
	// slots) so the test assertion can rely on the slot mapping.
	if count >= 1 {
		plan[0] = outcomeFailed
	}
	if count >= 2 {
		plan[1] = outcomeCancelled
	}
	return plan
}

// drawOutcome samples a single outcome from the 70/20/10 distribution.
func drawOutcome(r *rand.Rand) demoOutcome {
	if r == nil {
		panic("drawOutcome: r must not be nil")
	}
	roll := r.Intn(100)
	switch {
	case roll < 70:
		return outcomeCompleted
	case roll < 90:
		return outcomeFailed
	default:
		return outcomeCancelled
	}
}

// startNoopWorker builds a worker, registers the demo handler that
// dispatches each task to the planned outcome, and starts it.
// Caller is responsible for w.Stop().
func startNoopWorker(
	nc *nats.Conn, svc *api.Service,
) (*worker.Worker, error) {
	if nc == nil {
		panic("startNoopWorker: nc must not be nil")
	}
	if svc == nil {
		panic("startNoopWorker: svc must not be nil")
	}

	w := worker.NewWorker(nc)
	w.Handle(demoTaskType,
		func(tc worker.TaskContext) error {
			return noopHandle(tc, svc)
		},
	)
	w.Start()
	return w, nil
}

// noopHandle is the per-task dispatch. Reads the planned outcome
// from the input payload and drives the run accordingly.
func noopHandle(
	tc worker.TaskContext, svc *api.Service,
) error {
	if tc == nil {
		panic("noopHandle: tc must not be nil")
	}
	if svc == nil {
		panic("noopHandle: svc must not be nil")
	}

	outcome := decodeOutcome(tc.Input())
	switch outcome {
	case outcomeCompleted:
		return tc.Complete([]byte(`{"noop":"ok"}`))
	case outcomeFailed:
		return tc.FailPermanent(
			errors.New("demo noop: planned failure"),
		)
	case outcomeCancelled:
		// Order matters: publish the cancel event FIRST while the
		// run is still in Running state, THEN ack the step. The
		// engine's handleWorkflowCancelled only acts when the run
		// is Running (orchestrator.go:1651). If we ack the step
		// first, the run transitions to Completed and the cancel
		// becomes a no-op. With the cancel landing while the run
		// is Running, it moves the run to Cancelled. The subsequent
		// step.completed event hits the terminal-state guard in
		// handleStepCompleted (orchestrator.go:707) and is ignored.
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		defer cancel()
		if err := svc.CancelRun(ctx, tc.RunID()); err != nil {
			return err
		}
		return tc.Complete([]byte(`{"noop":"cancelled"}`))
	default:
		return tc.FailPermanent(fmt.Errorf(
			"demo noop: unknown outcome %q", outcome,
		))
	}
}

// decodeOutcome reads the planned outcome from the task input.
// Defaults to outcomeCompleted when input is empty or malformed —
// the noop worker must never panic on a bad payload because it
// owns no business-critical correctness.
func decodeOutcome(input []byte) demoOutcome {
	if len(input) == 0 {
		return outcomeCompleted
	}
	var ti demoTaskInput
	if err := json.Unmarshal(input, &ti); err != nil {
		return outcomeCompleted
	}
	switch ti.Outcome {
	case outcomeCompleted, outcomeFailed, outcomeCancelled:
		return ti.Outcome
	default:
		return outcomeCompleted
	}
}

// ensureDemoWorkflow registers the single-step demo workflow if it
// is not already present. Idempotent — re-registering the same
// definition is a no-op.
func ensureDemoWorkflow(svc *api.Service) error {
	if svc == nil {
		panic("ensureDemoWorkflow: svc must not be nil")
	}
	wb := dag.NewWorkflow(demoWorkflowName)
	wb.Task("noop", demoTaskType)
	def, err := wb.Build()
	if err != nil {
		return fmt.Errorf("build workflow: %w", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	return svc.RegisterWorkflow(ctx, def)
}

// startSeedRuns kicks off one run per planned outcome. Each run
// carries an input payload that tells the noop handler which
// terminal state to drive to. Returns the runIDs in the same order
// as the plan so callers can correlate.
func startSeedRuns(
	svc *api.Service, plan []demoOutcome,
) ([]string, error) {
	if svc == nil {
		panic("startSeedRuns: svc must not be nil")
	}
	if len(plan) == 0 {
		panic("startSeedRuns: plan must not be empty")
	}
	if len(plan) > 1000 {
		panic("startSeedRuns: plan exceeds upper bound (1000)")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	runIDs := make([]string, len(plan))
	for i, outcome := range plan {
		input, err := json.Marshal(demoTaskInput{Outcome: outcome})
		if err != nil {
			return nil, fmt.Errorf(
				"marshal input for run %d: %w", i, err,
			)
		}
		runID, err := svc.StartRun(ctx, demoWorkflowName, input)
		if err != nil {
			return nil, fmt.Errorf(
				"start run %d: %w", i, err,
			)
		}
		runIDs[i] = runID
	}
	return runIDs, nil
}

// waitForTerminal polls every run until each has reached a terminal
// state or the overall timeout fires. Returns a tally of how many
// landed in each state — including a `Stuck` bucket for runs that
// did not terminate within the timeout. Bounded by waitTimeout.
func waitForTerminal(
	svc *api.Service, runIDs []string, waitTimeout time.Duration,
) demoSeedResult {
	if svc == nil {
		panic("waitForTerminal: svc must not be nil")
	}
	if len(runIDs) == 0 {
		panic("waitForTerminal: runIDs must not be empty")
	}
	if waitTimeout <= 0 {
		panic("waitForTerminal: waitTimeout must be positive")
	}

	deadline := time.Now().Add(waitTimeout)
	finished := make(map[string]dag.RunStatus, len(runIDs))

	for time.Now().Before(deadline) && len(finished) < len(runIDs) {
		for _, runID := range runIDs {
			if _, done := finished[runID]; done {
				continue
			}
			ctx, cancel := context.WithTimeout(
				context.Background(), 1*time.Second,
			)
			run, err := svc.GetRun(ctx, runID)
			cancel()
			if err != nil {
				continue
			}
			if run.Status.IsTerminal() {
				finished[runID] = run.Status
			}
		}
		if len(finished) < len(runIDs) {
			time.Sleep(25 * time.Millisecond)
		}
	}

	return tallyResults(runIDs, finished)
}

// tallyResults counts terminal states. Any run whose ID is missing
// from the finished map is counted as Stuck.
func tallyResults(
	runIDs []string, finished map[string]dag.RunStatus,
) demoSeedResult {
	if runIDs == nil {
		panic("tallyResults: runIDs must not be nil")
	}
	if finished == nil {
		panic("tallyResults: finished must not be nil")
	}

	var res demoSeedResult
	for _, runID := range runIDs {
		status, ok := finished[runID]
		if !ok {
			res.Stuck++
			continue
		}
		switch status {
		case dag.RunStatusCompleted:
			res.Completed++
		case dag.RunStatusFailed:
			res.Failed++
		case dag.RunStatusCancelled:
			res.Cancelled++
		default:
			// Compensated / CompensateFailed are not expected in
			// the demo workflow (no compensations defined). Bucket
			// any unexpected terminal under Stuck so the test can
			// flag the surprise instead of silently passing.
			res.Stuck++
		}
	}
	return res
}
