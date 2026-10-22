// e2e/durability/soak_test.go
// The seeded chaos soak (#809): many scripted agent runs in parallel
// under a random but seeded schedule of kills and stalls at every
// crash point, with the duplicate deliveries they cause (an AckWait
// redelivery after a kill or stall, a DispatchUnsent resend after an
// engine kill between publish and sent marker), and, in cluster mode,
// stream-leader step-downs on the journal, task and history streams
// while runs are in flight. After every run: the crash-free answer
// (with the unsafe call interrupted exactly when the journal shows a
// takeover found its intent unanswered: a stall is drawn around
// AckWait, so the schedule alone does not say), the journal audit and
// the side-effect counters. The seed is printed
// up front and on failure; DAGNATS_CHAOS_SEED reproduces the
// schedules (the interleaving across runs is not reproducible, so a
// failure may need several runs of the same seed).
//
// Opt-in: DAGNATS_CHAOS=1. DAGNATS_CHAOS_DURATION bounds the soak
// (default 3m), DAGNATS_CHAOS_RUNS is how many runs are in flight at
// once (default 8), DAGNATS_CHAOS_CLUSTER=1 runs it on an in-process
// three-node cluster with leader step-downs. Not part of the PR test
// job or the race job: .github/workflows/chaos.yml runs it nightly and
// on the `chaos` label.
//
// Methodology: the shared fixture; each run draws its schedule from
// rand seeded with seed+index before it starts, so the schedule
// depends on the seed alone. Every run is bounded by runTimeout, the
// run count by soakRunsMax, and the leader-kill goroutine carries its
// own deadline. Every wait is bounded.
package durability

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/chaos"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
)

const (
	soakDurationDefault = 3 * time.Minute
	soakRunsDefault     = 8
	soakRunsMax         = 100_000
	soakParallelMax     = 64
	// soakKillsMax bounds the kills one run's schedule may hold, and
	// soakClaimKillsMax those right after a claim, so a run trips the
	// restart limit at most once and its retries never run out.
	soakKillsMax      = 6
	soakClaimKillsMax = 4
	// leaderKillPeriod is how often the cluster soak steps a stream
	// leader down.
	leaderKillPeriod = 2 * time.Second
	// pointProbability is the chance a schedule crashes at a point.
	pointProbability = 0.2
)

// leaderStreams are the streams whose leaders the cluster soak kills.
var leaderStreams = []string{natsutil.AgentJournalStream, "TASK_QUEUES", "WORKFLOW_HISTORY"}

func TestChaosSoak(t *testing.T) {
	if os.Getenv("DAGNATS_CHAOS") != "1" {
		t.Skip("set DAGNATS_CHAOS=1 to run the chaos soak")
	}
	cfg := soakConfig(t)
	t.Logf("chaos soak: seed=%d duration=%s runs_in_flight=%d cluster=%v "+
		"(reproduce with DAGNATS_CHAOS_SEED=%d)",
		cfg.seed, cfg.duration, cfg.parallel, cfg.cluster, cfg.seed)
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) < cfg.duration+2*runTimeout {
		t.Fatalf("go test -timeout leaves %s; the soak needs %s plus %s of drain",
			time.Until(deadline).Round(time.Second), cfg.duration, 2*runTimeout)
	}
	f := newFixture(t, fixtureOptions{cluster: cfg.cluster, engines: 2})
	r := &router{}
	r.install(t)
	a := f.newAgent(t)
	baselineID := a.startRun(t)
	baseline := f.waitCompleted(t, baselineID)
	f.audit(t, baselineID)
	stop := make(chan struct{})
	if cfg.cluster {
		go killLeaders(t, f, cfg, stop)
	}
	stats := runSoak(t, a, r, cfg, baseline)
	close(stop)
	t.Logf("chaos soak: seed=%d runs=%d kills=%d stalls=%d failed=%d",
		cfg.seed, stats.runs, stats.kills, stats.stalls, stats.failed)
	if stats.failed > 0 {
		t.Errorf("chaos soak: %d of %d runs violated an invariant; seed=%d",
			stats.failed, stats.runs, cfg.seed)
	}
	if stats.runs < 2 {
		t.Errorf("chaos soak: only %d runs finished in %s", stats.runs, cfg.duration)
	}
}

// soakSettings is the soak's configuration from the environment.
type soakSettings struct {
	seed     int64
	duration time.Duration
	parallel int
	cluster  bool
}

// soakConfig reads the environment with defaults.
func soakConfig(t *testing.T) soakSettings {
	t.Helper()
	cfg := soakSettings{
		seed: time.Now().UnixNano(), duration: soakDurationDefault,
		parallel: soakRunsDefault, cluster: os.Getenv("DAGNATS_CHAOS_CLUSTER") == "1",
	}
	if raw := os.Getenv("DAGNATS_CHAOS_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("DAGNATS_CHAOS_SEED=%q: %v", raw, err)
		}
		cfg.seed = seed
	}
	if raw := os.Getenv("DAGNATS_CHAOS_DURATION"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			t.Fatalf("DAGNATS_CHAOS_DURATION=%q: want a positive duration", raw)
		}
		cfg.duration = d
	}
	if raw := os.Getenv("DAGNATS_CHAOS_RUNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > soakParallelMax {
			t.Fatalf("DAGNATS_CHAOS_RUNS=%q: want 1..%d", raw, soakParallelMax)
		}
		cfg.parallel = n
	}
	return cfg
}

// soakStats counts what the soak did.
type soakStats struct {
	mu     sync.Mutex
	runs   int
	kills  int
	stalls int
	failed int
}

// runSoak keeps cfg.parallel runs in flight until the duration ends,
// then drains them.
func runSoak(t *testing.T, a *agent, r *router, cfg soakSettings, baseline answer) *soakStats {
	t.Helper()
	if cfg.parallel < 1 || cfg.parallel > soakParallelMax {
		panic("runSoak: parallel out of range")
	}
	stats := &soakStats{}
	deadline := time.Now().Add(cfg.duration)
	slots := make(chan struct{}, cfg.parallel)
	var wg sync.WaitGroup
	for index := 0; index < soakRunsMax && time.Now().Before(deadline); index++ {
		slots <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-slots }()
			soakRun(t, a, r, cfg, baseline, index, stats)
		}(index)
	}
	wg.Wait()
	return stats
}

// soakRun runs one scheduled run and audits it.
func soakRun(
	t *testing.T, a *agent, r *router, cfg soakSettings, baseline answer,
	index int, stats *soakStats,
) {
	t.Helper()
	if index < 0 {
		panic("soakRun: index must not be negative")
	}
	f := a.f
	schedule := drawSchedule(rand.New(rand.NewSource(cfg.seed + int64(index))))
	hook := &soakHook{schedule: schedule}
	runID := a.startRunRouted(t, r, hook.fire)
	defer r.remove(runID)
	got, ok := f.waitCompletedOK(runID)
	failed := !ok
	// The unsafe call is interrupted exactly when a takeover found its
	// intent without a result, which the journal records; a stall is
	// drawn around AckWait, so whether it became a takeover is not
	// known from the schedule. The audit below holds the invariant
	// that matters either way: the call started at most once.
	if ok {
		want := expectedAnswer(baseline, f.takeoverOnUnsafe(t, runID))
		if !sameAnswer(want, got) {
			t.Errorf("run %d (%s): answer %+v, want %+v; seed=%d schedule=%s",
				index, runID, got, want, cfg.seed, schedule)
			failed = true
		}
	}
	if !ok {
		t.Errorf("run %d (%s) did not complete within %s; seed=%d schedule=%s",
			index, runID, runTimeout, cfg.seed, schedule)
		f.dumpRun(t, runID)
	}
	if ok && !f.auditOK(t, runID, fmt.Sprintf("run %d seed=%d schedule=%s", index, cfg.seed, schedule)) {
		f.dumpRun(t, runID)
		failed = true
	}
	kills, stalls := hook.fired()
	stats.mu.Lock()
	stats.runs++
	stats.kills += kills
	stats.stalls += stalls
	if failed {
		stats.failed++
	}
	stats.mu.Unlock()
}

// soakAction is what a schedule does at one point: kill, or stall for
// stall, on the first handling of iteration.
type soakAction struct {
	kill      bool
	stall     time.Duration
	iteration int
}

// soakSchedule is a run's actions by point.
type soakSchedule map[chaos.Point]soakAction

// String renders the schedule for failure messages.
func (s soakSchedule) String() string {
	out := "{"
	for _, p := range chaos.Points() {
		a, ok := s[p]
		if !ok {
			continue
		}
		if a.kill {
			out += fmt.Sprintf(" %s:kill@%d", p, a.iteration)
		} else {
			out += fmt.Sprintf(" %s:stall(%s)@%d", p, a.stall, a.iteration)
		}
	}
	return out + " }"
}

// takeoverOnUnsafe reads the run's journal and reports, as the
// crashed map expectedAnswer takes, whether a takeover interrupted
// the unsafe call: a claim on the unsafe iteration landed after the
// call's intent and before its result (if any). A successor that
// found the result reused it, and a holder that resumed before any
// takeover finished the call itself; neither interrupts.
func (f *fixture) takeoverOnUnsafe(t *testing.T, runID string) map[chaos.Point]int {
	t.Helper()
	if runID == "" {
		panic("takeoverOnUnsafe: runID must not be empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	records, err := journal.Read(ctx, f.js, runID, stepID)
	if err != nil {
		t.Errorf("run %s: journal.Read: %v", runID, err)
		return map[chaos.Point]int{}
	}
	if len(records) > historyReadMax {
		panic("takeoverOnUnsafe: journal longer than the read bound")
	}
	var intentSeq, claimSeq, resultSeq uint64
	for _, rec := range records {
		if rec.Iteration != unsafeIteration {
			continue
		}
		switch {
		case rec.Type == journal.TypeToolIntent && intentSeq == 0:
			intentSeq = rec.Seq
		case rec.Type == journal.TypeClaim && intentSeq > 0 && claimSeq == 0:
			claimSeq = rec.Seq
		case rec.Type == journal.TypeToolResult && intentSeq > 0 && resultSeq == 0:
			resultSeq = rec.Seq
		}
	}
	if claimSeq == 0 || (resultSeq > 0 && resultSeq < claimSeq) {
		return map[chaos.Point]int{}
	}
	return map[chaos.Point]int{chaos.WorkerAfterToolIntent: unsafeIteration}
}

// drawSchedule draws a run's schedule from rng: each point crashes
// with pointProbability, kill or stall, on a random iteration; engine
// points only kill. Kills are bounded so the run always completes.
func drawSchedule(rng *rand.Rand) soakSchedule {
	if rng == nil {
		panic("drawSchedule: rng must not be nil")
	}
	schedule := soakSchedule{}
	kills, claimKills := 0, 0
	for _, p := range chaos.Points() {
		if rng.Float64() >= pointProbability {
			continue
		}
		action := soakAction{iteration: rng.Intn(len(script))}
		engine := p >= chaos.EngineEnqueueSaved
		action.kill = engine || rng.Intn(2) == 0
		if action.kill && kills >= soakKillsMax {
			continue
		}
		if action.kill && p == chaos.WorkerAfterClaim && claimKills >= soakClaimKillsMax {
			continue
		}
		if !action.kill {
			action.stall = ackWait/2 + time.Duration(rng.Int63n(int64(stallFor)))
		}
		if action.kill {
			kills++
			if p == chaos.WorkerAfterClaim {
				claimKills++
			}
		}
		schedule[p] = action
	}
	if len(schedule) > len(chaos.Points()) {
		panic("drawSchedule: more actions than points")
	}
	return schedule
}

// soakHook applies a schedule to one run, each point once (the router
// already routes by run).
type soakHook struct {
	schedule soakSchedule
	mu       sync.Mutex
	done     map[chaos.Point]bool
	kills    int
	stalls   int
}

// fire is the chaos.Hook.
func (h *soakHook) fire(ctx context.Context, p chaos.Point, site chaos.Site) error {
	if site.RunID == "" {
		panic("soakHook: site must name a run")
	}
	if h.schedule == nil {
		panic("soakHook: schedule must be drawn")
	}
	action, ok := h.schedule[p]
	if !ok {
		return nil
	}
	engine := p >= chaos.EngineEnqueueSaved
	if !engine && site.Iteration != action.iteration {
		return nil
	}
	h.mu.Lock()
	if h.done == nil {
		h.done = map[chaos.Point]bool{}
	}
	if h.done[p] {
		h.mu.Unlock()
		return nil
	}
	h.done[p] = true
	if action.kill {
		h.kills++
	} else {
		h.stalls++
	}
	h.mu.Unlock()
	if action.kill {
		return chaos.ErrKilled
	}
	select {
	case <-time.After(action.stall):
	case <-ctx.Done():
	}
	return nil
}

// fired is how many kills and stalls the hook applied.
func (h *soakHook) fired() (kills, stalls int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.kills, h.stalls
}

// killLeaders steps a random stream leader down every
// leaderKillPeriod until stop closes or the soak's deadline plus
// drain passes, whichever is first.
func killLeaders(t *testing.T, f *fixture, cfg soakSettings, stop <-chan struct{}) {
	t.Helper()
	if !cfg.cluster {
		panic("killLeaders: cluster mode only")
	}
	if len(leaderStreams) == 0 {
		panic("killLeaders: no streams")
	}
	rng := rand.New(rand.NewSource(cfg.seed))
	deadline := time.After(cfg.duration + 2*runTimeout)
	ticker := time.NewTicker(leaderKillPeriod)
	defer ticker.Stop()
	for kills := 0; kills < soakRunsMax; kills++ {
		select {
		case <-stop:
			return
		case <-deadline:
			return
		case <-ticker.C:
		}
		stream := leaderStreams[rng.Intn(len(leaderStreams))]
		subject := "$JS.API.STREAM.LEADER.STEPDOWN." + stream
		if _, err := f.nc.Request(subject, nil, 5*time.Second); err != nil {
			t.Logf("leader step-down %s: %v", stream, err)
		}
	}
}
