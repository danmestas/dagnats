package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/chaos"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runstate"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// TaskContext is the interface workers use to interact with the
// DagNats engine. Includes step completion, checkpointing, signals,
// and streaming. Workers call exactly one of Complete, Fail, or
// Continue per execution.
//
// Checkpoint and signal methods depend on optional KV buckets
// ("checkpoints" and "signals"). They return an error if the bucket
// was not provisioned at startup — check your natsutil.SetupAll call.
//
// BREAKING CHANGE (#624): LogOut() io.Writer and LogErr() io.Writer
// were added to this interface. Any out-of-repo Go type that
// implements TaskContext directly (rather than embedding one dagnats
// already constructs, e.g. via a handler-wrapping test double) stops
// compiling against this version until it adds both methods. The
// simplest fix for a test double or thin wrapper that doesn't care
// about log capture is to return io.Discard from both:
//
//	func (m *myFakeTaskContext) LogOut() io.Writer { return io.Discard }
//	func (m *myFakeTaskContext) LogErr() io.Writer { return io.Discard }
//
// A type that embeds worker.TaskContext (or a *testing double this
// module already provides, e.g. dagnatstest.MockTaskContext) needs no
// change — it inherits the new methods automatically.
type TaskContext interface {
	// Step identity and input
	Input() []byte
	RunID() string
	StepID() string
	RetryCount() int
	// Metadata returns static per-step metadata from the workflow step
	// definition; nil if the step declared none.
	Metadata() map[string]string
	// Context ends when the run is cancelled (#802). A handler must
	// honour it: stop work and return, or the work is wasted.
	Context() context.Context

	// Step completion — call exactly one per execution
	Complete(output []byte) error
	Fail(err error) error
	FailPermanent(err error) error
	FailRetryAfter(err error, after time.Duration) error
	// Continue asks for another iteration. opts (Delay, Until,
	// UntilInput) make the engine wait first, durably, with no worker
	// claim held (ADR-027).
	Continue(output []byte, opts ...ContinueOption) error

	// PlaceInputs moves the queued inputs of mode into the
	// conversation, exactly once each, and returns them (ADR-027).
	// Call it at a turn boundary: steers after a tool round,
	// follow-ups after a final answer. Needs Journal() first.
	PlaceInputs(mode inputs.Mode) ([]journal.InputPlaced, error)

	// Streaming and heartbeat. PutStream is a non-durable preview
	// (ADR-026): a journal step that crashes mid-stream re-sends the
	// request; Journal().Model holds the durable response. The
	// worker heartbeats on its own while the handler runs, so
	// Heartbeat is never needed against redelivery; it returns
	// ErrRunCancelled once the run is cancelled.
	PutStream(data []byte) error
	Heartbeat() error

	// LogOut and LogErr (#624) publish stdout/stderr-tagged chunks to
	// the BUILD_LOGS hot lane (logs.{runID}.{stepID}.{attempt}.{iteration}
	// — scoped to THIS attempt and iteration so a retry or agent-loop
	// Continue never collides with a prior dispatch's sequence numbers
	// within the stream's dedup window), buffered and flushed at
	// protocol.LogChunkBytesMax or after 250ms. Complete, Fail,
	// FailPermanent, FailRetryAfter, Continue, and Pause all drain any
	// buffered bytes AND emit an attempt-ending marker
	// (completed/failed/continued/paused) before publishing their own
	// resolution, so a consumer that observes the terminal event never
	// misses trailing log output and can treat the marker as a
	// reliable "this attempt is over" signal.
	//
	// The writers LogOut/LogErr return ARE safe to call concurrently —
	// each Write() holds the lane's own mutex — which matters because
	// the canonical use is handing them straight to an external
	// process: `cmd.Stdout = tc.LogOut(); cmd.Stderr = tc.LogErr()`
	// then `cmd.Run()`, where os/exec copies each stream on its own
	// goroutine.
	LogOut() io.Writer
	LogErr() io.Writer

	// Checkpointing — save/restore handler state across retries
	Checkpoint(state []byte) error
	LoadCheckpoint() ([]byte, error)
	Pause(name string, duration time.Duration) error

	// Journal claims this dispatch's agent journal (ADR-026) and
	// returns the handle for appending records. Call it first: from
	// then on Continue, Complete and Fail* commit a journal record
	// before their event, Checkpoint and LoadCheckpoint use the
	// journal's state, and Pause writes a pause record. The handle's
	// Tool and Model run tool calls and model requests durably, reused
	// on recovery. Return its error unchanged: journal.ErrStale,
	// ErrFenced, ErrCommitted and ErrRestartLimit mean this delivery
	// must do nothing more, and the Worker settles the task
	// accordingly.
	Journal() (*journal.Journal, error)

	// Signals — coordinate between steps
	WaitForSignal(
		name string, timeout time.Duration,
	) ([]byte, error)
	SendSignal(runID, name string, data []byte) error

	// ControlPlane returns the runtime control-plane handle for this
	// step, or nil. It is nil unless the step declared the
	// "control-plane" capability AND the deployment granted one via
	// WithControlPlane — deny-by-default. Always nil-check before use.
	ControlPlane() ControlPlane
}

// HandlerFunc is the function signature for task handlers
// registered with a Worker.
type HandlerFunc func(ctx TaskContext) error

// publishMsgFunc is the signature of jetstream.JetStream.PublishMsg.
// Stored on the Worker (and threaded into taskContext) as a single-method
// seam so tests can inject failures into publishStarted without a
// growing interface or vendored fakes. Default value is the bound
// w.tp.JSPublishMsg method (trace-context-injecting wrapper);
// tests override via withPublishMsgFunc.
type publishMsgFunc func(
	ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error)

// Worker subscribes to task subjects and dispatches messages to
// registered handlers. Each task type gets its own JetStream
// subscription; messages are ack'd after the handler returns so
// failures are retried by JetStream's MaxDeliver policy.
type Worker struct {
	nc *nats.Conn
	js jetstream.JetStream
	// tp wraps publish operations so step.started / completed /
	// failed events carry W3C trace context (#334). publishMsg is
	// retained as a test seam — withPublishMsgFunc overrides it to
	// inject failures into publishStarted. Production code goes
	// through tp.JSPublishMsg* on the publish path.
	tp           *natsutil.TracingPublisher
	publishMsg   publishMsgFunc
	tracer       trace.Tracer
	handlers     map[string]HandlerFunc
	stoppers     []interface{ Stop() } // all consumer lifecycles
	checkpointKV jetstream.KeyValue
	signalKV     jetstream.KeyValue
	groups       []string
	partitions   int
	// catchupKeysMax caps the WatchTriggers startup scan. Defaults to
	// maxCatchupKeys; a field rather than the bare constant so tests
	// can prove the cap with a small fixture instead of 10k KV writes.
	catchupKeysMax int
	singletons     map[string]bool
	// handlerAckWait holds per-task AckWait overrides registered via
	// WithAckWait. Sparse: only populated for task types whose caller
	// supplied an override. coalesceAckWait falls back to defaultAckWait
	// for absent keys.
	handlerAckWait map[string]time.Duration
	// handlerConcurrency holds per-task WithConcurrency overrides; absent
	// means 1. Written only at registration, before Start.
	handlerConcurrency map[string]int

	// Directory registration (observability only)
	dir           *Directory
	workerID      string
	stopHeartbeat chan struct{}
	stopOnce      sync.Once

	// cancels ends in-flight handlers of a cancelled run (#802);
	// cancelSub is the core NATS subscription feeding it.
	cancels   cancelRegistry
	cancelSub *nats.Subscription

	// running holds the messages in flight here, so a redelivery of one
	// is skipped rather than run twice (see skipRunning).
	running runningSet

	// handlerWG tracks in-flight handleMessage calls so Stop() can drain
	// them before returning (#498). Every Add happens through
	// beginHandler(), which is guarded by drainMu — that closes the race
	// between a handler starting and Stop() beginning to wait: once
	// draining flips to true, no further Add can occur, so Stop()'s
	// handlerWG.Wait() only ever waits for handlers that started before
	// shutdown began. drainTimeout bounds that wait so a wedged handler
	// can't block shutdown forever.
	handlerWG    sync.WaitGroup
	drainMu      sync.RWMutex
	draining     bool
	drainTimeout time.Duration
	// stopping closes once the consumers stop, so a consume callback
	// waiting for a WithConcurrency slot gives up instead of outliving Stop.
	stopping chan struct{}
	// lanes are the jetstream consumers Stop drains; laneReleases
	// counts the goroutines NAKing what each lane parked meanwhile.
	lanes        []drainingLane
	laneReleases sync.WaitGroup
	// abandoned closes when Stop's drain times out. The automatic
	// heartbeat of a handler still running then stops, so its message
	// redelivers to a live worker instead of staying held by this one.
	abandoned chan struct{}
	// heartbeatMax caps one task's automatic heartbeat (WithHeartbeatMax);
	// handlerMaxRunTime holds per-task WithMaxRunTime overrides.
	heartbeatMax      time.Duration
	handlerMaxRunTime map[string]time.Duration
	// autoHeartbeatOff is a test seam (withoutAutoHeartbeat) that models
	// a frozen process: a handler that blocks then sends no heartbeat.
	autoHeartbeatOff bool

	// Pre-allocated metric instruments — created once in constructor.
	stepDuration          metric.Float64Histogram
	stepRetries           metric.Int64Counter
	tasksActive           metric.Int64UpDownCounter
	cancelledTasksSkipped metric.Int64Counter
	// logChunkFailures counts BUILD_LOGS chunk publish failures
	// (#624) — threaded into every taskContext so LogOut()/LogErr()
	// writers can record a dropped chunk without blocking the handler.
	logChunkFailures metric.Int64Counter

	// workflowRunsKV is the optional binding to the workflow_runs KV
	// bucket. When present, the worker fast-skips tasks whose parent
	// run has been cancelled (issue #174). When absent (legacy NATS
	// setups without the bucket), the skip check defaults to execute.
	workflowRunsKV jetstream.KeyValue

	// workerStatusKV is the optional binding to the worker_status KV
	// bucket. When present, the worker writes a snapshot of its
	// cancelled-skip counter on each skip, for `dagnats status
	// --detail` aggregation (issue #182). Atomic counter holds the
	// authoritative in-memory value; the KV write is best-effort.
	workerStatusKV         jetstream.KeyValue
	cancelledSkippedAtomic uint64

	// triggerSubs holds NATS core subscriptions opened by
	// WatchTriggers (#333). Lifetime tied to Worker.Stop() so callers
	// don't manage a third lifecycle alongside Start/Stop. Guarded by
	// triggerSubsMu because RegisterTriggerType/WatchTriggers may be
	// called concurrently with Stop() in shutdown races (CLAUDE.md
	// "kill-mid-test patterns + t.Cleanup safe" invariant).
	triggerSubsMu sync.Mutex
	triggerSubs   []*nats.Subscription

	// restartLimit is the crash-loop guard of journal steps (ADR-028):
	// RestartLimitDefault when 0.
	restartLimit int

	// controlPlane is the deployment-wired runtime control-plane handle.
	// nil unless WithControlPlane was passed (the Tier-1 grant). Even
	// when non-nil, a per-step handle is granted only to steps that
	// declared the "control-plane" capability — see startTaskSpan.
	controlPlane ControlPlane
}

// WorkerOption configures optional Worker behavior.
type WorkerOption func(*Worker)

// WithEngineRelay routes the worker's step events and journal records
// through the engine relay at prefix ("api.p.<program>.") instead of
// publishing history.> and journal.> directly. program.Attach sets it:
// a program's NATS user may not publish those subjects, because the
// engine must check that each one is for the program's own step and its
// current dispatch (#885). Native workers with full credentials keep the
// direct path.
func WithEngineRelay(prefix string) WorkerOption {
	if prefix == "" {
		panic("WithEngineRelay: prefix must not be empty")
	}
	return func(w *Worker) {
		if w.tp == nil {
			panic("WithEngineRelay: worker has no publisher")
		}
		w.tp = w.tp.WithRelay(prefix)
		w.publishMsg = w.tp.JSPublishMsg
	}
}

// WithGroups configures the worker to subscribe only to specific
// worker groups. When provided, the worker subscribes to
// task.{taskType}.{group}.> instead of task.{taskType}.>.
func WithGroups(groups ...string) WorkerOption {
	if len(groups) == 0 {
		panic("WithGroups: groups must not be empty")
	}
	for _, g := range groups {
		if g == "" {
			panic("WithGroups: group name must not be empty")
		}
	}
	return func(w *Worker) { w.groups = groups }
}

// WithPartitions configures pcgroups elastic consumer groups
// with the given partition count. 0 = legacy consumer (default).
func WithPartitions(n int) WorkerOption {
	if n < 0 {
		panic("WithPartitions: n must be >= 0")
	}
	if n > 256 {
		panic("WithPartitions: n must be <= 256")
	}
	return func(w *Worker) { w.partitions = n }
}

// withPublishMsgFunc replaces the JetStream publish hook used by
// taskContext.publishStarted. Test-only seam for injecting publish
// failures (NATS unreachable, broker rejection) deterministically.
// Lowercase to keep the option package-internal — only worker tests
// need this. Panics if fn is nil (programmer error). The seam is a
// function pointer rather than an interface because publishStarted
// uses exactly one method; an interface would invite later growth
// and pull complexity sideways.
func withPublishMsgFunc(fn publishMsgFunc) WorkerOption {
	if fn == nil {
		panic("withPublishMsgFunc: fn must not be nil")
	}
	return func(w *Worker) { w.publishMsg = fn }
}

// WithControlPlane grants this worker the runtime control plane: gated
// steps (those declaring the "control-plane" capability) will receive a
// per-step handle via TaskContext.ControlPlane(). Without this option
// the field stays nil and every step's ControlPlane() returns nil —
// deny-by-default is structural, not a runtime check. Panics if cp is
// nil (a deployment that wires the grant must supply a real handle).
func WithControlPlane(cp ControlPlane) WorkerOption {
	if cp == nil {
		panic("WithControlPlane: cp must not be nil")
	}
	return func(w *Worker) { w.controlPlane = cp }
}

// controlPlaneCapability is the literal capability a step must declare in
// RequiredCapabilities to be granted a control-plane handle.
const controlPlaneCapability = "control-plane"

// stepDeclaresControlPlane reports whether the task payload's declared
// capabilities include the control-plane capability. Pure helper so the
// gate condition stays readable and testable.
func stepDeclaresControlPlane(payload protocol.TaskPayload) bool {
	for _, capability := range payload.RequiredCapabilities {
		if capability == controlPlaneCapability {
			return true
		}
	}
	return false
}

// generateWorkerID creates a unique worker ID using crypto/rand.
// Panics if crypto/rand fails — that is a system-level error.
func generateWorkerID() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic("generateWorkerID: crypto/rand failed: " + err.Error())
	}
	return fmt.Sprintf("worker-%x", b)
}

// defaultDrainTimeout bounds how long Worker.Stop() waits for in-flight
// handleMessage calls to finish (#498) before giving up and returning
// anyway. Shutdown must terminate in bounded time even if a handler is
// wedged; the timeout trades a theoretical "closed DB mid-handler" risk
// for a shutdown that always completes.
const defaultDrainTimeout = 30 * time.Second

// NewWorker creates a Worker using the given connection.
// Panics if nc is nil or if JetStream cannot be initialised
// — both are programmer errors at startup. A W3C
// TraceContext+Baggage propagator is installed on the global
// OTel registry if the global is still the no-op default;
// an already-installed propagator (custom or otherwise) is
// never overwritten. Tracing and metrics use the global OTel
// providers (noop by default).
func NewWorker(
	nc *nats.Conn, opts ...WorkerOption,
) *Worker {
	if nc == nil {
		panic("NewWorker: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewWorker: jetstream.New failed: " + err.Error())
	}
	observe.EnsureDefaultPropagator()
	m := otel.Meter("dagnats/worker")
	stepDur, _ := m.Float64Histogram(
		"step.duration_ms",
	)
	stepRet, _ := m.Int64Counter("step.retries")
	active, _ := m.Int64UpDownCounter(
		"worker.tasks.active",
	)
	skipped, _ := m.Int64Counter(
		"worker.tasks.cancelled_skipped",
	)
	logChunkFail, _ := m.Int64Counter(
		"worker.log_chunk.publish_failures",
	)
	tp := natsutil.NewTracingPublisher(nc, js)
	w := &Worker{
		nc: nc,
		js: js,
		tp: tp,
		// Default publishMsg routes through tp so the test seam still
		// has the same surface but production code auto-injects trace
		// context. Tests that supply withPublishMsgFunc replace this
		// pointer with their own fake; that path bypasses tp but is
		// only used for fault injection.
		publishMsg:            tp.JSPublishMsg,
		tracer:                otel.Tracer("dagnats/worker"),
		handlers:              make(map[string]HandlerFunc),
		workerID:              generateWorkerID(),
		stepDuration:          stepDur,
		stepRetries:           stepRet,
		tasksActive:           active,
		cancelledTasksSkipped: skipped,
		logChunkFailures:      logChunkFail,
		drainTimeout:          defaultDrainTimeout,
		heartbeatMax:          DefaultHeartbeatMax,
		catchupKeysMax:        maxCatchupKeys,
		stopping:              make(chan struct{}),
		abandoned:             make(chan struct{}),
	}
	for _, opt := range opts {
		if opt == nil {
			continue // a nil option is a no-op, not a crash (#784)
		}
		opt(w)
	}
	if w.publishMsg == nil {
		panic("NewWorker: publishMsg must not be nil after opts")
	}
	return w
}

// HandlerOption configures per-handler behavior at registration time.
// Distinct from WorkerOption (which configures the Worker itself):
// HandlerOptions bind a knob to a specific taskType. Variadic on Handle
// keeps existing callers source-compatible.
type HandlerOption func(w *Worker, taskType string)

// WithAckWait overrides the JetStream AckWait for the consumer that
// will be created for taskType. Sub-second tasks should use a short
// override so worker-crash redelivery latency is bounded; long-running
// agent loops can opt into a longer wait. Panics if d <= 0 — non-positive
// durations are programmer errors per TigerStyle.
func WithAckWait(d time.Duration) HandlerOption {
	if d <= 0 {
		panic("WithAckWait: d must be positive")
	}
	return func(w *Worker, taskType string) {
		if w == nil {
			panic("WithAckWait: w must not be nil")
		}
		if taskType == "" {
			panic("WithAckWait: taskType must not be empty")
		}
		if w.handlerAckWait == nil {
			w.handlerAckWait = make(map[string]time.Duration)
		}
		w.handlerAckWait[taskType] = d
	}
}

// coalesceAckWait returns the per-task AckWait override if registered,
// otherwise defaultAckWait. Keeps the consumer-config builder free of
// nil-map and missing-key bookkeeping.
func (w *Worker) coalesceAckWait(taskType string) time.Duration {
	if w == nil {
		panic("coalesceAckWait: w must not be nil")
	}
	if taskType == "" {
		panic("coalesceAckWait: taskType must not be empty")
	}
	if d, ok := w.handlerAckWait[taskType]; ok {
		return d
	}
	return defaultAckWait
}

// Handle registers a HandlerFunc for the given task type. Optional
// HandlerOptions (e.g. WithAckWait) tune per-task knobs. Panics on
// empty taskType or nil handler — both are programmer errors.
func (w *Worker) Handle(
	taskType string, handler HandlerFunc, opts ...HandlerOption,
) {
	if taskType == "" {
		panic("Worker.Handle: taskType must not be empty")
	}
	if handler == nil {
		panic("Worker.Handle: handler must not be nil")
	}
	w.handlers[taskType] = handler
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(w, taskType)
	}
}

// HandleSingleton registers a handler that runs as a single-
// partition elastic consumer group. Only one consumer processes
// messages at a time across all worker instances. Implicitly
// enables partitioned mode if not already configured.
func (w *Worker) HandleSingleton(
	taskType string, handler HandlerFunc,
) {
	if taskType == "" {
		panic("HandleSingleton: taskType must not be empty")
	}
	if handler == nil {
		panic("HandleSingleton: handler must not be nil")
	}
	if w.handlerConcurrency[taskType] > 1 {
		panic(singletonConcurrencyPanic(taskType))
	}
	w.handlers[taskType] = handler
	if w.singletons == nil {
		w.singletons = make(map[string]bool)
	}
	w.singletons[taskType] = true
	// Singleton requires pcgroups — enable partitioned mode
	// if the caller didn't explicitly set WithPartitions.
	if w.partitions == 0 {
		w.partitions = 1
	}
}

// newDirectoryOptional creates a Directory if the workers KV
// bucket exists. Returns error (not panic) if the bucket is
// missing — directory is observability only, not critical path.
func newDirectoryOptional(
	js jetstream.JetStream,
) (*Directory, error) {
	if js == nil {
		panic("newDirectoryOptional: js must not be nil")
	}
	kv, err := js.KeyValue(
		context.Background(), "workers",
	)
	if err != nil {
		return nil, err
	}
	stream, err := js.Stream(
		context.Background(), "KV_"+kv.Bucket(),
	)
	if err != nil {
		return nil, err
	}
	return &Directory{kv: kv, stream: stream, now: time.Now}, nil
}

// Start creates JetStream subscriptions for all registered task
// types, in task-type order. Binds optional KV buckets for
// checkpoints and signals (nil if not present). When groups are
// configured, subscribes to group-specific subjects.
//
// A failure to subscribe (stream not ready, permission denied) is
// returned, after Start stops what it already started: the Worker
// is then stopped, and retrying takes a new Worker. Programmer
// errors (no handlers, colliding task types) still panic.
func (w *Worker) Start() error {
	if len(w.handlers) == 0 {
		panic("Worker.Start: no handlers registered")
	}
	if w.js == nil {
		panic("Worker.Start: js must not be nil")
	}

	assertNoConsumerNameCollisions(w.handlers, w.groups)

	w.bindOptionalKV()
	w.registerDirectory()
	w.subscribeCancel()

	for _, taskType := range slices.Sorted(maps.Keys(w.handlers)) {
		err := w.subscribeTask(taskType, w.handlers[taskType])
		if err != nil {
			w.Stop()
			return fmt.Errorf("worker start: %w", err)
		}
	}
	return nil
}

// bindOptionalKV binds checkpoint and signal KV buckets if they
// exist. Missing buckets are fine — features degrade gracefully.
// Logs warnings so operators know which features are unavailable.
func (w *Worker) bindOptionalKV() {
	if w.tp.Relayed() {
		// A relayed worker is a program: its checkpoints, signals and
		// inputs go through the engine, and its NATS user may not open
		// those buckets or read run snapshots.
		w.bindWorkerStatusKV()
		return
	}
	w.checkpointKV, _ = w.js.KeyValue(
		context.Background(), "checkpoints",
	)
	if w.checkpointKV == nil {
		slog.Warn(
			"checkpoint KV bucket not found" +
				" — Checkpoint/LoadCheckpoint/Pause" +
				" will return errors",
		)
	}
	w.signalKV, _ = w.js.KeyValue(
		context.Background(), "signals",
	)
	if w.signalKV == nil {
		slog.Warn(
			"signal KV bucket not found" +
				" — WaitForSignal/SendSignal" +
				" will return errors",
		)
	}
	w.bindWorkflowRunsKV()
	w.bindWorkerStatusKV()
}

// bindWorkerStatusKV binds the per-worker counter snapshots; a missing
// bucket disables drain-progress aggregation.
func (w *Worker) bindWorkerStatusKV() {
	if w.js == nil {
		panic("bindWorkerStatusKV: js must not be nil")
	}
	w.workerStatusKV, _ = w.js.KeyValue(
		context.Background(), "worker_status",
	)
	if w.workerStatusKV == nil {
		slog.Warn(
			"worker_status KV bucket not found" +
				" — drain-progress aggregation disabled",
		)
	}
}

// bindWorkflowRunsKV binds the run snapshots the cancelled-run
// fast-skip reads; a missing bucket disables the skip.
func (w *Worker) bindWorkflowRunsKV() {
	if w.js == nil {
		panic("bindWorkflowRunsKV: js must not be nil")
	}
	w.workflowRunsKV, _ = w.js.KeyValue(
		context.Background(), "workflow_runs",
	)
	if w.workflowRunsKV == nil {
		slog.Warn(
			"workflow_runs KV bucket not found" +
				" — cancelled-run fast-skip disabled",
		)
	}
}

// registerDirectory registers this worker in the observability
// directory and starts a heartbeat goroutine. No-op if the
// workers KV bucket doesn't exist — directory is not critical.
func (w *Worker) registerDirectory() {
	d, err := newDirectoryOptional(w.js)
	if err != nil {
		return
	}
	w.dir = d
	w.stopHeartbeat = make(chan struct{})
	taskTypes := make([]string, 0, len(w.handlers))
	for t := range w.handlers {
		taskTypes = append(taskTypes, t)
	}
	ident := loadIdentity()
	reg := WorkerRegistration{
		WorkerID:  w.workerID,
		TaskTypes: taskTypes,
		Language:  "go",
		Transport: "nats",
		MaxTasks:  len(taskTypes),
		// The heartbeat re-registers this same value, so recording the
		// groups once here keeps them on every refresh (#719).
		WorkerGroups: w.groups,
		Pid:          ident.pid,
		Hostname:     ident.hostname,
		Version:      ident.version,
	}
	if err := w.dir.Register(reg); err != nil {
		slog.Warn(
			"initial directory registration failed",
			"worker_id", w.workerID, "error", err,
		)
	}
	go w.heartbeatLoop(reg)
}

// subscribeTask creates JetStream subscriptions for a single task
// type. When groups are configured, subscribes per-group. Also
// subscribes to the sticky subject for worker-affinity routing.
// Each consumer joins w.lanes (elastic ones w.stoppers) as soon as it
// exists, so a later failure's Stop ends it.
func (w *Worker) subscribeTask(
	taskType string, handler HandlerFunc,
) error {
	if taskType == "" {
		panic("subscribeTask: taskType must not be empty")
	}
	if handler == nil {
		panic("subscribeTask: handler must not be nil")
	}
	if w.partitions > 0 {
		return w.subscribeElastic(taskType, handler)
	}
	if len(w.groups) > 0 {
		for _, group := range w.groups {
			if _, err := w.subscribePullConsumer(
				taskType, group, handler,
			); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := w.subscribePullConsumer(taskType, "", handler); err != nil {
		return err
	}
	// Sticky subscription on STICKY_TASKS stream (separate from
	// TASK_QUEUES to avoid work queue filter conflict). Missing
	// stream is fine.
	w.createStickyConsumer(taskType, handler)
	return nil
}

// subscribeElastic joins taskType's pcgroups elastic consumer group,
// one per worker group when groups are configured.
//
// Routed through consumerFilterFor/consumerNameFor (not built inline)
// so a partitioned worker's filter carries the same one-trailing-token
// anchor and sanitization a non-partitioned worker's
// subscribePullConsumer uses — otherwise a partitioned "build" worker
// would (a) still wildcard-leak "build.linux" dispatches and (b)
// collide on TASK_QUEUES with a non-partitioned worker of the same task
// type, whose filter is now "*"-anchored (#674).
func (w *Worker) subscribeElastic(
	taskType string, handler HandlerFunc,
) error {
	if w.partitions <= 0 {
		panic("subscribeElastic: partitions must be positive")
	}
	if handler == nil {
		panic("subscribeElastic: handler must not be nil")
	}
	partCount := w.partitions
	if w.singletons[taskType] {
		partCount = 1
	}
	groups := w.groups
	if len(groups) == 0 {
		groups = []string{""}
	}
	for _, group := range groups {
		cc, err := w.createElasticConsumer(
			taskType, consumerNameFor(taskType, group),
			consumerFilterFor(taskType, group), partCount, handler,
		)
		if err != nil {
			return err
		}
		w.stoppers = append(w.stoppers, cc)
	}
	return nil
}

// subscribePullConsumer attaches a worker to a durable JetStream pull
// consumer on TASK_QUEUES, creating it if absent. Idempotent across
// worker restarts. Cleans up orphan ephemeral consumers with the same
// filter subject before creation (see ADR-006 §3, added in Task 7).
// Returns setup failures (stream missing, permission denied) for Start to
// report; panics only on programmer errors.
//
// The durable name and filter subject are derived from (taskType, group)
// via consumerNameFor and consumerFilterFor. AckWait derives from per-task
// override (see WithAckWait) or defaultAckWait.
func (w *Worker) subscribePullConsumer(
	taskType, group string, handler HandlerFunc,
) (jetstream.ConsumeContext, error) {
	if taskType == "" {
		panic("subscribePullConsumer: taskType must not be empty")
	}
	if handler == nil {
		panic("subscribePullConsumer: handler must not be nil")
	}
	if defaultAckWait <= 0 {
		panic("subscribePullConsumer: defaultAckWait must be positive")
	}

	durable := consumerNameFor(taskType, group)
	filter := consumerFilterFor(taskType, group)
	ackWait := w.coalesceAckWait(taskType)
	ctx := context.Background()

	// Orphan cleanup first, cross-process collision check second.
	// The two helpers operate on disjoint consumer sets — cleanup on
	// ephemerals with our filter, xprocess on durables with our name —
	// so order is interchangeable for correctness. Cleanup-first
	// preserves the timing of cleanup's concurrent-dedup race
	// (ADR-006 §3): adding work before cleanup tightens the window in
	// which two workers both observe the orphan as still present and
	// both log the deletion. xprocess (ADR-010) is best-effort — it
	// catches the steady-state collision after one worker creates the
	// durable, even though a TOCTOU window at CreateOrUpdateConsumer
	// remains; running it after cleanup does not change that property.
	if err := w.cleanupOrphanEphemerals(ctx, filter, durable); err != nil {
		return nil, err
	}
	if err := assertNoCrossProcessCollision(ctx, w.js, filter, durable); err != nil {
		return nil, err
	}
	if group != "" {
		checkStrandedGroupSubject(ctx, w.js, taskType, group)
	}

	cfg := jetstream.ConsumerConfig{
		Durable:       durable,
		Name:          durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       ackWait,
		// DLQ routing and retry budgets are the engine's responsibility
		// (NakWithDelay + attempt count in step state), not NATS's. We
		// leave NATS unbounded so engine policy isn't silently shadowed.
		MaxDeliver: -1,
	}
	cons, err := w.js.CreateOrUpdateConsumer(ctx, "TASK_QUEUES", cfg)
	if err != nil {
		return nil, fmt.Errorf(
			"create consumer %s on TASK_QUEUES: %w", durable, err,
		)
	}

	l := lane{taskType: taskType, handler: handler, ackWait: ackWait, park: &lanePark{}}
	cc, err := cons.Consume(
		w.messageHandler(l),
		jetstream.ConsumeErrHandler(consumeErrHandler(durable)),
	)
	if err != nil {
		return nil, fmt.Errorf("consume %s: %w", durable, err)
	}
	w.trackLane(cc, l.park)
	return cc, nil
}

// consumeErrHandler logs Consume() background-loop errors instead of
// letting nats.go's nil default silently discard them. This matters
// concretely for a durable racing consumer_collision_xprocess.go's
// legacy-filter upgrade: if a sibling process deletes this consumer out
// from under an in-flight Consume (the exact bug upgradeLegacyDurable's
// re-fetch-before-delete now prevents in the steady case, but a wider
// TOCTOU window can still exist — see its doc comment), an unhandled
// "consumer deleted" error would otherwise vanish instead of surfacing.
func consumeErrHandler(label string) jetstream.ConsumeErrHandlerFunc {
	if label == "" {
		panic("consumeErrHandler: label must not be empty")
	}
	return func(_ jetstream.ConsumeContext, err error) {
		if err == nil {
			panic("consumeErrHandler: err must not be nil")
		}
		slog.Warn("consumer error", "consumer", label, "error", err)
	}
}

// cleanupOrphanEphemerals deletes pre-existing ephemeral consumers on
// TASK_QUEUES whose FilterSubject matches the one we're about to claim.
// 3-prong identity: matching filter, Durable=="" (ephemeral), and Name
// not under the "workers-" prefix (belt-and-suspenders against future state
// we haven't anticipated). Iterator form, not single-page list — past page
// 1 a stale orphan would re-trigger #136 in deployments with enough state.
func (w *Worker) cleanupOrphanEphemerals(
	ctx context.Context, filter, durable string,
) error {
	if filter == "" {
		panic("cleanupOrphanEphemerals: filter must not be empty")
	}
	if durable == "" {
		panic("cleanupOrphanEphemerals: durable must not be empty")
	}

	stream, err := w.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return fmt.Errorf("orphan cleanup: open TASK_QUEUES: %w", err)
	}

	iter := stream.ListConsumers(ctx)
	for info := range iter.Info() {
		if info.Config.FilterSubject != filter {
			continue
		}
		if info.Config.Durable != "" {
			continue
		}
		if strings.HasPrefix(info.Name, "workers-") {
			continue
		}
		// Log only when WE actually deleted the orphan. Concurrent
		// workers that lose the race silently observe the consumer
		// is already gone; the audit trail records actions taken,
		// not actions attempted. NATS server's DeleteConsumer is
		// idempotent across concurrent callers (both succeed), so
		// we use a pre-delete existence check via Consumer() — the
		// metadata layer serializes lookups with deletes, giving
		// us a reliable "did we observe it as still present" signal.
		if _, err := stream.Consumer(ctx, info.Name); err != nil {
			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				continue // sibling worker beat us; nothing to log
			}
			return fmt.Errorf(
				"orphan cleanup: look up %s: %w", info.Name, err,
			)
		}
		err := stream.DeleteConsumer(ctx, info.Name)
		if err != nil {
			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				continue
			}
			return fmt.Errorf(
				"orphan cleanup: delete %s: %w", info.Name, err,
			)
		}
		slog.Info("removing orphan ephemeral consumer for migration to durable",
			"consumer_name", info.Name,
			"filter_subject", info.Config.FilterSubject,
			"stream", "TASK_QUEUES",
			"durable_being_claimed", durable,
			"reason", "ephemeral with matching filter; pre-fix dagnats orphan",
		)
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("orphan cleanup: list TASK_QUEUES consumers: %w", err)
	}
	return nil
}

// createStickyConsumer sets up a pull consumer on the STICKY_TASKS
// stream for worker-affinity routing. Returns nil if the stream
// does not exist — sticky routing is optional.
func (w *Worker) createStickyConsumer(
	taskType string, handler HandlerFunc,
) jetstream.ConsumeContext {
	if taskType == "" {
		panic(
			"createStickyConsumer: taskType must not be empty",
		)
	}
	if handler == nil {
		panic(
			"createStickyConsumer: handler must not be nil",
		)
	}
	ctx := context.Background()
	stream, err := w.js.Stream(ctx, "STICKY_TASKS")
	if err != nil {
		return nil // Stream not provisioned — skip
	}
	cfg := stickyConsumerConfig(taskType, w.workerID, stream.CachedInfo())
	subject := cfg.FilterSubject
	cons, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil
	}
	// The config leaves AckWait to the server default; the created
	// consumer's info carries the value the server chose.
	info := cons.CachedInfo()
	if info == nil || info.Config.AckWait <= 0 {
		return nil
	}
	l := lane{
		taskType: taskType, handler: handler, ackWait: info.Config.AckWait,
		park: &lanePark{},
	}
	cc, err := cons.Consume(
		w.messageHandler(l),
		jetstream.ConsumeErrHandler(consumeErrHandler(subject)),
	)
	if err != nil {
		return nil
	}
	w.trackLane(cc, l.park)
	return cc
}

// stickyInactiveThreshold is how long the server keeps a sticky consumer
// with no pull outstanding before reaping it. A live Worker always has
// a pull outstanding, except across a reconnect or a consumer leader
// election, which take seconds; the server also waits out the AckWait
// of any delivery still pending before counting. Five minutes, the task
// AckWait, gives a Worker the same grace the system already gives its
// claimed tasks, and still reaps a dead Worker's consumer well inside
// the stream's 30-minute MaxAge.
const stickyInactiveThreshold = defaultAckWait

// stickyConsumerConfig is the per-Worker sticky consumer: durable and
// replicated like its stream. An ephemeral on the Limits-retention
// STICKY_TASKS stream is R1, placed on any node; when that node
// restarted, the consumer was lost and Consume never recreated it, so
// sticky routing to this Worker stayed dead until the Worker restarted.
//
// The name ends in a random token rather than the worker ID: program
// credentials may pull from any STICKY_TASKS consumer whose name they
// know, and worker IDs are not secret, so the name stays unguessable
// as the ephemeral's generated name was.
func stickyConsumerConfig(
	taskType, workerID string, stream *jetstream.StreamInfo,
) jetstream.ConsumerConfig {
	if workerID == "" {
		panic("stickyConsumerConfig: workerID must not be empty")
	}
	if stream == nil || stream.Config.Replicas < 1 {
		panic("stickyConsumerConfig: stream info must carry its replicas")
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		panic("stickyConsumerConfig: crypto/rand failed: " + err.Error())
	}
	return jetstream.ConsumerConfig{
		Durable:           fmt.Sprintf("sticky-%s-%x", sanitizeConsumerName(taskType), token),
		FilterSubject:     "sticky." + taskType + "." + workerID + ".>",
		AckPolicy:         jetstream.AckExplicitPolicy,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		Replicas:          stream.Config.Replicas,
		InactiveThreshold: stickyInactiveThreshold,
	}
}

// elasticAckWait is the AckWait of every elastic member consumer. It is
// pcgroups' own default, set explicitly so the automatic heartbeat
// knows the deadline it has to beat. pcgroups also derives the member
// consumer's InactiveThreshold from it, so changing it changes how fast
// a departed member's consumer is reaped.
const elasticAckWait = 5 * time.Second

// createElasticConsumer sets up a pcgroups elastic consumer
// group for the given task type. Creates the group (idempotent)
// then joins as a member.
func (w *Worker) createElasticConsumer(
	taskType string,
	groupName string,
	filter string,
	partitions int,
	handler HandlerFunc,
) (pcgroups.ConsumerGroupConsumeContext, error) {
	if taskType == "" {
		panic("createElasticConsumer: taskType must not be empty")
	}
	if partitions <= 0 {
		panic("createElasticConsumer: partitions must be positive")
	}

	ctx := context.Background()

	_, err := pcgroups.CreateElastic(
		ctx, w.js, "TASK_QUEUES", groupName,
		uint(partitions),
		[]pcgroups.PartitioningFilter{{
			Filter: filter,
		}},
		1024, // maxBufferedMessages
		0,    // maxBufferedBytes (unlimited)
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create elastic group %s on TASK_QUEUES: %w", groupName, err,
		)
	}

	// Register this worker as a member so the group
	// assigns partitions and the consumer starts receiving.
	_, err = pcgroups.AddMembers(
		ctx, w.js, "TASK_QUEUES", groupName,
		[]string{w.workerID},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"join elastic group %s: %w", groupName, err,
		)
	}

	// TODO: an elastic lane has no park, so a delivery it turns away
	// during Stop is NAK'd at once and can redeliver to this member's
	// own still-open pull, then strand until AckWait. pcgroups' Stop only
	// cancels a context and its consume context reports no Closed, so
	// there is no drain to wait on until the library exposes one.
	l := lane{taskType: taskType, handler: handler, ackWait: elasticAckWait}
	cc, err := pcgroups.ElasticConsume(
		ctx, w.js, "TASK_QUEUES", groupName,
		w.workerID,
		w.messageHandler(l),
		jetstream.ConsumerConfig{
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait:   elasticAckWait,
		},
	)
	if err != nil {
		return nil, w.leaveElasticGroup(ctx, groupName, err)
	}

	slog.Info("elastic consumer group joined",
		"task_type", taskType,
		"group", groupName,
		"partitions", partitions,
	)

	return cc, nil
}

// leaveElasticGroup removes this worker from groupName after consuming
// failed: still a member, it would hold partitions nothing consumes.
// Returns consumeErr joined with any failure to leave.
func (w *Worker) leaveElasticGroup(
	ctx context.Context, groupName string, consumeErr error,
) error {
	if groupName == "" {
		panic("leaveElasticGroup: groupName must not be empty")
	}
	if consumeErr == nil {
		panic("leaveElasticGroup: consumeErr must not be nil")
	}
	_, leaveErr := pcgroups.DeleteMembers(
		ctx, w.js, "TASK_QUEUES", groupName, []string{w.workerID},
	)
	return errors.Join(
		fmt.Errorf("consume elastic group %s: %w", groupName, consumeErr),
		leaveErr,
	)
}

// heartbeatLoop re-registers the worker every 30s to refresh the
// KV TTL (bucket TTL is 60s). Stops when stopHeartbeat is closed.
func (w *Worker) heartbeatLoop(reg WorkerRegistration) {
	if w.dir == nil {
		panic("heartbeatLoop: dir must not be nil")
	}
	if w.stopHeartbeat == nil {
		panic("heartbeatLoop: stopHeartbeat must not be nil")
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := w.dir.Register(reg); err != nil {
				slog.Warn(
					"heartbeat directory registration failed",
					"worker_id", reg.WorkerID, "error", err,
				)
			}
		case <-w.stopHeartbeat:
			return
		}
	}
}

// beginHandler registers one in-flight handleMessage call with handlerWG,
// racing safely against Stop() via drainMu. Returns false (and adds
// nothing) once draining has started, so handleMessage knows to bail out
// instead of touching a Worker that is mid-shutdown. The RLock lets
// concurrent handlers start together — only Stop()'s draining transition
// takes the write lock — so this adds no contention on the hot path.
func (w *Worker) beginHandler() bool {
	w.drainMu.RLock()
	if w.draining {
		w.drainMu.RUnlock()
		return false
	}
	w.handlerWG.Add(1)
	w.drainMu.RUnlock()
	return true
}

// Stop unsubscribes all active subscriptions. Safe to call after
// Start. Idempotent — repeat calls are no-ops, which makes
// kill-mid-test patterns + t.Cleanup safe.
//
// Messages a consumer had buffered but not started are NAK'd once that
// consumer's subscription has drained, so they redeliver to a live
// worker rather than wait out AckWait. Draining costs a flush round
// trip per consumer, and a consumer running a task inline drains only
// when that task returns. Stop waits at most the drain timeout plus
// laneCloseGrace for that. A parked message gets no InProgress, so
// with a WithAckWait shorter than that wait the server may redeliver
// it before the NAK.
func (w *Worker) Stop() {
	if w.handlers == nil {
		panic("Worker.Stop: worker not initialized")
	}
	if w.nc == nil {
		panic("Worker.Stop: nc must not be nil")
	}
	w.stopOnce.Do(func() {
		// Flip draining before anything else so beginHandler() starts
		// rejecting new in-flight handlers immediately. Combined with
		// stopping the consumers below (no new deliveries) and then
		// waiting on handlerWG, this ordering — block new Adds, stop new
		// deliveries, wait for Adds already in flight — is what closes
		// the Add-after-Wait race: any handler that reaches beginHandler
		// after this point sees draining=true and never Adds, so
		// handlerWG.Wait() below only waits for handlers that were
		// already running (or had already passed beginHandler) at this
		// instant.
		w.drainMu.Lock()
		w.draining = true
		w.drainMu.Unlock()

		if w.cancelSub != nil {
			if err := w.cancelSub.Unsubscribe(); err != nil {
				slog.Warn("unsubscribe cancel signals failed", "error", err)
			}
		}
		if w.stopHeartbeat != nil {
			close(w.stopHeartbeat)
		}
		if w.dir != nil {
			// Best-effort: deregistration is observability-only; a failure
			// here just means stale entries linger until KV TTL evicts
			// them. Logged so the failure stays visible without aborting
			// shutdown.
			if err := w.dir.Deregister(w.workerID); err != nil {
				slog.Warn(
					"worker.Stop: directory deregister failed (non-fatal)",
					"worker_id", w.workerID,
					"error", err,
				)
			}
		}
		// Drain WatchTriggers subscriptions (#333). Held under
		// triggerSubsMu so a late RegisterTriggerType/WatchTriggers
		// caller racing shutdown does not append into a half-drained
		// slice. Unsubscribe errors are best-effort like Deregister
		// above — the connection is about to close.
		w.triggerSubsMu.Lock()
		subs := w.triggerSubs
		w.triggerSubs = nil
		w.triggerSubsMu.Unlock()
		for _, sub := range subs {
			if err := sub.Unsubscribe(); err != nil {
				slog.Warn(
					"worker.Stop: trigger sub unsubscribe failed (non-fatal)",
					"subject", sub.Subject,
					"error", err,
				)
			}
		}
		w.stopConsumers()
		w.awaitHandlers()
		// Bounded: each release goroutine waits at most drainTimeout plus
		// laneCloseGrace.
		w.laneReleases.Wait()
	})
}

// stopConsumers drains every jetstream lane and stops the rest, then
// releases consume callbacks waiting for a WithConcurrency slot. Drain,
// unlike Stop, hands the messages nats.go already buffered to the
// callback, which parks them because draining is set. releaseLanes NAKs
// each lane's parked messages once its pull is gone from the server,
// while running handlers still drain.
func (w *Worker) stopConsumers() {
	if w.stopping == nil {
		panic("stopConsumers: worker not built by NewWorker")
	}
	if !w.draining {
		panic("stopConsumers: draining must be set first")
	}
	for _, l := range w.lanes {
		l.consume.Drain()
	}
	for _, s := range w.stoppers {
		s.Stop()
	}
	close(w.stopping)
	w.releaseLanes()
}

// effectiveDrainTimeout is the handler drain bound Stop uses: the
// configured drainTimeout, or defaultDrainTimeout when unset. The lane
// releases derive their bound from it too, so the two cannot drift.
func (w *Worker) effectiveDrainTimeout() time.Duration {
	timeout := w.drainTimeout
	if timeout <= 0 {
		timeout = defaultDrainTimeout
	}
	if timeout <= 0 {
		panic("effectiveDrainTimeout: timeout must be positive")
	}
	return timeout
}

// awaitHandlers waits for in-flight handleMessage calls to finish before
// Stop returns (#498) — otherwise a caller's deferred cleanup (e.g.
// closing a DB pool) can run out from under a handler that's still
// mid-execution. Bounded by drainTimeout so a wedged handler can't block
// shutdown forever; if it fires we log and return anyway rather than
// hang the process.
func (w *Worker) awaitHandlers() {
	if !w.draining {
		panic("awaitHandlers: draining must be set first")
	}
	done := make(chan struct{})
	go func() {
		w.handlerWG.Wait()
		close(done)
	}()
	timeout := w.effectiveDrainTimeout()
	select {
	case <-done:
	case <-time.After(timeout):
		close(w.abandoned)
		slog.Warn(
			"worker.Stop: drain timeout — in-flight handlers did not finish",
			"worker_id", w.workerID,
			"timeout", timeout,
		)
	}
}

// startTaskSpan begins an OTel span for the task dispatch and constructs
// the taskContext bound to the message. Returns the (ctx, span, tc)
// trio so handleMessage stays focused on dispatch and error handling.
// Caller is responsible for `defer span.End()`.
func (w *Worker) startTaskSpan(
	payload protocol.TaskPayload, taskType string, msg jetstream.Msg,
) (context.Context, trace.Span, *taskContext) {
	if msg == nil {
		panic("startTaskSpan: msg must not be nil")
	}
	if w.tracer == nil {
		panic("startTaskSpan: tracer must not be nil")
	}
	ctx := observe.ExtractTraceContext(msg, nil)
	// Span name carries the task type only (bounded cardinality — see
	// task_publisher.go's doPublish for the matching engine-side
	// rationale). workflow_name is attached as an attribute only when
	// the payload carries one: an older engine build's payload has an
	// empty WorkflowName, and emitting an empty-string attribute would
	// be worse than omitting it entirely (#503).
	attrs := []attribute.KeyValue{
		attribute.String("run_id", payload.RunID),
		attribute.String("step_id", payload.StepID),
		attribute.String("task_name", taskType),
	}
	if payload.WorkflowName != "" {
		attrs = append(attrs,
			attribute.String("workflow_name", payload.WorkflowName),
		)
	}
	ctx, span := w.tracer.Start(ctx,
		"executeTask "+taskType,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
	tc := newTaskContext(
		w.nc, w.tracer, w.js, payload, ctx, span, msg,
		w.checkpointKV, w.signalKV,
	)
	ctx, tc.releaseCancel = w.bindCancel(ctx, tc)
	tc.workerID = w.workerID
	tc.restartLimit = w.restartLimit
	tc.publishMsg = w.publishMsg
	// The task publishes through the worker's publisher, so a relayed
	// worker (WithEngineRelay) relays its step events and journal too.
	tc.tp = w.tp
	tc.logChunkFailures = w.logChunkFailures
	// Deny-by-default gate: grant a control-plane handle only when the
	// deployment wired one (w.controlPlane != nil) AND this step declared
	// the capability. Both conditions must hold; either alone leaves the
	// handle nil. Scoped to this step's run + step so server-side lineage
	// and namespacing attach to the right owner.
	if w.controlPlane != nil && stepDeclaresControlPlane(payload) {
		tc.controlPlane = newControlPlaneFor(
			w.nc, payload.RunID, payload.StepID, payload.DispatchNonce,
		)
	}
	return ctx, span, tc
}

// shouldSkipForCancelledRun returns true when the parent run is
// already cancelled and the worker should fast-skip the task.
//
// Defensive default: any error path (KV not bound, missing run,
// transient lookup failure) returns false so the worker proceeds with
// normal execution. We'd rather over-execute a borderline-cancelled
// task than drop work due to a flaky lookup.
func (w *Worker) shouldSkipForCancelledRun(runID string) bool {
	if runID == "" {
		panic("shouldSkipForCancelledRun: runID must not be empty")
	}
	// Counter must have been initialized by NewWorker. A nil counter
	// signals the Worker was constructed without the constructor — a
	// programmer error that will surface here rather than silently in
	// the .Add call after a successful skip decision.
	if w.cancelledTasksSkipped == nil {
		panic("shouldSkipForCancelledRun: counter not initialized; " +
			"use NewWorker")
	}
	// runstate owns the lookup and its fail-open rule (a nil bucket,
	// missing run or failed read all proceed), shared with the HTTP
	// bridge's poll and resolve paths (#737).
	return runstate.Cancelled(context.Background(), w.workflowRunsKV, runID)
}

// publishWorkerStatus writes the latest cancelled-skip count to the
// worker_status KV under this worker's ID. Best-effort: log on
// failure rather than block the skip path. Idempotent
// (last-write-wins on workerID key) so missed writes self-heal on
// the next skip.
//
// Synchronous Put per skip is the simplest correct shape for the
// motivating scenario (one-time backlog drain of dozens of cancelled
// tasks during operator intervention). Each Put adds ~10ms to a skip,
// which is invisible at human scale. If sustained high-rate
// cancellation traffic ever needs a debounced/async writer, the
// atomic counter is already the source of truth — refactor at that
// point, not preemptively.
func (w *Worker) publishWorkerStatus(skipped uint64) {
	if w == nil {
		panic("publishWorkerStatus: w must not be nil")
	}
	if w.workerStatusKV == nil {
		return
	}
	snap := protocol.WorkerStatusSnapshot{
		WorkerID:              w.workerID,
		CancelledTasksSkipped: skipped,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		slog.Warn("worker_status marshal failed",
			"error", err)
		return
	}
	const writeTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(
		context.Background(), writeTimeout,
	)
	defer cancel()
	_, err = w.workerStatusKV.Put(ctx, w.workerID, data)
	if err != nil {
		slog.Warn("worker_status KV write failed",
			"error", err,
			"worker_id", w.workerID,
		)
	}
}

// handleMessage unmarshals the task payload, creates a traced
// context, executes the handler, and records metrics.
func (w *Worker) handleMessage(l lane, msg jetstream.Msg) {
	if msg == nil {
		panic("handleMessage: msg must not be nil")
	}
	if l.handler == nil {
		panic("handleMessage: handler must not be nil")
	}
	c, v := w.running.claim(msg, time.Now())
	if v != verdictRun {
		skipDelivery(l.taskType, msg, c, v)
		return
	}
	tracked := &ackTracker{Msg: msg}
	defer w.releaseClaim(l, tracked, c)
	// Register this call with handlerWG before doing any work so Stop()
	// can drain it (#498). If the worker is already draining, bail out
	// without Add-ing, and park the message for a NAK once this lane's
	// pull has drained, so it redelivers to a live worker instead of
	// bouncing back here or being silently dropped.
	if !w.beginHandler() {
		l.park.parkOrNak(msg)
		return
	}
	defer w.handlerWG.Done()
	w.runMessage(l, tracked, c)
}

// releaseClaim ends c once its delivery is settled. An acked message is
// remembered for two AckWaits: long enough to catch a delivery queued
// behind the run and a redelivery after a lost ack.
func (w *Worker) releaseClaim(l lane, tracked *ackTracker, c claim) {
	if tracked == nil {
		panic("releaseClaim: tracked must not be nil")
	}
	if l.ackWait <= 0 {
		panic("releaseClaim: ackWait must be positive")
	}
	w.running.release(c, tracked.acked.Load(), 2*l.ackWait, time.Now())
}

// decodeTask unmarshals msg's payload and screens out tasks of cancelled
// runs. On false the message is already acked and the caller stops.
func (w *Worker) decodeTask(
	taskType string, msg jetstream.Msg,
) (protocol.TaskPayload, bool) {
	if msg == nil {
		panic("decodeTask: msg must not be nil")
	}
	if taskType == "" {
		panic("decodeTask: taskType must not be empty")
	}
	var payload protocol.TaskPayload
	err := json.Unmarshal(msg.Data(), &payload)
	if err != nil {
		slog.Error(
			"failed to unmarshal task payload",
			"error", err,
			"task_type", taskType,
		)
		msg.Ack()
		return payload, false
	}
	if w.shouldSkipForCancelledRun(payload.RunID) {
		w.cancelledTasksSkipped.Add(context.Background(), 1)
		count := atomic.AddUint64(&w.cancelledSkippedAtomic, 1)
		w.publishWorkerStatus(count)
		slog.Info("skipping task — parent run cancelled",
			"task_type", taskType,
			"run_id", payload.RunID,
			"step_id", payload.StepID,
		)
		msg.Ack()
		return payload, false
	}
	return payload, true
}

// runMessage executes one task whose handlerWG slot the caller holds:
// inline from handleMessage, or on its own goroutine from
// dispatchConcurrent.
func (w *Worker) runMessage(l lane, msg jetstream.Msg, c claim) {
	if msg == nil {
		panic("runMessage: msg must not be nil")
	}
	if l.handler == nil {
		panic("runMessage: handler must not be nil")
	}
	payload, ok := w.decodeTask(l.taskType, msg)
	if !ok {
		return
	}
	ctx, span, tc := w.startTaskSpan(payload, l.taskType, msg)
	defer tc.releaseCancel()
	defer span.End()
	w.tasksActive.Add(ctx, 1)
	start := time.Now()
	slog.InfoContext(ctx, "executing task",
		"task_type", l.taskType,
		"run_id", payload.RunID,
		"step_id", payload.StepID,
	)

	if err := tc.publishStarted(msg); err != nil {
		slog.ErrorContext(ctx, "failed to begin attempt — NAK and retry",
			"error", err,
			"task_type", l.taskType,
			"run_id", payload.RunID,
			"step_id", payload.StepID,
		)
		msg.NakWithDelay(1 * time.Second)
		return
	}

	progress := w.startProgress(msg, l, tc, c)
	// The deferred stop covers a panicking handler; Pause stops the
	// ticker itself before its NAK, and the explicit stop below precedes
	// every Ack and NAK settleMessage sends.
	defer progress.stop()
	tc.progress = progress
	if chaos.Installed() {
		tc.ctx = chaos.WithFreezer(tc.ctx, progress)
	}
	err := l.handler(tc)
	progress.stop()
	elapsed := float64(time.Since(start).Milliseconds())
	w.stepDuration.Record(ctx, elapsed)
	w.tasksActive.Add(ctx, -1)
	w.settleMessage(err, tc, span, msg, l.taskType)
}

// settleMessage sends the one Ack or NAK that ends a delivery once its
// handler has returned with err.
func (w *Worker) settleMessage(
	err error, tc *taskContext, span trace.Span, msg jetstream.Msg,
	taskType string,
) {
	if tc == nil {
		panic("settleMessage: tc must not be nil")
	}
	if msg == nil {
		panic("settleMessage: msg must not be nil")
	}
	// A journal step the journal stopped (stale, fenced or already
	// committed) is settled here: never a step.failed for it.
	if done, ack := tc.journalOutcome(err); done {
		if ack {
			msg.Ack()
		}
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.handleTaskError(err, tc, msg, taskType, tc.runID)
		return
	}
	// #624 review round 2: a handler that returns nil WITHOUT ever
	// calling Complete/Fail*/Continue/Pause (a bug — TaskContext's
	// contract calls for exactly one of them, but nothing enforces it)
	// used to leave tc.logLane's ticker goroutine running forever: it
	// only stops when one of those methods calls drainWithMarker.
	// This call is unconditional and idempotent — drainWithMarker's
	// alreadyTerminal guard makes it a safe no-op if the handler
	// already resolved normally (worker/context.go), so it closes the
	// leak without double-emitting a marker on the common path.
	tc.drainLogs(protocol.LogMarkerCompleted)
	// Pause() already NAK'd the message — don't double-ack.
	if !tc.paused {
		msg.Ack()
	}
}

// handleTaskError processes a handler error by either failing
// permanently (NonRetryableError), scheduling a retry-after
// (RateLimitError), or — for any other error — publishing a
// retriable step.failed and acking the message so the engine
// owns the retry decision (issue #141).
//
// The engine is the sole retry authority. The hardcoded NAK delay
// that used to live here ignored the policy's initial_delay /
// multiplier / max_delay and produced no step.failed event, so
// the engine never observed the failure: attempts stayed at 0/N
// and the run wedged in 'running' indefinitely.
func (w *Worker) handleTaskError(
	err error,
	tc *taskContext,
	msg jetstream.Msg,
	taskType string,
	runID string,
) {
	if err == nil {
		panic("handleTaskError: err must not be nil")
	}
	if msg == nil {
		panic("handleTaskError: msg must not be nil")
	}
	var rle *RateLimitError
	if errors.As(err, &rle) {
		slog.Error(
			"task hit rate limit, will retry after delay",
			"error", rle.Err,
			"retry_after", rle.RetryAfter,
			"task_type", taskType,
			"run_id", runID,
		)
		tc.FailRetryAfter(rle.Err, rle.RetryAfter)
		msg.Ack()
		return
	}
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		slog.Error(
			"task failed permanently",
			"error", nre.Err,
			"task_type", taskType,
			"run_id", runID,
		)
		tc.FailPermanent(nre.Err)
		msg.Ack()
		return
	}
	slog.Error(
		"task handler returned error, will retry via engine",
		"error", err,
		"task_type", taskType,
		"run_id", runID,
	)
	w.stepRetries.Add(context.Background(), 1)
	if failErr := tc.Fail(err); failErr != nil {
		// Publishing step.failed itself failed (NATS unreachable, etc).
		// Falling back to NAK keeps the failure observable: silently
		// acking with no event would re-introduce the wedge symptom.
		slog.Error(
			"publishing step.failed failed — falling back to NAK",
			"error", failErr,
			"task_type", taskType,
			"run_id", runID,
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}
