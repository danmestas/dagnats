package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DataSource is the read-only surface the console needs from the
// running api.Service. Keeping it narrow lets tests substitute a fake
// without standing up NATS, and makes the surface PR-by-PR additive
// (later PRs widen it as new mutations land).
//
// Every method must be safe to call concurrently with the rest of the
// system; the underlying api.Service already meets that bar.
//
// PR 3 extends the surface with two streaming methods. Both return a
// receive-only channel that closes when ctx is cancelled. The KV /
// JetStream resources behind the stream are released exactly when
// ctx is cancelled — callers can rely on that for goroutine cleanup.
type DataSource interface {
	ListWorkflows(ctx context.Context) ([]dag.WorkflowDef, error)
	GetWorkflow(name string) (dag.WorkflowDef, error)
	ListRuns(ctx context.Context, workflowFilter string) ([]dag.WorkflowRun, error)
	GetRun(ctx context.Context, runID string) (dag.WorkflowRun, error)
	ListRunEvents(ctx context.Context, runID string, fullData bool) ([]api.RunEvent, error)
	ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error)

	// ListDeadLetters returns up to limit recent dead-letter entries.
	// Backed by api.Service.ListDeadLetters; PR 4 widens the surface
	// so the console can render the DLQ list/detail without touching
	// JetStream directly.
	ListDeadLetters(ctx context.Context, limit int) ([]api.DeadLetterView, error)

	// ReplayDeadLetter re-publishes the dead-letter entry with the
	// given sequence onto its original task subject. Returns nil on
	// success; api.ErrDLQBodyMissing when the entry pre-dates the
	// body-preservation schema; any other error on transport failure.
	ReplayDeadLetter(ctx context.Context, seq uint64) error

	// DiscardDeadLetter removes the dead-letter entry with the given
	// sequence permanently. Returns nil on success; an error when the
	// entry is missing or JetStream rejects the delete.
	DiscardDeadLetter(ctx context.Context, seq uint64) error

	// ListAuditEvents returns up to limit recent audit events from the
	// console_audit KV bucket. Returns nil + nil-error when the bucket
	// is empty / not configured; callers render the zero state.
	ListAuditEvents(ctx context.Context, limit int) ([]AuditEvent, error)

	// EmitAuditEvent writes one audit event into the console_audit
	// bucket. Returns an error on transport failure; callers should
	// log + continue rather than fail the operator action.
	EmitAuditEvent(ctx context.Context, evt AuditEvent) error

	// SetTriggerEnabled flips a single trigger's enabled bit. The
	// caller is responsible for emitting the audit event; the
	// DataSource only owns the mutation. Returns an error when the
	// trigger isn't found or KV write fails. Used by the trigger
	// toggle endpoint.
	SetTriggerEnabled(ctx context.Context, triggerID string, enabled bool) error

	// ListTriggerFires returns recent firings for one trigger, newest
	// first. Empty + nil-error when no firings exist (zero state).
	// limit must be positive; callers pass 25-50 for the recent-activity
	// panel.
	ListTriggerFires(
		ctx context.Context, triggerID string, limit int,
	) ([]TriggerFireRow, error)

	// WatchRuns streams the workflow_runs KV bucket. Each emitted
	// RunUpdate carries the latest snapshot for one run plus a flag
	// indicating whether this is the first emission for that key
	// (Created=true) or a status/state mutation (Created=false). The
	// channel closes when ctx is cancelled or the underlying watcher
	// fails. Caller is responsible for filtering — the stream emits
	// everything in the bucket.
	WatchRuns(ctx context.Context) (<-chan RunUpdate, error)

	// WatchRunHistory streams history.<runID> events. Events arrive
	// chronologically per the JetStream delivery order. fromSeq is the
	// stream sequence to resume from (0 means deliver-all). The channel
	// closes when ctx is cancelled. Bounded buffering on the channel
	// drops the oldest event if the consumer can't keep up — operators
	// always see the latest state, never a stale one.
	WatchRunHistory(
		ctx context.Context, runID string, fromSeq uint64,
	) (<-chan HistoryEvent, error)

	// WatchTriggers streams the triggers KV bucket. Each TriggerUpdate
	// is one observation: a new trigger landed, an existing trigger was
	// toggled / edited, or a delete tombstone arrived (Deleted=true,
	// Def fields cleared). The channel closes on ctx cancellation.
	WatchTriggers(ctx context.Context) (<-chan TriggerUpdate, error)

	// WatchDLQ streams the dead-letter stream. DLQUpdate carries the
	// rendered DeadLetterView plus the operation that produced it —
	// new entries arrive with Operation=DLQOpAdded; entries removed
	// by retry / discard arrive with Operation=DLQOpRemoved (and
	// View.Sequence is the only valid field). The channel closes on
	// ctx cancellation.
	WatchDLQ(ctx context.Context) (<-chan DLQUpdate, error)

	// ListKVBuckets returns the known KV buckets the console can
	// inspect. Returns nil + nil-error when JetStream isn't reachable
	// — callers render the zero state. Each entry carries a short
	// description so the UI can label the side nav without baking
	// strings into templates.
	ListKVBuckets(ctx context.Context) ([]KVBucketInfo, error)

	// ListKVKeys returns up to limit keys for one bucket. cursor is
	// the next-page token returned by the previous call; empty for
	// the first page. Returns the keys + the next cursor (empty when
	// the list is fully drained). Empty bucket: nil + empty cursor.
	ListKVKeys(
		ctx context.Context, bucket, cursor string, limit int,
	) ([]string, string, error)

	// GetKVEntry returns the value + revision metadata for one key.
	// Returns ErrKVNotFound when the key is missing.
	GetKVEntry(
		ctx context.Context, bucket, key string,
	) (KVEntryView, error)

	// SparklineData returns hourly activity buckets for one (kind, id)
	// over the last `hours` hours, newest-bucket-last. Used by the
	// workflows + triggers list pages to render a 24-hour at-a-glance
	// sparkline column.
	//
	// Honesty contract: returns (nil, nil) when no data exists or the
	// metrics aggregator is not wired. The template MUST hide the
	// canvas in that case rather than rendering a flat-line that would
	// lie about "all zeros." A non-nil slice always has exactly `hours`
	// elements so the renderer can map index → hour offset trivially.
	//
	// kind is "workflow" or "trigger"; id is the workflow name or
	// trigger ID. hours must be positive and ≤168 (one week) — the
	// implementation panics on out-of-range input so misuse fails at
	// the call site.
	SparklineData(
		ctx context.Context, kind, id string, hours int,
	) ([]float64, error)
}

// RunUpdate is one observation on the workflow_runs KV bucket. Created
// distinguishes a brand-new run (worth prepending to the list with a
// highlight) from a status mutation (worth replacing the existing row
// in place). Seq is the KV revision so reconnects can deduplicate.
type RunUpdate struct {
	Run     dag.WorkflowRun
	Created bool
	Seq     uint64
}

// HistoryEvent is one history.<runID> message materialised for the
// console's SSE writers. Seq is the JetStream stream sequence; it's
// the value clients hand back via the Last-Event-ID header to resume
// without replaying the prefix they already saw.
type HistoryEvent struct {
	Event api.RunEvent
	Seq   uint64
}

// TriggerFireRow is one observation on the TRIGGER_HISTORY stream
// scoped to a single trigger. The DataSource owns the cross-stream
// enrichment (run status, duration); the console only renders the
// projection. Empty RunID means the firing was skipped (e.g. trigger
// disabled at fire time) and produced no run.
type TriggerFireRow struct {
	FiredAt    time.Time
	RunID      string
	Skipped    bool
	SkipReason string
	Status     string
	Duration   time.Duration
}

// TriggerUpdate is one observation on the triggers KV bucket. Deleted
// marks a tombstone — Def is zeroed and the consumer should remove
// the matching row by ID. Otherwise Def is the post-write snapshot.
type TriggerUpdate struct {
	Def     trigger.TriggerDef
	Deleted bool
	Seq     uint64
}

// KVBucketInfo is one row in the KV inspector side nav. Description is
// rendered as a tooltip + secondary label so operators understand
// what each bucket holds without having to remember the schema.
type KVBucketInfo struct {
	Name        string
	Description string
	Keys        int
}

// KVEntryView is the materialised value of one KV key. Revision lets
// the UI show "rev 17 of <key>" — the engine writes monotonically so
// the number is meaningful.
type KVEntryView struct {
	Bucket   string
	Key      string
	Value    []byte
	Revision uint64
	Created  time.Time
	IsJSON   bool
}

// ErrKVNotFound is returned by GetKVEntry when no key exists with the
// given name. Lets callers render a "not found" message rather than
// confuse the operator with a 500.
var ErrKVNotFound = errors.New("kv key not found")

// DLQOp tags one DLQUpdate as additive or destructive.
type DLQOp string

const (
	// DLQOpAdded means the entry just appeared on the DLQ stream.
	DLQOpAdded DLQOp = "added"
	// DLQOpRemoved means an entry was retried or discarded and is
	// no longer in the stream. Only View.Sequence is populated.
	DLQOpRemoved DLQOp = "removed"
)

// DLQUpdate is one observation on the DLQ stream. View carries the
// full entry on additions; on removals only Sequence is meaningful.
type DLQUpdate struct {
	View      api.DeadLetterView
	Operation DLQOp
}

// apiServiceAdapter wraps *api.Service to satisfy DataSource. The
// adapter exists so callers in server/server.go can pass *api.Service
// directly without code there knowing about console.DataSource.
//
// PR 3 widens the adapter to also hold a raw *nats.Conn — the watch
// methods reach into the workflow_runs KV bucket and the
// WORKFLOW_HISTORY stream directly. We could route through api.Service
// but the watch shape is one-of-a-kind to the console and keeping it
// alongside the rest of the adapter keeps the wiring legible.
//
// PR 4 adds auditKV — the console_audit JetStream KV the adapter
// reads from / writes to. nil-safe: when auditKV is nil, Emit logs a
// warning and continues and List returns the zero state.
type apiServiceAdapter struct {
	svc     *api.Service
	nc      *nats.Conn
	auditKV jetstream.KeyValue
	logger  *slog.Logger
	// metrics is the read-side surface SparklineData buckets from.
	// nil when the operator never wired an aggregator; the adapter
	// then returns (nil, nil) so the renderer hides sparklines instead
	// of drawing a misleading flat-line at zero.
	metrics MetricsSource
}

// NewAPIDataSource returns a DataSource backed by the live api.Service.
// Panics on nil so misconfiguration fails at startup, not at first
// request. nc may be nil — in that case the streaming methods return an
// error rather than panic, so older callers that haven't been updated
// keep building. logger may be nil — the adapter falls back to slog
// default when audit emit needs to warn.
func NewAPIDataSource(
	svc *api.Service, nc *nats.Conn,
	auditKV jetstream.KeyValue, logger *slog.Logger,
) DataSource {
	if svc == nil {
		panic("NewAPIDataSource: svc is nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &apiServiceAdapter{
		svc: svc, nc: nc, auditKV: auditKV, logger: logger,
	}
}

// WithMetrics attaches a MetricsSource to the adapter. Returns the
// receiver so callers can chain in the Config wiring. Pass nil to
// detach; nil is the no-aggregator case and SparklineData will return
// (nil, nil) so the renderer can honestly hide the sparkline column.
//
// Kept separate from NewAPIDataSource so the bug-fix landed in #245
// (which surfaces aggregator-down state on /console/ops/metrics) can
// keep its construction surface unchanged — adding a parameter would
// have rippled into every test that built an adapter directly.
func WithMetrics(ds DataSource, src MetricsSource) DataSource {
	if ds == nil {
		panic("WithMetrics: ds is nil")
	}
	a, ok := ds.(*apiServiceAdapter)
	if !ok {
		// Fakes (tests) carry their own metrics state; the helper is a
		// no-op for them so wiring code can call it uniformly.
		return ds
	}
	a.metrics = src
	return a
}

func (a *apiServiceAdapter) ListWorkflows(
	ctx context.Context,
) ([]dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListWorkflows: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListWorkflows: ctx is nil")
	}
	return a.svc.ListWorkflows(ctx)
}

func (a *apiServiceAdapter) GetWorkflow(name string) (dag.WorkflowDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetWorkflow: svc is nil")
	}
	if name == "" {
		panic("apiServiceAdapter.GetWorkflow: name is empty")
	}
	return a.svc.GetWorkflow(name)
}

func (a *apiServiceAdapter) ListRuns(
	ctx context.Context, workflowFilter string,
) ([]dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRuns: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListRuns: ctx is nil")
	}
	return a.svc.ListRuns(ctx, workflowFilter)
}

func (a *apiServiceAdapter) GetRun(
	ctx context.Context, runID string,
) (dag.WorkflowRun, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.GetRun: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.GetRun: runID is empty")
	}
	return a.svc.GetRun(ctx, runID)
}

func (a *apiServiceAdapter) ListRunEvents(
	ctx context.Context, runID string, fullData bool,
) ([]api.RunEvent, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListRunEvents: svc is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.ListRunEvents: runID is empty")
	}
	return a.svc.ListRunEvents(ctx, runID, fullData)
}

func (a *apiServiceAdapter) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListTriggers: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListTriggers: ctx is nil")
	}
	return a.svc.ListTriggers(ctx)
}

func (a *apiServiceAdapter) ListDeadLetters(
	ctx context.Context, limit int,
) ([]api.DeadLetterView, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListDeadLetters: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListDeadLetters: ctx is nil")
	}
	if limit <= 0 {
		panic("apiServiceAdapter.ListDeadLetters: limit must be positive")
	}
	return a.svc.ListDeadLetters(ctx, limit)
}

func (a *apiServiceAdapter) ReplayDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if a.svc == nil {
		panic("apiServiceAdapter.ReplayDeadLetter: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ReplayDeadLetter: ctx is nil")
	}
	if seq == 0 {
		panic("apiServiceAdapter.ReplayDeadLetter: seq must be positive")
	}
	return a.svc.ReplayDeadLetter(ctx, seq)
}

func (a *apiServiceAdapter) DiscardDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if a.svc == nil {
		panic("apiServiceAdapter.DiscardDeadLetter: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.DiscardDeadLetter: ctx is nil")
	}
	if seq == 0 {
		panic("apiServiceAdapter.DiscardDeadLetter: seq must be positive")
	}
	return a.svc.DiscardDeadLetter(ctx, seq)
}

func (a *apiServiceAdapter) ListAuditEvents(
	ctx context.Context, limit int,
) ([]AuditEvent, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListAuditEvents: ctx is nil")
	}
	if limit <= 0 {
		panic("apiServiceAdapter.ListAuditEvents: limit must be positive")
	}
	return listAuditEventsInner(ctx, a.auditKV, limit)
}

func (a *apiServiceAdapter) EmitAuditEvent(
	ctx context.Context, evt AuditEvent,
) error {
	if ctx == nil {
		panic("apiServiceAdapter.EmitAuditEvent: ctx is nil")
	}
	logger := a.logger
	if logger == nil {
		logger = slog.Default()
	}
	err := emitAuditEventInner(ctx, a.auditKV, logger, evt)
	// Record the metric regardless of KV success: a denied/failed
	// outcome is still a meaningful audit observation. The outcome
	// label distinguishes them in the dashboard.
	recordAuditEvent(ctx, evt)
	return err
}

func (a *apiServiceAdapter) SetTriggerEnabled(
	ctx context.Context, triggerID string, enabled bool,
) error {
	if a.svc == nil {
		panic("apiServiceAdapter.SetTriggerEnabled: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.SetTriggerEnabled: ctx is nil")
	}
	if triggerID == "" {
		panic("apiServiceAdapter.SetTriggerEnabled: triggerID is empty")
	}
	return a.svc.SetTriggerEnabled(ctx, triggerID, enabled)
}

func (a *apiServiceAdapter) ListTriggerFires(
	ctx context.Context, triggerID string, limit int,
) ([]TriggerFireRow, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.ListTriggerFires: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.ListTriggerFires: ctx is nil")
	}
	if triggerID == "" {
		panic("apiServiceAdapter.ListTriggerFires: triggerID is empty")
	}
	if limit <= 0 {
		panic("apiServiceAdapter.ListTriggerFires: limit must be positive")
	}
	entries, err := a.svc.ListTriggerFires(ctx, triggerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list trigger fires: %w", err)
	}
	rows := make([]TriggerFireRow, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, triggerFireRowFromEntry(e))
	}
	return rows, nil
}

// triggerFireRowFromEntry projects one api.TriggerFireEntry into the
// console's render shape. Strips trigger-internal noise so the
// console only depends on the fields it renders.
func triggerFireRowFromEntry(e api.TriggerFireEntry) TriggerFireRow {
	return TriggerFireRow{
		FiredAt:    e.FiredAt,
		RunID:      e.RunID,
		Skipped:    e.Skipped,
		SkipReason: e.SkipReason,
		Status:     e.Status,
		Duration:   e.Duration,
	}
}

// WatchRuns opens a KV watcher against the workflow_runs bucket and
// translates each entry update into a RunUpdate. The channel has a
// small buffer; if the consumer can't keep up the goroutine drops the
// oldest queued update. That's intentional — the operator UI always
// wants the latest snapshot, never a stale one. Initial replay of
// existing keys is included, marked Created=true so the list page
// can pre-populate.
func (a *apiServiceAdapter) WatchRuns(
	ctx context.Context,
) (<-chan RunUpdate, error) {
	if ctx == nil {
		panic("apiServiceAdapter.WatchRuns: ctx is nil")
	}
	if a.nc == nil {
		return nil, fmt.Errorf("nats.Conn not configured")
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		return nil, fmt.Errorf("workflow_runs bucket: %w", err)
	}
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch workflow_runs: %w", err)
	}
	const bufSize = 32
	out := make(chan RunUpdate, bufSize)
	go runWatchPump(ctx, watcher, out)
	return out, nil
}

// runWatchPump translates KV updates into RunUpdate values until ctx
// is cancelled or the watcher closes. nil sentinel marks the end of
// the historical replay; we ignore it because we don't need to signal
// "live now" to the SSE consumer.
//
// out is the buffered channel both ends of the goroutine share; we
// take it as a read/write to allow the slow-consumer back-pressure
// path to drop the oldest queued value.
func runWatchPump(
	ctx context.Context,
	watcher jetstream.KeyWatcher, out chan RunUpdate,
) {
	defer close(out)
	defer watcher.Stop()           //nolint:errcheck
	const maxUpdates = 100_000_000 // bounded loop per project rule
	for i := 0; i < maxUpdates; i++ {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}
			ru, ok := parseRunUpdate(entry)
			if !ok {
				continue
			}
			if !sendOrDropOldest(ctx, out, ru) {
				return
			}
		}
	}
}

// sendOrDropOldest pushes value onto out. Falls through with a drop-
// the-oldest path if out is full; returns false when ctx is cancelled.
// Pulled out so runWatchPump stays under 70 lines.
func sendOrDropOldest(
	ctx context.Context, out chan RunUpdate, value RunUpdate,
) bool {
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case <-out:
	default:
	}
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// parseRunUpdate decodes a KV entry into a RunUpdate. PutOp is the
// only operation we surface (Save). DeleteOp / PurgeOp are unusual on
// workflow_runs — engine never deletes — and ignored.
func parseRunUpdate(entry jetstream.KeyValueEntry) (RunUpdate, bool) {
	if entry == nil {
		return RunUpdate{}, false
	}
	if entry.Operation() != jetstream.KeyValuePut {
		return RunUpdate{}, false
	}
	var run dag.WorkflowRun
	if err := json.Unmarshal(entry.Value(), &run); err != nil {
		return RunUpdate{}, false
	}
	return RunUpdate{
		Run:     run,
		Created: entry.Revision() == 1,
		Seq:     entry.Revision(),
	}, true
}

// WatchRunHistory subscribes to history.<runID> and pumps each message
// through as a HistoryEvent. fromSeq>0 starts the consumer from that
// sequence (used for Last-Event-ID resume). A nil channel return path
// is reserved for misconfiguration; callers must check the error.
func (a *apiServiceAdapter) WatchRunHistory(
	ctx context.Context, runID string, fromSeq uint64,
) (<-chan HistoryEvent, error) {
	if ctx == nil {
		panic("WatchRunHistory: ctx is nil")
	}
	if runID == "" {
		panic("WatchRunHistory: runID is empty")
	}
	if a.nc == nil {
		return nil, fmt.Errorf("nats.Conn not configured")
	}
	subject := "history." + runID
	const bufSize = 32
	out := make(chan HistoryEvent, bufSize)
	jsLegacy, err := a.nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream legacy: %w", err)
	}
	opts := []nats.SubOpt{nats.AckNone()}
	if fromSeq > 0 {
		opts = append(opts, nats.StartSequence(fromSeq+1))
	} else {
		opts = append(opts, nats.DeliverAll())
	}
	sub, err := jsLegacy.SubscribeSync(subject, opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	go historyPump(ctx, sub, out)
	return out, nil
}

// historyPump reads messages off sub and converts them to HistoryEvent
// values. Iterates until ctx is cancelled. The 250ms NextMsg deadline
// lets the loop respond to ctx cancellation without dangling: the worst
// case latency between cancel and channel-close is 250ms.
func historyPump(
	ctx context.Context, sub *nats.Subscription, out chan<- HistoryEvent,
) {
	defer close(out)
	defer sub.Unsubscribe()        //nolint:errcheck
	const maxIters = 1_000_000_000 // bounded loop
	const pollWait = 250 * time.Millisecond
	for i := 0; i < maxIters; i++ {
		if ctx.Err() != nil {
			return
		}
		msg, err := sub.NextMsg(pollWait)
		if err != nil {
			// nats.ErrTimeout: just poll again until ctx is done.
			// Other errors: subscription closed externally; bail.
			if err == nats.ErrTimeout {
				continue
			}
			return
		}
		he, ok := parseHistoryEvent(msg)
		if !ok {
			continue
		}
		select {
		case out <- he:
		case <-ctx.Done():
			return
		}
	}
}

// WatchTriggers watches the triggers KV bucket and translates each
// entry update into a TriggerUpdate. Tombstones (delete / purge)
// surface as Deleted=true so the SSE consumer can patch the row out.
// Initial replay is included — caller filters as needed.
func (a *apiServiceAdapter) WatchTriggers(
	ctx context.Context,
) (<-chan TriggerUpdate, error) {
	if ctx == nil {
		panic("apiServiceAdapter.WatchTriggers: ctx is nil")
	}
	if a.nc == nil {
		return nil, errors.New("nats.Conn not configured")
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		return nil, fmt.Errorf("triggers bucket: %w", err)
	}
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch triggers: %w", err)
	}
	const bufSize = 16
	out := make(chan TriggerUpdate, bufSize)
	go triggerWatchPump(ctx, watcher, out)
	return out, nil
}

// triggerWatchPump translates KV updates into TriggerUpdate values.
// Mirrors runWatchPump's bounded-loop + drop-oldest discipline so a
// slow consumer can never wedge the goroutine.
func triggerWatchPump(
	ctx context.Context,
	watcher jetstream.KeyWatcher, out chan TriggerUpdate,
) {
	defer close(out)
	defer watcher.Stop()           //nolint:errcheck
	const maxUpdates = 100_000_000 // bounded loop per project rule
	for i := 0; i < maxUpdates; i++ {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}
			tu, ok := parseTriggerUpdate(entry)
			if !ok {
				continue
			}
			if !sendTriggerOrDrop(ctx, out, tu) {
				return
			}
		}
	}
}

// sendTriggerOrDrop is the trigger-update variant of sendOrDropOldest.
// Copied rather than generic-ified — the channel type prevents the
// straightforward generic, and the code is small enough that the
// duplication is honest.
func sendTriggerOrDrop(
	ctx context.Context, out chan TriggerUpdate, value TriggerUpdate,
) bool {
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case <-out:
	default:
	}
	select {
	case out <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// parseTriggerUpdate decodes a KV entry into a TriggerUpdate. Put ops
// carry a fresh def; delete / purge ops surface as Deleted=true with
// the def's ID copied from the entry key so the consumer can target
// the row.
func parseTriggerUpdate(
	entry jetstream.KeyValueEntry,
) (TriggerUpdate, bool) {
	if entry == nil {
		return TriggerUpdate{}, false
	}
	switch entry.Operation() {
	case jetstream.KeyValuePut:
		var def trigger.TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			return TriggerUpdate{}, false
		}
		return TriggerUpdate{Def: def, Seq: entry.Revision()}, true
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		return TriggerUpdate{
			Def:     trigger.TriggerDef{ID: entry.Key()},
			Deleted: true,
			Seq:     entry.Revision(),
		}, true
	}
	return TriggerUpdate{}, false
}

// WatchDLQ watches the DEAD_LETTERS stream via an ephemeral consumer
// and translates each message into a DLQUpdate. Removals aren't
// observable on the stream level (DLQ entries are deleted by sequence
// after a retry); the SSE handler synthesises DLQOpRemoved events
// directly from the mutation handler, not from this watcher.
func (a *apiServiceAdapter) WatchDLQ(
	ctx context.Context,
) (<-chan DLQUpdate, error) {
	if ctx == nil {
		panic("apiServiceAdapter.WatchDLQ: ctx is nil")
	}
	if a.nc == nil {
		return nil, errors.New("nats.Conn not configured")
	}
	jsLegacy, err := a.nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream legacy: %w", err)
	}
	const bufSize = 16
	out := make(chan DLQUpdate, bufSize)
	sub, err := jsLegacy.SubscribeSync(
		"dead_letters.>",
		nats.AckNone(),
		nats.DeliverNew(),
	)
	if err != nil {
		return nil, fmt.Errorf("dlq subscribe: %w", err)
	}
	go dlqWatchPump(ctx, sub, out, a.svc)
	return out, nil
}

// dlqWatchPump pumps DLQ messages off the subscription. Reaches into
// api.Service.ListDeadLetters for the rendered view so the SSE writer
// receives the same projection the list page rendered. svc may be nil
// in degraded operation; in that case we ignore the message rather
// than synthesise a partial view.
func dlqWatchPump(
	ctx context.Context, sub *nats.Subscription,
	out chan DLQUpdate, svc *api.Service,
) {
	defer close(out)
	defer sub.Unsubscribe() //nolint:errcheck
	const pollWait = 250 * time.Millisecond
	const maxIters = 1_000_000_000
	for i := 0; i < maxIters; i++ {
		if ctx.Err() != nil {
			return
		}
		msg, err := sub.NextMsg(pollWait)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return
		}
		du, ok := dlqUpdateFrom(ctx, svc, msg)
		if !ok {
			continue
		}
		select {
		case out <- du:
		case <-ctx.Done():
			return
		}
	}
}

// dlqUpdateFrom builds a DLQUpdate from one DLQ message. The simplest
// shape — the message carries enough envelope to read the sequence
// from JetStream metadata; we re-fetch the view from the service so
// the same projection helpers stay the single source of truth.
func dlqUpdateFrom(
	ctx context.Context, svc *api.Service, msg *nats.Msg,
) (DLQUpdate, bool) {
	if msg == nil {
		return DLQUpdate{}, false
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return DLQUpdate{}, false
	}
	if svc == nil {
		return DLQUpdate{}, false
	}
	// Pull a small window of recent entries and pick the matching seq.
	// 64 is generous for the live case (we just appended); ListDeadLetters
	// returns newest-first so the entry is in the leading slot.
	views, err := svc.ListDeadLetters(ctx, 64)
	if err != nil {
		return DLQUpdate{}, false
	}
	for _, v := range views {
		if v.Sequence == meta.Sequence.Stream {
			return DLQUpdate{
				View:      v,
				Operation: DLQOpAdded,
			}, true
		}
	}
	return DLQUpdate{}, false
}

// kvBucketsKnown is the list the console exposes by default. Adding a
// bucket here surfaces it in the side nav; missing buckets are
// silently skipped (the engine may not have created them yet).
var kvBucketsKnown = []KVBucketInfo{
	{Name: "workflow_defs", Description: "registered workflow definitions"},
	{Name: "workflow_runs", Description: "live workflow run snapshots"},
	{Name: "triggers", Description: "registered trigger definitions"},
	{Name: "dead_letters", Description: "DLQ projection (auxiliary)"},
	{Name: AuditBucket, Description: "console audit log"},
}

// ListKVBuckets returns the registered KV buckets and their current
// key count. Buckets that don't exist (yet) come back with Keys=0; the
// UI renders them grey-disabled so operators can see the inventory.
func (a *apiServiceAdapter) ListKVBuckets(
	ctx context.Context,
) ([]KVBucketInfo, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListKVBuckets: ctx is nil")
	}
	if a.nc == nil {
		return nil, nil
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	out := make([]KVBucketInfo, 0, len(kvBucketsKnown))
	for _, info := range kvBucketsKnown {
		out = append(out, kvBucketCount(ctx, js, info))
	}
	return out, nil
}

// kvBucketCount enriches one bucket info with its live key count. A
// missing bucket reports zero so the UI doesn't show errors for buckets
// the engine hasn't created yet.
func kvBucketCount(
	ctx context.Context, js jetstream.JetStream, info KVBucketInfo,
) KVBucketInfo {
	kv, err := js.KeyValue(ctx, info.Name)
	if err != nil {
		return info
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return info
	}
	info.Keys = int(status.Values())
	return info
}

// ListKVKeys returns up to limit keys from one bucket. cursor is
// currently unused — JetStream KV.ListKeys doesn't support cursored
// pagination directly. We return an empty next-cursor when the page
// fits; callers can detect "more" by len(keys) == limit.
func (a *apiServiceAdapter) ListKVKeys(
	ctx context.Context, bucket, _ string, limit int,
) ([]string, string, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListKVKeys: ctx is nil")
	}
	if bucket == "" {
		panic("apiServiceAdapter.ListKVKeys: bucket is empty")
	}
	if limit <= 0 {
		panic("apiServiceAdapter.ListKVKeys: limit must be positive")
	}
	if a.nc == nil {
		return nil, "", nil
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, "", fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		// Empty / nonexistent bucket — render the zero state.
		return nil, "", nil
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, "", nil //nolint:nilerr
	}
	defer lister.Stop() //nolint:errcheck
	out := make([]string, 0, limit)
	for key := range lister.Keys() {
		out = append(out, key)
		if len(out) >= limit {
			break
		}
	}
	return out, "", nil
}

// GetKVEntry fetches the value + revision for one key. Returns
// ErrKVNotFound when the key is missing. The value is returned as
// raw bytes so the UI can attempt JSON pretty-printing client-side
// or fall back to a hex dump.
func (a *apiServiceAdapter) GetKVEntry(
	ctx context.Context, bucket, key string,
) (KVEntryView, error) {
	if ctx == nil {
		panic("apiServiceAdapter.GetKVEntry: ctx is nil")
	}
	if bucket == "" {
		panic("apiServiceAdapter.GetKVEntry: bucket is empty")
	}
	if key == "" {
		panic("apiServiceAdapter.GetKVEntry: key is empty")
	}
	if a.nc == nil {
		return KVEntryView{}, ErrKVNotFound
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return KVEntryView{}, fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		return KVEntryView{}, ErrKVNotFound
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		return KVEntryView{}, ErrKVNotFound
	}
	val := entry.Value()
	return KVEntryView{
		Bucket:   bucket,
		Key:      key,
		Value:    val,
		Revision: entry.Revision(),
		Created:  entry.Created(),
		IsJSON:   looksLikeJSON(val),
	}, nil
}

// SparklineData reads the adapter's MetricsSource and buckets the
// matching series into hours-many hourly slots. metrics is nil when
// the operator hasn't wired the aggregator; we honour the honesty
// contract and return (nil, nil) so the renderer hides the canvas.
func (a *apiServiceAdapter) SparklineData(
	ctx context.Context, kind, id string, hours int,
) ([]float64, error) {
	if ctx == nil {
		panic("apiServiceAdapter.SparklineData: ctx is nil")
	}
	if kind == "" {
		panic("apiServiceAdapter.SparklineData: kind is empty")
	}
	if id == "" {
		panic("apiServiceAdapter.SparklineData: id is empty")
	}
	if hours <= 0 || hours > sparklineHoursMax {
		panic("apiServiceAdapter.SparklineData: hours out of range")
	}
	if a.metrics == nil {
		return nil, nil
	}
	name, labelKey, labelValue := sparklineMetricFor(kind)
	if name == "" {
		return nil, nil
	}
	series, ok := a.metrics.MetricSnapshot(name)
	if !ok {
		return nil, nil
	}
	buckets := bucketHourly(series.Points, labelKey, id, hours, time.Now())
	if buckets == nil {
		return nil, nil
	}
	// labelValue is reserved for future variants where the canonical
	// id differs from the trigger/workflow id; today they match.
	_ = labelValue
	return buckets, nil
}

// sparklineHoursMax bounds the request window. 168h = one week — well
// past what a list-row sparkline needs, but the upper bound is here
// so a typo'd caller can't ask for a million-element slice.
const sparklineHoursMax = 168

// sparklineMetricFor maps a (kind) to the metric name + label dimension
// to filter by. Returning the empty metric name signals "no sparkline
// available for this kind" — the adapter then renders the empty state.
// When the engine emits per-workflow or per-trigger metric names with
// the id baked in, this helper is the single place to update.
func sparklineMetricFor(kind string) (name, labelKey, labelValue string) {
	switch kind {
	case "workflow":
		return "workflow.runs.completed", "workflow_id", ""
	case "trigger":
		return "trigger.fires.total", "trigger_id", ""
	}
	return "", "", ""
}

// bucketHourly drops each point matching labelKey==id into the hour
// slot it lands in, counting how many fell into each slot. Returns nil
// when no point matched — the honesty contract: empty data must look
// empty, not flat-zero, in the renderer.
func bucketHourly(
	points []MetricPoint, labelKey, id string, hours int, now time.Time,
) []float64 {
	if hours <= 0 {
		return nil
	}
	out := make([]float64, hours)
	end := now.UTC().Truncate(time.Hour).Add(time.Hour)
	start := end.Add(-time.Duration(hours) * time.Hour)
	matched := false
	for i := range points {
		p := points[i]
		if labelKey != "" && id != "" {
			if p.Labels == nil || p.Labels[labelKey] != id {
				continue
			}
		}
		if p.Timestamp.Before(start) || !p.Timestamp.Before(end) {
			continue
		}
		offset := p.Timestamp.UTC().Sub(start) / time.Hour
		slot := int(offset)
		if slot < 0 || slot >= hours {
			continue
		}
		out[slot] += p.Value
		matched = true
	}
	if !matched {
		return nil
	}
	return out
}

// looksLikeJSON returns true when b begins with `{` or `[` after
// whitespace. Used to flip the renderer into the syntax-tinted view
// without a full parse pass on every read.
func looksLikeJSON(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		}
		return false
	}
	return false
}

// parseHistoryEvent decodes one history message. Returns ok=false on
// malformed JSON — the dropped event is logged at the caller's slog
// elsewhere; here we just signal "skip". Seq comes from the JetStream
// metadata.
func parseHistoryEvent(msg *nats.Msg) (HistoryEvent, bool) {
	if msg == nil {
		return HistoryEvent{}, false
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		return HistoryEvent{}, false
	}
	meta, err := msg.Metadata()
	var seq uint64
	if err == nil && meta != nil {
		seq = meta.Sequence.Stream
	}
	return HistoryEvent{
		Event: api.RunEvent{
			Type:        string(evt.Type),
			RunID:       evt.RunID,
			StepID:      evt.StepID,
			Timestamp:   evt.Timestamp,
			Data:        string(evt.Payload),
			TraceParent: evt.TraceParent,
		},
		Seq: seq,
	}, true
}
