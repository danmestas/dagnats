package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// osGetenv is the default backing of osLookupEnv. Pulled out so the
// variable assignment above can take a function value of the same
// shape without an inline closure.
func osGetenv(key string) string { return os.Getenv(key) }

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

	// StartRun publishes a WorkflowStarted event for the named
	// workflow with the supplied input payload. Returns the new run
	// id on success. Used by the inline Run button on the workflows
	// list (#329). The caller is responsible for the read-only and
	// runnability checks; the DataSource owns only the publish.
	StartRun(ctx context.Context, workflowName string, input []byte) (string, error)

	// FireTrigger publishes one manual workflow.started + TriggerFire
	// history record for the targeted trigger. Returns the new run id
	// on success, api.ErrTriggerKindNotFireable for kinds the manual
	// fire-now path doesn't support (subject / http), or
	// api.ErrTriggerDisabled when the trigger's enabled bit is false.
	// Used by POST /console/triggers/{id}/fire (#352); the caller is
	// responsible for read-only, CSRF, and rate-limit gating.
	FireTrigger(ctx context.Context, triggerID string) (string, error)

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

	// Search returns up to `limit` cross-entity hits matching `query`.
	// Powers the cmd+k command palette (T11): workflows match on name
	// substring; triggers match on id substring; runs match on id
	// prefix only when len(query) >= 4 (cardinality guard — run ids
	// are uuids and a 3-char query would scan the world).
	//
	// Honesty contract: returns (nil, nil) when query is empty/blank.
	// limit must be positive. Hits arrive in workflow → run → trigger
	// order so the palette renders deterministic results; callers can
	// re-sort if they want a different priority.
	Search(ctx context.Context, query string, limit int) ([]SearchHit, error)

	// ConfigSnapshot returns the deployment self-portrait the /config
	// page renders. Bundles workers (#289 directory), JetStream
	// streams + KV buckets, and the NATS endpoint metadata in a
	// single round-trip. Returns a zero-value snapshot + nil error
	// when the underlying NATS connection isn't configured — the
	// page degrades to empty-state cards rather than 500ing on a
	// JetStream-unreachable deployment. #312.
	ConfigSnapshot(ctx context.Context) (ConfigSnapshot, error)

	// AggregateTaskTypes fans out across every live worker (from the
	// `workers` KV bucket, #289) and the `services` KV bucket (#322)
	// to assemble one row per distinct task type. Deduplicates across
	// workers — two workers reporting `email` collapse into one row
	// with two OwnerWorkerIDs.
	//
	// Deliberately a separate method from ConfigSnapshot: the
	// deployment self-portrait is a different lifecycle. ConfigSnapshot
	// answers "what's plugged in"; AggregateTaskTypes answers "what
	// task types could fire right now". Two questions, two methods.
	// See ADR-015 R11 audit (Q5 audit-locked) for the decision trail.
	//
	// Returns nil + nil-error when no workers report — the renderer
	// paints the empty state. #328.
	AggregateTaskTypes(ctx context.Context) ([]TaskTypeRow, error)

	// ListWorkerRows folds the live `workers` KV directory into one
	// projected row per registered worker for the /console/workers
	// page: id, task types, host, last-seen, and a derived liveness
	// status (active when LastSeen is within worker.MaxWorkerStaleness,
	// stale otherwise). Reuses the same ListWorkers read that backs
	// AggregateTaskTypes — no second directory round-trip.
	//
	// Returns nil + nil-error when no workers are registered so the
	// renderer paints the honest empty state rather than 500ing.
	ListWorkerRows(ctx context.Context) ([]WorkerStatusRow, error)

	// ListConsumers returns every JetStream consumer on the engine's
	// known streams, one ConsumerRow each, for the /console/consumers
	// page. A stream that isn't provisioned yet is skipped silently —
	// the page lists what exists. Returns nil + nil-error when NATS
	// isn't wired so the renderer paints the empty state instead of
	// 500ing.
	ListConsumers(ctx context.Context) ([]ConsumerRow, error)

	// ServerHealth returns the embedded NATS server's identity and its
	// JetStream account capacity for the /console/server page. Identity
	// comes from the live connection; capacity from js.AccountInfo().
	// Degrades gracefully: an AccountInfo failure returns identity-only
	// health (no error), and an unwired connection returns the zero
	// value + nil so the renderer paints empty fields rather than 500ing.
	ServerHealth(ctx context.Context) (ServerHealth, error)

	// ListConnections returns the embedded NATS server's live client
	// connections for the /console/connections page, read in-process via
	// Connz(). Degrades gracefully: when no server handle is wired the
	// adapter returns (nil, nil) so the renderer paints the empty state
	// rather than 500ing.
	ListConnections(ctx context.Context) ([]ConnRow, error)

	// AdmissionState returns the engine's read-side admission-gate
	// snapshot for the /console/concurrency page: which singleton locks
	// are currently held (singleton_locks bucket) and which task types
	// have live in-flight concurrency counters (concurrency_tasks
	// bucket). Both gate buckets are empty on an idle engine — the gates
	// are lazy — so the zero value is a first-class result. Degrades
	// gracefully: a missing bucket yields an empty section rather than a
	// whole-page failure, and a malformed KV value is skipped, not fatal.
	AdmissionState(ctx context.Context) (AdmissionState, error)

	// GetRunTrace reads the OTLP spans for one run from the TELEMETRY
	// stream and flattens them into a pre-order span tree the console
	// Trace tab renders. Returns (nil, nil) when no spans exist (the
	// run produced no telemetry or it aged out) or when the underlying
	// NATS connection isn't wired — callers paint the empty state
	// rather than 500ing. The web counterpart of `dagnats trace <id>`.
	GetRunTrace(ctx context.Context, runID string) ([]TraceRow, error)
}

// ConsumerRow is a single JetStream consumer's operational state for the
// console Consumers page. Lag = Delivered-AckFloor; Stalled flags a
// backlog with no waiting pulls (pending>0 && waiting==0) — the
// work-queue "no worker is consuming" signal.
type ConsumerRow struct {
	Stream         string
	Name           string
	Filter         string
	AckPolicy      string
	NumPending     uint64
	NumAckPending  int
	NumWaiting     int
	NumRedelivered int
	Delivered      uint64
	AckFloor       uint64
	Lag            uint64
	AckWait        string
	MaxDeliver     string
	Stalled        bool
}

// ServerHealth is the embedded NATS server identity + JetStream capacity
// (and, when a server-stats handle is wired, live traffic + host) for the
// console Server page. Identity comes from the live nats.Conn. Capacity
// comes from either the embedded server's Jsz snapshot (real ceiling) or,
// when no stats handle is wired, js.AccountInfo() (account tier, usually
// unlimited). Max fields of -1 mean "unlimited" (no configured ceiling).
//
// HasStats is true only when the Jsz snapshot was read, which is what
// drives the template between the rich (Varz/Jsz) view and the lean
// AccountInfo fallback. The Varz-sourced fields (Uptime, Connections,
// traffic, host) are meaningful only when HasStats is true.
type ServerHealth struct {
	ServerName    string
	ServerVersion string
	NATSURL       string
	Domain        string
	MemoryUsed    uint64
	MemoryMax     int64
	StoreUsed     uint64
	StoreMax      int64
	StorePct      int
	Streams       int
	StreamsMax    int
	Consumers     int
	ConsumersMax  int
	APITotal      uint64
	APIErrors     uint64

	// HasStats is true when the embedded server's Jsz snapshot was read,
	// so the template can switch to the rich view. The fields below are
	// populated from Varz/Jsz and are zero on the lean (nil-stats) path.
	HasStats      bool
	Uptime        string
	Connections   int
	TotalConns    uint64
	Subscriptions uint32
	InMsgs        int64
	OutMsgs       int64
	InBytes       int64
	OutBytes      int64
	SlowConsumers int64
	MemBytes      int64
	CPUPercent    float64
	Cores         int
}

// ConnRow is one connected NATS client for the console Connections
// page. PendingBytes is the slow-consumer signal (outbound bytes queued
// for a client that is not reading fast enough).
type ConnRow struct {
	CID          uint64
	Name         string
	Kind         string
	Lang         string
	Version      string
	RTT          string
	Uptime       string
	Idle         string
	Subs         uint32
	PendingBytes int
	InMsgs       int64
	OutMsgs      int64
}

// NATSServerStats exposes the embedded server's in-process monitoring
// snapshots to the console. *natsserver.Server satisfies it; tests
// supply a fake. Connz backs the Connections page; Varz/Jsz back the
// Server page's live traffic, host, and real storage-headroom view.
type NATSServerStats interface {
	Connz(*natsserver.ConnzOptions) (*natsserver.Connz, error)
	Varz(*natsserver.VarzOptions) (*natsserver.Varz, error)
	Jsz(*natsserver.JSzOptions) (*natsserver.JSInfo, error)
}

// connRowFrom maps one Connz ConnInfo onto a ConnRow. Pure; panics on a
// nil info so a malformed Connz snapshot fails loudly rather than
// silently dropping a row.
func connRowFrom(info *natsserver.ConnInfo) ConnRow {
	if info == nil {
		panic("connRowFrom: info is nil")
	}
	return ConnRow{
		CID:          info.Cid,
		Name:         info.Name,
		Kind:         info.Kind,
		Lang:         info.Lang,
		Version:      info.Version,
		RTT:          info.RTT,
		Uptime:       info.Uptime,
		Idle:         info.Idle,
		Subs:         info.NumSubs,
		PendingBytes: info.Pending,
		InMsgs:       info.InMsgs,
		OutMsgs:      info.OutMsgs,
	}
}

// storePct is the integer percentage of used over max, guarded against a
// non-positive max (unlimited tier = -1, or a zero ceiling) so the page
// reports 0% headroom-pressure rather than dividing by zero.
func storePct(used uint64, max int64) int {
	if max <= 0 {
		return 0
	}
	return int(used * 100 / uint64(max))
}

// consumerRowFrom maps one JetStream ConsumerInfo onto a ConsumerRow.
// Pure: the stream name comes from the caller's loop variable, not the
// info, so the page can label each row with the stream it iterated.
func consumerRowFrom(stream string, info *jetstream.ConsumerInfo) ConsumerRow {
	if info == nil {
		panic("consumerRowFrom: info is nil")
	}
	if stream == "" {
		panic("consumerRowFrom: stream is empty")
	}
	delivered := info.Delivered.Stream
	ackFloor := info.AckFloor.Stream
	var lag uint64
	if delivered > ackFloor {
		lag = delivered - ackFloor
	}
	ackWait := "—"
	if info.Config.AckWait > 0 {
		ackWait = info.Config.AckWait.String()
	}
	return ConsumerRow{
		Stream:         stream,
		Name:           info.Name,
		Filter:         consumerFilterLabel(info.Config),
		AckPolicy:      ackPolicyLabel(info.Config.AckPolicy),
		NumPending:     info.NumPending,
		NumAckPending:  info.NumAckPending,
		NumWaiting:     info.NumWaiting,
		NumRedelivered: info.NumRedelivered,
		Delivered:      delivered,
		AckFloor:       ackFloor,
		Lag:            lag,
		AckWait:        ackWait,
		MaxDeliver:     strconv.Itoa(info.Config.MaxDeliver),
		Stalled:        info.NumPending > 0 && info.NumWaiting == 0,
	}
}

// consumerFilterLabel renders the subject filter for a consumer. A
// single FilterSubject wins; a multi-subject filter joins on ", ";
// an unfiltered consumer renders the em-dash placeholder.
func consumerFilterLabel(cfg jetstream.ConsumerConfig) string {
	if cfg.FilterSubject != "" {
		return cfg.FilterSubject
	}
	if len(cfg.FilterSubjects) > 0 {
		return strings.Join(cfg.FilterSubjects, ", ")
	}
	return "—"
}

// ackPolicyLabel maps the JetStream ack policy enum to the operator
// token the table renders. Unknown policies fall through to the
// em-dash so a future enum addition renders something neutral.
func ackPolicyLabel(p jetstream.AckPolicy) string {
	switch p {
	case jetstream.AckExplicitPolicy:
		return "explicit"
	case jetstream.AckNonePolicy:
		return "none"
	case jetstream.AckAllPolicy:
		return "all"
	}
	return "—"
}

// ConfigSnapshot is the resource inventory the /config page renders.
// Counts that come from existing list calls (workflows, triggers,
// DLQ) stay outside this snapshot — the page sources them via the
// already-extant DataSource methods so we don't duplicate enumeration
// into one place.
//
// Empty / zero-valued fields are the documented "unreachable" signal:
// the page paints a clear empty state on a per-section basis.
type ConfigSnapshot struct {
	NATSURL           string
	NATSServerVersion string
	OTLPEndpoint      string
	Streams           []StreamSnapshot
	KVBuckets         []KVBucketInfo
	Workers           []worker.WorkerRegistration
	// NATSEmbedded is true when the NATS server is the in-process
	// embedded one this dagnats binary started, false when the
	// connection is against an external server. The R9 build-info
	// footer surfaces this so operators can tell at a glance whether
	// they're looking at a self-contained deployment or a
	// participant in a larger cluster. The adapter populates this
	// from whether the engine owns the NATS lifecycle (#320).
	NATSEmbedded bool
}

// StreamSnapshot is one JetStream stream entry. Retention is the
// human-readable form ("limits", "interest", "workqueue") so the
// renderer doesn't have to map jetstream.RetentionPolicy values.
// Provisioned is false when the stream is listed as known but the
// JetStream account couldn't confirm it — the row renders muted.
type StreamSnapshot struct {
	Name        string
	Subjects    []string
	Messages    uint64
	Bytes       uint64
	Consumers   int
	Retention   string
	Provisioned bool
}

// WorkerStatusRow is one projected row on the /console/workers page.
// Every field is read straight from the worker's KV registration —
// no synthetic columns. Status is "active" when the worker's LastSeen
// is within worker.MaxWorkerStaleness of now, "stale" otherwise.
// TaskTypes is the comma-joined task-type list the worker advertised.
type WorkerStatusRow struct {
	WorkerID  string
	TaskTypes string
	Host      string
	LastSeen  string
	Status    string
}

// SearchHit is one result row in the cmd+k command palette. Kind tags
// the entity ("workflow" | "run" | "trigger") so the palette can show
// a category badge; Label is the primary display string; Subtitle is
// supporting context (step count, workflow id, trigger kind); Href is
// where the palette navigates on selection.
type SearchHit struct {
	Kind     string
	ID       string
	Label    string
	Subtitle string
	Href     string
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
	// stats is the embedded server's in-process monitoring surface used
	// by ListConnections. nil when no server handle was wired; the
	// adapter then returns (nil, nil) so the page paints empty state.
	stats NATSServerStats
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

// WithServerStats attaches a NATSServerStats (the embedded server's
// in-process monitoring surface) to the adapter. Returns the receiver so
// callers can chain in the Config wiring. Pass nil to detach; nil is the
// no-server case and ListConnections will return (nil, nil) so the page
// paints empty state rather than 500ing.
//
// Mirrors WithMetrics: kept separate from NewAPIDataSource so the
// construction surface stays unchanged for the many tests that build an
// adapter directly. A no-op for fakes (tests carry their own state).
func WithServerStats(ds DataSource, stats NATSServerStats) DataSource {
	if ds == nil {
		panic("WithServerStats: ds is nil")
	}
	a, ok := ds.(*apiServiceAdapter)
	if !ok {
		return ds
	}
	a.stats = stats
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

func (a *apiServiceAdapter) StartRun(
	ctx context.Context, workflowName string, input []byte,
) (string, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.StartRun: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.StartRun: ctx is nil")
	}
	if workflowName == "" {
		panic("apiServiceAdapter.StartRun: workflowName is empty")
	}
	return a.svc.StartRun(ctx, workflowName, input)
}

func (a *apiServiceAdapter) FireTrigger(
	ctx context.Context, triggerID string,
) (string, error) {
	if a.svc == nil {
		panic("apiServiceAdapter.FireTrigger: svc is nil")
	}
	if ctx == nil {
		panic("apiServiceAdapter.FireTrigger: ctx is nil")
	}
	if triggerID == "" {
		panic("apiServiceAdapter.FireTrigger: triggerID is empty")
	}
	return a.svc.FireTrigger(ctx, triggerID)
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

// LockRow is a held singleton lock on the Concurrency page.
type LockRow struct {
	Key    string // workflow name, or workflow.<keypath-value> when keyed
	Scope  string // "workflow" | "keyed"
	HeldBy string // run id holding the lock
}

// SlotRow is a live per-task-type in-flight concurrency counter.
type SlotRow struct {
	Name     string // task type (concurrency_tasks key with the "task." prefix stripped)
	InFlight int
}

// RateLimitRow is a token-bucket rate limiter on the Concurrency page.
// Tokens is the current balance; Limit/Period the refill config.
type RateLimitRow struct {
	Key    string
	Tokens int
	Limit  int
	Period string // humanized period_ns
}

// DebounceRow is an open trigger-debounce window.
type DebounceRow struct {
	Trigger  string
	TimerSeq uint64
}

// AdmissionState is the read-side snapshot for /console/concurrency:
// which singleton locks are held, which task types have in-flight
// concurrency counters, and the two lazy admission gates — token-bucket
// rate limits and open trigger-debounce windows. Empty on an idle engine
// (every gate is lazy — populated only under contention).
type AdmissionState struct {
	Locks      []LockRow
	TaskSlots  []SlotRow
	RateLimits []RateLimitRow
	Debouncers []DebounceRow
}

// admissionLocksBucket / admissionTasksBucket are the engine KV buckets
// the Concurrency page reads. admissionTaskPrefix is stripped from each
// concurrency_tasks key (e.g. "task.email" -> "email") to recover the
// task type. These mirror the engine's bucket layout; the console reads
// the wire shape and never imports engine internals.
const (
	admissionLocksBucket    = "singleton_locks"
	admissionTasksBucket    = "concurrency_tasks"
	admissionRateLimitsBkt  = "rate_limits"
	admissionDebounceBucket = "debounce_state"
	admissionTaskPrefix     = "task."
	// admissionKeyMax bounds the per-bucket key scan so a runaway bucket
	// can't unbound the page render.
	admissionKeyMax = 500
)

// parseLockRunID extracts the run id holding a singleton lock from its
// JSON value ({"run_id":"..."}). An empty run_id is valid (returns ""
// + nil); malformed JSON returns an error rather than panicking.
func parseLockRunID(b []byte) (string, error) {
	if b == nil {
		panic("parseLockRunID: value is nil")
	}
	var v struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return "", fmt.Errorf("parse lock value: %w", err)
	}
	return v.RunID, nil
}

// parseCounterValue parses a concurrency_tasks counter value: a raw
// ASCII decimal string (e.g. "3"). Non-numeric input returns an error.
func parseCounterValue(b []byte) (int, error) {
	if b == nil {
		panic("parseCounterValue: value is nil")
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("parse counter value: %w", err)
	}
	return n, nil
}

// parseRateLimit decodes a rate_limits token-bucket value
// ({"tokens":..,"limit":..,"period_ns":..}). period_ns is humanized via
// time.Duration. The caller sets the row Key from the KV key. Malformed
// JSON returns an error rather than panicking so one corrupt entry is
// skipped, not fatal.
func parseRateLimit(b []byte) (tokens, limit int, period string, err error) {
	if b == nil {
		panic("parseRateLimit: value is nil")
	}
	var v struct {
		Tokens   int   `json:"tokens"`
		Limit    int   `json:"limit"`
		PeriodNs int64 `json:"period_ns"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return 0, 0, "", fmt.Errorf("parse rate-limit value: %w", err)
	}
	return v.Tokens, v.Limit, time.Duration(v.PeriodNs).String(), nil
}

// parseDebounce decodes a debounce_state value, surfacing only the
// timer_seq field the page renders. Malformed JSON returns an error so
// the entry is skipped rather than blanking the page.
func parseDebounce(b []byte) (timerSeq uint64, err error) {
	if b == nil {
		panic("parseDebounce: value is nil")
	}
	var v struct {
		TimerSeq uint64 `json:"timer_seq"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return 0, fmt.Errorf("parse debounce value: %w", err)
	}
	return v.TimerSeq, nil
}

// lockScopeOf classifies a singleton-lock key: a key carrying a keypath
// value is "<workflow>.<value>" (keyed scope); a bare workflow name is
// workflow scope.
func lockScopeOf(key string) string {
	if strings.Contains(key, ".") {
		return "keyed"
	}
	return "workflow"
}

// buildAdmissionState assembles an AdmissionState from already-read KV
// bytes: locks maps singleton_locks key -> JSON value, taskCounters maps
// concurrency_tasks key -> decimal counter value, rateLimits maps
// rate_limits key -> token-bucket JSON, debouncers maps debounce_state
// key -> debounce JSON. Malformed values are skipped (not fatal) so one
// corrupt entry can't blank the whole page.
func buildAdmissionState(
	locks, taskCounters, rateLimits, debouncers map[string][]byte,
) AdmissionState {
	if locks == nil {
		panic("buildAdmissionState: locks map is nil")
	}
	if taskCounters == nil {
		panic("buildAdmissionState: taskCounters map is nil")
	}
	if rateLimits == nil {
		panic("buildAdmissionState: rateLimits map is nil")
	}
	if debouncers == nil {
		panic("buildAdmissionState: debouncers map is nil")
	}
	state := AdmissionState{
		Locks:      make([]LockRow, 0, len(locks)),
		TaskSlots:  make([]SlotRow, 0, len(taskCounters)),
		RateLimits: buildRateLimits(rateLimits),
		Debouncers: buildDebouncers(debouncers),
	}
	for key, value := range locks {
		runID, err := parseLockRunID(value)
		if err != nil {
			continue
		}
		state.Locks = append(state.Locks, LockRow{
			Key: key, Scope: lockScopeOf(key), HeldBy: runID,
		})
	}
	for key, value := range taskCounters {
		count, err := parseCounterValue(value)
		if err != nil {
			continue
		}
		name := strings.TrimPrefix(key, admissionTaskPrefix)
		state.TaskSlots = append(state.TaskSlots, SlotRow{
			Name: name, InFlight: count,
		})
	}
	return state
}

// buildRateLimits projects rate_limits KV bytes into RateLimitRows,
// skipping malformed values. The Key is the raw KV key (e.g.
// "<taskType>._global"); the parser owns the token-bucket fields.
func buildRateLimits(rateLimits map[string][]byte) []RateLimitRow {
	if rateLimits == nil {
		panic("buildRateLimits: rateLimits map is nil")
	}
	rows := make([]RateLimitRow, 0, len(rateLimits))
	for key, value := range rateLimits {
		tokens, limit, period, err := parseRateLimit(value)
		if err != nil {
			continue
		}
		rows = append(rows, RateLimitRow{
			Key: key, Tokens: tokens, Limit: limit, Period: period,
		})
	}
	return rows
}

// buildDebouncers projects debounce_state KV bytes into DebounceRows,
// skipping malformed values. The Trigger is the raw KV key.
func buildDebouncers(debouncers map[string][]byte) []DebounceRow {
	if debouncers == nil {
		panic("buildDebouncers: debouncers map is nil")
	}
	rows := make([]DebounceRow, 0, len(debouncers))
	for key, value := range debouncers {
		timerSeq, err := parseDebounce(value)
		if err != nil {
			continue
		}
		rows = append(rows, DebounceRow{Trigger: key, TimerSeq: timerSeq})
	}
	return rows
}

// AdmissionState reads the two admission-gate KV buckets and assembles
// the /console/concurrency snapshot. A missing bucket degrades to an
// empty section (readBucketValues returns an empty map, not an error);
// a malformed value is skipped by buildAdmissionState.
func (a *apiServiceAdapter) AdmissionState(
	ctx context.Context,
) (AdmissionState, error) {
	if ctx == nil {
		panic("apiServiceAdapter.AdmissionState: ctx is nil")
	}
	if a == nil {
		panic("apiServiceAdapter.AdmissionState: adapter is nil")
	}
	locks, err := a.readBucketValues(ctx, admissionLocksBucket)
	if err != nil {
		return AdmissionState{}, fmt.Errorf("read locks bucket: %w", err)
	}
	tasks, err := a.readBucketValues(ctx, admissionTasksBucket)
	if err != nil {
		return AdmissionState{}, fmt.Errorf("read tasks bucket: %w", err)
	}
	rateLimits, err := a.readBucketValues(ctx, admissionRateLimitsBkt)
	if err != nil {
		return AdmissionState{}, fmt.Errorf("read rate-limits bucket: %w", err)
	}
	debouncers, err := a.readBucketValues(ctx, admissionDebounceBucket)
	if err != nil {
		return AdmissionState{}, fmt.Errorf("read debounce bucket: %w", err)
	}
	return buildAdmissionState(locks, tasks, rateLimits, debouncers), nil
}

// readBucketValues reads up to admissionKeyMax key/value pairs from one
// bucket using the existing ListKVKeys + GetKVEntry plumbing. A missing
// key (ErrKVNotFound, racing a delete) is skipped; an absent bucket
// surfaces as an empty key list, so this returns an empty map + nil.
func (a *apiServiceAdapter) readBucketValues(
	ctx context.Context, bucket string,
) (map[string][]byte, error) {
	if ctx == nil {
		panic("apiServiceAdapter.readBucketValues: ctx is nil")
	}
	if bucket == "" {
		panic("apiServiceAdapter.readBucketValues: bucket is empty")
	}
	keys, _, err := a.ListKVKeys(ctx, bucket, "", admissionKeyMax)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		entry, getErr := a.GetKVEntry(ctx, bucket, key)
		if getErr != nil {
			continue
		}
		out[key] = entry.Value
	}
	return out, nil
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

// runIDSearchMinChars is the minimum query length before Search will
// scan run ids. Run ids are uuids (high cardinality) and any shorter
// query would match an unreadable swarm of nonsense; the floor keeps
// the palette honest about what it can find.
const runIDSearchMinChars = 4

// Search powers the cmd+k command palette. Workflows + triggers match
// on lowercase substring; runs match on lowercase prefix (≥4 chars).
// Caps the result slice at `limit` so the palette always renders a
// bounded list — large worker fleets shouldn't cause the response to
// balloon. See SearchHit godoc for the field contract.
func (a *apiServiceAdapter) Search(
	ctx context.Context, query string, limit int,
) ([]SearchHit, error) {
	if ctx == nil {
		panic("apiServiceAdapter.Search: ctx is nil")
	}
	if limit <= 0 {
		panic("apiServiceAdapter.Search: limit must be positive")
	}
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return nil, nil
	}
	hits := make([]SearchHit, 0, limit)
	hits = appendWorkflowHits(ctx, a.svc, q, limit, hits)
	hits = appendRunHits(ctx, a.svc, q, limit, hits)
	hits = appendTriggerHits(ctx, a.svc, q, limit, hits)
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

// appendWorkflowHits scans the workflow list and appends every match.
// We bound the loop with limit so a worker fleet with 10k workflows
// can't make one keystroke trigger a 10k-element response.
func appendWorkflowHits(
	ctx context.Context, svc *api.Service, q string,
	limit int, hits []SearchHit,
) []SearchHit {
	if svc == nil {
		return hits
	}
	wfs, err := svc.ListWorkflows(ctx)
	if err != nil {
		return hits
	}
	for i := 0; i < len(wfs) && len(hits) < limit; i++ {
		wf := wfs[i]
		if !strings.Contains(strings.ToLower(wf.Name), q) {
			continue
		}
		hits = append(hits, SearchHit{
			Kind:     "workflow",
			ID:       wf.Name,
			Label:    wf.Name,
			Subtitle: fmt.Sprintf("%d steps", len(wf.Steps)),
			Href:     "/console/workflows/" + wf.Name,
		})
	}
	return hits
}

// appendRunHits scans recent runs for an id-prefix match. Runs are
// uuids (high cardinality); the min-chars floor (runIDSearchMinChars)
// keeps short queries from returning whatever happened to sort first.
// We list-and-filter rather than calling GetRun directly so an operator
// who knows only the first 4-8 chars of a run id still finds it.
//
// We try a direct GetRun first as a cheap path for the full-id case
// (paste-the-whole-thing), then fall back to a prefix scan over the
// recent runs list.
func appendRunHits(
	ctx context.Context, svc *api.Service, q string,
	limit int, hits []SearchHit,
) []SearchHit {
	if svc == nil || len(q) < runIDSearchMinChars || len(hits) >= limit {
		return hits
	}
	if run, err := svc.GetRun(ctx, q); err == nil {
		hits = append(hits, makeRunHit(run))
		return hits
	}
	runs, err := svc.ListRuns(ctx, "")
	if err != nil {
		return hits
	}
	for i := 0; i < len(runs) && len(hits) < limit; i++ {
		if !strings.HasPrefix(strings.ToLower(runs[i].RunID), q) {
			continue
		}
		hits = append(hits, makeRunHit(runs[i]))
	}
	return hits
}

// makeRunHit renders one run as a SearchHit row. Pulled out so both
// the exact-match shortcut and the prefix-scan path produce identical
// rows (same label trimming, same subtitle).
func makeRunHit(run dag.WorkflowRun) SearchHit {
	label := run.RunID
	if len(label) > 12 {
		label = label[:12] + "…"
	}
	return SearchHit{
		Kind:     "run",
		ID:       run.RunID,
		Label:    label,
		Subtitle: run.WorkflowID,
		Href:     "/console/runs/" + run.RunID,
	}
}

// appendTriggerHits walks the trigger list. Subtitle is the trigger
// kind ("cron", "http", "subject", "webhook") because the kind is the
// useful disambiguator when an operator searches for a trigger id.
func appendTriggerHits(
	ctx context.Context, svc *api.Service, q string,
	limit int, hits []SearchHit,
) []SearchHit {
	if svc == nil {
		return hits
	}
	trs, err := svc.ListTriggers(ctx)
	if err != nil {
		return hits
	}
	for i := 0; i < len(trs) && len(hits) < limit; i++ {
		tr := trs[i]
		if !strings.Contains(strings.ToLower(tr.ID), q) {
			continue
		}
		kind, _ := triggerKindAndTarget(tr)
		hits = append(hits, SearchHit{
			Kind:     "trigger",
			ID:       tr.ID,
			Label:    tr.ID,
			Subtitle: kind,
			Href:     "/console/triggers/" + tr.ID,
		})
	}
	return hits
}

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

// ConfigSnapshot fans out into JetStream + the workers directory to
// assemble the /console/config view. Each section is best-effort: a
// JetStream miss leaves Streams nil; an empty workers bucket leaves
// Workers nil. The renderer paints empty-state copy per section.
//
// The adapter does NOT hard-fail on partial reachability — the
// operator's expectation on /config is "show me what you can see",
// not "fail loudly when one bucket is missing."
func (a *apiServiceAdapter) ConfigSnapshot(
	ctx context.Context,
) (ConfigSnapshot, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ConfigSnapshot: ctx is nil")
	}
	snap := ConfigSnapshot{}
	if a.nc != nil {
		snap.NATSURL = a.nc.ConnectedUrl()
		snap.NATSServerVersion = a.nc.ConnectedServerVersion()
		// dagnats today always starts its NATS in-process; the
		// console always shows (embedded). When an external-NATS
		// deployment mode lands, replace this with the engine's
		// authoritative ownership flag — the field is the seam.
		snap.NATSEmbedded = true
	}
	snap.OTLPEndpoint = otlpEndpointFromEnv()
	if buckets, err := a.ListKVBuckets(ctx); err == nil {
		snap.KVBuckets = buckets
	}
	if a.nc != nil {
		snap.Streams = listKnownStreams(ctx, a.nc)
	}
	if a.svc != nil {
		if regs, err := a.svc.ListWorkers(ctx); err == nil {
			snap.Workers = regs
		}
	}
	return snap, nil
}

// otlpEndpointFromEnv reads the standard OTLP env vars. We keep the
// console out of the OTEL initialization path (a separate observability
// arc owns that); the env var read here is the same surface every
// OTEL-instrumented Go process honours. Empty string ⇒ the section
// hides on the page.
func otlpEndpointFromEnv() string {
	for _, v := range []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
	} {
		if got := osLookupEnv(v); got != "" {
			return got
		}
	}
	return ""
}

// listKnownStreams probes every stream natsutil provisions. Each
// stream that can't be looked up renders as Provisioned=false so the
// operator sees the planned shape even when one stream hasn't been
// created yet (e.g. fresh boot). Bounded loop by the static list.
func listKnownStreams(
	ctx context.Context, nc *nats.Conn,
) []StreamSnapshot {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil
	}
	known := configStreamNames()
	out := make([]StreamSnapshot, 0, len(known))
	for _, name := range known {
		out = append(out, lookupStreamSnapshot(ctx, js, name))
	}
	return out
}

// lookupStreamSnapshot fetches one stream's info. Missing streams
// surface as a row with Provisioned=false so the renderer can show
// the planned-but-absent state.
func lookupStreamSnapshot(
	ctx context.Context, js jetstream.JetStream, name string,
) StreamSnapshot {
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return StreamSnapshot{Name: name, Provisioned: false}
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return StreamSnapshot{Name: name, Provisioned: false}
	}
	return StreamSnapshot{
		Name:        info.Config.Name,
		Subjects:    info.Config.Subjects,
		Messages:    info.State.Msgs,
		Bytes:       info.State.Bytes,
		Consumers:   info.State.Consumers,
		Retention:   retentionLabel(info.Config.Retention),
		Provisioned: true,
	}
}

// retentionLabel converts a JetStream retention enum into the
// human-readable token the YAML export + table render. Unknown
// values fall through to the enum's String() so a future addition
// at least renders something rather than blank.
func retentionLabel(r jetstream.RetentionPolicy) string {
	switch r {
	case jetstream.LimitsPolicy:
		return "limits"
	case jetstream.InterestPolicy:
		return "interest"
	case jetstream.WorkQueuePolicy:
		return "workqueue"
	}
	return r.String()
}

// configStreamNames returns the streams the console expects to find.
// Mirrors api.knownStreams (the cluster-health surface) but kept
// local so the console doesn't reach into the api package's
// unexported var. Adding a stream here surfaces it on /config.
func configStreamNames() []string {
	return []string{
		"WORKFLOW_HISTORY",
		"TASK_QUEUES",
		"EVENTS",
		"DEAD_LETTERS",
		"SLEEP_TIMERS",
	}
}

// ListConsumers walks every known stream and folds its JetStream
// consumers into ConsumerRows. A stream that isn't provisioned yet is
// skipped (its Stream() lookup errors) so the page lists what exists
// rather than surfacing phantom rows. Bounded by the static stream
// list and JetStream's paged consumer lister. Only a jetstream.New
// failure errors; nc-not-wired degrades to (nil, nil).
func (a *apiServiceAdapter) ListConsumers(
	ctx context.Context,
) ([]ConsumerRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListConsumers: ctx is nil")
	}
	if a.nc == nil {
		return nil, nil
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	out := make([]ConsumerRow, 0)
	for _, name := range configStreamNames() {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			continue
		}
		lister := stream.ListConsumers(ctx)
		for info := range lister.Info() {
			out = append(out, consumerRowFrom(name, info))
		}
	}
	return out, nil
}

// ServerHealth reads the embedded NATS server's identity off the live
// connection, then its capacity from one of two sources. When a
// server-stats handle is wired it reads the REAL ceiling + live traffic +
// host from Varz/Jsz (HasStats=true); otherwise it falls back to the
// JetStream account capacity from js.AccountInfo() (HasStats=false), whose
// tier is usually unlimited. Identity is taken from the connection when
// present so both paths still label the server. Graceful degradation: an
// unwired everything returns the zero value, and a capacity read failure
// returns identity-only health — neither errors, so a hiccup paints a
// partial page instead of a 500.
func (a *apiServiceAdapter) ServerHealth(
	ctx context.Context,
) (ServerHealth, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ServerHealth: ctx is nil")
	}
	if a == nil {
		panic("apiServiceAdapter.ServerHealth: receiver is nil")
	}
	if a.nc == nil && a.stats == nil {
		return ServerHealth{}, nil
	}
	health := ServerHealth{}
	if a.nc != nil {
		health.ServerName = a.nc.ConnectedServerName()
		health.ServerVersion = a.nc.ConnectedServerVersion()
		health.NATSURL = a.nc.ConnectedUrl()
	}
	if a.stats != nil {
		a.serverHealthFromStats(&health)
		return health, nil
	}
	a.serverHealthFromAccount(ctx, &health)
	return health, nil
}

// serverHealthFromStats reads the embedded server's Varz + Jsz snapshots
// and folds them into health, preferring the real per-server ceiling and
// live traffic. Each read degrades independently: a Varz error skips the
// traffic/host half, and HasStats is set only when Jsz (the capacity
// half) succeeds.
func (a *apiServiceAdapter) serverHealthFromStats(h *ServerHealth) {
	if h == nil {
		panic("apiServiceAdapter.serverHealthFromStats: h is nil")
	}
	if a.stats == nil {
		panic("apiServiceAdapter.serverHealthFromStats: stats is nil")
	}
	if varz, err := a.stats.Varz(nil); err == nil && varz != nil {
		serverHealthFromVarz(h, varz)
	}
	if jsz, err := a.stats.Jsz(&natsserver.JSzOptions{}); err == nil && jsz != nil {
		serverHealthFromJsz(h, jsz)
		h.HasStats = true
	}
}

// serverHealthFromVarz maps the process-wide Varz snapshot (identity,
// uptime, live traffic, host) onto health. Pure; panics on nil so a
// malformed read fails loudly.
func serverHealthFromVarz(h *ServerHealth, v *natsserver.Varz) {
	if h == nil || v == nil {
		panic("serverHealthFromVarz: nil arg")
	}
	if v.Version != "" {
		h.ServerVersion = v.Version
	}
	h.Uptime = v.Uptime
	h.Connections = v.Connections
	h.TotalConns = v.TotalConnections
	h.Subscriptions = v.Subscriptions
	h.InMsgs = v.InMsgs
	h.OutMsgs = v.OutMsgs
	h.InBytes = v.InBytes
	h.OutBytes = v.OutBytes
	h.SlowConsumers = v.SlowConsumers
	h.MemBytes = v.Mem
	h.CPUPercent = v.CPU
	h.Cores = v.Cores
}

// serverHealthFromJsz maps the JetStream Jsz snapshot (real per-server
// storage/memory ceiling and counts) onto health. Pure; panics on nil so
// a malformed read fails loudly.
func serverHealthFromJsz(h *ServerHealth, j *natsserver.JSInfo) {
	if h == nil || j == nil {
		panic("serverHealthFromJsz: nil arg")
	}
	h.Domain = j.Config.Domain
	h.StoreUsed = j.Store
	h.StoreMax = j.Config.MaxStore
	h.StorePct = storePct(j.Store, j.Config.MaxStore)
	h.MemoryUsed = j.Memory
	h.MemoryMax = j.Config.MaxMemory
	h.Streams = j.Streams
	h.StreamsMax = -1
	h.Consumers = j.Consumers
	h.ConsumersMax = -1
	h.APITotal = j.API.Total
	h.APIErrors = j.API.Errors
}

// serverHealthFromAccount is the lean fallback when no server-stats handle
// is wired: capacity comes from js.AccountInfo() (tier, usually
// unlimited). HasStats stays false so the template paints the lean view. A
// JetStream failure leaves identity-only health.
func (a *apiServiceAdapter) serverHealthFromAccount(
	ctx context.Context, h *ServerHealth,
) {
	if h == nil {
		panic("apiServiceAdapter.serverHealthFromAccount: h is nil")
	}
	if a.nc == nil {
		return
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return
	}
	info, err := js.AccountInfo(ctx)
	if err != nil {
		return
	}
	h.Domain = info.Domain
	h.MemoryUsed = info.Memory
	h.MemoryMax = info.Limits.MaxMemory
	h.StoreUsed = info.Store
	h.StoreMax = info.Limits.MaxStore
	h.StorePct = storePct(info.Store, info.Limits.MaxStore)
	h.Streams = info.Streams
	h.StreamsMax = info.Limits.MaxStreams
	h.Consumers = info.Consumers
	h.ConsumersMax = info.Limits.MaxConsumers
	h.APITotal = info.API.Total
	h.APIErrors = info.API.Errors
}

// ListConnections folds the embedded server's live client connections
// into ConnRows via an in-process Connz() snapshot. An unwired server
// handle degrades to (nil, nil) so the page paints empty state instead
// of 500ing; a Connz read failure errors so the operator learns the
// monitoring read broke. Bounded by the connection list Connz returns.
func (a *apiServiceAdapter) ListConnections(
	ctx context.Context,
) ([]ConnRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListConnections: ctx is nil")
	}
	if a == nil {
		panic("apiServiceAdapter.ListConnections: receiver is nil")
	}
	if a.stats == nil {
		return nil, nil
	}
	cz, err := a.stats.Connz(&natsserver.ConnzOptions{Subscriptions: false})
	if err != nil {
		return nil, fmt.Errorf("connz: %w", err)
	}
	rows := make([]ConnRow, 0, len(cz.Conns))
	for _, info := range cz.Conns {
		rows = append(rows, connRowFrom(info))
	}
	return rows, nil
}

// osLookupEnv is a thin seam so tests can intercept env-var reads
// without standing up the OS. Default delegates to os.Getenv; tests
// reassign this package-level var to feed deterministic values.
var osLookupEnv = osGetenv

// AggregateTaskTypes folds the live workers directory and the
// `services` KV bucket (#322) into one TaskTypeRow per distinct task
// type. Grouping itself is structural on the `service::task` substring
// from ADR-017; the services bucket is consulted to attach each
// registered service's Description onto every row in its group (#335).
// Unknown service prefixes still group — they just carry an empty
// description, which the renderer treats as "no tooltip" rather than
// surfacing an empty popover.
//
// Best-effort across both reads. A miss on the workers list collapses
// to (nil, nil) so the page paints empty state instead of 500ing. A
// miss on the services bucket leaves rows un-augmented; the page
// renders the groups without descriptions rather than failing.
func (a *apiServiceAdapter) AggregateTaskTypes(
	ctx context.Context,
) ([]TaskTypeRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.AggregateTaskTypes: ctx is nil")
	}
	if a.svc == nil {
		panic("apiServiceAdapter.AggregateTaskTypes: svc is nil")
	}
	regs, err := a.svc.ListWorkers(ctx)
	if err != nil {
		return nil, nil
	}
	rows := aggregateTaskTypesFromWorkers(regs)
	// Best-effort services cross-reference. The nc handle is the only
	// path to JetStream from the adapter; when it's missing we skip
	// the lookup and let rows render without descriptions rather than
	// erroring the whole page.
	if a.nc == nil {
		return rows, nil
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return rows, nil
	}
	defs, err := worker.ListServices(js)
	if err != nil {
		return rows, nil
	}
	return attachServiceDescriptions(rows, defs), nil
}

// attachServiceDescriptions copies the Description field from each
// registered ServiceDef onto every TaskTypeRow whose Service prefix
// matches. Pure function so tests can drive it without a JetStream
// handle. Rows whose Service is empty (the "(default)" bucket at
// render time) or whose Service is not in defs are returned unchanged.
//
// Bounded by len(rows) — the rows slice is already capped at the
// worker SDK's per-registration slice length × maxRegs in
// aggregateTaskTypesFromWorkers, so an extra pass here is cheap.
func attachServiceDescriptions(
	rows []TaskTypeRow, defs []worker.ServiceDef,
) []TaskTypeRow {
	if len(rows) == 0 {
		return rows
	}
	const maxDefs = 10000
	if len(defs) > maxDefs {
		panic("attachServiceDescriptions: defs exceeds max bound")
	}
	if len(defs) == 0 {
		return rows
	}
	byName := make(map[string]string, len(defs))
	for _, d := range defs {
		if d.Name == "" {
			continue
		}
		byName[d.Name] = d.Description
	}
	for i := range rows {
		if rows[i].Service == "" {
			continue
		}
		if desc, ok := byName[rows[i].Service]; ok {
			rows[i].ServiceDescription = desc
		}
	}
	return rows
}

// aggregateTaskTypesFromWorkers turns the worker registrations into
// the deduplicated row set. Pure function so tests can drive it
// without standing up an adapter. Bounded by len(regs) * maxTypesPer
// — registrations carry a slice of task types each, but the worker
// SDK keeps that slice short (one to a few entries per worker).
func aggregateTaskTypesFromWorkers(
	regs []worker.WorkerRegistration,
) []TaskTypeRow {
	if len(regs) == 0 {
		return nil
	}
	const maxRegs = 10000
	if len(regs) > maxRegs {
		panic(
			"aggregateTaskTypesFromWorkers: regs exceeds max bound",
		)
	}
	byType := make(map[string]*TaskTypeRow, len(regs))
	for _, reg := range regs {
		for _, t := range reg.TaskTypes {
			if t == "" {
				continue
			}
			row, ok := byType[t]
			if !ok {
				row = &TaskTypeRow{
					TaskType:          t,
					Service:           splitServicePrefix(t),
					RecentInvocations: -1,
					AvgDurationMS:     -1,
					FailureRate:       -1,
					RunHref:           "/console/runs",
				}
				byType[t] = row
			}
			row.OwnerWorkerIDs = append(
				row.OwnerWorkerIDs, reg.WorkerID,
			)
		}
	}
	out := make([]TaskTypeRow, 0, len(byType))
	for _, row := range byType {
		out = append(out, *row)
	}
	return out
}

// ListWorkerRows reads the live worker directory and projects each
// registration into a render row. Reuses ListWorkers — the same read
// AggregateTaskTypes folds — so the workers page and the functions
// page share one directory round-trip's worth of cost. A read miss
// collapses to (nil, nil) so the page paints empty state.
func (a *apiServiceAdapter) ListWorkerRows(
	ctx context.Context,
) ([]WorkerStatusRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.ListWorkerRows: ctx is nil")
	}
	if a.svc == nil {
		panic("apiServiceAdapter.ListWorkerRows: svc is nil")
	}
	regs, err := a.svc.ListWorkers(ctx)
	if err != nil {
		return nil, nil
	}
	return workerRowsFromRegistrations(regs, time.Now()), nil
}

// workerRowsFromRegistrations projects worker registrations into render
// rows. Pure function so tests can drive it without an adapter. now is
// passed in so liveness classification is deterministic under test.
// Bounded by len(regs).
func workerRowsFromRegistrations(
	regs []worker.WorkerRegistration, now time.Time,
) []WorkerStatusRow {
	if len(regs) == 0 {
		return nil
	}
	const maxRegs = 10000
	if len(regs) > maxRegs {
		panic("workerRowsFromRegistrations: regs exceeds max bound")
	}
	out := make([]WorkerStatusRow, 0, len(regs))
	for _, reg := range regs {
		status := "active"
		lastSeen := ""
		if !reg.LastSeen.IsZero() {
			lastSeen = reg.LastSeen.UTC().Format(time.RFC3339)
			if now.Sub(reg.LastSeen) > worker.MaxWorkerStaleness {
				status = "stale"
			}
		}
		out = append(out, WorkerStatusRow{
			WorkerID:  reg.WorkerID,
			TaskTypes: strings.Join(reg.TaskTypes, ", "),
			Host:      reg.Hostname,
			LastSeen:  lastSeen,
			Status:    status,
		})
	}
	return out
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
