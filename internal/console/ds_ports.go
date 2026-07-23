package console

import (
	"context"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// The console reads and mutates the running system through a set of
// small domain ports rather than one mega-interface. Each page handler
// depends only on the port(s) it needs, so a unit test substitutes a
// fake implementing just that slice — not the whole world. The concrete
// apiServiceAdapter implements every port; NewAPIDataSource returns it
// as a DataSource, so the compiler enforces full coverage at build time.
//
// Every method must be safe to call concurrently with the rest of the
// system; the underlying api.Service already meets that bar.
//
// Streaming methods return a receive-only channel that closes when ctx
// is cancelled. The KV / JetStream resources behind the stream are
// released exactly when ctx is cancelled — callers rely on that for
// goroutine cleanup. Bounded buffering drops the oldest event when a
// consumer can't keep up: operators see the latest state, never a stale
// one.

// RunStore reads runs and their history, drives run mutations (start /
// cancel / signal), and streams live run + per-run-history updates. The
// watches carry the console-specific projection; the list/get/mutation
// methods delegate to api.Service but live here so run-page tests fake a
// single coherent port.
type RunStore interface {
	ListRuns(ctx context.Context, workflowFilter string) ([]dag.WorkflowRun, error)
	GetRun(ctx context.Context, runID string) (dag.WorkflowRun, error)
	ListRunEvents(ctx context.Context, runID string, fullData bool) ([]api.RunEvent, error)

	// StartRun publishes a WorkflowStarted event for the named
	// workflow with the supplied input payload. Returns the new run
	// id on success. Used by the inline Run button on the workflows
	// list (#329). The caller is responsible for the read-only and
	// runnability checks; the DataSource owns only the publish.
	StartRun(ctx context.Context, workflowName string, input []byte) (string, error)

	// CancelRun publishes a workflow.cancelled event for the run.
	// Cancellation is asynchronous: success means the event was
	// accepted, not that the run has stopped. Used by POST
	// /console/runs/{id}/cancel; the caller is responsible for the
	// read-only, CSRF, and terminality checks.
	CancelRun(ctx context.Context, runID string) error

	// SendSignal writes one named signal to the signals KV bucket at
	// {runID}.{name}. data may be empty. Used by POST
	// /console/runs/{id}/signal; the caller is responsible for the
	// read-only, CSRF, payload-validation, and run-existence checks.
	SendSignal(ctx context.Context, runID, name string, data []byte) error

	// WatchRuns streams the workflow_runs KV bucket. Each emitted
	// RunUpdate carries the latest snapshot for one run plus a flag
	// indicating whether this is the first emission for that key
	// (Created=true) or a status/state mutation (Created=false). The
	// channel closes when ctx is cancelled or the underlying watcher
	// fails. Caller is responsible for filtering — the stream emits
	// everything in the bucket.
	//
	// liveOnly suppresses the initial replay of existing keys: when
	// true the watch opens with NATS UpdatesOnly so only mutations
	// after connect are emitted. The runs list passes liveOnly=true on
	// page>1, where a history replay would prepend the most-recent runs
	// over the server-rendered offset rows. liveOnly=false (the zero
	// value) preserves the replay so page 1 pre-populates the live list.
	WatchRuns(
		ctx context.Context, liveOnly bool,
	) (<-chan RunUpdate, error)

	// WatchRunHistory streams history.<runID> events. Events arrive
	// chronologically per the JetStream delivery order. fromSeq is the
	// stream sequence to resume from (0 means deliver-all). The channel
	// closes when ctx is cancelled. Bounded buffering on the channel
	// drops the oldest event if the consumer can't keep up — operators
	// always see the latest state, never a stale one.
	WatchRunHistory(
		ctx context.Context, runID string, fromSeq uint64,
	) (<-chan HistoryEvent, error)

	// GetRunTrace reads the OTLP spans for one run from the TELEMETRY
	// stream and flattens them into a pre-order span tree the console
	// Trace tab renders. Returns (nil, nil) when no spans exist (the
	// run produced no telemetry or it aged out) or when the underlying
	// NATS connection isn't wired — callers paint the empty state
	// rather than 500ing. The web counterpart of `dagnats trace <id>`.
	GetRunTrace(ctx context.Context, runID string) ([]TraceRow, error)
}

// TriggerStore reads trigger definitions, drives trigger CRUD + enable /
// fire mutations, reads recent firings, and streams live trigger
// updates. WatchTriggers is the console-specific projection the CRUD and
// fire methods share a port with.
type TriggerStore interface {
	ListTriggers(ctx context.Context) ([]trigger.TriggerDef, error)

	// SetTriggerEnabled flips a single trigger's enabled bit. The
	// caller is responsible for emitting the audit event; the
	// DataSource only owns the mutation. Returns an error when the
	// trigger isn't found or KV write fails. Used by the trigger
	// toggle endpoint.
	SetTriggerEnabled(ctx context.Context, triggerID string, enabled bool) error

	// CreateTrigger writes a fully-formed trigger definition. The caller
	// is responsible for validating the def (non-empty ID + WorkflowID,
	// a populated per-kind sub-config) before calling — the underlying
	// api.Service panics on an empty ID / WorkflowID as a programmer-
	// error invariant. Returns an error on validation / route-conflict /
	// KV-write failure. Used by POST /console/triggers (add).
	CreateTrigger(ctx context.Context, def trigger.TriggerDef) error

	// UpdateTrigger applies the config-only overrides in updates to an
	// existing trigger. Only the non-nil fields are written; type,
	// target workflow, and the enabled bit are NOT mutable here (the
	// toggle endpoint owns enabled). Returns an error when the trigger
	// isn't found or the write fails. Used by POST
	// /console/triggers/{id}/edit.
	UpdateTrigger(
		ctx context.Context, triggerID string, updates api.TriggerUpdates,
	) error

	// DeleteTrigger removes one trigger by id. Returns an error when the
	// trigger isn't found or the KV delete fails. Used by POST
	// /console/triggers/{id}/delete.
	DeleteTrigger(ctx context.Context, triggerID string) error

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

	// WatchTriggers streams the triggers KV bucket. Each TriggerUpdate
	// is one observation: a new trigger landed, an existing trigger was
	// toggled / edited, or a delete tombstone arrived (Deleted=true,
	// Def fields cleared). The channel closes on ctx cancellation.
	WatchTriggers(ctx context.Context) (<-chan TriggerUpdate, error)
}

// DLQStore reads dead-letter entries, drives replay / discard, and
// streams live DLQ add/remove updates.
type DLQStore interface {
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

	// WatchDLQ streams the dead-letter stream. DLQUpdate carries the
	// rendered DeadLetterView plus the operation that produced it —
	// new entries arrive with Operation=DLQOpAdded; entries removed
	// by retry / discard arrive with Operation=DLQOpRemoved (and
	// View.Sequence is the only valid field). The channel closes on
	// ctx cancellation.
	WatchDLQ(ctx context.Context) (<-chan DLQUpdate, error)
}

// AuditLog reads and appends console audit events. Both methods project
// the console_audit KV bucket directly (not api.Service): reads degrade
// empty→nil, and EmitAuditEvent records a metric regardless of KV
// outcome so denied/failed writes are still observable.
type AuditLog interface {
	// ListAuditEvents returns up to limit recent audit events from the
	// console_audit KV bucket. Returns nil + nil-error when the bucket
	// is empty / not configured; callers render the zero state.
	ListAuditEvents(ctx context.Context, limit int) ([]AuditEvent, error)

	// EmitAuditEvent writes one audit event into the console_audit
	// bucket. Returns an error on transport failure; callers should
	// log + continue rather than fail the operator action.
	EmitAuditEvent(ctx context.Context, evt AuditEvent) error
}

// AgentRuntimeView projects agent spawn-tree provenance from existing
// run snapshots (#379, ADR-021 Phase A) for the /console/agents page and
// its SSE re-projection.
type AgentRuntimeView interface {
	// ListAgentRuntimes returns up to limit spawn-tree rows for the
	// /console/agents provenance page (#379, ADR-021 Phase A). The
	// lineage is reconstructed from existing run snapshots
	// (RootRunID / ParentRunID) — NO new event type is minted. A lone
	// top-level run is NOT a runtime and is omitted. Budget per root
	// is read from the #378 control plane; a failed Budget read
	// degrades that row (BudgetOK=false) rather than failing the page.
	// Returns nil + nil-error on an empty / unconfigured store.
	ListAgentRuntimes(ctx context.Context, limit int) ([]AgentRuntimeRow, error)

	// AgentRuntime re-projects a single spawn-tree by its tree-root run
	// ID — the single-root path the SSE pump uses to avoid a full
	// re-scan on each run update. Returns (row, true, nil) when root
	// names an actual runtime, (zero, false, nil) when root is a lone
	// non-tree run, and a non-nil error only on a store read failure.
	AgentRuntime(ctx context.Context, root string) (AgentRuntimeRow, bool, error)
}

// OpsInventory projects the deployment self-portrait the ops pages
// render: KV inspector, config snapshot, JetStream consumers, server
// health, and live connections. Every method degrades an unwired NATS
// connection to an empty/zero result + nil error so pages paint the
// honest empty state rather than 500ing.
type OpsInventory interface {
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

	// ConfigSnapshot returns the deployment self-portrait the /config
	// page renders. Bundles workers (#289 directory), JetStream
	// streams + KV buckets, and the NATS endpoint metadata in a
	// single round-trip. Returns a zero-value snapshot + nil error
	// when the underlying NATS connection isn't configured — the
	// page degrades to empty-state cards rather than 500ing on a
	// JetStream-unreachable deployment. #312.
	ConfigSnapshot(ctx context.Context) (ConfigSnapshot, error)

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
}

// WorkerDirectory folds the workers / services KV buckets into the
// projected rows the workers, functions, task-types, and services pages
// render. Every method degrades an empty / unwired directory to nil +
// nil error.
type WorkerDirectory interface {
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

	// ListServiceRows reads the `services` KV bucket and projects each
	// registration into a roster row for the /console/services page.
	// Reuses worker.ListServices — the same read AggregateTaskTypes folds
	// for descriptions — so no new bucket is introduced. Rows carry only
	// the three fields the registration emits (see ServiceRow); no
	// synthetic liveness/version columns.
	//
	// Returns nil + nil-error on an unwired connection or a read miss so
	// the renderer paints the honest empty state rather than 500ing.
	ListServiceRows(ctx context.Context) ([]ServiceRow, error)

	// WorkerDetail reads one worker's full KV registration for the
	// read-only /console/workers/{id} detail page. Surfaces the fields
	// the lossy WorkerStatusRow list projection drops (Language,
	// Transport, MaxTasks, Pid, Version) plus the registered task types.
	// Reuses the same ListWorkers round-trip that backs ListWorkerRows —
	// no second directory read. An unknown id (or a read miss) returns
	// WorkerDetail{Found:false} + nil so the page paints the honest
	// not-found state rather than 500ing.
	WorkerDetail(ctx context.Context, id string) (WorkerDetail, error)
}

// AdmissionView projects the engine's read-side admission-gate snapshot
// for the /console/concurrency page.
type AdmissionView interface {
	// AdmissionState returns the engine's read-side admission-gate
	// snapshot for the /console/concurrency page: which singleton locks
	// are currently held (singleton_locks bucket) and which task types
	// have live in-flight concurrency counters (concurrency_tasks
	// bucket). Both gate buckets are empty on an idle engine — the gates
	// are lazy — so the zero value is a first-class result. Degrades
	// gracefully: a missing bucket yields an empty section rather than a
	// whole-page failure, and a malformed KV value is skipped, not fatal.
	AdmissionState(ctx context.Context) (AdmissionState, error)
}

// SearchIndex powers cmd+k cross-entity search and the list-page
// sparkline column. Both carry strict honesty contracts (empty query /
// no data → nil, nil).
type SearchIndex interface {
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

// DataSource is the full read/mutate surface the production console
// wiring binds (Config.Data). It is the composition of every domain
// port — page handlers should depend on the narrow port they need via
// requirePort, not on this union. Kept as a single name so the
// production adapter and the legacy full fake have one thing to satisfy.
type DataSource interface {
	// Workflow-definition reads have no port of their own: both bodies
	// are nil-checks around a straight api.Service delegation, with no
	// projection, watch, or honesty logic to hide. A named port would
	// be pure indirection, so they sit on the union directly.
	ListWorkflows(ctx context.Context) ([]dag.WorkflowDef, error)
	GetWorkflow(name string) (dag.WorkflowDef, error)

	RunStore
	TriggerStore
	DLQStore
	AuditLog
	AgentRuntimeView
	OpsInventory
	WorkerDirectory
	AdmissionView
	SearchIndex
}
