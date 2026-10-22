package natsutil

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Durability windows for the history/event streams (issue #441). These
// streams are the real storage grower; aging them is recovery-safe because
// recovery is snapshot-first (the workflow_runs KV is authoritative and has
// no TTL). A window only needs to exceed (max orchestrator downtime + max run
// duration); a shorter window risks at most a reconciler-recoverable run
// stall, never data loss. max_age is the retention bound.
const (
	// historyMaxAge bounds WORKFLOW_HISTORY (run step events). 30d is well
	// past any plausible downtime+run-duration sum for an LLM pipeline.
	historyMaxAge = 30 * 24 * time.Hour
	// eventsMaxAge bounds EVENTS (workflow signal/event log). Events are
	// consumed promptly; 14d is generous slack for replay/debug.
	eventsMaxAge = 14 * 24 * time.Hour
	// deadLettersMaxAge bounds DEAD_LETTERS. 30d gives operators a month to
	// triage a failed run before its dead-letter record ages out.
	deadLettersMaxAge = 30 * 24 * time.Hour
	// checkpointsTTL bounds the checkpoints bucket. A Pause marker or
	// bridge checkpoint is read again within the hour; nothing deletes
	// one once its run ends, so without a TTL they pile up forever.
	// Matches history: a checkpoint never outlives the events of its run.
	checkpointsTTL = historyMaxAge
)

// Proportional per-stream byte ceilings (#441 follow-up). Each file-based
// JetStream stream gets a MaxBytes sized as a fraction of the configured
// JetStreamMaxStore budget. Absolute MaxBytes ceilings don't scale across
// host/cluster store budgets: an absolute sum exceeding JetStreamMaxStore
// fails stream creation (err 10047 insufficient storage resources), which
// is exactly what broke the 2 GiB e2e cluster. Sizing each ceiling as a
// fraction keeps the sum a fixed share of the budget on ANY host — the
// 2 GiB test cluster and a 10 GiB+ prod node alike.
//
// The fractions sum to 0.84, leaving ~16% headroom under the budget.
// Dominant growers are weighted higher (WORKFLOW_HISTORY largest, then
// EVENTS/TASK_QUEUES), smaller for the low-volume streams. TASK_QUEUES and
// SLEEP_TIMERS hold live/pending work so they carry NO max_age, but a byte
// ceiling on a work-queue is safe and gives the backstop its teeth.
//
//	WORKFLOW_HISTORY  0.25   run step events — the real grower
//	EVENTS            0.15   workflow signal/event log
//	TASK_QUEUES       0.12   live task backstop
//	TELEMETRY         0.10   spans/metrics/logs (was an absolute 1 GiB)
//	BUILD_LOGS        0.05   step stdout/stderr hot lane (#624)
//	DEAD_LETTERS      0.05   failed-run records
//	AGENT_JOURNAL     0.04   per-step agent journal (ADR-026)
//	TRIGGER_HISTORY   0.04   trigger fire log
//	SLEEP_TIMERS      0.04   pending timer backstop
//	KV submissions    0.005  agent inputs awaiting placement (ADR-027)
//	------------------------
//	total             0.845
//
// ADR-026 added AGENT_JOURNAL out of the headroom, not out of any
// stream: every existing ceiling keeps its as-shipped size, so an
// upgrade's shrink pass touches nothing, and the journal is small (one
// record per claim, pause, entry and commit, aged out at 30 days like
// history) next to history's lifecycle traffic. ADR-027 took the
// submissions bucket's half percent from the headroom the same way; it
// is the only KV bucket with an explicit ceiling, because its values
// are caller-supplied model input rather than engine state. The sum
// stays under the 85% guard TestMaxBytesSumUnderBudget enforces.
//
// #624 added BUILD_LOGS by trimming WORKFLOW_HISTORY from 0.30 to 0.25
// rather than adding on top of 0.80: WORKFLOW_HISTORY is bounded by
// history's own 30d MaxAge and dominated in practice by step lifecycle
// events, not step output, so it had headroom to give up; every other
// fraction (including the new one) keeps its as-shipped size, and the
// table's total — and hence the store-budget headroom — stays exactly
// where #441 set it.
const (
	fractionWorkflowHistory = 0.25
	fractionEvents          = 0.15
	fractionTaskQueues      = 0.12
	fractionTelemetry       = 0.10
	fractionBuildLogs       = 0.05
	fractionDeadLetters     = 0.05
	fractionAgentJournal    = 0.04
	fractionTriggerHistory  = 0.04
	fractionSleepTimers     = 0.04
)

// defaultMaxStoreBytes is the last-resort store budget, used only when the
// connected server reports an UNLIMITED store (JetStream encodes unlimited
// as MaxStore <= 0). Fractions of "unlimited" are meaningless, so the
// ceilings are sized off this concrete figure instead. 10 GiB matches
// server.defaultMaxStoreBytes.
//
// It is deliberately NOT a fallback for callers that omit a budget: those
// resolve the connected server's real limit (see resolveStoreBudget).
// Guessing 10 GiB is what overran any server whose budget was smaller,
// failing stream creation with err 10047.
const defaultMaxStoreBytes int64 = 10 << 30

// storeBudgetResolveTimeout bounds the AccountInfo round trip that resolves
// the server's store budget. It is a single request/reply against an
// already-connected server, so seconds are generous.
const storeBudgetResolveTimeout = 5 * time.Second

// storeBudgetFromLimit maps a server-reported JetStream store limit to the
// budget the proportional ceilings are sized against. NATS encodes an
// unlimited store as MaxStore <= 0; sizing fractions off that would yield a
// 0 (no-ceiling) budget, silently removing the storage backstop, so an
// unlimited server falls back to defaultMaxStoreBytes.
func storeBudgetFromLimit(maxStore int64) int64 {
	if maxStore <= 0 {
		return defaultMaxStoreBytes
	}
	if defaultMaxStoreBytes <= 0 {
		panic("storeBudgetFromLimit: defaultMaxStoreBytes must be positive")
	}
	return maxStore
}

// resolveStoreBudget reports the store budget of the server js is connected
// to, so per-stream ceilings track the budget that server will actually
// enforce rather than a compile-time guess. A failed query is returned, not
// swallowed: the alternative is inventing a budget that may exceed the
// server's and fail stream creation later with an error that names storage
// rather than the failed lookup.
func resolveStoreBudget(
	ctx context.Context, js jetstream.JetStream,
) (int64, error) {
	if js == nil {
		panic("resolveStoreBudget: js must not be nil")
	}
	if ctx == nil {
		panic("resolveStoreBudget: ctx must not be nil")
	}
	info, err := js.AccountInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve JetStream store budget: %w", err)
	}
	return storeBudgetFromLimit(info.Limits.MaxStore), nil
}

// proportionalMaxBytes returns floor(budget*fraction), or 0 when the budget
// is unset (<= 0) so the caller skips MaxBytes entirely (no ceiling) rather
// than setting an invalid 0-byte cap.
func proportionalMaxBytes(maxStoreBytes int64, fraction float64) int64 {
	if maxStoreBytes <= 0 {
		return 0
	}
	if fraction <= 0 || fraction >= 1 {
		panic(fmt.Sprintf(
			"proportionalMaxBytes: fraction out of range: %v", fraction,
		))
	}
	return int64(float64(maxStoreBytes) * fraction)
}

// SetupStreams creates the core JetStream streams required by
// DagNats. WORKFLOW_HISTORY uses a 5s dedup window.
// TASK_QUEUES uses WorkQueuePolicy for exactly-once delivery.
// maxStoreBytes is the JetStreamMaxStore budget; each file stream gets a
// MaxBytes sized as a fraction of it (see the fraction table). A budget of
// 0 (or less) disables the byte ceilings.
func SetupStreams(
	js jetstream.JetStream, replicas int, maxStoreBytes int64,
) error {
	ctx, cancel := provisionContext(false)
	defer cancel()
	return setupStreams(ctx, js, replicas, maxStoreBytes)
}

func setupStreams(
	ctx context.Context,
	js jetstream.JetStream, replicas int, maxStoreBytes int64,
) error {
	if js == nil {
		panic("SetupStreams: js must not be nil")
	}
	if replicas != 1 && replicas != 3 && replicas != 5 {
		panic(fmt.Sprintf(
			"SetupStreams: replicas must be 1, 3, or 5; got %d",
			replicas,
		))
	}
	streams := []jetstream.StreamConfig{
		{
			Name:       "WORKFLOW_HISTORY",
			Subjects:   []string{"history.>"},
			Retention:  jetstream.LimitsPolicy,
			Storage:    jetstream.FileStorage,
			Duplicates: 5_000_000_000,
			MaxAge:     historyMaxAge,
			MaxBytes: proportionalMaxBytes(
				maxStoreBytes, fractionWorkflowHistory,
			),
			Replicas: replicas,
		},
		{
			Name:      "TASK_QUEUES",
			Subjects:  []string{"task.>"},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
			// NO MaxAge: un-acked work-queue messages are live tasks;
			// an age bound would silently delete pending work (#441).
			// A byte ceiling is safe — it caps the workqueue without
			// dropping by time.
			MaxBytes: proportionalMaxBytes(
				maxStoreBytes, fractionTaskQueues,
			),
			Replicas: replicas,
			// AllowDirect enables Stream.GetMsg(WithGetMsgSubject),
			// which the queue-depth API (#632) uses to read the
			// oldest pending message per task-type subject without a
			// consumer. The update fallback below applies this to an
			// already-existing TASK_QUEUES too, so operators upgrading
			// from an older binary get direct-get without a manual
			// stream edit.
			AllowDirect: true,
		},
		{
			Name:      "EVENTS",
			Subjects:  []string{"event.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			MaxAge:    eventsMaxAge,
			MaxBytes: proportionalMaxBytes(
				maxStoreBytes, fractionEvents,
			),
			Replicas: replicas,
		},
		{
			Name:      "DEAD_LETTERS",
			Subjects:  []string{"dead.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			// Dedup window: the engine's WORKFLOW_HISTORY consumer
			// uses default AckWait (30s) and no MaxDeliver cap, so a
			// redelivered step.failed can re-enter the failure path.
			// PublishDeadLetter sets Nats-Msg-Id keyed on
			// (runID, stepID, attempts); the window must cover the
			// longest plausible republish interval — slow operator
			// reruns, reconciler-driven dupes, and the observed
			// ~2min engine redelivery cycles from #202. 24h is
			// conservative; cost is small (header-only state per
			// dedup-id).
			Duplicates: 24 * time.Hour,
			MaxAge:     deadLettersMaxAge,
			MaxBytes: proportionalMaxBytes(
				maxStoreBytes, fractionDeadLetters,
			),
			Replicas: replicas,
		},
		{
			Name:      "SLEEP_TIMERS",
			Subjects:  []string{"sleep.>", "scheduled.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			// NO MaxAge: holds pending future sleep messages; an age
			// bound would drop un-fired timers (#441). A byte ceiling
			// is safe — it caps storage without dropping by time.
			MaxBytes: proportionalMaxBytes(
				maxStoreBytes, fractionSleepTimers,
			),
			Replicas: replicas,
		},
		agentJournalStreamConfig(replicas, maxStoreBytes),
	}
	if len(streams) == 0 {
		panic("SetupStreams: streams config must not be empty")
	}
	for _, cfg := range streams {
		err := createStreamWithRetry(ctx, js, cfg)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}

// SetupKVBuckets creates the KV buckets used to store workflow
// definitions and runtime state for active workflow runs.
func SetupKVBuckets(js jetstream.JetStream, replicas int) error {
	ctx, cancel := provisionContext(false)
	defer cancel()
	return setupKVBuckets(ctx, js, replicas)
}

func setupKVBuckets(
	ctx context.Context, js jetstream.JetStream, replicas int,
) error {
	if js == nil {
		panic("SetupKVBuckets: js must not be nil")
	}
	if replicas != 1 && replicas != 3 && replicas != 5 {
		panic(fmt.Sprintf(
			"SetupKVBuckets: replicas must be 1, 3, or 5; got %d",
			replicas,
		))
	}
	buckets := []jetstream.KeyValueConfig{
		{Bucket: "workflow_defs", Replicas: replicas},
		{Bucket: "workflow_runs", Replicas: replicas},
		{Bucket: "scheduled_runs", Replicas: replicas},
		{Bucket: "workers", TTL: 60 * time.Second, Replicas: replicas},
		// worker_tokens: mintable, revocable, scoped bearer tokens for
		// worker access (#627). No TTL — a token's lifetime is governed
		// by explicit Revoke, not staleness; revoked records are kept
		// for audit. History 1: readers only need the latest state per
		// token, same rationale as "services" below.
		{Bucket: "worker_tokens", History: 1, Replicas: replicas},
		// worker_status: per-worker counter snapshots (cancelled-task
		// skip count, etc.) used by `dagnats status --detail`. TTL'd
		// so dead workers' entries age out of the aggregate (#182).
		{
			Bucket:   "worker_status",
			TTL:      120 * time.Second,
			Replicas: replicas,
		},
		{Bucket: "event_waiters", Replicas: replicas},
		{Bucket: "rate_limits", Replicas: replicas},
		{Bucket: "concurrency_tasks", History: 1, Replicas: replicas},
		{
			Bucket:   "approval_tokens",
			History:  1,
			TTL:      168 * time.Hour,
			Replicas: replicas,
		},
		{
			Bucket:   "debounce_state",
			TTL:      14 * 24 * time.Hour,
			Replicas: replicas,
		},
		{
			Bucket:   "idempotency_keys",
			TTL:      24 * time.Hour,
			Replicas: replicas,
		},
		{
			// http_idempotency: maps an HTTP trigger's
			// (triggerID, Idempotency-Key header value) → originalRunID
			// so duplicate requests subscribe to the original run's
			// response subject (ADR-013 Q6 / PR 3). 1h TTL: long enough
			// for a typical retry window without bloating state. If
			// Stripe-style 24h is needed, surface as an HTTPConfig
			// field; for v1 the value is fixed.
			Bucket:   "http_idempotency",
			TTL:      1 * time.Hour,
			Replicas: replicas,
		},
		{
			Bucket:   "sticky_bindings",
			TTL:      25 * time.Hour,
			Replicas: replicas,
		},
		{Bucket: "singleton_locks", Replicas: replicas},
		// trigger_types: registry of External trigger type defs
		// keyed by trigger-type Name. History 1 because callers
		// only need the latest schema for validation (#313/#273
		// Phase 2.1); no TTL — entries are stable definitions
		// owned by long-lived workers.
		{
			Bucket:   "trigger_types",
			History:  1,
			Replicas: replicas,
		},
		// services: metadata namespace for grouping task types
		// under a logical service name (#321 / ADR-017 / #273
		// Phase 3.1). Pure descriptive surface — no invocation
		// gating, no heartbeat, no TTL. Deliberately separated
		// from the `workers` bucket (#289): workers have a 60s
		// TTL and a heartbeat loop; services are stable
		// definitions that should persist across worker restarts.
		// History 1 because last-write-wins on Put — readers
		// only need the latest metadata.
		{
			Bucket:   "services",
			History:  1,
			Replicas: replicas,
		},
		// Runtime state the trigger service, engine, API and workers
		// open by name. Provisioned here, not by `dagnats serve`, so a
		// split deployment (dagnats-engine, dagnats-api) gets them and
		// a cluster gets them at its replication factor (#784).
		{Bucket: "triggers", Replicas: replicas},
		{Bucket: "trigger_state", Replicas: replicas},
		{Bucket: "signals", Replicas: replicas},
		{Bucket: "checkpoints", TTL: checkpointsTTL, Replicas: replicas},
		{Bucket: "concurrency_runs", Replicas: replicas},
	}
	if len(buckets) == 0 {
		panic("SetupKVBuckets: buckets config must not be empty")
	}
	for _, cfg := range buckets {
		err := createKeyValueWithRetry(ctx, js, cfg)
		if err != nil {
			return fmt.Errorf("create kv bucket %s: %w", cfg.Bucket, err)
		}
	}
	// leases (ADR-024) last and non-fatal: it needs nats-server >= 2.11.
	return provisionWithRetry(ctx, func(c context.Context) error {
		return provisionLeasesBucket(c, js, replicas)
	})
}

// SetupStickyStream creates the STICKY_TASKS stream for worker-
// specific task routing. Separated from SetupStreams because it's
// only needed when sticky workflows are in use.
func SetupStickyStream(js jetstream.JetStream, replicas int) error {
	ctx, cancel := provisionContext(false)
	defer cancel()
	return setupStickyStream(ctx, js, replicas)
}

func setupStickyStream(
	ctx context.Context, js jetstream.JetStream, replicas int,
) error {
	if js == nil {
		panic("setupStickyStream: js must not be nil")
	}
	if replicas < 1 {
		panic("setupStickyStream: replicas must be positive")
	}
	return createStreamWithRetry(ctx, js, jetstream.StreamConfig{
		Name:     "STICKY_TASKS",
		Subjects: []string{"sticky.>"},
		Storage:  jetstream.MemoryStorage,
		MaxAge:   30 * time.Minute,
		Replicas: replicas,
	})
}

// SetupTelemetryStream creates the TELEMETRY stream for all
// observability signals (spans, metrics, logs). 7-day retention,
// proportional byte ceiling, 5s dedup window. maxStoreBytes is the
// JetStreamMaxStore budget; the byte ceiling is a fraction of it (was an
// absolute 1 GiB cap, which is 50% of the 2 GiB test cluster — folded into
// the proportional scheme). A budget of 0 disables the ceiling.
func SetupTelemetryStream(
	js jetstream.JetStream, maxStoreBytes int64, replicas int,
) error {
	ctx, cancel := provisionContext(false)
	defer cancel()
	return setupTelemetryStream(ctx, js, maxStoreBytes, replicas)
}

func setupTelemetryStream(
	ctx context.Context,
	js jetstream.JetStream, maxStoreBytes int64, replicas int,
) error {
	if js == nil {
		panic("SetupTelemetryStream: js must not be nil")
	}
	if replicas < 1 {
		panic("SetupTelemetryStream: replicas must be positive")
	}
	cfg := jetstream.StreamConfig{
		Name:      "TELEMETRY",
		Subjects:  []string{"telemetry.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    7 * 24 * time.Hour,
		MaxBytes: proportionalMaxBytes(
			maxStoreBytes, fractionTelemetry,
		),
		Duplicates: 5 * time.Second,
		Replicas:   replicas,
	}
	if cfg.Name == "" {
		panic("SetupTelemetryStream: stream name must not be empty")
	}
	return createStreamWithRetry(ctx, js, cfg)
}

// SetupTriggerHistoryStream creates the TRIGGER_HISTORY stream
// for recording trigger fire events. 30-day retention, file
// storage, discard oldest messages when limits are reached.
// maxStoreBytes is the JetStreamMaxStore budget; the byte ceiling is a
// fraction of it. A budget of 0 disables the ceiling.
func SetupTriggerHistoryStream(
	js jetstream.JetStream, maxStoreBytes int64, replicas int,
) error {
	ctx, cancel := provisionContext(false)
	defer cancel()
	return setupTriggerHistoryStream(ctx, js, maxStoreBytes, replicas)
}

func setupTriggerHistoryStream(
	ctx context.Context,
	js jetstream.JetStream, maxStoreBytes int64, replicas int,
) error {
	if replicas < 1 {
		panic("SetupTriggerHistoryStream: replicas must be positive")
	}
	if js == nil {
		panic(
			"SetupTriggerHistoryStream: js must not be nil",
		)
	}
	cfg := jetstream.StreamConfig{
		Name:      "TRIGGER_HISTORY",
		Subjects:  []string{"trigger.fire.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    30 * 24 * time.Hour,
		MaxBytes: proportionalMaxBytes(
			maxStoreBytes, fractionTriggerHistory,
		),
		Discard:  jetstream.DiscardOld,
		Replicas: replicas,
	}
	if cfg.Name == "" {
		panic(
			"SetupTriggerHistoryStream: name must not be empty",
		)
	}
	return createStreamWithRetry(ctx, js, cfg)
}

// StreamConfig defines an additional JetStream stream for SetupAll to provision.
type StreamConfig struct {
	Name     string
	Subjects []string
}

// KVConfig defines an additional KV bucket for SetupAll to provision.
type KVConfig struct {
	Bucket string
}

// SetupOption configures additional NATS resources for SetupAll.
type SetupOption func(*setupOptions)

type setupOptions struct {
	streams               []StreamConfig
	kvs                   []KVConfig
	cluster               ClusterOptions
	maxStoreBytes         int64
	storeAdmissionCeiling int64
}

// WithStoreBudget sets the JetStreamMaxStore budget SetupAll uses to size
// each file stream's proportional MaxBytes ceiling. When unset (or 0),
// SetupAll queries the connected server for its real budget instead (see
// resolveStoreBudget). Pass this only to size against a budget the server
// itself does not report — e.g. the server passes its own configured
// MaxStoreBytes before the value is observable, and the e2e harness pins
// the cluster budget it provisioned its nodes with.
func WithStoreBudget(maxStoreBytes int64) SetupOption {
	return func(o *setupOptions) {
		o.maxStoreBytes = maxStoreBytes
	}
}

// WithStoreAdmissionCeiling tells preflightStoreBudget to check existing
// stream reservations against ceilingBytes instead of the WithStoreBudget
// figure. The two are ordinarily equal -- a caller starts its NATS process
// and calls SetupAll with the same store budget -- so this is unnecessary
// for nearly every caller.
//
// It exists for #688's auto-shrink recovery: an embedded-server caller that
// hit StoreBudgetExceededError can restart its NATS process at a temporarily
// larger JetStreamMaxStore (covering what StoreBudgetExceededError.Reserved
// reported) so the process actually admits the existing streams, then retry
// SetupAll with WithStoreBudget still at the operator's configured (smaller)
// value -- so ceilings still shrink to it -- and WithStoreAdmissionCeiling
// set to that larger, actually-enforced limit, so the preflight does not
// refuse a shrink the restarted process can, in fact, perform.
//
// resolveStoreBudget/AccountInfo cannot discover this ceiling on its own:
// AccountInfo reports the account's JetStream tier limit, not the
// server-level JetStreamMaxStore the caller passed to natsserver.Options,
// which is what actually gates admission (10047 insufficient storage
// resources). Only the caller that started the process knows it.
func WithStoreAdmissionCeiling(ceilingBytes int64) SetupOption {
	return func(o *setupOptions) {
		o.storeAdmissionCeiling = ceilingBytes
	}
}

// WithStreams adds extra JetStream streams to provision.
func WithStreams(configs ...StreamConfig) SetupOption {
	return func(o *setupOptions) {
		o.streams = append(o.streams, configs...)
	}
}

// WithKVBuckets adds extra KV buckets to provision.
func WithKVBuckets(configs ...KVConfig) SetupOption {
	return func(o *setupOptions) {
		o.kvs = append(o.kvs, configs...)
	}
}

// WithCluster declares cluster topology for SetupAll. When
// ClusterOptions.Routes is non-empty, SetupAll blocks until cluster
// quorum forms (60s internal timeout) before creating streams at the
// derived replication factor.
func WithCluster(c ClusterOptions) SetupOption {
	return func(o *setupOptions) {
		o.cluster = c
	}
}

// awaitQuorum blocks (60s cap inside ctx) until the cluster's JetStream
// answers, before SetupAll asserts anything against it.
func awaitQuorum(ctx context.Context, js jetstream.JetStream, size int) error {
	if ctx == nil {
		panic("awaitQuorum: ctx must not be nil")
	}
	if size < 1 {
		panic("awaitQuorum: size must be positive")
	}
	quorumCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if _, err := WaitForClusterQuorum(quorumCtx, js, size); err != nil {
		return fmt.Errorf("cluster quorum did not form: %w", err)
	}
	return nil
}

// resolveReplicas picks the replication factor for one SetupAll sweep: an
// explicit override, else the count of declared routes, else what the
// connected JetStream cluster can place (1 outside a cluster).
func resolveReplicas(
	ctx context.Context, nc *nats.Conn, cluster ClusterOptions,
) (int, error) {
	if ctx == nil {
		panic("resolveReplicas: ctx must not be nil")
	}
	if cluster.ReplicasOverride < 0 {
		panic("resolveReplicas: ReplicasOverride must not be negative")
	}
	if cluster.ReplicasOverride > 0 || len(cluster.Routes) > 0 {
		return DeriveReplicas(cluster.Routes, cluster.ReplicasOverride), nil
	}
	return detectReplicas(ctx, nc)
}

// SetupAll creates all streams and KV buckets on the given
// connection. Optional SetupOption args provision additional
// streams and KV buckets for downstream packages. With no routes and no
// override, the replication factor is derived from the cluster the
// connection reached. An existing stream or bucket with more replicas than
// the factor keeps its count: provisioning never scales anything down.
func SetupAll(nc *nats.Conn, opts ...SetupOption) error {
	if nc == nil {
		panic("natsutil: connection must not be nil")
	}
	if !nc.IsConnected() {
		panic("natsutil: connection must be connected")
	}

	var options setupOptions
	for _, opt := range opts {
		opt(&options)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	// A caller that did not declare a budget gets ceilings sized against
	// the budget THIS server enforces, so the sum can never overrun it.
	if options.maxStoreBytes <= 0 {
		budgetCtx, budgetCancel := context.WithTimeout(
			context.Background(), storeBudgetResolveTimeout,
		)
		options.maxStoreBytes, err = resolveStoreBudget(budgetCtx, js)
		budgetCancel()
		if err != nil {
			return err
		}
	}

	// ONE budget for the whole provisioning sweep, shared by every Setup*
	// call below. Standalone keeps the short bound so a misconfigured
	// server fails fast; a cluster gets longer for meta-leader elections.
	// A process with no routes of its own (dagnats-engine, dagnats-api)
	// may still be attached to a cluster; the server it reached says so.
	clustered := len(options.cluster.Routes) > 0 ||
		nc.ConnectedClusterName() != ""
	setupCtx, setupCancel := provisionContext(clustered)
	defer setupCancel()

	if clustered {
		if err := awaitQuorum(setupCtx, js, len(options.cluster.Routes)+1); err != nil {
			return err
		}
	}

	// Ahead of any assertion, while the failure can still be explained.
	preflightCtx, preflightCancel := context.WithTimeout(
		context.Background(), storeBudgetResolveTimeout,
	)
	admissionCeiling := options.storeAdmissionCeiling
	if admissionCeiling <= 0 {
		admissionCeiling = options.maxStoreBytes
	} else if admissionCeiling < options.maxStoreBytes {
		panic(fmt.Sprintf(
			"SetupAll: WithStoreAdmissionCeiling(%d) is below the store "+
				"budget %d; the ceiling exists to admit MORE than the "+
				"budget, never less",
			admissionCeiling, options.maxStoreBytes,
		))
	}
	err = preflightStoreBudget(
		preflightCtx, js, options.maxStoreBytes, admissionCeiling,
	)
	preflightCancel()
	if err != nil {
		return err
	}

	replicas, err := resolveReplicas(setupCtx, nc, options.cluster)
	if err != nil {
		return err
	}
	if err := setupStreams(
		setupCtx, js, replicas, options.maxStoreBytes,
	); err != nil {
		return err
	}
	if err := setupKVBuckets(setupCtx, js, replicas); err != nil {
		return err
	}
	if err := setupSubmissionsBucket(
		setupCtx, js, replicas, options.maxStoreBytes,
	); err != nil {
		return err
	}
	if err := setupTelemetryStream(
		setupCtx, js, options.maxStoreBytes, replicas,
	); err != nil {
		return err
	}
	if err := setupStickyStream(setupCtx, js, replicas); err != nil {
		return err
	}
	if err := setupTriggerHistoryStream(
		setupCtx, js, options.maxStoreBytes, replicas,
	); err != nil {
		return err
	}
	buildLogsTTL, err := resolveBuildLogsTTL(os.Getenv("DAGNATS_BUILD_LOGS_TTL"))
	if err != nil {
		return err
	}
	if err := setupBuildLogsStream(
		setupCtx, js, options.maxStoreBytes, buildLogsTTL, replicas,
	); err != nil {
		return err
	}

	if err := enableAtomicPublish(setupCtx, js, "TASK_QUEUES"); err != nil {
		return err
	}

	ctx := setupCtx
	for _, sc := range options.streams {
		err := createStreamWithRetry(
			ctx, js, jetstream.StreamConfig{
				Name:      sc.Name,
				Subjects:  sc.Subjects,
				Retention: jetstream.WorkQueuePolicy,
				Storage:   jetstream.FileStorage,
				Replicas:  replicas,
			},
		)
		if err != nil {
			return err
		}
	}

	return setupExtraKVBuckets(ctx, js, options.kvs, replicas)
}

// setupExtraKVBuckets provisions WithKVBuckets extras at the cluster's
// replication factor. An extra naming a bucket SetupAll already provisions
// is skipped: re-asserting it with the extra's bare config would replace
// that bucket's TTL and history settings.
func setupExtraKVBuckets(
	ctx context.Context, js jetstream.JetStream, kvs []KVConfig, replicas int,
) error {
	if js == nil {
		panic("setupExtraKVBuckets: js must not be nil")
	}
	if replicas < 1 {
		panic("setupExtraKVBuckets: replicas must be positive")
	}
	for _, kc := range kvs {
		if slices.Contains(provisionedKVBuckets, kc.Bucket) {
			continue
		}
		err := createKeyValueWithRetry(ctx, js, jetstream.KeyValueConfig{
			Bucket: kc.Bucket, Replicas: replicas,
		})
		if err != nil {
			return fmt.Errorf("create kv bucket %s: %w", kc.Bucket, err)
		}
	}
	return nil
}

// enableAtomicPublish updates an existing stream to allow atomic
// batch publishing. Requires NATS server >= 2.12.
func enableAtomicPublish(
	ctx context.Context, js jetstream.JetStream, streamName string,
) error {
	if js == nil {
		panic("enableAtomicPublish: js must not be nil")
	}
	if streamName == "" {
		panic("enableAtomicPublish: streamName must not be empty")
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf(
			"natsutil: get stream %q: %w", streamName, err,
		)
	}

	cfg := stream.CachedInfo().Config
	cfg.AllowAtomicPublish = true

	err = provisionWithRetry(ctx, func(c context.Context) error {
		_, updateErr := js.UpdateStream(c, cfg)
		return updateErr
	})
	if err != nil {
		return fmt.Errorf(
			"natsutil: update stream %q: %w", streamName, err,
		)
	}
	return nil
}

// StoreBudgetReservation names one stream's contribution to a
// StoreBudgetExceededError's Reserved total, largest first.
type StoreBudgetReservation struct {
	Name  string
	Bytes int64
}

// StoreBudgetExceededError reports that budget is below the aggregate
// MaxBytes already reserved by existing streams (#685).
//
// JetStream admits a stream only if its MaxBytes fits the budget remaining
// after every OTHER stream's MaxBytes. A store established under a LARGER
// budget therefore refuses EVERY assertion that follows, including the very
// updates that would have shrunk those ceilings: a single connection to the
// server enforcing the new budget cannot lower its own reservation, because
// doing so requires writes the lowered budget forbids.
//
// An embedded-server caller that also owns the NATS process (server.startNATS)
// can recover from this: restart the process with a temporarily larger
// JetStreamMaxStore covering Reserved, then retry SetupAll at the original
// budget so the fractional ceilings still shrink to what the operator
// configured, one restart later (#688). Reserved and Largest are exported so
// that caller doesn't need to reparse Error()'s text. A caller connected to
// an EXTERNAL server it does not control has no such restart available and
// must trim streams by hand -- for that caller this remains a hard failure.
type StoreBudgetExceededError struct {
	Budget   int64
	Reserved int64
	Counted  int
	Largest  []StoreBudgetReservation
}

// Error renders the operator-facing message: left to JetStream the operator
// would see only "insufficient storage resources available", which names
// neither the budget nor the aggregate it was measured against (#685).
func (e *StoreBudgetExceededError) Error() string {
	if e == nil {
		panic("StoreBudgetExceededError.Error: e is nil")
	}
	worst := make([]string, 0, len(e.Largest))
	for _, r := range e.Largest {
		worst = append(worst, fmt.Sprintf(
			"%s=%s", r.Name, humanStoreBytes(r.Bytes),
		))
	}
	return fmt.Sprintf(
		"max_store_bytes=%s is below the %s already reserved by %d existing "+
			"stream(s); largest: %s. JetStream validates the combined "+
			"reservation before admitting any stream, so dagnats cannot "+
			"shrink these ceilings under the new budget. Lower each stream's "+
			"MaxBytes until the total fits, then set max_store_bytes",
		humanStoreBytes(e.Budget), humanStoreBytes(e.Reserved), e.Counted,
		strings.Join(worst, ", "),
	)
}

// preflightStoreBudget returns a *StoreBudgetExceededError when the streams
// already on disk reserve more bytes than admissionCeiling. See
// StoreBudgetExceededError's doc for why this happens and how an
// embedded-server caller recovers from it.
//
// admissionCeiling, not budget, is what streams are actually admitted
// against: it must be the server's real, currently-enforced JetStreamMaxStore
// (SetupAll's caller either passes budget straight through, the ordinary
// case, or supplies a larger WithStoreAdmissionCeiling during a #688
// shrink-pass retry). budget is only used to label the operator-facing
// error with what the caller is sizing ceilings from downstream.
//
// A listing failure is deliberately NOT fatal. This is a diagnostic that runs
// ahead of the real assertions and those report their own errors; refusing to
// start because the diagnostic could not run would convert a message
// improvement into a new failure mode.
func preflightStoreBudget(
	ctx context.Context, js jetstream.JetStream, budget, admissionCeiling int64,
) error {
	if budget <= 0 || admissionCeiling <= 0 {
		return nil
	}
	var reserved int64
	var counted int
	var largest []StoreBudgetReservation
	lister := js.ListStreams(ctx)
	for info := range lister.Info() {
		if info == nil || info.Config.MaxBytes <= 0 {
			continue
		}
		reserved += info.Config.MaxBytes
		counted++
		largest = append(largest, StoreBudgetReservation{
			Name: info.Config.Name, Bytes: info.Config.MaxBytes,
		})
	}
	if lister.Err() != nil || reserved <= admissionCeiling {
		return nil
	}
	sort.Slice(largest, func(i, j int) bool {
		return largest[i].Bytes > largest[j].Bytes
	})
	// Name only the biggest few: those are the ones worth trimming, and a
	// full list on a busy node is dozens of streams long.
	if len(largest) > 3 {
		largest = largest[:3]
	}
	if reserved <= 0 {
		panic("preflightStoreBudget: reserved must be positive to exceed admissionCeiling")
	}
	if counted <= 0 {
		panic("preflightStoreBudget: counted must be positive when reserved > 0")
	}
	return &StoreBudgetExceededError{
		Budget:   budget,
		Reserved: reserved,
		Counted:  counted,
		Largest:  largest,
	}
}

// CheckStoreBudget reports whether the connected server's existing stream
// reservations still fit budget, returning the same *StoreBudgetExceededError
// preflightStoreBudget produces during SetupAll.
//
// Exported for #688's auto-shrink recovery: after a shrink pass, the caller
// rechecks against the operator's CONFIGURED budget (not the temporarily
// inflated admission ceiling the process is running at this boot) to catch
// a stream SetupAll does not manage -- operator-created, a mirror, an
// orphan -- still holding the aggregate over budget. Without this recheck
// that case would silently re-enter recovery, inflated, on every future
// boot instead of surfacing the same actionable message #686 already
// produces.
func CheckStoreBudget(
	ctx context.Context, js jetstream.JetStream, budget int64,
) error {
	if js == nil {
		panic("CheckStoreBudget: js must not be nil")
	}
	if budget <= 0 {
		panic("CheckStoreBudget: budget must be positive")
	}
	return preflightStoreBudget(ctx, js, budget, budget)
}

// humanStoreBytes renders a store size the way an operator would write it in
// config, so the error can be pasted back as a setting.
func humanStoreBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0fMiB", float64(n)/float64(1<<20))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
