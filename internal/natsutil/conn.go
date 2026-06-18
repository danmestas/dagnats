package natsutil

import (
	"context"
	"fmt"
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
// The fractions sum to 0.80, leaving ~20% headroom under the budget.
// Dominant growers are weighted higher (WORKFLOW_HISTORY largest, then
// EVENTS/TASK_QUEUES), smaller for the low-volume streams. TASK_QUEUES and
// SLEEP_TIMERS hold live/pending work so they carry NO max_age, but a byte
// ceiling on a work-queue is safe and gives the backstop its teeth.
//
//	WORKFLOW_HISTORY  0.30   run step events — the real grower
//	EVENTS            0.15   workflow signal/event log
//	TASK_QUEUES       0.12   live task backstop
//	TELEMETRY         0.10   spans/metrics/logs (was an absolute 1 GiB)
//	DEAD_LETTERS      0.05   failed-run records
//	TRIGGER_HISTORY   0.04   trigger fire log
//	SLEEP_TIMERS      0.04   pending timer backstop
//	------------------------
//	total             0.80
const (
	fractionWorkflowHistory = 0.30
	fractionEvents          = 0.15
	fractionTaskQueues      = 0.12
	fractionTelemetry       = 0.10
	fractionDeadLetters     = 0.05
	fractionTriggerHistory  = 0.04
	fractionSleepTimers     = 0.04
)

// defaultMaxStoreBytes is the fallback store budget used by callers that
// cannot easily obtain the real JetStreamMaxStore (test helpers). 10 GiB
// matches server.defaultMaxStoreBytes; the proportional ceilings derived
// from it stay well under any realistic host store.
const defaultMaxStoreBytes int64 = 10 << 30

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
	}
	if len(streams) == 0 {
		panic("SetupStreams: streams config must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	for _, cfg := range streams {
		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupKVBuckets creates the KV buckets used to store workflow
// definitions and runtime state for active workflow runs.
func SetupKVBuckets(js jetstream.JetStream, replicas int) error {
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
	}
	if len(buckets) == 0 {
		panic("SetupKVBuckets: buckets config must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	for _, cfg := range buckets {
		_, err := js.CreateOrUpdateKeyValue(ctx, cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupStickyStream creates the STICKY_TASKS stream for worker-
// specific task routing. Separated from SetupStreams because it's
// only needed when sticky workflows are in use.
func SetupStickyStream(js jetstream.JetStream) error {
	if js == nil {
		panic("SetupStickyStream: js must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	_, err := js.CreateOrUpdateStream(
		ctx,
		jetstream.StreamConfig{
			Name:     "STICKY_TASKS",
			Subjects: []string{"sticky.>"},
			Storage:  jetstream.MemoryStorage,
			MaxAge:   30 * time.Minute,
		},
	)
	return err
}

// SetupTelemetryStream creates the TELEMETRY stream for all
// observability signals (spans, metrics, logs). 7-day retention,
// proportional byte ceiling, 5s dedup window. maxStoreBytes is the
// JetStreamMaxStore budget; the byte ceiling is a fraction of it (was an
// absolute 1 GiB cap, which is 50% of the 2 GiB test cluster — folded into
// the proportional scheme). A budget of 0 disables the ceiling.
func SetupTelemetryStream(
	js jetstream.JetStream, maxStoreBytes int64,
) error {
	if js == nil {
		panic("SetupTelemetryStream: js must not be nil")
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
	}
	if cfg.Name == "" {
		panic("SetupTelemetryStream: stream name must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	_, err := js.CreateOrUpdateStream(ctx, cfg)
	return err
}

// SetupTriggerHistoryStream creates the TRIGGER_HISTORY stream
// for recording trigger fire events. 30-day retention, file
// storage, discard oldest messages when limits are reached.
// maxStoreBytes is the JetStreamMaxStore budget; the byte ceiling is a
// fraction of it. A budget of 0 disables the ceiling.
func SetupTriggerHistoryStream(
	js jetstream.JetStream, maxStoreBytes int64,
) error {
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
		Discard: jetstream.DiscardOld,
	}
	if cfg.Name == "" {
		panic(
			"SetupTriggerHistoryStream: name must not be empty",
		)
	}
	_, err := js.CreateOrUpdateStream(
		context.Background(), cfg,
	)
	return err
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
	streams       []StreamConfig
	kvs           []KVConfig
	cluster       ClusterOptions
	maxStoreBytes int64
}

// WithStoreBudget sets the JetStreamMaxStore budget SetupAll uses to size
// each file stream's proportional MaxBytes ceiling. When unset (or 0), the
// default budget (defaultMaxStoreBytes) is used so callers that cannot
// easily obtain the real budget still get sane ceilings. The server passes
// its configured MaxStoreBytes; the e2e harness passes its smaller cluster
// budget so the proportional sizing fits the test store.
func WithStoreBudget(maxStoreBytes int64) SetupOption {
	return func(o *setupOptions) {
		o.maxStoreBytes = maxStoreBytes
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

// SetupAll creates all streams and KV buckets on the given
// connection. Optional SetupOption args provision additional
// streams and KV buckets for downstream packages.
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
	// A caller that did not declare a budget still gets proportional
	// ceilings off the default budget, never a 0 (uncapped) store.
	if options.maxStoreBytes <= 0 {
		options.maxStoreBytes = defaultMaxStoreBytes
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	if len(options.cluster.Routes) > 0 {
		quorumCtx, quorumCancel := context.WithTimeout(
			context.Background(), 60*time.Second,
		)
		_, err := WaitForClusterQuorum(
			quorumCtx, js, len(options.cluster.Routes)+1,
		)
		quorumCancel()
		if err != nil {
			return fmt.Errorf("cluster quorum did not form: %w", err)
		}
	}

	replicas := DeriveReplicas(
		options.cluster.Routes, options.cluster.ReplicasOverride,
	)
	if err := SetupStreams(js, replicas, options.maxStoreBytes); err != nil {
		return err
	}
	if err := SetupKVBuckets(js, replicas); err != nil {
		return err
	}
	if err := SetupTelemetryStream(js, options.maxStoreBytes); err != nil {
		return err
	}
	if err := SetupStickyStream(js); err != nil {
		return err
	}
	if err := SetupTriggerHistoryStream(
		js, options.maxStoreBytes,
	); err != nil {
		return err
	}

	if err := enableAtomicPublish(js, "TASK_QUEUES"); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()
	for _, sc := range options.streams {
		_, err := js.CreateOrUpdateStream(
			ctx, jetstream.StreamConfig{
				Name:      sc.Name,
				Subjects:  sc.Subjects,
				Retention: jetstream.WorkQueuePolicy,
				Storage:   jetstream.FileStorage,
			},
		)
		if err != nil {
			return err
		}
	}

	for _, kc := range options.kvs {
		_, err := js.CreateOrUpdateKeyValue(
			ctx, jetstream.KeyValueConfig{
				Bucket: kc.Bucket,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// enableAtomicPublish updates an existing stream to allow atomic
// batch publishing. Requires NATS server >= 2.12.
func enableAtomicPublish(
	js jetstream.JetStream, streamName string,
) error {
	if js == nil {
		panic("enableAtomicPublish: js must not be nil")
	}
	if streamName == "" {
		panic("enableAtomicPublish: streamName must not be empty")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf(
			"natsutil: get stream %q: %w", streamName, err,
		)
	}

	cfg := stream.CachedInfo().Config
	cfg.AllowAtomicPublish = true

	_, err = js.UpdateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf(
			"natsutil: update stream %q: %w", streamName, err,
		)
	}
	return nil
}
