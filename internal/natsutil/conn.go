package natsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SetupStreams creates the core JetStream streams required by
// DagNats. WORKFLOW_HISTORY uses a 5s dedup window.
// TASK_QUEUES uses WorkQueuePolicy for exactly-once delivery.
func SetupStreams(js jetstream.JetStream, replicas int) error {
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
			Replicas:   replicas,
		},
		{
			Name:      "TASK_QUEUES",
			Subjects:  []string{"task.>"},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
		},
		{
			Name:      "EVENTS",
			Subjects:  []string{"event.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
		},
		{
			Name:      "DEAD_LETTERS",
			Subjects:  []string{"dead.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
		},
		{
			Name:      "SLEEP_TIMERS",
			Subjects:  []string{"sleep.>", "scheduled.>"},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
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
			Bucket:   "sticky_bindings",
			TTL:      25 * time.Hour,
			Replicas: replicas,
		},
		{Bucket: "singleton_locks", Replicas: replicas},
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
// 1GB cap, 5s dedup window.
func SetupTelemetryStream(js jetstream.JetStream) error {
	if js == nil {
		panic("SetupTelemetryStream: js must not be nil")
	}
	cfg := jetstream.StreamConfig{
		Name:       "TELEMETRY",
		Subjects:   []string{"telemetry.>"},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		MaxAge:     7 * 24 * time.Hour,
		MaxBytes:   1 << 30,
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
func SetupTriggerHistoryStream(
	js jetstream.JetStream,
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
		Discard:   jetstream.DiscardOld,
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
	streams []StreamConfig
	kvs     []KVConfig
	cluster ClusterOptions
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
	if err := SetupStreams(js, replicas); err != nil {
		return err
	}
	if err := SetupKVBuckets(js, replicas); err != nil {
		return err
	}
	if err := SetupTelemetryStream(js); err != nil {
		return err
	}
	if err := SetupStickyStream(js); err != nil {
		return err
	}
	if err := SetupTriggerHistoryStream(js); err != nil {
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
