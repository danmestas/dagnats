package natsutil

import (
	"time"

	"github.com/nats-io/nats.go"
)

// SetupStreams creates the three core JetStream streams required by DagNats.
// WORKFLOW_HISTORY uses dedup window of 5s to prevent duplicate event writes.
// TASK_QUEUES uses WorkQueuePolicy so each message is consumed exactly once.
func SetupStreams(js nats.JetStreamContext) error {
	streams := []nats.StreamConfig{
		{
			Name:       "WORKFLOW_HISTORY",
			Subjects:   []string{"history.>"},
			Retention:  nats.LimitsPolicy,
			Storage:    nats.FileStorage,
			Duplicates: 5_000_000_000,
		},
		{
			Name:      "TASK_QUEUES",
			Subjects:  []string{"task.>"},
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		},
		{
			Name:      "EVENTS",
			Subjects:  []string{"event.>"},
			Retention: nats.LimitsPolicy,
			Storage:   nats.FileStorage,
		},
	}
	for _, cfg := range streams {
		_, err := js.AddStream(&cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupKVBuckets creates the KV buckets used to store workflow definitions
// and runtime state for active workflow runs.
func SetupKVBuckets(js nats.JetStreamContext) error {
	buckets := []nats.KeyValueConfig{
		{Bucket: "workflow_defs"},
		{Bucket: "workflow_runs"},
	}
	for _, cfg := range buckets {
		_, err := js.CreateKeyValue(&cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetupTelemetryStream creates the TELEMETRY stream for all observability
// signals (spans, metrics, logs). 7-day retention, 1GB cap, 5s dedup window.
func SetupTelemetryStream(js nats.JetStreamContext) error {
	if js == nil {
		panic("SetupTelemetryStream: js must not be nil")
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TELEMETRY",
		Subjects:   []string{"telemetry.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		MaxAge:     7 * 24 * time.Hour,
		MaxBytes:   1 << 30,
		Duplicates: 5 * time.Second,
	})
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

// SetupAll creates all streams and KV buckets on the given connection.
// Optional SetupOption args provision additional streams and KV buckets
// for downstream packages without forking natsutil.
func SetupAll(nc *nats.Conn, opts ...SetupOption) error {
	if nc == nil {
		panic("natsutil: connection must not be nil")
	}

	var options setupOptions
	for _, opt := range opts {
		opt(&options)
	}

	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	if err := SetupStreams(js); err != nil {
		return err
	}
	if err := SetupKVBuckets(js); err != nil {
		return err
	}
	if err := SetupTelemetryStream(js); err != nil {
		return err
	}

	for _, sc := range options.streams {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      sc.Name,
			Subjects:  sc.Subjects,
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
		})
		if err != nil {
			return err
		}
	}

	for _, kc := range options.kvs {
		_, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: kc.Bucket,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
