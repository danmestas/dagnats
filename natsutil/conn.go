package natsutil

import "github.com/nats-io/nats.go"

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

// SetupAll creates all streams and KV buckets on the given connection.
// This is the single entry point for bootstrapping a DagNats NATS namespace.
func SetupAll(nc *nats.Conn) error {
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	if err := SetupStreams(js); err != nil {
		return err
	}
	return SetupKVBuckets(js)
}
