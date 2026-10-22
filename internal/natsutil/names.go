package natsutil

// provisionedStreams names every JetStream stream SetupAll creates.
// Health reporting reads it via ProvisionedStreamNames so it cannot drift
// from provisioning; TestProvisionedNamesMatchSetupAll fails when SetupAll
// and this list disagree.
var provisionedStreams = []string{
	"WORKFLOW_HISTORY",
	"TASK_QUEUES",
	"EVENTS",
	"DEAD_LETTERS",
	"SLEEP_TIMERS",
	"TELEMETRY",
	"STICKY_TASKS",
	"TRIGGER_HISTORY",
	"BUILD_LOGS",
	AgentJournalStream,
}

// provisionedKVBuckets names every KV bucket SetupAll creates, guarded by
// the same drift test as provisionedStreams.
var provisionedKVBuckets = []string{
	"workflow_defs",
	"workflow_runs",
	"scheduled_runs",
	"workers",
	"worker_tokens",
	"worker_status",
	"event_waiters",
	"rate_limits",
	"concurrency_tasks",
	"approval_tokens",
	"debounce_state",
	"idempotency_keys",
	"http_idempotency",
	"sticky_bindings",
	"singleton_locks",
	"trigger_types",
	"services",
	"triggers",
	"trigger_state",
	"signals",
	"checkpoints",
	"concurrency_runs",
	// Provisioned non-fatally (needs nats-server >= 2.11); the embedded
	// server always satisfies that.
	LeasesBucket,
}

// ProvisionedStreamNames returns a copy of the stream names SetupAll
// provisions, excluding caller-supplied WithStreams extras.
func ProvisionedStreamNames() []string {
	return append([]string(nil), provisionedStreams...)
}

// ProvisionedKVBucketNames returns a copy of the KV bucket names SetupAll
// provisions, excluding caller-supplied WithKVBuckets extras.
func ProvisionedKVBucketNames() []string {
	return append([]string(nil), provisionedKVBuckets...)
}
