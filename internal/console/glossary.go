package console

// glossary maps genuine technical jargon to operator-facing definitions.
//
// Status words (running/failed/completed/pending/cancelled/skipped) are
// deliberately excluded: operators know what those mean. Annotating
// them adds visual clutter without information, which violates Norman's
// "signifiers > tooltips" rule. Tests in glossary_test.go guard against
// drift toward an "annotate everything" anti-pattern.
//
// Entries are kept under a couple of sentences so the popover fits in
// the 280px-wide layout defined in basecoat-raw.css.
var glossary = map[string]string{
	"DLQ": "Dead-letter queue: tasks that exhausted their retry budget. " +
		"Operator intervention required to retry or discard.",
	"lease": "An exclusive claim a worker holds on a task while " +
		"executing it. Expires automatically if the worker dies.",
	"trigger": "An input that starts a workflow run. Types: cron " +
		"(schedule), webhook (HTTP push), subject (NATS message), " +
		"http (synchronous).",
	"p50": "Median latency. Half of runs finish faster, half slower.",
	"p95": "95th-percentile latency. 95% of runs finish faster; the " +
		"slowest 5% take longer.",
	"p99": "99th-percentile latency. Tail latency — useful for " +
		"spotting outliers without optimizing for them.",
	"KV": "Key-value store. NATS JetStream's persistent KV layer; " +
		"dagnats uses it for workflow defs, runs, audit log, and metrics.",
	"SSE": "Server-Sent Events: a one-way streaming protocol the " +
		"console uses for live updates without WebSockets.",
}

// GlossaryTooltip returns the definition text for term and a boolean
// reporting whether the term is registered. The two-value return is
// the load-bearing contract — template helpers branch on `ok` so they
// can fall back to a plain label for unregistered words rather than
// rendering an empty popover.
func GlossaryTooltip(term string) (string, bool) {
	text, ok := glossary[term]
	return text, ok
}
