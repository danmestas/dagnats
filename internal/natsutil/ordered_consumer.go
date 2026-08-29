package natsutil

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// OrderedConsumerResetMax bounds jetstream's ordered-consumer reset
// retry loop for EVERY ordered consumer dagnats opens — CLI reads
// (cli/logs.go, cli/logs_search.go, cli/trace.go, cli/metrics.go,
// cli/clean.go), internal/api's log readers, and
// internal/observe/spanread all open their consumer through
// OpenConsumer/OpenStreamConsumer (below) so this bound cannot drift
// between them. A CI lint step ("Ordered consumer bound lint") fails
// the build if OrderedConsumer( or jetstream.OrderedConsumerConfig{
// appears in any non-test file outside this one, so neither the config
// nor the call that opens it can be reintroduced unbounded elsewhere.
//
// It MUST be set, and this is a deliberate local workaround for
// upstream behavior, not a tuning knob (#672, #675; owner decision:
// no upstream filing). In nats.go v1.53.1:
//
//   - jetstream/ordered.go:614 (getConsumerConfig) rewrites an unset
//     MaxResetAttempts (the zero value) to -1.
//   - jetstream/ordered.go:820 (retryWithBackoff) only stops on
//     `opts.attempts > 0 && i >= opts.attempts-1`, so -1 means the
//     retry loop has no exit.
//   - orderedConsumer.Next calls Fetch, which calls reset() on EVERY
//     invocation (ordered.go:441, :529).
//
// Observed consequence: once the stream is deleted, Next() NEVER
// RETURNS. It recreates the consumer forever on a 1s/2s/4s/8s/10s
// backoff, and because reset() issues each CreateOrUpdateConsumer on
// context.Background(), the caller's own context deadline cannot stop
// it. That hung an SSE follow handler (and the http.Server waiting on
// it) indefinitely (#672's original finding in internal/api); every
// other ordered-consumer site in the repo carried the same exposure if
// its stream was deleted or pruned mid-read (#675).
//
// The workaround is to set the bound explicitly. Four attempts spend
// ~7s (1+2+4) before Next() surfaces the real "stream not found" (or
// equivalent), which callers then classify and report instead of
// hanging.
const OrderedConsumerResetMax = 4

// OrderedConsumerSpec is the caller-supplied half of an ordered
// consumer's config — everything that legitimately varies by call
// site (which subjects, where to start reading). MaxResetAttempts is
// deliberately absent: OrderedConsumerConfig always sets it, so a spec
// cannot accidentally carry (and therefore bypass) the bound.
type OrderedConsumerSpec struct {
	FilterSubjects []string
	DeliverPolicy  jetstream.DeliverPolicy
	OptStartSeq    uint64
	OptStartTime   *time.Time
}

// OrderedConsumerConfig builds a jetstream.OrderedConsumerConfig from
// spec with MaxResetAttempts set to OrderedConsumerResetMax. It is the
// ONE place any ordered-consumer config in this repo is assembled;
// OpenConsumer and OpenStreamConsumer (below) are the only callers, so
// application code never spells out jetstream.OrderedConsumerConfig{}
// directly.
func OrderedConsumerConfig(spec OrderedConsumerSpec) jetstream.OrderedConsumerConfig {
	if spec.OptStartSeq != 0 && spec.OptStartTime != nil {
		panic("OrderedConsumerConfig: OptStartSeq and OptStartTime are mutually exclusive")
	}
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: spec.FilterSubjects,
		DeliverPolicy:  spec.DeliverPolicy,
		OptStartSeq:    spec.OptStartSeq,
		OptStartTime:   spec.OptStartTime,

		MaxResetAttempts: OrderedConsumerResetMax,
	}
	if cfg.MaxResetAttempts <= 0 {
		panic("OrderedConsumerConfig: bound must be positive")
	}
	return cfg
}

// OpenConsumer opens a bounded ordered consumer on streamName via js.
// This is the ONLY sanctioned way to call jetstream.JetStream's
// OrderedConsumer in this repo — the CI lint referenced above enforces
// it by forbidding the raw call anywhere else.
func OpenConsumer(
	ctx context.Context, js jetstream.JetStream, streamName string, spec OrderedConsumerSpec,
) (jetstream.Consumer, error) {
	if js == nil {
		panic("OpenConsumer: js must not be nil")
	}
	if streamName == "" {
		panic("OpenConsumer: streamName must not be empty")
	}
	return js.OrderedConsumer(ctx, streamName, OrderedConsumerConfig(spec))
}

// OpenStreamConsumer opens a bounded ordered consumer directly on an
// already-resolved jetstream.Stream handle (the shape cli/clean.go
// needs — it has a Stream, not a JetStream + name). This is the ONLY
// sanctioned way to call jetstream.Stream's OrderedConsumer in this
// repo, for the same reason as OpenConsumer above.
func OpenStreamConsumer(
	ctx context.Context, stream jetstream.Stream, spec OrderedConsumerSpec,
) (jetstream.Consumer, error) {
	if stream == nil {
		panic("OpenStreamConsumer: stream must not be nil")
	}
	if ctx == nil {
		panic("OpenStreamConsumer: ctx must not be nil")
	}
	return stream.OrderedConsumer(ctx, OrderedConsumerConfig(spec))
}
