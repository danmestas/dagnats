package natsutil

import (
	"context"
	"errors"
	"log/slog"
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

// OrderedConsumerInactiveThreshold is the server-side backstop for a
// consumer nobody released (a crashed process, a Close that could not
// reach the server). nats.go defaults an ordered consumer to 5 minutes,
// long enough for a busy reader to pile up toward the server's
// 1000-consumers-per-stream cap, so it is shortened. It is only a
// backstop: OrderedConsumer.Close deletes the consumer when the read
// ends, and a live reader is never inactive (every fetch is a pull).
const OrderedConsumerInactiveThreshold = time.Minute

// orderedCloseTimeout bounds the delete in Close, which runs on its own
// context because the read's context is often the thing that ended.
const orderedCloseTimeout = 5 * time.Second

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

		MaxResetAttempts:  OrderedConsumerResetMax,
		InactiveThreshold: OrderedConsumerInactiveThreshold,
	}
	if cfg.MaxResetAttempts <= 0 {
		panic("OrderedConsumerConfig: bound must be positive")
	}
	return cfg
}

// OrderedConsumer is an ordered consumer that must be Closed when the
// read ends. It embeds jetstream.Consumer, so readers use it as one.
// nats.go gives an ordered consumer no way to delete itself, and every
// reader that skipped the delete left a server-side consumer behind
// until the inactivity threshold, which exhausted the stream's
// consumer cap under load.
type OrderedConsumer struct {
	jetstream.Consumer
	stream string
	remove func(ctx context.Context, name string) error
}

// Close deletes the server-side consumer the reader is currently on
// (nats.go renames it on every reset, so the name is read now). It is
// idempotent and best effort: a failure is logged, and the inactivity
// threshold reaps the consumer. Stop any message iterator first.
func (c *OrderedConsumer) Close() {
	if c == nil || c.Consumer == nil {
		panic("OrderedConsumer.Close: consumer must not be nil")
	}
	if c.remove == nil {
		return
	}
	remove := c.remove
	c.remove = nil
	info := c.CachedInfo()
	if info == nil {
		return // lazy: no fetch ever created the server-side consumer
	}
	ctx, cancel := context.WithTimeout(context.Background(), orderedCloseTimeout)
	defer cancel()
	err := remove(ctx, info.Name)
	if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) &&
		!errors.Is(err, jetstream.ErrStreamNotFound) {
		slog.Warn("delete ordered consumer failed",
			"stream", c.stream, "consumer", info.Name, "error", err)
	}
}

// OpenConsumer opens a bounded ordered consumer on streamName via js.
// This is the ONLY sanctioned way to call jetstream.JetStream's
// OrderedConsumer in this repo — the CI lint referenced above enforces
// it by forbidding the raw call anywhere else. The caller must Close
// the result.
func OpenConsumer(
	ctx context.Context, js jetstream.JetStream, streamName string, spec OrderedConsumerSpec,
) (*OrderedConsumer, error) {
	if js == nil {
		panic("OpenConsumer: js must not be nil")
	}
	if streamName == "" {
		panic("OpenConsumer: streamName must not be empty")
	}
	cons, err := js.OrderedConsumer(ctx, streamName, OrderedConsumerConfig(spec))
	if err != nil {
		return nil, err
	}
	return &OrderedConsumer{Consumer: cons, stream: streamName,
		remove: func(ctx context.Context, name string) error {
			return js.DeleteConsumer(ctx, streamName, name)
		}}, nil
}

// OpenStreamConsumer opens a bounded ordered consumer directly on an
// already-resolved jetstream.Stream handle (the shape cli/clean.go
// needs — it has a Stream, not a JetStream + name). This is the ONLY
// sanctioned way to call jetstream.Stream's OrderedConsumer in this
// repo, for the same reason as OpenConsumer above. The caller must
// Close the result.
func OpenStreamConsumer(
	ctx context.Context, stream jetstream.Stream, spec OrderedConsumerSpec,
) (*OrderedConsumer, error) {
	if stream == nil {
		panic("OpenStreamConsumer: stream must not be nil")
	}
	if ctx == nil {
		panic("OpenStreamConsumer: ctx must not be nil")
	}
	cons, err := stream.OrderedConsumer(ctx, OrderedConsumerConfig(spec))
	if err != nil {
		return nil, err
	}
	return &OrderedConsumer{Consumer: cons, stream: "stream-handle",
		remove: func(ctx context.Context, name string) error {
			return stream.DeleteConsumer(ctx, name)
		}}, nil
}
