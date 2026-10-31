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

// orderedCloseTimeout bounds each delete of a server-side consumer.
const orderedCloseTimeout = 5 * time.Second

// orderedGonePoll is how often a delete re-checks that the consumer has
// left the stream; orderedGonePollMax bounds the checks to the timeout.
const (
	orderedGonePoll    = 10 * time.Millisecond
	orderedGonePollMax = int(orderedCloseTimeout / orderedGonePoll)
)

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
//
// nats.go creates the server-side consumer when the ordered consumer is
// opened, and every Fetch, FetchBytes, FetchNoWait or Next replaces it
// with a new one (jetstream/ordered.go reset). It deletes the replaced
// consumer from a goroutine nobody waits on, so the delete can land
// after the read ends or never, when the process exits first. The
// wrappers below delete it before returning; Messages and Consume reuse
// the consumer created at open and need no wrapper.
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
	name := c.currentName()
	c.release(name)
	c.remove = nil
}

// Fetch is jetstream.Consumer.Fetch that deletes the consumer it replaced.
func (c *OrderedConsumer) Fetch(
	batch int, opts ...jetstream.FetchOpt,
) (jetstream.MessageBatch, error) {
	previous := c.currentName()
	result, err := c.Consumer.Fetch(batch, opts...)
	c.releaseReplaced(previous)
	return result, err
}

// FetchBytes is jetstream.Consumer.FetchBytes that deletes the consumer
// it replaced.
func (c *OrderedConsumer) FetchBytes(
	bytesMax int, opts ...jetstream.FetchOpt,
) (jetstream.MessageBatch, error) {
	previous := c.currentName()
	result, err := c.Consumer.FetchBytes(bytesMax, opts...)
	c.releaseReplaced(previous)
	return result, err
}

// FetchNoWait is jetstream.Consumer.FetchNoWait that deletes the
// consumer it replaced.
func (c *OrderedConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	previous := c.currentName()
	result, err := c.Consumer.FetchNoWait(batch)
	c.releaseReplaced(previous)
	return result, err
}

// Next is jetstream.Consumer.Next that deletes the consumer it replaced.
func (c *OrderedConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	previous := c.currentName()
	msg, err := c.Consumer.Next(opts...)
	c.releaseReplaced(previous)
	return msg, err
}

// currentName is the server-side consumer the reader is on, or "" when
// nats.go holds none.
func (c *OrderedConsumer) currentName() string {
	if c.Consumer == nil {
		panic("OrderedConsumer.currentName: consumer must not be nil")
	}
	info := c.CachedInfo()
	if info == nil {
		return ""
	}
	return info.Name
}

// releaseReplaced deletes previous once a reset has moved the reader
// off it. A failed reset leaves the reader on previous, which stays.
func (c *OrderedConsumer) releaseReplaced(previous string) {
	if c.Consumer == nil {
		panic("OrderedConsumer.releaseReplaced: consumer must not be nil")
	}
	if previous == "" || previous == c.currentName() {
		return
	}
	c.release(previous)
}

// release deletes the named server-side consumer on its own context,
// because the read's context is often the thing that ended. nats.go's
// own delete of a replaced consumer races this one, so not-found is
// success.
func (c *OrderedConsumer) release(name string) {
	if c.remove == nil || name == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), orderedCloseTimeout)
	defer cancel()
	err := c.remove(ctx, name)
	if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) &&
		!errors.Is(err, jetstream.ErrStreamNotFound) {
		slog.Warn("delete ordered consumer failed",
			"stream", c.stream, "consumer", name, "error", err)
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
			return deleteConsumer(ctx, name,
				func(ctx context.Context, name string) error {
					return js.DeleteConsumer(ctx, streamName, name)
				},
				func(ctx context.Context, name string) error {
					_, err := js.Consumer(ctx, streamName, name)
					return err
				})
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
			return deleteConsumer(ctx, name, stream.DeleteConsumer,
				func(ctx context.Context, name string) error {
					_, err := stream.Consumer(ctx, name)
					return err
				})
		}}, nil
}

// deleteConsumer deletes the named consumer and returns once the stream
// no longer holds it. nats.go deletes a replaced consumer too, and when
// two deletes race, nats-server answers the loser with success while
// the winner is still removing it (server/consumer.go stopWithFlags
// returns on o.closed before mset.removeConsumer), so success alone
// does not mean gone.
func deleteConsumer(
	ctx context.Context, name string, remove, lookup func(context.Context, string) error,
) error {
	if name == "" {
		panic("deleteConsumer: name must not be empty")
	}
	if remove == nil || lookup == nil {
		panic("deleteConsumer: remove and lookup must not be nil")
	}
	if err := remove(ctx, name); err != nil {
		return err
	}
	for poll := 0; poll < orderedGonePollMax; poll++ {
		err := lookup(ctx, name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(orderedGonePoll):
		}
	}
	return errors.New("consumer " + name + " still present after delete")
}
