package worker

import (
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// laneConsumeHeartbeat is the idle heartbeat a lane asks of each pull.
	// A pull answered "no responders" (on a cluster, sent before the
	// worker's server learned the consumer leader's MSG.NEXT interest)
	// leaves nothing outstanding, and nats.go only pulls again once two
	// heartbeats go missing. Its default heartbeat is half the 30s expiry,
	// so a missed first pull idled the lane for 30s (#992); one second
	// re-pulls within about two. The cost is idle heartbeats: up to 60 a
	// minute per lane instead of 4, with pulls unchanged at 2 a minute.
	laneConsumeHeartbeat = time.Second
	// laneStartQuietWindow is how long after a lane starts a missed pull
	// and the heartbeat gap that follows it are expected, not news.
	laneStartQuietWindow = 10 * time.Second
)

// consumeLane starts Consume on a lane's consumer with the lane's pull
// timings. Every lane consumes through here so a missed pull costs every
// lane the same short gap.
func consumeLane(
	cons jetstream.Consumer, handler jetstream.MessageHandler,
	onError jetstream.ConsumeErrHandlerFunc,
) (jetstream.ConsumeContext, error) {
	if cons == nil {
		panic("consumeLane: consumer must not be nil")
	}
	if handler == nil || onError == nil {
		panic("consumeLane: handler and onError are required")
	}
	return cons.Consume(
		handler,
		jetstream.ConsumeErrHandler(onError),
		jetstream.PullHeartbeat(laneConsumeHeartbeat),
	)
}

// consumeErrLevel is the level a Consume error is logged at, sinceStart
// after its lane started. Inside the start window, "no responders" and
// the missed heartbeat after it are the cluster interest race that the
// lane's short heartbeat recovers from, so they log at Debug; anything
// else, or either one later, warns.
func consumeErrLevel(err error, sinceStart time.Duration) slog.Level {
	if err == nil {
		panic("consumeErrLevel: err must not be nil")
	}
	if sinceStart < 0 {
		panic("consumeErrLevel: sinceStart must not be negative")
	}
	if sinceStart >= laneStartQuietWindow {
		return slog.LevelWarn
	}
	if errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoHeartbeat) {
		return slog.LevelDebug
	}
	return slog.LevelWarn
}
