package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// stickyRetryMinDefault is the first wait after a failed recovery
	// attempt, and the least time between two recovery passes, so a
	// burst of consume errors costs at most one pass per interval.
	stickyRetryMinDefault = time.Second
	// stickyRetryMax caps the doubling backoff between attempts.
	stickyRetryMax = 30 * time.Second
	// stickyRecoverAttemptsMax bounds one recovery pass, about twenty
	// minutes of failures. A pass that runs out starts another: by then
	// Consume has usually ended, so no consume error would.
	stickyRecoverAttemptsMax = 40
	// stickyAPITimeout bounds each JetStream API call of an attempt.
	stickyAPITimeout = 5 * time.Second
)

// stickyRecovery tunes a sticky lane's recovery: the first backoff and
// the attempts in one pass. Workers use the defaults; tests shorten them.
type stickyRecovery struct {
	retryMin    time.Duration
	attemptsMax int
}

// stickyConsume is the sticky lane's ConsumeContext, and it keeps the
// lane alive. The server reaps a sticky consumer whose pull lapses past
// its InactiveThreshold, as across a long leadership change, and
// nats.go never recreates one: a delete seen with a pull outstanding
// ends Consume, and a later pull only reports no responders, every
// heartbeat, forever. Either error starts a recovery here that
// recreates the consumer under its name and consumes it afresh.
type stickyConsume struct {
	stream   jetstream.Stream
	config   jetstream.ConsumerConfig
	handler  jetstream.MessageHandler
	logError jetstream.ConsumeErrHandlerFunc
	label    string
	retryMin time.Duration
	// attemptsMax bounds one recovery pass.
	attemptsMax int
	// kick holds at most one pending recovery request.
	kick   chan struct{}
	done   chan struct{}
	exited chan struct{}
	// lastPublished is the newest stream timestamp delivered here, where
	// a recreated consumer resumes so handled tasks are not replayed.
	lastPublished atomic.Int64
	attempts      atomic.Int64

	mu      sync.Mutex
	current jetstream.ConsumeContext
	stopped bool
	endOnce sync.Once
}

// startStickyConsume consumes cons as the sticky lane and starts the
// goroutine that recovers it, which runs until Stop or Drain.
func startStickyConsume(
	stream jetstream.Stream, cons jetstream.Consumer,
	handler jetstream.MessageHandler, tune stickyRecovery,
) (*stickyConsume, error) {
	if stream == nil || cons == nil || handler == nil {
		panic("startStickyConsume: stream, consumer and handler are required")
	}
	retryMin := tune.retryMin
	if retryMin <= 0 || retryMin > stickyRetryMax*time.Duration(stickyRecoverAttemptsMax) {
		panic("startStickyConsume: retryMin must be positive and bounded")
	}
	if tune.attemptsMax <= 0 || tune.attemptsMax > stickyRecoverAttemptsMax {
		panic("startStickyConsume: attemptsMax must be in 1..stickyRecoverAttemptsMax")
	}
	config := cons.CachedInfo().Config
	s := &stickyConsume{
		stream: stream, config: config, handler: handler,
		logError: consumeErrHandler(config.FilterSubject),
		label:    config.FilterSubject, retryMin: retryMin,
		attemptsMax: tune.attemptsMax,
		kick:        make(chan struct{}, 1), done: make(chan struct{}),
		exited: make(chan struct{}),
	}
	current, err := s.consume(cons)
	if err != nil {
		return nil, err
	}
	s.current = current
	go s.supervise()
	return s, nil
}

func (s *stickyConsume) consume(cons jetstream.Consumer) (jetstream.ConsumeContext, error) {
	if cons == nil {
		panic("consume: consumer must not be nil")
	}
	return consumeLane(cons, s.deliver, s.onError)
}

func (s *stickyConsume) deliver(msg jetstream.Msg) {
	if msg == nil {
		panic("deliver: msg must not be nil")
	}
	if meta, err := msg.Metadata(); err == nil {
		published := meta.Timestamp.UnixNano()
		for range 8 {
			last := s.lastPublished.Load()
			if published <= last || s.lastPublished.CompareAndSwap(last, published) {
				break
			}
		}
	}
	s.handler(msg)
}

func (s *stickyConsume) onError(cc jetstream.ConsumeContext, err error) {
	if err == nil {
		panic("onError: err must not be nil")
	}
	s.logError(cc, err)
	if stickyConsumerGone(err) {
		s.requestRecovery()
	}
}

// stickyConsumerGone reports the consume errors that mean the consumer
// no longer exists. No responders is also what a pull meets while a new
// consumer leader is elected; recovery tells the two apart by looking.
func stickyConsumerGone(err error) bool {
	if err == nil {
		panic("stickyConsumerGone: err must not be nil")
	}
	return errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrConsumerNotFound)
}

func (s *stickyConsume) Stop()  { s.end(jetstream.ConsumeContext.Stop) }
func (s *stickyConsume) Drain() { s.end(jetstream.ConsumeContext.Drain) }

// Closed is the current consumption's: after Stop or Drain no recovery
// replaces it, so it is the last one.
func (s *stickyConsume) Closed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.Closed()
}

func (s *stickyConsume) end(finish func(jetstream.ConsumeContext)) {
	s.endOnce.Do(func() {
		s.mu.Lock()
		s.stopped = true
		finish(s.current)
		s.mu.Unlock()
		close(s.done)
	})
}

// supervise runs one recovery pass per kick, at most one per retryMin,
// for the life of the lane. A pass that ends unrecovered queues the next
// itself.
func (s *stickyConsume) supervise() {
	defer close(s.exited)
	for {
		select {
		case <-s.done:
			return
		case <-s.kick:
		}
		recovered, live := s.recover()
		if !live {
			return
		}
		if !recovered {
			s.requestRecovery()
		}
		if !s.wait(s.retryMin) {
			return
		}
	}
}

// requestRecovery queues a recovery pass unless one is already queued.
func (s *stickyConsume) requestRecovery() {
	if s.kick == nil {
		panic("requestRecovery: stickyConsume not built by startStickyConsume")
	}
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// recover retries with a doubling backoff until the lane consumes a
// live consumer or the pass runs out of attempts. live is false once the
// lane is stopping.
func (s *stickyConsume) recover() (recovered, live bool) {
	if s.retryMin <= 0 || s.label == "" {
		panic("recover: stickyConsume not built by startStickyConsume")
	}
	backoff := s.retryMin
	var lastErr error
	for attempt := 1; attempt <= s.attemptsMax; attempt++ {
		s.attempts.Add(1)
		lastErr = s.tryRecover()
		if lastErr == nil {
			return true, true
		}
		if attempt == 1 {
			slog.Warn("sticky consumer recovery attempt failed; retrying",
				"consumer", s.label, "error", lastErr)
		}
		if !s.wait(backoff) {
			return false, false
		}
		backoff = min(backoff*2, stickyRetryMax)
	}
	slog.Error("sticky consumer not recovered; starting another pass",
		"consumer", s.label, "attempts", s.attemptsMax, "error", lastErr)
	return false, true
}

// wait sleeps for d, returning false early when the lane stops.
func (s *stickyConsume) wait(d time.Duration) bool {
	if d <= 0 || d > stickyRetryMax*time.Duration(stickyRecoverAttemptsMax) {
		panic("wait: duration must be positive and bounded")
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-s.done:
		return false
	case <-timer.C:
		return true
	}
}

// tryRecover recreates the consumer when the server no longer has it,
// then consumes it afresh if it was recreated or Consume had ended. A
// consumer that is still there with Consume running needs nothing:
// its pulls recover on their own once the leader is back.
func (s *stickyConsume) tryRecover() error {
	ctx, cancel := context.WithTimeout(context.Background(), stickyAPITimeout)
	defer cancel()
	cons, err := s.stream.Consumer(ctx, s.config.Durable)
	recreated := false
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		cons, err = s.stream.CreateOrUpdateConsumer(ctx, s.resumeConfig())
		if err == nil {
			recreated = true
			slog.Warn("sticky consumer was deleted by the server; recreated it",
				"consumer", s.label)
		}
	}
	if err != nil {
		return err
	}
	return s.resume(cons, recreated)
}

// resumeConfig is the original config, starting just past the newest
// message this lane was delivered, if any.
func (s *stickyConsume) resumeConfig() jetstream.ConsumerConfig {
	if s.config.Durable == "" {
		panic("resumeConfig: the sticky consumer must be durable")
	}
	config := s.config
	last := s.lastPublished.Load()
	if last <= 0 {
		return config
	}
	start := time.Unix(0, last+1)
	config.DeliverPolicy = jetstream.DeliverByStartTimePolicy
	config.OptStartTime = &start
	return config
}

func (s *stickyConsume) resume(cons jetstream.Consumer, recreated bool) error {
	if cons == nil {
		panic("resume: consumer must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	if !recreated && !isClosed(s.current.Closed()) {
		return nil
	}
	s.current.Stop()
	current, err := s.consume(cons)
	if err != nil {
		return err
	}
	s.current = current
	return nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
