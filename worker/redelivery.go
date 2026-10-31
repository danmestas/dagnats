package worker

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// lane is what one consumer dispatches to: the task type, its handler,
// and the AckWait that consumer was created with, which the automatic
// heartbeat must beat.
type lane struct {
	taskType string
	handler  HandlerFunc
	ackWait  time.Duration
}

// deliveryKey names one stream message. Sequences are per stream, and a
// Worker consumes both TASK_QUEUES and STICKY_TASKS.
type deliveryKey struct {
	stream   string
	sequence uint64
}

// claim is one delivery's hold on its key. The generation tells a
// release or forget by a claim that was forgotten apart from one by the
// claim that replaced it. The zero claim tracks nothing.
type claim struct {
	key        deliveryKey
	generation uint64
}

// verdict is what claim decides for a delivery.
type verdict int

const (
	verdictRun     verdict = iota + 1
	verdictRunning         // the same message is running here
	verdictSettled         // the same message was acked here moments ago
)

// recentMax bounds the recently settled keys; past it the oldest go
// first, which only shortens how long a stale delivery is recognised.
const recentMax = 4096

type recentEntry struct {
	key     deliveryKey
	expires time.Time
}

// runningSet holds the messages this Worker is running, and those it
// acked within the last two AckWaits, so a redelivery of one is not run a
// second time. A redelivery can arrive during the run (a WithConcurrency
// lane, or another lane on the same stream) or queue behind it in a lane
// of one, reaching the callback only after the ack.
type runningSet struct {
	mu         sync.Mutex
	running    map[deliveryKey]uint64
	generation uint64
	// capacity is the sum over lanes of concurrency + 1. It is a soft
	// bound: pcgroups can briefly run two callbacks on one lane while
	// membership changes, so exceeding it is logged, never fatal.
	capacity  int
	warned    bool
	recent    map[deliveryKey]time.Time
	recentLog []recentEntry // oldest first
}

// addLane grows capacity for one consumer lane of concurrency n. Called
// only while Start subscribes, before any delivery.
func (s *runningSet) addLane(n int) {
	if n < 1 {
		panic("runningSet.addLane: n must be >= 1")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running == nil {
		s.running = make(map[deliveryKey]uint64)
		s.recent = make(map[deliveryKey]time.Time)
	}
	s.capacity += n + 1
}

// claim decides whether msg runs. A message without JetStream metadata
// is not tracked: it runs under the zero claim.
func (s *runningSet) claim(msg jetstream.Msg, now time.Time) (claim, verdict) {
	if msg == nil {
		panic("runningSet.claim: msg must not be nil")
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return claim{}, verdictRun
	}
	key := deliveryKey{stream: meta.Stream, sequence: meta.Sequence.Stream}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running == nil {
		panic("runningSet.claim: no lane added")
	}
	if _, running := s.running[key]; running {
		return claim{key: key}, verdictRunning
	}
	s.pruneRecent(now)
	if _, settled := s.recent[key]; settled {
		return claim{key: key}, verdictSettled
	}
	if len(s.running) >= s.capacity && !s.warned {
		s.warned = true
		slog.Warn("more tasks in flight than this worker's lanes allow",
			"in_flight", len(s.running), "capacity", s.capacity)
	}
	s.generation++
	s.running[key] = s.generation
	return claim{key: key, generation: s.generation}, verdictRun
}

// release ends c. An acked message is remembered for ttl, so a stale
// delivery of it is recognised. A claim forget already ended does
// nothing.
func (s *runningSet) release(c claim, acked bool, ttl time.Duration, now time.Time) {
	if c.generation == 0 {
		return
	}
	if ttl <= 0 {
		panic("runningSet.release: ttl must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running[c.key] != c.generation {
		return
	}
	delete(s.running, c.key)
	if !acked {
		// A NAK'd message comes back on purpose (Pause, a failed
		// step.started); that redelivery must run.
		return
	}
	s.recent[c.key] = now.Add(ttl)
	s.recentLog = append(s.recentLog, recentEntry{key: c.key, expires: now.Add(ttl)})
	s.pruneRecent(now)
}

// forget ends c without remembering it, so the next delivery of its
// message runs here. The heartbeat cap uses it: past the cap the running
// handler is presumed hung, and its redelivery is the retry.
func (s *runningSet) forget(c claim) {
	if c.generation == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running[c.key] == c.generation {
		delete(s.running, c.key)
	}
}

// pruneRecent drops expired keys, and the oldest past recentMax. The
// caller holds mu. Bounded by len(recentLog), itself at most recentMax+1.
func (s *runningSet) pruneRecent(now time.Time) {
	if s.recent == nil {
		panic("runningSet.pruneRecent: no lane added")
	}
	dropped := 0
	for dropped < len(s.recentLog) {
		entry := s.recentLog[dropped]
		if len(s.recentLog)-dropped <= recentMax && now.Before(entry.expires) {
			break
		}
		if s.recent[entry.key].Equal(entry.expires) {
			delete(s.recent, entry.key)
		}
		dropped++
	}
	s.recentLog = s.recentLog[dropped:]
	if len(s.recentLog) > recentMax {
		panic("runningSet.pruneRecent: log over its bound")
	}
}

// skipDelivery drops a delivery claim did not let run.
//
// verdictRunning: no Ack, since the running handler's outcome must be
// the one that settles the message. No NAK or Term either: the running
// handler's heartbeat keeps extending the deadline on the same stream
// sequence, so the server will not hand the message out again while it
// runs, and the running handler's Ack, Fail or Pause settles it.
//
// verdictSettled: an Ack. The message's work finished and was acked; this
// delivery is either one the server sent before that ack and the client
// buffered, which the ack already settled, or a redelivery because that
// ack was lost, which this ack settles. Either way acking is right and
// running it again is not.
func skipDelivery(taskType string, msg jetstream.Msg, c claim, v verdict) {
	if taskType == "" {
		panic("skipDelivery: taskType must not be empty")
	}
	if v != verdictRunning && v != verdictSettled {
		panic("skipDelivery: verdict must be a skip")
	}
	slog.Info("skipping a redelivery of a task this worker ran",
		"task_type", taskType, "stream", c.key.stream,
		"stream_sequence", c.key.sequence, "still_running", v == verdictRunning)
	if v == verdictSettled {
		if err := msg.Ack(); err != nil {
			slog.Debug("ack of a settled redelivery failed", "error", err)
		}
	}
}

// ackTracker reports whether a delivery was settled with an Ack, which
// release needs: only an acked message's later delivery is stale.
type ackTracker struct {
	jetstream.Msg
	acked atomic.Bool
}

func (m *ackTracker) Ack() error {
	err := m.Msg.Ack()
	if err == nil {
		m.acked.Store(true)
	}
	return err
}
