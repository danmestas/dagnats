// api/service_dlq.go
// Split out of service.go (#566): dead-letter list/replay/discard + parsers domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
)

// DeadLetter represents a message that failed processing. The
// final schema (#200) extends the legacy fields with Body, Headers,
// DeliveryCount, and Consumer so replay can re-publish the original
// task verbatim and operators can see delivery metadata.
type DeadLetter struct {
	Sequence  uint64    `json:"sequence"`
	Subject   string    `json:"subject"`
	RunID     string    `json:"run_id"`
	StepID    string    `json:"step_id"`
	Task      string    `json:"task"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`

	// Body is the original task message payload at the moment of
	// DLQ entry — the marshalled protocol.TaskPayload bytes that
	// would have been on the task subject. Empty for legacy entries
	// written before this schema landed; replay against a legacy
	// entry returns ErrDLQBodyMissing.
	Body []byte `json:"body,omitempty"`

	// Headers carries the original NATS headers verbatim so replay
	// reproduces the same dispatch context.
	Headers nats.Header `json:"headers,omitempty"`

	// DeliveryCount is the JetStream redelivery count at the moment
	// of DLQ publish — i.e. the value that triggered exhaustion.
	DeliveryCount int `json:"delivery_count,omitempty"`

	// Consumer is the JetStream consumer name that delivered the
	// original message. Surfaces in the CLI so operators can tell
	// which path the task came through.
	Consumer string `json:"consumer,omitempty"`
}

// DeadLetterView is the operator-facing rendering of a DLQ entry:
// the raw DeadLetter plus derived fields the CLI surfaces directly.
// CLI code does no derivation of its own — all derivation lives here.
type DeadLetterView struct {
	DeadLetter
	BodyPreserved bool `json:"body_preserved"`
}

// newDeadLetterView returns the operator-facing rendering of a
// DeadLetter. BodyPreserved is true when the stored Body is
// non-empty — only such entries are replayable.
func newDeadLetterView(dl DeadLetter) DeadLetterView {
	return DeadLetterView{
		DeadLetter:    dl,
		BodyPreserved: len(dl.Body) > 0,
	}
}

// ErrDLQBodyMissing is returned by ReplayDeadLetter when the DLQ
// entry's Body is empty — typically a legacy entry written before
// the body-preservation schema landed. Operators recover such
// entries via upstream reconstruction; the CLI must not silently
// re-publish a stub.
var ErrDLQBodyMissing = errors.New(
	"dlq entry body not preserved; replay unsupported",
)

// CountDeadLetters returns the current number of entries on the
// DEAD_LETTERS stream. CLI uses this to surface truncation footers
// without rolling its own NATS plumbing.
func (s *Service) CountDeadLetters(ctx context.Context) (int, error) {
	if ctx == nil {
		panic("CountDeadLetters: ctx must not be nil")
	}
	if s.js == nil {
		panic("CountDeadLetters: js must not be nil")
	}
	stream, err := s.js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return int(info.State.Msgs), nil
}

// ListDeadLetters retrieves up to limit dead letter messages.
// Returns operator-facing views with derived fields (e.g.
// BodyPreserved) so CLI rendering is pure transport.
func (s *Service) ListDeadLetters(
	ctx context.Context, limit int,
) ([]DeadLetterView, error) {
	if ctx == nil {
		panic("ListDeadLetters: ctx must not be nil")
	}
	if limit <= 0 {
		panic("ListDeadLetters: limit must be positive")
	}
	var views []DeadLetterView
	err := s.observed(ctx, "listDeadLetters", nil,
		func(_ context.Context) error {
			var innerErr error
			views, innerErr = s.listDeadLettersInner(limit)
			return innerErr
		},
	)
	return views, err
}

// listDeadLettersInner fetches messages from the DEAD_LETTERS
// stream using a legacy SubscribeSync via the raw connection.
// Returns DeadLetterView so derived fields (BodyPreserved) are
// computed exactly once at the engine boundary.
func (s *Service) listDeadLettersInner(
	limit int,
) ([]DeadLetterView, error) {
	if limit <= 0 {
		panic("listDeadLettersInner: limit must be positive")
	}
	if s.nc == nil {
		panic("listDeadLettersInner: nc must not be nil")
	}
	jsLegacy, err := s.nc.JetStream()
	if err != nil {
		return nil, err
	}
	sub, err := jsLegacy.SubscribeSync("dead.>")
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe() //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	msgs := fetchMessages(sub, limit, deadline)
	views := make([]DeadLetterView, 0, len(msgs))
	for _, msg := range msgs {
		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			continue
		}
		views = append(views,
			newDeadLetterView(parseDLQMessage(msg, meta)))
	}
	return views, nil
}

// parseDLQMessage decodes a DLQ stream message into a DeadLetter,
// supporting both the post-#200 shape (body in msg.Data, metadata
// in structured headers) and the pre-#200 legacy shape (metadata
// JSON in msg.Data, no body preserved). Detection key:
// HeaderDLQRunID is set only for post-#200 entries.
func parseDLQMessage(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	if msg == nil {
		panic("parseDLQMessage: msg must not be nil")
	}
	if meta == nil {
		panic("parseDLQMessage: meta must not be nil")
	}
	if msg.Header.Get(engine.HeaderDLQRunID) != "" {
		return parseModernDLQ(msg, meta)
	}
	return parseLegacyDLQ(msg, meta)
}

// parseModernDLQ decodes a post-#200 DLQ entry: body in msg.Data,
// metadata in structured Dagnats-Dlq-* headers, original task
// subject preserved via HeaderDLQTaskSubject.
func parseModernDLQ(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	attempts, _ := strconv.Atoi(
		msg.Header.Get(engine.HeaderDLQAttempts),
	)
	deliveryCount, _ := strconv.Atoi(
		msg.Header.Get(engine.HeaderDLQDeliveryCount),
	)
	taskSubject := msg.Header.Get(engine.HeaderDLQTaskSubject)
	stored := DeadLetter{
		Sequence:      meta.Sequence.Stream,
		Subject:       msg.Subject,
		RunID:         msg.Header.Get(engine.HeaderDLQRunID),
		StepID:        msg.Header.Get(engine.HeaderDLQStepID),
		Task:          msg.Header.Get(engine.HeaderDLQTask),
		Error:         msg.Header.Get(engine.HeaderDLQError),
		Timestamp:     meta.Timestamp,
		Body:          msg.Data,
		DeliveryCount: deliveryCount,
		Consumer:      msg.Header.Get(engine.HeaderDLQConsumer),
	}
	if stored.Error == "" {
		stored.Error = msg.Header.Get("Error")
	}
	// Stash attempts (legacy) into headers map for downstream use;
	// also preserve the original task subject so replay knows where
	// to re-publish without re-deriving from (task, runID).
	stored.Headers = nats.Header{}
	if taskSubject != "" {
		stored.Headers[engine.HeaderDLQTaskSubject] = []string{taskSubject}
	}
	if attempts > 0 {
		stored.Headers[engine.HeaderDLQAttempts] = []string{
			strconv.Itoa(attempts),
		}
	}
	return stored
}

// parseLegacyDLQ decodes a pre-#200 DLQ entry: metadata JSON in
// msg.Data, no body preserved. The returned DeadLetter has empty
// Body so newDeadLetterView reports BodyPreserved=false and replay
// returns ErrDLQBodyMissing.
func parseLegacyDLQ(
	msg *nats.Msg, meta *nats.MsgMetadata,
) DeadLetter {
	var raw struct {
		RunID    string `json:"run_id"`
		StepID   string `json:"step_id"`
		Task     string `json:"task"`
		Error    string `json:"error"`
		Attempts int    `json:"attempts"`
	}
	_ = json.Unmarshal(msg.Data, &raw) //nolint:errcheck
	errStr := raw.Error
	if errStr == "" {
		errStr = msg.Header.Get("Error")
	}
	taskName := raw.Task
	if taskName == "" {
		taskName = extractTaskFromSubject(msg.Subject)
	}
	return DeadLetter{
		Sequence:      meta.Sequence.Stream,
		Subject:       msg.Subject,
		RunID:         raw.RunID,
		StepID:        raw.StepID,
		Task:          taskName,
		Error:         errStr,
		Timestamp:     meta.Timestamp,
		DeliveryCount: raw.Attempts,
	}
}

// fetchMessages drains up to limit messages from sub within the
// given total deadline. Returns on first NextMsg error (timeout or
// stream exhaustion). Owns the timeout algebra so callers don't.
//
// The per-message timeout is two-tier:
//
//   - 100ms "warm" window for the first message — covers the
//     consumer-creation roundtrip plus any backlog delivery. On
//     loopback / LAN the first message lands in <10ms; 100ms is a
//     generous ceiling that still cuts page-load TTFB by ~5x vs the
//     original 500ms.
//   - 5ms "tail" window for subsequent messages — once one message
//     arrived the NATS client's local pending queue already holds
//     the rest of the prefix (the server streams the full set on
//     the consumer pull), so 5ms is plenty to drain the buffer
//     and detect end-of-stream.
//
// Previously every fetchMessages call paid 500ms on both the first
// and the tail, which taxed every page that walked a NATS
// subscription synchronously (DLQ list, DLQ detail, run-detail
// event timeline) by ~505ms TTFB even when there were no messages
// to read.
func fetchMessages(
	sub *nats.Subscription, limit int, deadline time.Time,
) []*nats.Msg {
	if sub == nil {
		panic("fetchMessages: sub must not be nil")
	}
	if limit <= 0 {
		panic("fetchMessages: limit must be positive")
	}
	const firstWait = 100 * time.Millisecond
	const tailWait = 5 * time.Millisecond
	msgs := make([]*nats.Msg, 0, limit)
	for i := 0; i < limit; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timeout := firstWait
		if len(msgs) > 0 {
			timeout = tailWait
		}
		if remaining < timeout {
			timeout = remaining
		}
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// extractTaskFromSubject extracts the task name from a subject.
func extractTaskFromSubject(subject string) string {
	if len(subject) > 5 && subject[:5] == "dead." {
		return subject[5:]
	}
	return subject
}

// ReplayDeadLetter fetches a dead letter by sequence and republishes it.
func (s *Service) ReplayDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if ctx == nil {
		panic("ReplayDeadLetter: ctx must not be nil")
	}
	if seq == 0 {
		panic("ReplayDeadLetter: seq must be positive")
	}
	return s.observed(ctx, "replayDeadLetter",
		[]attribute.KeyValue{
			attribute.Int64("sequence", int64(seq)),
		},
		func(ctx context.Context) error {
			return s.replayDeadLetterInner(ctx, seq)
		},
	)
}

// replayDeadLetterInner fetches the DLQ entry by sequence and
// re-publishes its stored body verbatim onto the original task
// subject. Returns ErrDLQBodyMissing when the entry pre-dates the
// body-preservation schema (no Body field) — operators must recover
// such entries upstream rather than replay.
func (s *Service) replayDeadLetterInner(
	ctx context.Context, seq uint64,
) error {
	if seq == 0 {
		panic("replayDeadLetterInner: seq must be positive")
	}
	if s.js == nil {
		panic("replayDeadLetterInner: js must not be nil")
	}
	views, err := s.listDeadLettersInner(100)
	if err != nil {
		return err
	}
	var target *DeadLetterView
	for i := range views {
		if views[i].Sequence == seq {
			target = &views[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf(
			"dead letter with sequence %d not found", seq,
		)
	}
	if len(target.Body) == 0 {
		return fmt.Errorf("dlq sequence %d: %w",
			seq, ErrDLQBodyMissing)
	}
	taskSubject := ""
	if target.Headers != nil {
		taskSubject = target.Headers.Get(
			engine.HeaderDLQTaskSubject,
		)
	}
	if taskSubject == "" {
		taskSubject = deriveTaskSubject(target.Task)
	}
	msg := &nats.Msg{
		Subject: taskSubject,
		Data:    target.Body,
		Header:  target.Headers,
	}
	if _, err := s.tp.JSPublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("dlq replay: publish: %w", err)
	}
	return nil
}

// DiscardDeadLetter removes the entry at the given stream sequence
// from the DEAD_LETTERS stream permanently. Returns an error when the
// sequence is missing or JetStream rejects the delete. Operators
// trigger this via /console/dlq/<seq>/discard after a typed
// confirmation; CLI may expose it later.
func (s *Service) DiscardDeadLetter(
	ctx context.Context, seq uint64,
) error {
	if ctx == nil {
		panic("DiscardDeadLetter: ctx must not be nil")
	}
	if seq == 0 {
		panic("DiscardDeadLetter: seq must be positive")
	}
	return s.observed(ctx, "discardDeadLetter",
		[]attribute.KeyValue{
			attribute.Int64("sequence", int64(seq)),
		},
		func(ctx context.Context) error {
			stream, err := s.js.Stream(ctx, "DEAD_LETTERS")
			if err != nil {
				return fmt.Errorf("dlq stream: %w", err)
			}
			if err := stream.DeleteMsg(ctx, seq); err != nil {
				return fmt.Errorf("delete dlq seq %d: %w", seq, err)
			}
			return nil
		},
	)
}

// deriveTaskSubject is the legacy-shape fallback when the DLQ
// entry's stored task subject is missing — best-effort recovery for
// entries written before HeaderDLQTaskSubject existed.
func deriveTaskSubject(task string) string {
	if isTaskSubject(task) {
		return task
	}
	return "task." + task
}

// isTaskSubject checks if a subject starts with "task.".
func isTaskSubject(subject string) bool {
	return len(subject) >= 5 && subject[:5] == "task."
}
