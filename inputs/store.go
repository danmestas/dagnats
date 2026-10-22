// Package inputs is the submissions store of ADR-027: inputs sent to a
// running agent step (a steer that joins the current turn after a tool
// round, a follow-up that starts the next turn after a final answer),
// keyed by a caller-supplied request_id so a retry is answered with the
// original submission. The store is a KV projection: the journal's
// input.placed record is the truth about placement, and a status here
// that disagrees with the journal is repaired from it (worker.PlaceInputs
// and the API's status read both do so).
//
// Status moves queued -> placed -> done, or queued -> unanswered when the
// step finishes first. Withdrawal is a delete while queued, and a
// deleted request_id may be submitted again.
package inputs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Mode says where an input joins the conversation.
type Mode string

const (
	// ModeSteer joins the current turn after the next tool round.
	ModeSteer Mode = "steer"
	// ModeFollowUp starts the next turn after a final answer.
	ModeFollowUp Mode = "follow_up"
)

// Status is where a submission is in its life.
type Status string

const (
	StatusQueued     Status = "queued"
	StatusPlaced     Status = "placed"
	StatusDone       Status = "done"
	StatusUnanswered Status = "unanswered"
)

// Bounds.
const (
	// RequestIDMaxLen bounds a request_id; it is part of a KV key and
	// of event msg-ids.
	RequestIDMaxLen = 128
	// ContentMaxBytes bounds a submission's content. The input is
	// copied into an input.placed journal record with its envelope,
	// and that message must fit the server's 1 MiB max_payload with
	// room to spare; a quarter of it is more than a model turn needs.
	ContentMaxBytes = 256 << 10
	// perStepMax bounds one step's submissions. A step with more has
	// outgrown a per-step inbox.
	perStepMax = 10_000
	// awaitMax bounds one long-poll.
	awaitMax = 60 * time.Second
)

// Sentinel errors.
var (
	ErrNotProvisioned = errors.New(
		"inputs: submissions bucket not provisioned" +
			" — ensure natsutil.SetupAll() has been called" +
			" or start the server with 'dagnats serve'",
	)
	// ErrNotFound: no such submission, or it was withdrawn.
	ErrNotFound = errors.New("inputs: no such submission")
	// ErrNotQueued: the operation needs a queued submission.
	ErrNotQueued = errors.New("inputs: submission is no longer queued")
	// ErrConflict: the submission changed under a compare-and-swap.
	ErrConflict = errors.New("inputs: submission changed since it was read")
	// ErrInvalidRequestID: request_id is empty, too long, or not a
	// subject token ([A-Za-z0-9_-]).
	ErrInvalidRequestID = errors.New(
		"inputs: request_id must be 1-128 characters of [A-Za-z0-9_-]")
	// ErrInvalidMode: mode is neither steer nor follow_up.
	ErrInvalidMode = errors.New("inputs: mode must be steer or follow_up")
	// ErrInvalidContent: content is not JSON.
	ErrInvalidContent = errors.New("inputs: content must be JSON")
	// ErrContentTooLarge: content is over ContentMaxBytes.
	ErrContentTooLarge = fmt.Errorf("inputs: content must be at most %d bytes", ContentMaxBytes)
)

// Submission is one input, as stored.
type Submission struct {
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	RequestID string          `json:"request_id"`
	Mode      Mode            `json:"mode"`
	Content   json.RawMessage `json:"content,omitempty"`
	Status    Status          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	// Iteration is the loop iteration the input was placed in; 0
	// until placed.
	Iteration int `json:"iteration,omitempty"`
}

// Entry is a stored submission with the KV revision it was read at,
// which Transition and Withdraw use as their compare-and-swap token.
type Entry struct {
	Submission
	Revision uint64 `json:"-"`
}

// Store is the bucket handle.
type Store struct {
	nc     *nats.Conn
	kv     jetstream.KeyValue
	stream jetstream.Stream
}

// Open binds the bucket. ErrNotProvisioned when it does not exist.
func Open(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	if ctx == nil {
		panic("inputs.Open: ctx must not be nil")
	}
	if js == nil {
		panic("inputs.Open: js must not be nil")
	}
	kv, err := js.KeyValue(ctx, natsutil.SubmissionsBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, ErrNotProvisioned
		}
		return nil, fmt.Errorf("inputs: bucket: %w", err)
	}
	stream, err := js.Stream(ctx, "KV_"+natsutil.SubmissionsBucket)
	if err != nil {
		return nil, fmt.Errorf("inputs: bucket stream: %w", err)
	}
	if js.Conn() == nil {
		return nil, errors.New("inputs: jetstream context has no connection")
	}
	return &Store{nc: js.Conn(), kv: kv, stream: stream}, nil
}

// ValidateRequestID reports whether id can key a submission.
func ValidateRequestID(id string) error {
	if id == "" || len(id) > RequestIDMaxLen || natsutil.SubjectToken(id) != id {
		return ErrInvalidRequestID
	}
	return nil
}

// Validate checks the caller-supplied fields of a submission.
func (s Submission) Validate() error {
	if s.RunID == "" || s.StepID == "" {
		panic("Submission.Validate: run and step must not be empty")
	}
	if err := ValidateRequestID(s.RequestID); err != nil {
		return err
	}
	if s.Mode != ModeSteer && s.Mode != ModeFollowUp {
		return ErrInvalidMode
	}
	if !json.Valid(s.Content) {
		return ErrInvalidContent
	}
	if len(s.Content) > ContentMaxBytes {
		return ErrContentTooLarge
	}
	return nil
}

// Submit creates sub as queued, once per request_id: a repeat returns
// the stored submission and created=false, whatever its content.
func (s *Store) Submit(ctx context.Context, sub Submission) (Submission, bool, error) {
	if s.kv == nil {
		panic("inputs.Submit: store must be open")
	}
	if err := sub.Validate(); err != nil {
		return Submission{}, false, err
	}
	now := time.Now().UTC()
	sub.Status, sub.CreatedAt, sub.UpdatedAt, sub.Iteration = StatusQueued, now, now, 0
	data, err := json.Marshal(sub)
	if err != nil {
		return Submission{}, false, fmt.Errorf("inputs: marshal: %w", err)
	}
	key := natsutil.SubmissionKey(sub.RunID, sub.StepID, sub.RequestID)
	if _, err := s.kv.Create(ctx, key, data); err == nil {
		return sub, true, nil
	} else if !errors.Is(err, jetstream.ErrKeyExists) {
		return Submission{}, false, fmt.Errorf("inputs: create %s: %w", key, err)
	}
	existing, err := s.Get(ctx, sub.RunID, sub.StepID, sub.RequestID)
	if err != nil {
		return Submission{}, false, err
	}
	return existing.Submission, false, nil
}

// Get reads one submission. ErrNotFound covers a withdrawn one.
func (s *Store) Get(ctx context.Context, runID, stepID, requestID string) (Entry, error) {
	if s.kv == nil {
		panic("inputs.Get: store must be open")
	}
	if err := ValidateRequestID(requestID); err != nil {
		return Entry{}, err
	}
	key := natsutil.SubmissionKey(runID, stepID, requestID)
	kvEntry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, fmt.Errorf("inputs: get %s: %w", key, err)
	}
	return decodeEntry(kvEntry)
}

// decodeEntry turns a KV entry into an Entry.
func decodeEntry(kvEntry jetstream.KeyValueEntry) (Entry, error) {
	if kvEntry == nil {
		panic("decodeEntry: entry must not be nil")
	}
	if kvEntry.Revision() == 0 {
		panic("decodeEntry: revision must be positive")
	}
	var entry Entry
	if err := json.Unmarshal(kvEntry.Value(), &entry.Submission); err != nil {
		return Entry{}, fmt.Errorf("inputs: decode %s: %w", kvEntry.Key(), err)
	}
	entry.Revision = kvEntry.Revision()
	return entry, nil
}

// Await returns the submission once its status differs from seen, or
// the current one when timeout (capped at a minute) passes first.
func (s *Store) Await(
	ctx context.Context, runID, stepID, requestID string, seen Status, timeout time.Duration,
) (Entry, error) {
	if s.kv == nil {
		panic("inputs.Await: store must be open")
	}
	if timeout <= 0 {
		panic("inputs.Await: timeout must be positive")
	}
	key := natsutil.SubmissionKey(runID, stepID, requestID)
	if _, err := s.Get(ctx, runID, stepID, requestID); err != nil {
		return Entry{}, err // unknown or withdrawn: nothing to wait for
	}
	watchCtx, cancel := context.WithTimeout(ctx, min(timeout, awaitMax))
	defer cancel()
	watcher, err := s.kv.Watch(watchCtx, key)
	if err != nil {
		return Entry{}, fmt.Errorf("inputs: watch %s: %w", key, err)
	}
	defer func() {
		if err := watcher.Stop(); err != nil {
			slog.DebugContext(ctx, "inputs: stop watcher", "key", key, "error", err)
		}
	}()
	var latest Entry
	found := false
	for updates := 0; updates < perStepMax; updates++ {
		select {
		case kvEntry, ok := <-watcher.Updates():
			if !ok {
				return s.awaitResult(ctx, runID, stepID, requestID, latest, found)
			}
			if kvEntry == nil {
				continue // end of the initial replay
			}
			if kvEntry.Operation() != jetstream.KeyValuePut {
				return Entry{}, ErrNotFound
			}
			latest, err = decodeEntry(kvEntry)
			if err != nil {
				return Entry{}, err
			}
			found = true
			if latest.Status != seen {
				return latest, nil
			}
		case <-watchCtx.Done():
			return s.awaitResult(ctx, runID, stepID, requestID, latest, found)
		}
	}
	return latest, nil
}

// awaitResult is what Await returns when the watch ends without a
// change: the last entry it saw, else a fresh read.
func (s *Store) awaitResult(
	ctx context.Context, runID, stepID, requestID string, latest Entry, found bool,
) (Entry, error) {
	if s.kv == nil {
		panic("awaitResult: store must be open")
	}
	if found && latest.Revision == 0 {
		panic("awaitResult: a found entry has a revision")
	}
	if found {
		return latest, nil
	}
	return s.Get(ctx, runID, stepID, requestID)
}

// Withdraw deletes a submission that is still queued.
func (s *Store) Withdraw(ctx context.Context, runID, stepID, requestID string) error {
	if s.kv == nil {
		panic("inputs.Withdraw: store must be open")
	}
	entry, err := s.Get(ctx, runID, stepID, requestID)
	if err != nil {
		return err
	}
	if entry.Status != StatusQueued {
		return ErrNotQueued
	}
	key := natsutil.SubmissionKey(runID, stepID, requestID)
	err = s.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSequence(err) {
			return ErrConflict
		}
		return fmt.Errorf("inputs: withdraw %s: %w", key, err)
	}
	return nil
}

// Transition moves entry to status, by compare-and-swap on the revision
// it was read at. Finding the submission already at status is success
// (a repeated projection); any other change is ErrConflict.
func (s *Store) Transition(
	ctx context.Context, entry Entry, status Status, iteration int,
) (Entry, error) {
	if s.kv == nil {
		panic("inputs.Transition: store must be open")
	}
	if entry.Revision == 0 || entry.RequestID == "" {
		panic("inputs.Transition: entry must have been read from the store")
	}
	updated := entry.Submission
	updated.Status, updated.UpdatedAt, updated.Iteration = status, time.Now().UTC(), iteration
	data, err := json.Marshal(updated)
	if err != nil {
		return Entry{}, fmt.Errorf("inputs: marshal: %w", err)
	}
	key := natsutil.SubmissionKey(entry.RunID, entry.StepID, entry.RequestID)
	rev, err := s.kv.Update(ctx, key, data, entry.Revision)
	if err == nil {
		return Entry{Submission: updated, Revision: rev}, nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) && !isWrongLastSequence(err) {
		return Entry{}, fmt.Errorf("inputs: update %s: %w", key, err)
	}
	current, err := s.Get(ctx, entry.RunID, entry.StepID, entry.RequestID)
	if err != nil {
		return Entry{}, err
	}
	if current.Status == status {
		return current, nil
	}
	return Entry{}, ErrConflict
}

// Claim is the strict form of Transition the placer uses: queued (or a
// dead claim's placed, see worker.PlaceInputs) to placed, succeeding
// only when the submission is exactly as entry read it. Any change
// since, a withdrawal included, is ErrConflict (ErrNotFound when the
// key is gone), never "already placed": two claimers must not both
// believe they own the input.
func (s *Store) Claim(ctx context.Context, entry Entry, iteration int) (Entry, error) {
	if s.kv == nil {
		panic("inputs.Claim: store must be open")
	}
	if entry.Revision == 0 || entry.RequestID == "" {
		panic("inputs.Claim: entry must have been read from the store")
	}
	claimed := entry.Submission
	claimed.Status, claimed.UpdatedAt, claimed.Iteration = StatusPlaced, time.Now().UTC(), iteration
	data, err := json.Marshal(claimed)
	if err != nil {
		return Entry{}, fmt.Errorf("inputs: marshal: %w", err)
	}
	key := natsutil.SubmissionKey(entry.RunID, entry.StepID, entry.RequestID)
	rev, err := s.kv.Update(ctx, key, data, entry.Revision)
	if err == nil {
		return Entry{Submission: claimed, Revision: rev}, nil
	}
	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSequence(err) {
		if _, getErr := s.Get(ctx, entry.RunID, entry.StepID, entry.RequestID); getErr != nil {
			return Entry{}, getErr
		}
		return Entry{}, ErrConflict
	}
	return Entry{}, fmt.Errorf("inputs: claim %s: %w", key, err)
}

// Project writes status unconditionally: the journal has recorded the
// placement, so whatever the bucket says (a withdrawal that raced the
// placement included) is overwritten.
func (s *Store) Project(
	ctx context.Context, sub Submission, status Status, iteration int,
) error {
	if s.kv == nil {
		panic("inputs.Project: store must be open")
	}
	if sub.RequestID == "" {
		panic("inputs.Project: submission must name its request")
	}
	sub.Status, sub.UpdatedAt, sub.Iteration = status, time.Now().UTC(), iteration
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = sub.UpdatedAt
	}
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("inputs: marshal: %w", err)
	}
	key := natsutil.SubmissionKey(sub.RunID, sub.StepID, sub.RequestID)
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		return fmt.Errorf("inputs: project %s: %w", key, err)
	}
	return nil
}

// List returns a step's live submissions in creation order. Keys come
// from the bucket's stream, never ListKeys, which can drop a key written
// while it is enumerating (natsutil.ListKeysUnder); a key whose last
// operation is a delete (withdrawn) is skipped.
func (s *Store) List(ctx context.Context, runID, stepID string) ([]Entry, error) {
	if s.stream == nil {
		panic("inputs.List: store must be open")
	}
	if runID == "" || stepID == "" {
		panic("inputs.List: run and step must not be empty")
	}
	prefix := natsutil.SubmissionKeyPrefix(runID, stepID)
	keys, err := natsutil.ListKeysUnder(ctx, s.stream, natsutil.SubmissionsBucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("inputs: list %s: %w", prefix, err)
	}
	if len(keys) > perStepMax {
		return nil, fmt.Errorf("inputs: step %s has more than %d submissions", prefix, perStepMax)
	}
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		// Through the leader, like the keys: a direct get that a lagging
		// replica answers can miss a submission that was just created,
		// and the engine's check on entering a wait must not (ADR-027,
		// no lost wakeup). The leader also tells a withdrawal (a delete
		// marker, skipped) from a value it has not seen yet.
		leaderEntry, err := natsutil.KVGetLeader(ctx, s.nc, natsutil.SubmissionsBucket, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inputs: get %s: %w", key, err)
		}
		var entry Entry
		if err := json.Unmarshal(leaderEntry.Value, &entry.Submission); err != nil {
			return nil, fmt.Errorf("inputs: decode %s: %w", key, err)
		}
		entry.Revision = leaderEntry.Revision
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.Before(entries[j].CreatedAt)
		}
		return entries[i].RequestID < entries[j].RequestID
	})
	return entries, nil
}

// Queued returns the step's queued submissions in creation order.
func (s *Store) Queued(ctx context.Context, runID, stepID string) ([]Entry, error) {
	if runID == "" || stepID == "" {
		panic("inputs.Queued: run and step must not be empty")
	}
	all, err := s.List(ctx, runID, stepID)
	if err != nil {
		return nil, err
	}
	queued := all[:0]
	for _, entry := range all {
		if entry.Status == StatusQueued {
			queued = append(queued, entry)
		}
	}
	if len(queued) > len(all) {
		panic("inputs.Queued: filtered more than listed")
	}
	return queued, nil
}

// SettleUnanswered is the completion sweep: every queued submission of
// the step becomes unanswered, and so does every placed one that
// recorded reports no input.placed record for (a claim whose writer
// was fenced or died before appending; nothing will reclaim it once
// the step has finished). nil recorded settles queued ones only.
// Returns how many it settled.
func (s *Store) SettleUnanswered(
	ctx context.Context, runID, stepID string, recorded func(requestID string) bool,
) (int, error) {
	if runID == "" || stepID == "" {
		panic("inputs.SettleUnanswered: run and step must not be empty")
	}
	all, err := s.List(ctx, runID, stepID)
	if err != nil {
		return 0, err
	}
	queued := all[:0]
	for _, entry := range all {
		unrecorded := entry.Status == StatusPlaced && recorded != nil && !recorded(entry.RequestID)
		if entry.Status == StatusQueued || unrecorded {
			queued = append(queued, entry)
		}
	}
	settled := 0
	for _, entry := range queued {
		_, err := s.Transition(ctx, entry, StatusUnanswered, 0)
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
			continue // placed or withdrawn meanwhile: not ours to settle
		}
		if err != nil {
			return settled, err
		}
		settled++
	}
	if settled > len(queued) {
		panic("inputs.SettleUnanswered: settled more than queued")
	}
	return settled, nil
}

// isWrongLastSequence reports the server's compare-and-swap rejection.
func isWrongLastSequence(err error) bool {
	if err == nil {
		panic("isWrongLastSequence: err must not be nil")
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
