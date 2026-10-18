package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// current is what the last message stored for a key says. revision is
// that message's sequence -- the value a CAS write must expect -- and is
// 0 when the key has no message at all.
type current struct {
	lease    Lease
	live     bool
	revision uint64
}

// leaseStream resolves the bucket's stream once and caches it. A failure
// is not cached, so a bucket provisioned after startup is picked up.
func (s *Store) leaseStream(ctx context.Context) (jetstream.Stream, error) {
	if ctx == nil {
		panic("leaseStream: ctx must not be nil")
	}
	if s.tp == nil {
		panic("leaseStream: publisher must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream != nil {
		return s.stream, nil
	}
	stream, err := s.tp.JS().Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("leases bucket not provisioned: %w", err)
	}
	s.stream = stream
	return stream, nil
}

// read loads the last message stored for key. Raw stream access (not
// kv.Get) because a CAS after a release or expiry must expect the delete
// marker's sequence, which kv.Get hides.
func (s *Store) read(ctx context.Context, key string) (current, error) {
	if key == "" {
		panic("read: key must not be empty")
	}
	stream, err := s.leaseStream(ctx)
	if err != nil {
		return current{}, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subjectPrefix+key)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return current{}, nil
	}
	if err != nil {
		return current{}, fmt.Errorf("lease %q: read: %w", key, err)
	}
	cur := current{revision: msg.Sequence}
	if msg.Header.Get(kvOperationHeader) != "" ||
		msg.Header.Get(MarkerReasonHeader) != "" {
		return cur, nil
	}
	var rec record
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		return current{}, fmt.Errorf(
			"lease %q: corrupt record at revision %d: %w",
			key, msg.Sequence, err,
		)
	}
	ttl := time.Duration(rec.TTLMs) * time.Millisecond
	cur.live = true
	cur.lease = Lease{
		Key: key, Holder: rec.Holder, Revision: msg.Sequence,
		AcquiredAt: rec.AcquiredAt, ExpiresAt: msg.Time.Add(ttl), TTL: ttl,
	}
	return cur, nil
}

// create writes a fresh grant, expecting the key's last sequence to be
// expect (0 for a never-written key, else the marker left by the last
// release or expiry).
func (s *Store) create(
	ctx context.Context, key, holder string, ttl time.Duration,
	expect uint64,
) (Lease, error) {
	if holder == "" {
		panic("create: holder must not be empty")
	}
	if ttl < TTLMin {
		panic("create: ttl must be validated before create")
	}
	now := time.Now().UTC()
	rec := record{Holder: holder, AcquiredAt: now, TTLMs: ttl.Milliseconds()}
	revision, err := s.write(ctx, key, rec, EventAcquired, expect)
	if err != nil {
		return Lease{}, err
	}
	return Lease{
		Key: key, Holder: holder, Revision: revision,
		AcquiredAt: now, ExpiresAt: now.Add(ttl), TTL: ttl,
	}, nil
}

// write stores rec under key with a per-message TTL, as a CAS against
// expect. event stamps the change event the bucket republishes.
func (s *Store) write(
	ctx context.Context, key string, rec record, event string,
	expect uint64,
) (uint64, error) {
	if event != EventAcquired && event != EventRenewed {
		panic("write: event must be acquired or renewed, got " + event)
	}
	if rec.TTLMs < TTLMin.Milliseconds() {
		panic("write: record TTL must be validated before write")
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("lease %q: encode: %w", key, err)
	}
	msg := &nats.Msg{
		Subject: subjectPrefix + key, Header: nats.Header{}, Data: data,
	}
	msg.Header.Set(EventHeader, event)
	ack, err := s.tp.JSPublishMsg(
		ctx, msg,
		jetstream.WithExpectLastSequencePerSubject(expect),
		jetstream.WithMsgTTL(time.Duration(rec.TTLMs)*time.Millisecond),
	)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// purge replaces key's lease with a KV purge marker (CAS against
// expect). The marker itself expires after natsutil.LeaseMarkerTTL so
// released keys do not accumulate.
func (s *Store) purge(ctx context.Context, key string, expect uint64) error {
	if key == "" {
		panic("purge: key must not be empty")
	}
	if expect == 0 {
		panic("purge: expect must be a real revision")
	}
	msg := nats.NewMsg(subjectPrefix + key)
	msg.Header.Set(kvOperationHeader, "PURGE")
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	msg.Header.Set(EventHeader, EventReleased)
	_, err := s.tp.JSPublishMsg(
		ctx, msg,
		jetstream.WithExpectLastSequencePerSubject(expect),
		jetstream.WithMsgTTL(natsutil.LeaseMarkerTTL),
	)
	return err
}

// checkHolder reports ErrLost unless cur is a live lease held by holder.
// The revision half of the fence is not checked here: the write that
// follows is a CAS against the caller's revision, and the server is the
// only authority on whether it is still current.
func checkHolder(key string, cur current, holder string) error {
	if key == "" || holder == "" {
		panic("checkHolder: key and holder must not be empty")
	}
	if cur.live && cur.lease.Revision == 0 {
		panic("checkHolder: a live lease must carry a revision")
	}
	if !cur.live {
		return fmt.Errorf("%w: %q is not held", ErrLost, key)
	}
	if cur.lease.Holder != holder {
		return fmt.Errorf("%w: %q is held by %q", ErrLost, key, cur.lease.Holder)
	}
	return nil
}

// lostRevision is the ErrLost a fenced write reports when its CAS fails.
func lostRevision(key string, revision uint64) error {
	if key == "" {
		panic("lostRevision: key must not be empty")
	}
	if revision == 0 {
		panic("lostRevision: revision must be a real fence")
	}
	return fmt.Errorf("%w: %q is no longer at revision %d", ErrLost, key, revision)
}

// isWrongLastSequence reports a CAS rejection. Replicated (R>1) streams
// report it as 10164 rather than 10071.
func isWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
}
