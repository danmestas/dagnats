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

// storedMsg is the message half of a STREAM.MSG.GET reply. []byte
// fields arrive base64-encoded, which encoding/json decodes.
type storedMsg struct {
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs"`
	Data     []byte    `json:"data"`
	Time     time.Time `json:"time"`
}

// msgGetReply is the STREAM.MSG.GET reply envelope.
type msgGetReply struct {
	Error *struct {
		ErrCode     int    `json:"err_code"`
		Description string `json:"description"`
	} `json:"error"`
	Message *storedMsg `json:"message"`
}

// getLast fetches the last message on key's subject through the stream
// LEADER. jetstream's Stream.GetLastMsgForSubject would use a direct get
// (KV buckets set AllowDirect), which any replica may answer; a lagging
// replica could then hand an old holder its own expired lease back as
// "granted". Returns (nil, nil) when the key has never been written.
func (s *Store) getLast(ctx context.Context, key string) (*storedMsg, error) {
	if key == "" {
		panic("getLast: key must not be empty")
	}
	if s.tp == nil {
		panic("getLast: publisher must not be nil")
	}
	body, err := json.Marshal(map[string]string{"last_by_subj": subjectPrefix + key})
	if err != nil {
		return nil, fmt.Errorf("lease %q: encode read: %w", key, err)
	}
	resp, err := s.tp.NC().RequestWithContext(ctx, msgGetSubject, body)
	if err != nil {
		return nil, fmt.Errorf("lease %q: read: %w", key, err)
	}
	var reply msgGetReply
	if err := json.Unmarshal(resp.Data, &reply); err != nil {
		return nil, fmt.Errorf("lease %q: decode read: %w", key, err)
	}
	if reply.Error != nil {
		switch reply.Error.ErrCode {
		case int(jetstream.JSErrCodeMessageNotFound):
			return nil, nil
		case int(jetstream.JSErrCodeStreamNotFound):
			return nil, ErrUnsupported
		}
		return nil, fmt.Errorf("lease %q: read: %s", key, reply.Error.Description)
	}
	if reply.Message == nil {
		return nil, fmt.Errorf("lease %q: read: empty reply", key)
	}
	return reply.Message, nil
}

// read loads the last message stored for key. Raw stream access (not
// kv.Get) because a CAS after a release or expiry must expect the delete
// marker's sequence, which kv.Get hides.
func (s *Store) read(ctx context.Context, key string) (current, error) {
	if key == "" {
		panic("read: key must not be empty")
	}
	if ctx == nil {
		panic("read: ctx must not be nil")
	}
	msg, err := s.getLast(ctx, key)
	if err != nil || msg == nil {
		return current{}, err
	}
	header := nats.Header{}
	if len(msg.Header) > 0 {
		header, err = nats.DecodeHeadersMsg(msg.Header)
		if err != nil {
			return current{}, fmt.Errorf("lease %q: decode headers: %w", key, err)
		}
	}
	cur := current{revision: msg.Sequence}
	if header.Get(kvOperationHeader) != "" ||
		header.Get(MarkerReasonHeader) != "" {
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
