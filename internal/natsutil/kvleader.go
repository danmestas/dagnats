package natsutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// kvLeaderReadTimeout bounds a leader read whose ctx carries no
// deadline, matching jetstream's own default API timeout.
const kvLeaderReadTimeout = 5 * time.Second

// kvOperationHeader marks a KV delete or purge marker. jetstream keeps
// its constant unexported.
const kvOperationHeader = "KV-Operation"

// KVLeaderEntry is the latest live value of a KV key, read from the
// bucket's stream leader.
type KVLeaderEntry struct {
	Value    []byte
	Revision uint64
	Created  time.Time
}

// StoredMsg is the message half of a STREAM.MSG.GET reply. []byte
// fields arrive base64-encoded, which encoding/json decodes.
type StoredMsg struct {
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs"`
	Data     []byte    `json:"data"`
	Time     time.Time `json:"time"`
}

// MsgGetReply is the STREAM.MSG.GET reply envelope. Shared with the
// lease store, which reads through the stream leader for the same
// reason KVGetLeader does.
type MsgGetReply struct {
	Error *struct {
		ErrCode     int    `json:"err_code"`
		Description string `json:"description"`
	} `json:"error"`
	Message *StoredMsg `json:"message"`
}

// KVGetLeader reads key's latest value in bucket through the stream
// LEADER. jetstream's KeyValue.Get uses a direct get (KV buckets set
// AllowDirect), which any replica may answer, so on a replicated
// bucket it can return a value older than a write that was already
// acknowledged. A read-modify-write built on that stale read silently
// erases the newer write. The leader has applied every acknowledged
// write, so this read cannot go backwards.
//
// Returns jetstream.ErrKeyNotFound when the key was never written or
// its latest entry is a delete/purge marker, as KeyValue.Get does.
func KVGetLeader(
	ctx context.Context, nc *nats.Conn, bucket, key string,
) (KVLeaderEntry, error) {
	if nc == nil {
		panic("KVGetLeader: nc must not be nil")
	}
	if bucket == "" {
		panic("KVGetLeader: bucket must not be empty")
	}
	entry, _, err := KVGetLeaderOrMarker(ctx, nc, bucket, key)
	return entry, err
}

// KVGetLeaderOrMarker is KVGetLeader that also reports whether a missing
// key was deleted or purged (deleted=true) rather than never written: a
// reader that falls back to another record of the key's existence must
// not resurrect a key that was removed on purpose. A key jetstream would
// refuse returns jetstream.ErrInvalidKey: as a last_by_subj subject a
// wildcard key would match, and return, another key's value.
func KVGetLeaderOrMarker(
	ctx context.Context, nc *nats.Conn, bucket, key string,
) (entry KVLeaderEntry, deleted bool, err error) {
	if nc == nil {
		panic("KVGetLeaderOrMarker: nc must not be nil")
	}
	if bucket == "" || key == "" {
		panic("KVGetLeaderOrMarker: bucket and key must not be empty")
	}
	if !ValidKVKey(key) {
		return KVLeaderEntry{}, false, jetstream.ErrInvalidKey
	}
	msg, err := StreamMsgLeader(ctx, nc, "KV_"+bucket,
		map[string]any{"last_by_subj": "$KV." + bucket + "." + key})
	if err != nil {
		return KVLeaderEntry{}, false, fmt.Errorf("kv %s/%s: %w", bucket, key, err)
	}
	if msg == nil {
		return KVLeaderEntry{}, false, jetstream.ErrKeyNotFound
	}
	if len(msg.Header) > 0 {
		header, err := nats.DecodeHeadersMsg(msg.Header)
		if err != nil {
			return KVLeaderEntry{}, false,
				fmt.Errorf("kv %s/%s: decode headers: %w", bucket, key, err)
		}
		if header.Get(kvOperationHeader) != "" {
			return KVLeaderEntry{}, true, jetstream.ErrKeyNotFound
		}
	}
	return KVLeaderEntry{
		Value: msg.Data, Revision: msg.Sequence, Created: msg.Time,
	}, false, nil
}

// LeaderReadKV returns kv with Get answered by the bucket's stream
// leader (KVGetLeader); every other method is kv's own. A direct get
// can land on a follower that has not applied a write the writer
// already saw acknowledged, so a workflow registered through one node
// and started at once could be "not found" (measured on an R3 bucket:
// up to 11 misses per 1000 register-then-read pairs). Bind buckets
// whose readers act on a write they just made, or one another client
// just made, through this.
func LeaderReadKV(nc *nats.Conn, kv jetstream.KeyValue) jetstream.KeyValue {
	if nc == nil {
		panic("LeaderReadKV: nc must not be nil")
	}
	if kv == nil {
		panic("LeaderReadKV: kv must not be nil")
	}
	return &leaderReadKV{KeyValue: kv, nc: nc}
}

type leaderReadKV struct {
	jetstream.KeyValue
	nc *nats.Conn
}

// Get is KeyValue.Get read through the stream leader. Delta is 0: the
// leader read does not report how far the entry is behind the tail.
func (kv *leaderReadKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if kv.nc == nil {
		panic("leaderReadKV.Get: nc must not be nil")
	}
	if !ValidKVKey(key) {
		return nil, jetstream.ErrInvalidKey
	}
	bucket := kv.Bucket()
	got, err := KVGetLeader(ctx, kv.nc, bucket, key)
	if err != nil {
		return nil, err
	}
	return leaderEntry{bucket: bucket, key: key, entry: got}, nil
}

// ValidKVKey is exactly jetstream's key rule (kv.go keyValid, which
// is unexported): letters, digits and -/_=., no leading or trailing
// dot, and no empty token (".."). It keeps a wildcard out of the
// last_by_subj request, where it would match another key, and tells
// callers which keys every KV call would refuse.
func ValidKVKey(key string) bool {
	if key == "" || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	if strings.Contains(key, "..") {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '/', c == '_', c == '=', c == '.':
		default:
			return false
		}
	}
	return true
}

// leaderEntry is a KVLeaderEntry as a jetstream.KeyValueEntry.
type leaderEntry struct {
	bucket, key string
	entry       KVLeaderEntry
}

func (e leaderEntry) Bucket() string                  { return e.bucket }
func (e leaderEntry) Key() string                     { return e.key }
func (e leaderEntry) Value() []byte                   { return e.entry.Value }
func (e leaderEntry) Revision() uint64                { return e.entry.Revision }
func (e leaderEntry) Created() time.Time              { return e.entry.Created }
func (e leaderEntry) Delta() uint64                   { return 0 }
func (e leaderEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }
