package natsutil

import (
	"context"
	"encoding/json"
	"fmt"
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
	if bucket == "" || key == "" {
		panic("KVGetLeader: bucket and key must not be empty")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, kvLeaderReadTimeout)
		defer cancel()
	}
	body, err := json.Marshal(map[string]string{
		"last_by_subj": "$KV." + bucket + "." + key,
	})
	if err != nil {
		return KVLeaderEntry{}, fmt.Errorf("kv %s/%s: encode read: %w", bucket, key, err)
	}
	// The default "$JS.API" prefix, hardcoded like the lease store's
	// read: dagnats never runs JetStream under a domain or a custom API
	// prefix. Supporting one means threading it through both readers.
	resp, err := nc.RequestWithContext(
		ctx, "$JS.API.STREAM.MSG.GET.KV_"+bucket, body,
	)
	if err != nil {
		return KVLeaderEntry{}, fmt.Errorf("kv %s/%s: read: %w", bucket, key, err)
	}
	return decodeKVLeaderReply(bucket, key, resp.Data)
}

// decodeKVLeaderReply turns a STREAM.MSG.GET reply into the entry, or
// into ErrKeyNotFound for a missing key or a delete/purge marker.
func decodeKVLeaderReply(bucket, key string, data []byte) (KVLeaderEntry, error) {
	if bucket == "" || key == "" {
		panic("decodeKVLeaderReply: bucket and key must not be empty")
	}
	var reply MsgGetReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return KVLeaderEntry{}, fmt.Errorf("kv %s/%s: decode read: %w", bucket, key, err)
	}
	if reply.Error != nil {
		if reply.Error.ErrCode == int(jetstream.JSErrCodeMessageNotFound) {
			return KVLeaderEntry{}, jetstream.ErrKeyNotFound
		}
		return KVLeaderEntry{}, fmt.Errorf(
			"kv %s/%s: read: %s", bucket, key, reply.Error.Description,
		)
	}
	if reply.Message == nil || reply.Message.Sequence == 0 {
		return KVLeaderEntry{}, fmt.Errorf("kv %s/%s: read: empty reply", bucket, key)
	}
	if len(reply.Message.Header) > 0 {
		header, err := nats.DecodeHeadersMsg(reply.Message.Header)
		if err != nil {
			return KVLeaderEntry{}, fmt.Errorf("kv %s/%s: decode headers: %w", bucket, key, err)
		}
		if header.Get(kvOperationHeader) != "" {
			return KVLeaderEntry{}, jetstream.ErrKeyNotFound
		}
	}
	return KVLeaderEntry{Value: reply.Message.Data, Revision: reply.Message.Sequence}, nil
}
