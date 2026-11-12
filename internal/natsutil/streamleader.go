package natsutil

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamMsgLeader reads one message of stream through the stream
// LEADER, for the reason KVGetLeader gives: a direct get may be answered
// by a replica that has not applied a write already acknowledged to its
// publisher. request is a STREAM.MSG.GET body, e.g. {"last_by_subj": s}
// or {"seq": n, "next_by_subj": s}. Returns nil, nil when no message
// matches; a missing stream is an error.
func StreamMsgLeader(
	ctx context.Context, nc *nats.Conn, stream string, request map[string]any,
) (*StoredMsg, error) {
	if nc == nil {
		panic("StreamMsgLeader: nc must not be nil")
	}
	if stream == "" || len(request) == 0 {
		panic("StreamMsgLeader: stream and request must not be empty")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, kvLeaderReadTimeout)
		defer cancel()
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("stream %s: encode read: %w", stream, err)
	}
	// The default "$JS.API" prefix, hardcoded like the lease store's
	// read: dagnats never runs JetStream under a domain or a custom API
	// prefix. Supporting one means threading it through both readers.
	resp, err := nc.RequestWithContext(ctx, "$JS.API.STREAM.MSG.GET."+stream, body)
	if err != nil {
		return nil, fmt.Errorf("stream %s: read: %w", stream, err)
	}
	var reply MsgGetReply
	if err := json.Unmarshal(resp.Data, &reply); err != nil {
		return nil, fmt.Errorf("stream %s: decode read: %w", stream, err)
	}
	if reply.Error != nil {
		if reply.Error.ErrCode == int(jetstream.JSErrCodeMessageNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("stream %s: read: %s", stream, reply.Error.Description)
	}
	if reply.Message == nil || reply.Message.Sequence == 0 {
		return nil, fmt.Errorf("stream %s: read: empty reply", stream)
	}
	return reply.Message, nil
}
