// api/natsapi_lease_forward.go
// Wire adapter for api.leases.forward (ADR-024, "Forwarding to the
// owner"): decode, run lease.Store.Forward off the subscription
// goroutine, reply. Lookup, fencing and the retry rule live in
// internal/lease.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/danmestas/dagnats/internal/lease"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Forward-only error kinds. Only owner_timeout (and internal) can mean
// the owner processed the request; no_owner and owner_unavailable
// guarantee it did not.
const (
	leaseKindNoOwner          = "no_owner"
	leaseKindOwnerUnavailable = "owner_unavailable"
	leaseKindOwnerTimeout     = "owner_timeout"
)

// forwardInFlightMax bounds concurrent forwards per dagnats instance.
// A micro endpoint runs its handler on one goroutine, so a forward
// waiting on a slow owner would otherwise stall every other forward
// behind it; past this bound the caller gets an explicit error instead.
const forwardInFlightMax = 256

// forwardRequest is the api.leases.forward body. Payload is base64 in
// JSON, so any bytes travel intact.
type forwardRequest struct {
	Key       string `json:"key"`
	Payload   []byte `json:"payload"`
	TimeoutMs int64  `json:"timeout_ms"`
}

// forwardReply is the api.leases.forward reply. On error, Holder and
// Revision name the owner last tried, when there was one.
type forwardReply struct {
	Key      string `json:"key,omitempty"`
	Holder   string `json:"holder,omitempty"`
	Revision uint64 `json:"revision,omitempty"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
	Kind     string `json:"kind,omitempty"`
}

// leaseForwardHandler returns the api.leases.forward handler. Each
// forward runs on its own goroutine, at most forwardInFlightMax at once,
// and each is bounded by its own timeout_ms (<= lease.ForwardTimeoutMax).
func (n *NATSAPI) leaseForwardHandler() micro.HandlerFunc {
	if n.leases == nil {
		panic("leaseForwardHandler: lease store must not be nil")
	}
	slots := make(chan struct{}, forwardInFlightMax)
	if cap(slots) != forwardInFlightMax {
		panic("leaseForwardHandler: slot pool must hold the in-flight bound")
	}
	return func(req micro.Request) {
		var r forwardRequest
		if err := json.Unmarshal(req.Data(), &r); err != nil {
			n.reply(req, forwardReply{Error: err.Error(), Kind: leaseKindTransport})
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			n.reply(req, forwardReply{
				Key: r.Key, Kind: leaseKindInternal,
				Error: "forward capacity exhausted; retry shortly",
			})
			return
		}
		base := observe.ExtractTraceContextHeader(nats.Header(req.Headers()))
		go func() {
			defer func() { <-slots }()
			n.reply(req, n.forwardLease(base, r))
		}()
	}
}

func (n *NATSAPI) forwardLease(ctx context.Context, r forwardRequest) forwardReply {
	if ctx == nil {
		panic("forwardLease: ctx must not be nil")
	}
	if n.leases == nil {
		panic("forwardLease: lease store must not be nil")
	}
	got, err := n.leases.Forward(
		ctx, r.Key, r.Payload, time.Duration(r.TimeoutMs)*time.Millisecond,
	)
	reply := forwardReply{
		Key: got.Key, Holder: got.Holder, Revision: got.Revision, Payload: got.Payload,
	}
	if err != nil {
		reply.Payload = nil
		reply.Error, reply.Kind = err.Error(), forwardErrorKind(err)
	}
	return reply
}

// forwardErrorKind maps a Forward error onto the wire kind.
func forwardErrorKind(err error) string {
	if err == nil {
		panic("forwardErrorKind: err must not be nil")
	}
	switch {
	case errors.Is(err, lease.ErrNoOwner):
		return leaseKindNoOwner
	case errors.Is(err, lease.ErrOwnerUnavailable):
		return leaseKindOwnerUnavailable
	case errors.Is(err, lease.ErrOwnerTimeout):
		return leaseKindOwnerTimeout
	}
	kind := leaseErrorReply(err).Kind
	if kind == "" {
		panic("forwardErrorKind: kind must be set")
	}
	return kind
}
