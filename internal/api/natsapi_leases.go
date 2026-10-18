// api/natsapi_leases.go
// Wire adapter for coordination leases (ADR-024): four request/reply
// subjects on the dagnats-api micro service that translate JSON to
// lease.Store calls. All lease semantics live in internal/lease.
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

// leaseQueueGroup makes each lease request land on exactly ONE dagnats
// instance. The other control-plane endpoints fan out to every instance;
// for a lease that would be wrong rather than wasteful -- N instances
// racing the same acquire would hand the caller whichever reply arrives
// first, not necessarily the one that reflects the grant.
const leaseQueueGroup = "dagnats-leases"

// leaseRequestTimeout bounds one lease call's NATS round trips (a read
// and at most a few CAS writes).
const leaseRequestTimeout = 5 * time.Second

// Error kinds in the {error, kind} reply. "lost" is the one a holder
// must act on: stop writing as owner now.
const (
	leaseKindInvalid     = "invalid"
	leaseKindLost        = "lost"
	leaseKindTransport   = "transport"
	leaseKindInternal    = "internal"
	leaseKindUnsupported = "unsupported"
)

// endpointSpec is one micro endpoint. queueGroup "" means every
// instance receives each request (the pre-micro behavior).
type endpointSpec struct {
	name       string
	subject    string
	handler    micro.HandlerFunc
	queueGroup string
}

// leaseRequest is the body of every api.leases.* request; each subject
// reads the fields it needs.
type leaseRequest struct {
	Key      string `json:"key"`
	Holder   string `json:"holder"`
	Revision uint64 `json:"revision"`
	TTLMs    int64  `json:"ttl_ms"`
}

// leaseReply is the body of every api.leases.* reply.
type leaseReply struct {
	Granted    bool       `json:"granted"`
	Held       bool       `json:"held"`
	Key        string     `json:"key,omitempty"`
	Holder     string     `json:"holder,omitempty"`
	Revision   uint64     `json:"revision,omitempty"`
	AcquiredAt *time.Time `json:"acquired_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	TTLMs      int64      `json:"ttl_ms,omitempty"`
	Error      string     `json:"error,omitempty"`
	Kind       string     `json:"kind,omitempty"`
}

// leaseEndpoints lists the lease subjects for Start.
func (n *NATSAPI) leaseEndpoints() []endpointSpec {
	if n.leases == nil {
		panic("leaseEndpoints: lease store must not be nil")
	}
	specs := []endpointSpec{
		{"leases-acquire", "api.leases.acquire",
			n.leaseHandler(n.acquireLease), leaseQueueGroup},
		{"leases-renew", "api.leases.renew",
			n.leaseHandler(n.renewLease), leaseQueueGroup},
		{"leases-release", "api.leases.release",
			n.leaseHandler(n.releaseLease), leaseQueueGroup},
		{"leases-owner", "api.leases.owner",
			n.leaseHandler(n.leaseOwner), leaseQueueGroup},
		{"leases-forward", "api.leases.forward",
			n.leaseForwardHandler(), leaseQueueGroup},
	}
	if len(specs) != 5 {
		panic("leaseEndpoints: expected five lease subjects")
	}
	return specs
}

// leaseHandler adapts one lease operation to a micro handler: decode,
// bound the call, run it, reply.
func (n *NATSAPI) leaseHandler(
	op func(context.Context, leaseRequest) leaseReply,
) micro.HandlerFunc {
	if op == nil {
		panic("leaseHandler: op must not be nil")
	}
	if n.leases == nil {
		panic("leaseHandler: lease store must not be nil")
	}
	return func(req micro.Request) {
		var r leaseRequest
		if err := json.Unmarshal(req.Data(), &r); err != nil {
			n.reply(req, leaseReply{
				Error: err.Error(), Kind: leaseKindTransport,
			})
			return
		}
		ctx, cancel := context.WithTimeout(
			observe.ExtractTraceContextHeader(nats.Header(req.Headers())),
			leaseRequestTimeout,
		)
		defer cancel()
		n.reply(req, op(ctx, r))
	}
}

func (n *NATSAPI) acquireLease(ctx context.Context, r leaseRequest) leaseReply {
	if ctx == nil {
		panic("acquireLease: ctx must not be nil")
	}
	if n.leases == nil {
		panic("acquireLease: lease store must not be nil")
	}
	l, granted, err := n.leases.Acquire(
		ctx, r.Key, r.Holder, time.Duration(r.TTLMs)*time.Millisecond,
	)
	if err != nil {
		return leaseErrorReply(err)
	}
	reply := leaseStateReply(l, true)
	reply.Granted = granted
	return reply
}

func (n *NATSAPI) renewLease(ctx context.Context, r leaseRequest) leaseReply {
	if ctx == nil {
		panic("renewLease: ctx must not be nil")
	}
	if n.leases == nil {
		panic("renewLease: lease store must not be nil")
	}
	l, err := n.leases.Renew(
		ctx, r.Key, r.Holder, r.Revision,
		time.Duration(r.TTLMs)*time.Millisecond,
	)
	if err != nil {
		return leaseErrorReply(err)
	}
	reply := leaseStateReply(l, true)
	reply.Granted = true
	return reply
}

func (n *NATSAPI) releaseLease(ctx context.Context, r leaseRequest) leaseReply {
	if ctx == nil {
		panic("releaseLease: ctx must not be nil")
	}
	if n.leases == nil {
		panic("releaseLease: lease store must not be nil")
	}
	if err := n.leases.Release(ctx, r.Key, r.Holder, r.Revision); err != nil {
		return leaseErrorReply(err)
	}
	return leaseReply{Key: r.Key}
}

func (n *NATSAPI) leaseOwner(ctx context.Context, r leaseRequest) leaseReply {
	if ctx == nil {
		panic("leaseOwner: ctx must not be nil")
	}
	if n.leases == nil {
		panic("leaseOwner: lease store must not be nil")
	}
	l, held, err := n.leases.Owner(ctx, r.Key)
	if err != nil {
		return leaseErrorReply(err)
	}
	if !held {
		return leaseReply{Key: r.Key}
	}
	return leaseStateReply(l, true)
}

// leaseStateReply renders a held lease. Times are UTC so every replica
// renders the same instant the same way.
func leaseStateReply(l lease.Lease, held bool) leaseReply {
	if l.Key == "" {
		panic("leaseStateReply: lease must carry its key")
	}
	if held && l.Revision == 0 {
		panic("leaseStateReply: a held lease must carry a revision")
	}
	acquired, expires := l.AcquiredAt.UTC(), l.ExpiresAt.UTC()
	return leaseReply{
		Held: held, Key: l.Key, Holder: l.Holder, Revision: l.Revision,
		AcquiredAt: &acquired, ExpiresAt: &expires,
		TTLMs: l.TTL.Milliseconds(),
	}
}

// leaseErrorReply maps a Store error onto the {error, kind} envelope.
func leaseErrorReply(err error) leaseReply {
	if err == nil {
		panic("leaseErrorReply: err must not be nil")
	}
	kind := leaseKindInternal
	switch {
	case errors.Is(err, lease.ErrInvalid):
		kind = leaseKindInvalid
	case errors.Is(err, lease.ErrLost):
		kind = leaseKindLost
	case errors.Is(err, lease.ErrUnsupported):
		kind = leaseKindUnsupported
	}
	if kind == "" {
		panic("leaseErrorReply: kind must be set")
	}
	return leaseReply{Error: err.Error(), Kind: kind}
}
