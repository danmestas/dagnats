package lease

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Owner-inbox convention (ADR-024, "Forwarding to the owner"). A holder
// that accepts forwarded requests subscribes on
// dagnats.lease.inbox.<holder>.> ; a request for key K arrives on
// dagnats.lease.inbox.<holder>.<K> carrying the fence in FenceHeader.
// The owner refuses a request by replying with RejectHeader set.
const (
	InboxSubjectPrefix = "dagnats.lease.inbox"
	FenceHeader        = "Dagnats-Lease-Fence"
	RejectHeader       = "Dagnats-Lease-Reject"
	RejectStale        = "stale"
)

// Bounds on a forward's total time budget (every lookup and delivery).
const (
	ForwardTimeoutMin = 100 * time.Millisecond
	ForwardTimeoutMax = 30 * time.Second
	// forwardAttemptsMax bounds lookup+delivery rounds. A round is only
	// repeated after an outcome that proves the owner did NOT process
	// the request, and only when the lease has moved since; one handoff
	// in flight needs two rounds, the third is slack for a second move.
	forwardAttemptsMax = 3
)

// ErrNoOwner means nobody holds the key: the request was not delivered.
var ErrNoOwner = errors.New("lease: no owner")

// ErrOwnerUnavailable means the holder named by the lease did not take
// the request -- no subscriber on its inbox, or it refused the fence --
// and the lease did not move to anyone who would. Not delivered.
var ErrOwnerUnavailable = errors.New("lease: owner unavailable")

// ErrOwnerTimeout means the owner did not reply within the budget. The
// request MAY have been processed; retry only if it is idempotent.
var ErrOwnerTimeout = errors.New("lease: owner timeout")

// errOwnerRefused marks an attempt the owner provably did not process.
var errOwnerRefused = errors.New("lease: owner refused")

// Forwarded is the owner's reply and the lease it was delivered under.
type Forwarded struct {
	Key      string
	Holder   string
	Revision uint64
	Payload  []byte
}

// InboxSubject is the subject holder's inbox receives key's requests on.
func InboxSubject(holder, key string) string {
	if holder == "" || key == "" {
		panic("InboxSubject: holder and key must not be empty")
	}
	// A dot or wildcard would make the subject address other tokens.
	if strings.ContainsAny(holder, ".*> ") || strings.ContainsAny(key, ".*> ") {
		panic("InboxSubject: holder and key must each be one subject token")
	}
	return InboxSubjectPrefix + "." + holder + "." + key
}

// Forward delivers payload to whoever holds key, stamped with the lease
// revision as the fence, and returns the owner's reply. timeout bounds
// the whole call. When the owner provably did not process the request
// and the lease has since moved, Forward re-reads and tries the new
// owner; it never re-sends after an outcome that may have been processed.
// A failed call's Forwarded still names the holder it last tried.
func (s *Store) Forward(
	ctx context.Context, key string, payload []byte, timeout time.Duration,
) (Forwarded, error) {
	if ctx == nil {
		panic("Store.Forward: ctx must not be nil")
	}
	if s == nil || s.tp == nil {
		panic("Store.Forward: store must be built by NewStore")
	}
	if err := validateToken("key", key, KeyLengthMax); err != nil {
		return Forwarded{}, err
	}
	if err := validateForwardTimeout(timeout); err != nil {
		return Forwarded{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tried := Forwarded{Key: key}
	for attempt := 0; attempt < forwardAttemptsMax; attempt++ {
		cur, err := s.read(ctx, key)
		if err != nil {
			return tried, err
		}
		if !cur.live {
			return Forwarded{Key: key}, fmt.Errorf("%w: key %q is not held", ErrNoOwner, key)
		}
		// The record is data: a corrupt or foreign write must fail the
		// call, not reach InboxSubject's assertion.
		if err := validateToken("stored holder", cur.lease.Holder, HolderLengthMax); err != nil {
			// %v, not %w: this is corrupt state (internal), not bad input.
			return tried, fmt.Errorf("lease %q: corrupt record at revision %d: %v",
				key, cur.lease.Revision, err)
		}
		moved := cur.lease.Holder != tried.Holder || cur.lease.Revision != tried.Revision
		if attempt > 0 && !moved {
			break
		}
		tried = Forwarded{Key: key, Holder: cur.lease.Holder, Revision: cur.lease.Revision}
		reply, err := s.deliver(ctx, cur.lease, payload)
		if err == nil {
			tried.Payload = reply
			return tried, nil
		}
		if !errors.Is(err, errOwnerRefused) {
			return tried, err
		}
	}
	return tried, fmt.Errorf("%w: %q did not take the request for key %q at revision %d",
		ErrOwnerUnavailable, tried.Holder, key, tried.Revision)
}

// deliver sends one request to l's holder. errOwnerRefused means the
// owner provably did not process it (no inbox subscriber, or a reject).
func (s *Store) deliver(ctx context.Context, l Lease, payload []byte) ([]byte, error) {
	if l.Holder == "" || l.Revision == 0 {
		panic("deliver: lease must name a holder and revision")
	}
	if s.tp == nil {
		panic("deliver: publisher must not be nil")
	}
	msg := nats.NewMsg(InboxSubject(l.Holder, l.Key))
	msg.Header.Set(FenceHeader, strconv.FormatUint(l.Revision, 10))
	msg.Data = payload
	resp, err := s.tp.NC().RequestMsgWithContext(ctx, msg)
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return nil, errOwnerRefused
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout):
		return nil, fmt.Errorf("%w: %q did not reply for key %q",
			ErrOwnerTimeout, l.Holder, l.Key)
	case err != nil:
		return nil, fmt.Errorf("lease %q: forward to %q: %w", l.Key, l.Holder, err)
	}
	if resp.Header.Get(RejectHeader) != "" {
		return nil, errOwnerRefused
	}
	return resp.Data, nil
}

// validateForwardTimeout checks timeout is within the forward bounds.
func validateForwardTimeout(timeout time.Duration) error {
	if ForwardTimeoutMin <= 0 || ForwardTimeoutMin > ForwardTimeoutMax {
		panic("validateForwardTimeout: bounds out of order")
	}
	if timeout >= ForwardTimeoutMin && timeout <= ForwardTimeoutMax {
		return nil
	}
	return fmt.Errorf("%w: timeout_ms must be within [%d, %d], got %d",
		ErrInvalid, ForwardTimeoutMin.Milliseconds(),
		ForwardTimeoutMax.Milliseconds(), timeout.Milliseconds())
}
