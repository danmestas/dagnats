package lease

import (
	"context"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName    = "KV_" + natsutil.LeasesBucket
	subjectPrefix = "$KV." + natsutil.LeasesBucket + "."
	// acquireAttemptsMax bounds the read/CAS loop. A CAS can only fail
	// because another write landed in between; the re-read then sees that
	// writer's lease and returns, so two attempts settle every race. The
	// third is slack for a lease that expired between read and write.
	acquireAttemptsMax = 3
	// kvOperationHeader marks a KV delete/purge; kept literal because
	// jetstream does not export it.
	kvOperationHeader = "KV-Operation"
	// msgGetSubject is the leader-answered (non-direct) message get.
	msgGetSubject = "$JS.API.STREAM.MSG.GET." + streamName
)

// Store grants, renews, releases and reads leases. Safe for concurrent
// use. It holds no NATS resources of its own; every read goes to the
// stream leader and every write is a CAS, so a Store is stateless.
type Store struct {
	tp *natsutil.TracingPublisher
}

// NewStore binds a Store to nc. The leases bucket must already exist
// (natsutil.SetupAll provisions it); a missing bucket surfaces as an
// error from the first call, not here.
func NewStore(nc *nats.Conn) *Store {
	if nc == nil {
		panic("lease.NewStore: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("lease.NewStore: jetstream.New: " + err.Error())
	}
	store := &Store{tp: natsutil.NewTracingPublisher(nc, js)}
	if store.tp == nil {
		panic("lease.NewStore: publisher must not be nil")
	}
	return store
}

// Acquire grants key to holder for ttl if nobody holds it. When holder
// already holds key the call is idempotent: it returns the existing
// lease (same revision, TTL not extended -- use Renew to extend) with
// granted=true, so a retry after a lost reply is safe. When someone else
// holds it, granted=false and the returned Lease names the holder.
func (s *Store) Acquire(
	ctx context.Context, key, holder string, ttl time.Duration,
) (Lease, bool, error) {
	if ctx == nil {
		panic("Store.Acquire: ctx must not be nil")
	}
	if s == nil || s.tp == nil {
		panic("Store.Acquire: store must be built by NewStore")
	}
	if err := validateToken("key", key, KeyLengthMax); err != nil {
		return Lease{}, false, err
	}
	if err := validateToken("holder", holder, HolderLengthMax); err != nil {
		return Lease{}, false, err
	}
	if err := validateTTL(ttl); err != nil {
		return Lease{}, false, err
	}
	for attempt := 0; attempt < acquireAttemptsMax; attempt++ {
		cur, err := s.read(ctx, key)
		if err != nil {
			return Lease{}, false, err
		}
		if cur.live {
			return cur.lease, cur.lease.Holder == holder, nil
		}
		granted, err := s.create(ctx, key, holder, ttl, cur.revision)
		if err == nil {
			return granted, true, nil
		}
		if !isWrongLastSequence(err) {
			return Lease{}, false, err
		}
	}
	return Lease{}, false, fmt.Errorf(
		"lease %q: contention did not settle after %d attempts",
		key, acquireAttemptsMax,
	)
}

// Renew extends holder's lease on key to ttl from now, provided revision
// is still the current fence. It returns the new lease, whose revision
// replaces the old one as the fence. Any mismatch is ErrLost.
func (s *Store) Renew(
	ctx context.Context, key, holder string, revision uint64,
	ttl time.Duration,
) (Lease, error) {
	if ctx == nil {
		panic("Store.Renew: ctx must not be nil")
	}
	if s == nil || s.tp == nil {
		panic("Store.Renew: store must be built by NewStore")
	}
	if err := validateFence(key, holder, revision); err != nil {
		return Lease{}, err
	}
	if err := validateTTL(ttl); err != nil {
		return Lease{}, err
	}
	cur, err := s.read(ctx, key)
	if err != nil {
		return Lease{}, err
	}
	if err := checkHolder(key, cur, holder); err != nil {
		return Lease{}, err
	}
	now := time.Now().UTC()
	rec := record{
		Holder: holder, AcquiredAt: cur.lease.AcquiredAt,
		TTLMs: ttl.Milliseconds(),
	}
	next, err := s.write(ctx, key, rec, EventRenewed, revision)
	if isWrongLastSequence(err) {
		return Lease{}, lostRevision(key, revision)
	}
	if err != nil {
		return Lease{}, err
	}
	return Lease{
		Key: key, Holder: holder, Revision: next,
		AcquiredAt: rec.AcquiredAt, ExpiresAt: now.Add(ttl), TTL: ttl,
	}, nil
}

// Release frees key if holder holds it at revision. Any mismatch --
// including a lease that already expired -- is ErrLost, so the caller
// learns it was no longer the owner when it thought it was.
func (s *Store) Release(
	ctx context.Context, key, holder string, revision uint64,
) error {
	if ctx == nil {
		panic("Store.Release: ctx must not be nil")
	}
	if s == nil || s.tp == nil {
		panic("Store.Release: store must be built by NewStore")
	}
	if err := validateFence(key, holder, revision); err != nil {
		return err
	}
	cur, err := s.read(ctx, key)
	if err != nil {
		return err
	}
	if err := checkHolder(key, cur, holder); err != nil {
		return err
	}
	err = s.purge(ctx, key, revision)
	if isWrongLastSequence(err) {
		return lostRevision(key, revision)
	}
	return err
}

// Owner reports key's current lease; held=false when nobody holds it.
func (s *Store) Owner(ctx context.Context, key string) (Lease, bool, error) {
	if ctx == nil {
		panic("Store.Owner: ctx must not be nil")
	}
	if s == nil || s.tp == nil {
		panic("Store.Owner: store must be built by NewStore")
	}
	if err := validateToken("key", key, KeyLengthMax); err != nil {
		return Lease{}, false, err
	}
	cur, err := s.read(ctx, key)
	if err != nil {
		return Lease{}, false, err
	}
	return cur.lease, cur.live, nil
}
