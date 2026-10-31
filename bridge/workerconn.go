package bridge

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/danmestas/dagnats/internal/workertoken"
)

// workerConn is one open SSE connection of a bridge worker. A poll that
// names the worker's id tags its claims with the connection instance, not
// the id: the same id reconnecting before the old stream's cleanup runs
// must not have the new connection's claims released by the old one.
type workerConn struct {
	id      string
	tokenID string
	admin   bool
	// release says whether the close released the connection's claims:
	// false when the closing token no longer owned the id. It is set
	// before closed, and closed before the claims are collected, so a
	// poll that stores a claim after the scan still sees both and acts
	// the same way as the scan.
	release atomic.Bool
	closed  atomic.Bool
}

// workerConns is the bridge's registry of open connections by worker id.
// Only the newest connection of an id is current. It holds no entry
// beyond a connection's life.
type workerConns struct {
	mu   sync.Mutex
	byID map[string]*workerConn
}

func newWorkerConns() *workerConns {
	return &workerConns{byID: map[string]*workerConn{}}
}

// open registers the connection for id and makes it the current one.
func (wc *workerConns) open(id string, claims workertoken.Claims) *workerConn {
	if id == "" {
		panic("workerConns.open: id must not be empty")
	}
	conn := &workerConn{id: id, tokenID: claims.TokenID, admin: claims.Admin}
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.byID[id] = conn
	return conn
}

// close marks conn closed and forgets it if it is still the current
// connection of its id.
func (wc *workerConns) close(conn *workerConn, release bool) {
	if conn == nil {
		panic("workerConns.close: conn must not be nil")
	}
	conn.release.Store(release)
	conn.closed.Store(true)
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.byID[conn.id] == conn {
		delete(wc.byID, conn.id)
	}
}

// current returns the open connection of id.
func (wc *workerConns) current(id string) *workerConn {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	return wc.byID[id]
}

// pollWorkerRefusalBody is the 409 body of a poll whose worker_id the
// caller may not use.
type pollWorkerRefusalBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// pollWorkerConn resolves the connection a poll's worker_id names. An id
// open on this bridge under another token is refused with a 409
// (worker_id_owned), by the same rule as connect: same token, or an
// admin. An id with no open connection here is not refused: the SSE
// stream and the poll may reach different bridges, or the stream may
// have just dropped, so the poll is accepted unbound (redelivery after
// AckWait, as for a poll without worker_id), with a warning and a count.
// ok false means the refusal was written.
func (b *Bridge) pollWorkerConn(
	ctx context.Context, w http.ResponseWriter, claims workertoken.Claims, workerID string,
) (conn *workerConn, ok bool) {
	if workerID == "" {
		panic("pollWorkerConn: workerID must not be empty")
	}
	conn = b.conns.current(workerID)
	switch {
	case conn == nil:
		slog.WarnContext(ctx, "poll names a worker_id with no open connection here; claims unbound",
			"worker_id", workerID)
		b.unboundPolls.Add(ctx, 1)
		return nil, true
	case !claims.Admin && conn.tokenID != claims.TokenID:
		writeJSON(w, http.StatusConflict, pollWorkerRefusalBody{
			Error: "worker_id is registered to another token",
			Code:  "worker_id_owned",
		})
		return nil, false
	}
	return conn, true
}

type workerConnKey struct{}

// withWorkerConn carries the poll's connection down to storeClaimedTask.
func withWorkerConn(ctx context.Context, conn *workerConn) context.Context {
	return context.WithValue(ctx, workerConnKey{}, conn)
}

func workerConnFrom(ctx context.Context) *workerConn {
	conn, _ := ctx.Value(workerConnKey{}).(*workerConn)
	return conn
}

// closeWorkerConn ends conn. When the closing token still owns the
// worker id, every claim polled under this connection that is still
// unresolved is Nak'd so NATS redelivers it now instead of after AckWait.
func (b *Bridge) closeWorkerConn(ctx context.Context, conn *workerConn, owned bool) {
	b.conns.close(conn, owned)
	if owned {
		b.releaseWorkerClaims(ctx, conn)
	}
}

// releaseWorkerClaims releases the claims recorded under conn.
func (b *Bridge) releaseWorkerClaims(ctx context.Context, conn *workerConn) {
	if !conn.closed.Load() {
		panic("releaseWorkerClaims: connection must be closed first")
	}
	released := 0
	for _, ref := range b.ackMap.connClaims(conn) {
		if b.releaseClaim(ctx, conn, ref) {
			released++
		}
	}
	if released > 0 {
		slog.InfoContext(ctx, "released claims of a disconnected worker",
			"worker_id", conn.id, "count", released)
	}
}

// releaseClaim Naks one claim and drops it from the ack map, so a late
// resolve from a worker that was only partitioned answers "task not
// found" rather than racing the redelivery. It holds the claim's
// resolveMu: a resolve that ends the claim (Load through Ack) either
// finished first, and the claim is gone, or waits and finds it gone, so
// a delivery is never both acknowledged and Nak'd. A fenced claim
// belongs to its successor's delivery and is left.
func (b *Bridge) releaseClaim(ctx context.Context, conn *workerConn, ref claimRef) bool {
	ref.mu.Lock()
	defer ref.mu.Unlock()
	if !b.ackMap.takeUnresolved(ref) {
		return false
	}
	if err := ref.msg.Nak(); err != nil {
		slog.WarnContext(ctx, "release claim on disconnect failed; it redelivers after AckWait",
			"worker_id", conn.id, "task_id", ref.taskID, "error", err)
	}
	return true
}

// releaseIfClosed releases the claim of taskID just stored under conn
// when conn has already closed and that close released its claims (a
// long poll that outlives its connection stores them after the scan
// ran). True means the claim was Nak'd and must not be handed out.
func (b *Bridge) releaseIfClosed(ctx context.Context, conn *workerConn, taskID string) bool {
	if conn == nil || !conn.closed.Load() || !conn.release.Load() {
		return false
	}
	ref, ok := b.ackMap.connClaim(conn, taskID)
	if !ok {
		return false
	}
	return b.releaseClaim(ctx, conn, ref)
}
