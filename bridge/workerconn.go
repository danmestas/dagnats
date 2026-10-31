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
	// closed is set before the claims are collected, so a poll that
	// stores a claim after the scan still sees it and releases it.
	closed atomic.Bool
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
func (wc *workerConns) close(conn *workerConn) {
	if conn == nil {
		panic("workerConns.close: conn must not be nil")
	}
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

// pollWorkerConn resolves the connection a poll's worker_id names: it
// must be open on this bridge and owned by the caller by the same rule
// as connect (same token, or an admin). On refusal it writes the 409 and
// returns false.
func (b *Bridge) pollWorkerConn(
	w http.ResponseWriter, claims workertoken.Claims, workerID string,
) (*workerConn, bool) {
	if workerID == "" {
		panic("pollWorkerConn: workerID must not be empty")
	}
	conn := b.conns.current(workerID)
	switch {
	case conn == nil:
		writeJSON(w, http.StatusConflict, pollWorkerRefusalBody{
			Error: "worker_id has no open connection on this bridge; connect first",
			Code:  "worker_not_connected",
		})
		return nil, false
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
	b.conns.close(conn)
	if owned {
		b.releaseWorkerClaims(ctx, conn)
	}
}

// releaseWorkerClaims Naks the claims recorded under conn and drops them
// from the ack map, so a late resolve from a worker that was only
// partitioned answers "task not found" rather than racing the
// redelivery. Resolved claims are already gone from the map, and a
// fenced one belongs to its successor's delivery, so neither is Nak'd.
// Bounded by the ack map's own entry cap.
func (b *Bridge) releaseWorkerClaims(ctx context.Context, conn *workerConn) {
	if !conn.closed.Load() {
		panic("releaseWorkerClaims: connection must be closed first")
	}
	released := b.ackMap.takeWorkerClaims(conn)
	for _, msg := range released {
		if err := msg.Nak(); err != nil {
			slog.WarnContext(ctx, "release claim on disconnect failed; it redelivers after AckWait",
				"worker_id", conn.id, "error", err)
		}
	}
	if len(released) > 0 {
		slog.InfoContext(ctx, "released claims of a disconnected worker",
			"worker_id", conn.id, "count", len(released))
	}
}

// releaseIfClosed releases claims stored under conn after it closed: a
// long poll that outlives its connection stores them once the scan has
// already run.
func (b *Bridge) releaseIfClosed(ctx context.Context, conn *workerConn) {
	if conn != nil && conn.closed.Load() {
		b.releaseWorkerClaims(ctx, conn)
	}
}
