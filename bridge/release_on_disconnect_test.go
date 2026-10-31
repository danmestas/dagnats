// release_on_disconnect_test.go
// Methodology: real embedded NATS and a bridge over httptest. A worker
// connects (SSE), polls with its worker_id and then its connection is
// dropped by cancelling the client request; a second worker polling
// without a worker_id must be handed the task well inside AckWait (the
// stream's AckWait is minutes, every wait here is seconds). Negative
// space: a poll without worker_id keeps the AckWait behaviour, a claim
// resolved before the drop is not Nak'd, a token that does not own the
// id can neither poll under it nor release anything, and a stale
// connection's cleanup after a reconnect releases only its own claims.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/protocol"
)

// pollAs posts a poll with an optional worker_id and bearer and returns
// the status, body and decoded tasks.
func pollAs(
	t *testing.T, ts *httptest.Server, bearer, workerID string, timeoutMs int,
) (int, string, []pollResponse) {
	t.Helper()
	body := fmt.Sprintf(`{"task_types":["echo"],"max_tasks":1,"timeout_ms":%d`, timeoutMs)
	if workerID != "" {
		body += fmt.Sprintf(`,"worker_id":%q`, workerID)
	}
	body += "}"
	req, err := http.NewRequest("POST", ts.URL+"/v1/tasks/poll", strings.NewReader(body))
	if err != nil {
		t.Fatalf("poll request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("poll body: %v", err)
	}
	var tasks []pollResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &tasks); err != nil {
			t.Fatalf("decode poll: %v (%s)", err, raw)
		}
	}
	return resp.StatusCode, string(raw), tasks
}

// openConn connects workerID and returns the function that drops the
// connection and waits for the bridge to have processed the close.
func openConn(t *testing.T, ts *httptest.Server, b *Bridge, bearer, workerID string) func() {
	t.Helper()
	resp, cancel := connectWorker(t, ts.URL, bearer, workerID)
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("connect %s: %d", workerID, resp.StatusCode)
	}
	conn := b.conns.current(workerID)
	if conn == nil {
		cancel()
		t.Fatalf("bridge holds no connection for %s", workerID)
	}
	t.Cleanup(cancel)
	return func() {
		cancel()
		resp.Body.Close()
		deadline := time.Now().Add(5 * time.Second)
		for !conn.closed.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("bridge did not see %s close", workerID)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func publishEcho(t *testing.T, rig *journalRig, runID string) {
	t.Helper()
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: "s", DispatchNonce: "n-" + runID})
}

func TestReleaseOnDisconnect_RedeliversBeforeAckWait(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	publishEcho(t, rig, "run-rel")
	drop := openConn(t, ts, b, "", "w1")
	status, body, tasks := pollAs(t, ts, "", "w1", 3000)
	if status != 200 || len(tasks) != 1 {
		t.Fatalf("poll with worker_id = %d %s, want one task", status, body)
	}
	drop()
	start := time.Now()
	_, _, again := pollAs(t, ts, "", "", 8000)
	if len(again) != 1 || again[0].RunID != "run-rel" || again[0].Delivered < 2 {
		t.Fatalf("second worker got %+v, want the released task redelivered", again)
	}
	if time.Since(start) > 7*time.Second {
		t.Errorf("redelivery took %s, want seconds", time.Since(start))
	}
}

func TestReleaseOnDisconnect_PollWithoutWorkerIDKeepsAckWait(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	publishEcho(t, rig, "run-norel")
	drop := openConn(t, ts, b, "", "w1")
	if _, _, tasks := pollAs(t, ts, "", "", 3000); len(tasks) != 1 {
		t.Fatal("poll without worker_id got no task")
	}
	drop()
	if _, _, again := pollAs(t, ts, "", "", 1500); len(again) != 0 {
		t.Fatalf("a claim without worker_id was released on disconnect: %+v", again)
	}
}

func TestReleaseOnDisconnect_ResolvedClaimIsNotNakked(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	publishEcho(t, rig, "run-done")
	drop := openConn(t, ts, b, "", "w1")
	_, _, tasks := pollAs(t, ts, "", "w1", 3000)
	if len(tasks) != 1 {
		t.Fatal("no task")
	}
	if status, body := resolveBody(t, ts, tasks[0].TaskID, `{"action":"complete","output":{}}`); status != 200 {
		t.Fatalf("complete: %d %s", status, body)
	}
	drop()
	if _, _, again := pollAs(t, ts, "", "", 1500); len(again) != 0 {
		t.Fatalf("a resolved claim was redelivered after the drop: %+v", again)
	}
}

func TestReleaseOnDisconnect_NonOwnerCannotPollOrRelease(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	store := openTokenStore(t, rig.js)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	mintCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, bearerA, err := store.Mint(mintCtx, "a", []string{"echo"}, nil, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}
	_, bearerB, err := store.Mint(mintCtx, "b", []string{"echo"}, nil, "tester")
	if err != nil {
		t.Fatalf("Mint B: %v", err)
	}
	publishEcho(t, rig, "run-own")
	openConn(t, ts, b, bearerA, "w1")
	if _, _, tasks := pollAs(t, ts, bearerA, "w1", 3000); len(tasks) != 1 {
		t.Fatal("owner poll got no task")
	}

	status, body, _ := pollAs(t, ts, bearerB, "w1", 500)
	if status != http.StatusConflict || !strings.Contains(body, `"worker_id_owned"`) {
		t.Fatalf("non-owner poll = %d %s, want 409 worker_id_owned", status, body)
	}
	status, body, _ = pollAs(t, ts, bearerB, "nobody", 500)
	if status != http.StatusConflict || !strings.Contains(body, `"worker_not_connected"`) {
		t.Fatalf("poll for an unconnected id = %d %s, want 409 worker_not_connected", status, body)
	}
	respB, cancelB := connectWorker(t, ts.URL, bearerB, "w1")
	respB.Body.Close()
	cancelB()
	if respB.StatusCode != http.StatusConflict {
		t.Fatalf("non-owner connect = %d, want 409", respB.StatusCode)
	}
	if _, _, again := pollAs(t, ts, bearerA, "", 1500); len(again) != 0 {
		t.Fatalf("a non-owner's connect attempt released the owner's claim: %+v", again)
	}
}

// The same id reconnects before the old stream's cleanup: closing the
// old connection releases the claims polled under it, not the new one's.
func TestReleaseOnDisconnect_ReconnectRaceKeepsNewConnectionClaims(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	publishEcho(t, rig, "run-old")
	publishEcho(t, rig, "run-new")
	dropOld := openConn(t, ts, b, "", "w1")
	_, _, first := pollAs(t, ts, "", "w1", 3000)
	if len(first) != 1 {
		t.Fatal("old connection poll got no task")
	}
	dropNew := openConn(t, ts, b, "", "w1")
	_, _, second := pollAs(t, ts, "", "w1", 3000)
	if len(second) != 1 {
		t.Fatal("new connection poll got no task")
	}

	dropOld()
	_, _, got := pollAs(t, ts, "", "", 8000)
	if len(got) != 1 || got[0].RunID != first[0].RunID {
		t.Fatalf("after the old close got %+v, want only the old connection's task %s", got, first[0].RunID)
	}
	if _, _, more := pollAs(t, ts, "", "", 1500); len(more) != 0 {
		t.Fatalf("the old close released the new connection's claim: %+v", more)
	}
	if _, ok := b.ackMap.Load(second[0].TaskID); !ok {
		t.Fatal("the new connection's claim left the ack map")
	}
	dropNew()
	_, _, last := pollAs(t, ts, "", "", 8000)
	if len(last) != 1 || last[0].RunID != second[0].RunID {
		t.Fatalf("after the new close got %+v, want %s", last, second[0].RunID)
	}
}
