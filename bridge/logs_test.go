// bridge/logs_test.go
// Tests for POST /v1/tasks/{id}/logs (#624). Methodology: real NATS
// server, claim a task via poll (dev-mode open bridge for the ingest/
// size-cap tests, a minted-token fixture for the ownership test),
// POST chunks, drain BUILD_LOGS to assert what landed.
package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// postLogs POSTs a logs body for taskID, authenticating with bearer
// (empty means no Authorization header).
func postLogs(
	t *testing.T, baseURL, taskID, bearer, body string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequest(
		"POST", baseURL+"/v1/tasks/"+taskID+"/logs",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("logs request: %v", err)
	}
	return resp
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func drainBuildLogs(
	t *testing.T, nc *nats.Conn, runID, stepID string, attempt, want int,
	timeout time.Duration,
) []protocol.LogChunk {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	subject := fmt.Sprintf("logs.%s.%s.%d", runID, stepID, attempt)
	cons, err := js.OrderedConsumer(ctx, "BUILD_LOGS",
		jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	if err != nil {
		t.Fatalf("OrderedConsumer: %v", err)
	}
	var chunks []protocol.LogChunk
	for len(chunks) < want {
		if ctx.Err() != nil {
			t.Fatalf("timed out with %d/%d chunks: %+v", len(chunks), want, chunks)
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			continue
		}
		var c protocol.LogChunk
		if err := json.Unmarshal(msg.Data(), &c); err != nil {
			t.Fatalf("unmarshal LogChunk: %v", err)
		}
		msg.Ack()
		chunks = append(chunks, c)
	}
	return chunks
}

func TestPostLogs_ChunksLandOnBuildLogs(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(t, nc, b, ts, "run-logs-1", "step-1")

	body := `{"chunks":[{"stream":"out","data":"` + b64("hello") + `"},` +
		`{"stream":"err","data":"` + b64("oops") + `"}]}`
	resp := postLogs(t, ts.URL, taskID, "", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// attempt=1: taskAttemptNumber (bridge/poll.go) resolves a fresh
	// poll's payload.Attempt=0 via NATS NumDelivered, same as
	// worker/log_writer.go's resolveAttemptNumber (#624 review).
	chunks := drainBuildLogs(t, nc, "run-logs-1", "step-1", 1, 2, 10*time.Second)
	if chunks[0].Seq != 0 || chunks[0].Attempt != 1 || chunks[0].Stream != protocol.LogStreamOut ||
		string(chunks[0].Data) != "hello" {
		t.Fatalf("chunks[0] = %+v, want seq=0 attempt=1 out=hello", chunks[0])
	}
	if chunks[1].Seq != 1 || chunks[1].Attempt != 1 || chunks[1].Stream != protocol.LogStreamErr ||
		string(chunks[1].Data) != "oops" {
		t.Fatalf("chunks[1] = %+v, want seq=1 attempt=1 err=oops", chunks[1])
	}
}

func TestPostLogs_RejectsDifferentMintedToken(t *testing.T) {
	baseURL, svc, bearerA, bearerB, _ := tokenResolveFixture(t)
	task := startRunAndPollTask(t, baseURL, svc, bearerA)

	body := `{"chunks":[{"stream":"out","data":"` + b64("x") + `"}]}`
	resp := postLogs(t, baseURL, task.TaskID, bearerB, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestPostLogs_BodyOverOneMiBIs413(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	taskID := publishAndPollTask(t, nc, b, ts, "run-logs-2", "step-1")

	// Build a body deliberately over the 1 MiB cap: json overhead plus
	// a big base64 payload comfortably exceeds it.
	big := bytes.Repeat([]byte("a"), logsBodyBytesMax+1024)
	body := `{"chunks":[{"stream":"out","data":"` + b64(string(big)) + `"}]}`
	resp := postLogs(t, ts.URL, taskID, "", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

func TestPostLogs_UnknownTaskIs404(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	body := `{"chunks":[{"stream":"out","data":"` + b64("x") + `"}]}`
	resp := postLogs(t, ts.URL, "no-such-run.no-such-step", "", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
