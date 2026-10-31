// bridge/resolve_toolarge_test.go
// The 413s a resolve can answer: loop state over the task's
// loop_state_max_bytes, and a request body over 8 MiB.
//
// Methodology: real embedded NATS and a bridge over HTTP, as in
// journal_test.go. The cap travels in the task payload (state_max_bytes),
// so a small one is set per task. Positive space: state at the cap is
// accepted; an oversize checkpoint and an oversize continue answer 413
// {"code":"state_too_large","max_bytes","bytes"}; an oversize body
// answers 413 body_too_large. Negative space: a refused checkpoint
// buffers nothing (a continue after it still sees the polled state), and
// the body limit does not touch an ordinary request.
package bridge

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/protocol"
)

type tooLargeReply struct {
	Code     string `json:"code"`
	MaxBytes int    `json:"max_bytes"`
	Bytes    int    `json:"bytes"`
}

func decodeTooLarge(t *testing.T, body string) tooLargeReply {
	t.Helper()
	var reply tooLargeReply
	if err := json.Unmarshal([]byte(body), &reply); err != nil {
		t.Fatalf("decode 413 body %q: %v", body, err)
	}
	return reply
}

// quotedBytes is a JSON string literal of n bytes in total, quotes included.
func quotedBytes(n int) string {
	return `"` + strings.Repeat("a", n-2) + `"`
}

func TestBridgeCheckpointOverTheStateCapAnswers413(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{
		RunID: "run-cap-cp", StepID: "step", DispatchNonce: "n1", StateMaxBytes: 64,
	})
	taskID := pollOne(t, ts)

	status, body := resolveBody(t, ts, taskID,
		`{"action":"checkpoint","data":`+quotedBytes(65)+`}`)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("checkpoint over cap = %d %s, want 413", status, body)
	}
	if got := decodeTooLarge(t, body); got.Code != "state_too_large" || got.MaxBytes != 64 || got.Bytes != 65 {
		t.Fatalf("413 body = %+v, want state_too_large max 64 bytes 65", got)
	}
	status, body = resolveBody(t, ts, taskID,
		`{"action":"checkpoint","data":`+quotedBytes(64)+`}`)
	if status != http.StatusOK {
		t.Fatalf("checkpoint at the cap = %d %s, want 200", status, body)
	}
}

func TestBridgeContinueOverTheStateCapAnswers413(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{
		RunID: "run-cap-cont", StepID: "step", DispatchNonce: "n1",
		State: []byte(strings.Repeat("a", 100)), StateMaxBytes: 64,
	})
	taskID := pollOne(t, ts)

	status, body := resolveBody(t, ts, taskID, `{"action":"continue","output":{}}`)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("continue over cap = %d %s, want 413", status, body)
	}
	if got := decodeTooLarge(t, body); got.Code != "state_too_large" || got.MaxBytes != 64 || got.Bytes != 100 {
		t.Fatalf("413 body = %+v, want state_too_large max 64 bytes 100", got)
	}
	// The task is still claimed: a refused continue ends nothing.
	if status, body := resolveBody(t, ts, taskID, `{"action":"heartbeat"}`); status != http.StatusOK {
		t.Fatalf("heartbeat after the refusal = %d %s, want 200", status, body)
	}
}

func TestBridgeResolveBodyOverEightMiBAnswers413(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-cap-body", StepID: "step", DispatchNonce: "n1"})
	taskID := pollOne(t, ts)

	huge := `{"action":"stream","data":` + quotedBytes(resolveBodyBytesMax+1) + `}`
	status, body := resolveBody(t, ts, taskID, huge)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("9 MiB body = %d, want 413", status)
	}
	if got := decodeTooLarge(t, body); got.Code != "body_too_large" || got.MaxBytes != resolveBodyBytesMax {
		t.Fatalf("413 body = %+v, want body_too_large max %d", got, resolveBodyBytesMax)
	}
	if status, body := resolveBody(t, ts, taskID, `{"action":"heartbeat"}`); status != http.StatusOK {
		t.Fatalf("ordinary request after the refusal = %d %s, want 200", status, body)
	}
}

func TestBridgeJournalAppendOverMaxPayloadAnswers413(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-cap-rec", StepID: "step", DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	max := int(b.nc.MaxPayload())

	big := `{"action":"journal.append","type":"entry","data":` + quotedBytes(max+1024) + `}`
	status, body := resolveBody(t, ts, taskID, big)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("append over max_payload = %d %s, want 413", status, body)
	}
	if got := decodeTooLarge(t, body); got.Code != "record_too_large" || got.MaxBytes != max {
		t.Fatalf("413 body = %+v, want record_too_large max %d", got, max)
	}
	small := `{"action":"journal.append","type":"entry","data":"fits"}`
	if status, body := resolveBody(t, ts, taskID, small); status != http.StatusOK {
		t.Fatalf("append that fits after the refusal = %d %s, want 200", status, body)
	}
}
