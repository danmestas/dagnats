// protocol/inputs_test.go
// The wire shape of durable waits (ADR-027): a step.continue payload
// carries the wait its handler asked for, a payload without one is
// unchanged on the wire, and the two wake events have stable msg-ids.
//
// Methodology: pure JSON round trips, no NATS. Each test asserts the
// positive shape and the negative (absent field omitted, no collision).
package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStepContinuePayload_WaitRoundTrip(t *testing.T) {
	until := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	in := StepContinuePayload{
		Output: json.RawMessage(`"o"`),
		Wait:   &ContinueWait{DelayMs: 1500, Until: &until, Input: true},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out StepContinuePayload
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Wait == nil || out.Wait.DelayMs != 1500 || !out.Wait.Input ||
		out.Wait.Until == nil || !out.Wait.Until.Equal(until) {
		t.Fatalf("wait = %+v, want delay 1500, until %s, input", out.Wait, until)
	}
	plain, err := json.Marshal(StepContinuePayload{Output: json.RawMessage(`"o"`)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(plain), "wait") {
		t.Errorf("payload without a wait carries one on the wire: %s", plain)
	}
}

func TestContinueWait_IsZero(t *testing.T) {
	var none *ContinueWait
	if !none.IsZero() {
		t.Error("nil wait must be zero")
	}
	if (&ContinueWait{}).IsZero() != true {
		t.Error("empty wait must be zero")
	}
	if (&ContinueWait{Input: true}).IsZero() {
		t.Error("input wait must not be zero")
	}
	if (&ContinueWait{DelayMs: 1}).IsZero() {
		t.Error("delay wait must not be zero")
	}
}

func TestWakeEvents_HaveDistinctStableMsgIDs(t *testing.T) {
	queued := NewStepEvent(EventStepInputQueued, "r", "s", []byte(`{"request_id":"q1"}`))
	queued.InputRequestID = "q1"
	elapsed := NewStepEvent(EventStepWaitElapsed, "r", "s", nil)
	elapsed.SetIdentity(3, 1, "n")
	ids := map[string]bool{queued.NATSMsgID(): true, elapsed.NATSMsgID(): true}
	if len(ids) != 2 {
		t.Fatalf("msg-ids collide: %v", ids)
	}
	if queued.NATSMsgID() != "r.s.step.input.queued.q1" {
		t.Errorf("input queued msg-id = %q, want the request id in it", queued.NATSMsgID())
	}
	other := NewStepEvent(EventStepInputQueued, "r", "s", nil)
	other.InputRequestID = "q2"
	if other.NATSMsgID() == queued.NATSMsgID() {
		t.Error("two requests share a msg-id; the second would be deduplicated")
	}
	if elapsed.NATSMsgID() != "r.s.step.wait.elapsed.1.3" {
		t.Errorf("wait elapsed msg-id = %q", elapsed.NATSMsgID())
	}
}
