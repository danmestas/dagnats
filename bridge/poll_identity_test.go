// bridge/poll_identity_test.go
// What a poll item says about the task it hands out: the dispatch nonce
// of its claim and the task type it was polled under.
//
// Methodology: real embedded NATS and a bridge over HTTP, as in
// journal_view_test.go. Positive space: dispatch_nonce on the poll item
// equals the claim.dispatch_nonce journal.view answers for the same
// delivery, and each item of a multi-type poll names its own type.
// Negative space: a task published without a nonce has no dispatch_nonce
// key at all, and a type that was not polled never appears.
package bridge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/protocol"
)

func TestBridgePollItemDispatchNonceEqualsTheClaimsNonce(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-pnonce", StepID: "step", DispatchNonce: "nonce-7"})
	tasks := postPoll(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 || tasks[0].DispatchNonce != "nonce-7" {
		t.Fatalf("poll = %+v, want dispatch_nonce nonce-7", tasks)
	}
	view := resolveView(t, ts, tasks[0].TaskID)
	if string(view.Claim["dispatch_nonce"]) != `"`+tasks[0].DispatchNonce+`"` {
		t.Fatalf("claim %v disagrees with poll nonce %q", view.Claim, tasks[0].DispatchNonce)
	}
}

func TestBridgePollItemWithoutNonceOmitsTheKey(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-pnononce", StepID: "step"})
	status, body := postPollRaw(t, ts, `{"task_types":["echo"],"max_tasks":1,"timeout_ms":3000}`)
	if status != 200 || strings.Contains(body, "dispatch_nonce") {
		t.Fatalf("poll = %d %s, want no dispatch_nonce key", status, body)
	}
}

func TestBridgePollItemNamesTheTypeItWasPolledUnder(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-ptype-a", StepID: "step"})
	data, err := json.Marshal(protocol.TaskPayload{RunID: "run-ptype-b", StepID: "step"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := rig.js.Publish(t.Context(), "task.other.run-ptype-b", data); err != nil {
		t.Fatalf("publish other: %v", err)
	}
	status, body := postPollRaw(t, ts,
		`{"task_types":["echo","other"],"max_tasks":2,"timeout_ms":3000}`)
	var tasks []pollResponse
	if status != 200 || json.Unmarshal([]byte(body), &tasks) != nil {
		t.Fatalf("poll = %d %s", status, body)
	}
	got := map[string]string{}
	for _, task := range tasks {
		got[task.RunID] = task.TaskType
	}
	if got["run-ptype-a"] != "echo" || got["run-ptype-b"] != "other" || len(got) != 2 {
		t.Fatalf("task types by run = %v, want a=echo b=other", got)
	}
}
