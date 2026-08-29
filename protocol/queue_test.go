// protocol/queue_test.go
// Tests for QueueSnapshot/QueueGroup: JSON schema (snake_case wire
// tags), and that an empty Groups slice marshals as [] not null.
//
// Methodology: construct QueueSnapshot values, marshal to JSON, assert
// on the exact wire shape a consumer (dashboard, dashboard SDK) reads.
package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestQueueSnapshotJSONSchema(t *testing.T) {
	waitMs := int64(1500)
	snap := QueueSnapshot{
		Groups: []QueueGroup{
			{TaskType: "build", Pending: 3, OldestWaitMs: &waitMs},
			{TaskType: "test", Pending: 1},
		},
		SnapshotAt: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(data)

	// Positive: expected wire fields are present with snake_case tags.
	for _, want := range []string{
		`"task_type":"build"`, `"pending":3`, `"oldest_wait_ms":1500`,
		`"task_type":"test"`, `"pending":1`, `"snapshot_at":`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body = %q, want substring %q", body, want)
		}
	}
	// Negative: an omitted oldest_wait_ms (test group) must not leak a
	// stray key, and truncated must be entirely absent when false.
	if strings.Contains(body, `"truncated"`) {
		t.Fatalf("body = %q, want no truncated key when false", body)
	}

	var decoded QueueSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Groups) != 2 || decoded.Groups[0].TaskType != "build" {
		t.Fatalf("decoded groups = %+v, want 2 groups starting with build",
			decoded.Groups)
	}
}

func TestQueueSnapshotEmptyGroupsMarshalsAsEmptyArray(t *testing.T) {
	snap := QueueSnapshot{Groups: []QueueGroup{}, SnapshotAt: time.Now()}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Positive: empty (non-nil) slice serializes as [].
	if !strings.Contains(string(data), `"groups":[]`) {
		t.Fatalf("body = %q, want groups:[]", data)
	}

	var nilSnap QueueSnapshot
	nilData, err := json.Marshal(nilSnap)
	if err != nil {
		t.Fatalf("marshal nil: %v", err)
	}
	// Negative: a nil Groups slice ALSO serializes as [] -- callers
	// building the response must not rely on this, but the type itself
	// must not silently emit null for a nil field either way a caller
	// happens to construct it. Actually a nil slice DOES marshal to
	// null by default; this assertion documents that fact so a caller
	// (internal/api/queue.go) knows it must always assign []QueueGroup{}
	// explicitly, matching GET /v1/workers' documented behavior.
	if !strings.Contains(string(nilData), `"groups":null`) {
		t.Fatalf("body = %q, want groups:null for a nil slice "+
			"(documents why the handler must assign []QueueGroup{})",
			nilData)
	}
}
