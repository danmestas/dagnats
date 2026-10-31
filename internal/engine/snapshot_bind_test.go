// Methodology: unit test of NewSnapshotStore against a scripted
// jetstream.JetStream whose first workflow_runs lookup goes unanswered,
// the way a clustered STREAM.INFO does while the bucket's stream elects a
// leader. A node used to panic on that one drop at startup (the cluster
// e2e journal-follow flake); the store must bind on the re-sent lookup.
package engine

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// droppedOnceJS times out the first KeyValue lookup, then answers.
type droppedOnceJS struct {
	jetstream.JetStream
	calls int
}

func (d *droppedOnceJS) KeyValue(
	ctx context.Context, bucket string,
) (jetstream.KeyValue, error) {
	d.calls++
	if d.calls == 1 {
		return nil, context.DeadlineExceeded
	}
	return nil, nil
}

func (d *droppedOnceJS) Conn() *nats.Conn { return nil }

func TestNewSnapshotStore_SurvivesUnansweredLookup(t *testing.T) {
	js := &droppedOnceJS{}
	var store *SnapshotStore
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("NewSnapshotStore panicked on a dropped lookup: %v", r)
			}
		}()
		store = NewSnapshotStore(js)
	}()
	if store == nil {
		t.Fatal("NewSnapshotStore returned nil")
	}
	if js.calls != 2 {
		t.Fatalf("calls=%d, want 2 (one drop, one answer)", js.calls)
	}
}
