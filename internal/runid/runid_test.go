// runid_test.go
//
// Methodology: pure unit tests for the run-id generator. Asserts
// the shape contract (length, alphabet) and the collision-freeness
// contract under tight concurrent calls. The concurrency test is
// what the HTTP trigger relies on -- if two callers in the same
// nanosecond can collide, JetStream dedup drops one workflow.started
// and the surviving run's response goes to both waiting handlers.
package runid

import (
	"strings"
	"sync"
	"testing"
)

// TestNew_ShapeAndAlphabet asserts the surface contract: 32 hex chars,
// lowercase only. Callers concatenate the result into subjects and
// log lines that assume that shape.
func TestNew_ShapeAndAlphabet(t *testing.T) {
	id := New()
	if len(id) != 32 {
		t.Fatalf("New() length = %d, want 32", len(id))
	}
	const hex = "0123456789abcdef"
	for i, r := range id {
		if !strings.ContainsRune(hex, r) {
			t.Fatalf("New()[%d] = %q, not lowercase hex", i, r)
		}
	}
}

// TestNew_NoCollisionsUnderConcurrency asserts the contract that
// underlies the HTTP trigger fix: 10k concurrent calls produce 10k
// distinct values. crypto/rand makes a collision astronomically
// unlikely (16 random bytes = 128 bits of entropy); the previous
// time.Now().UnixNano() approach collided routinely on a single
// modern CPU.
func TestNew_NoCollisionsUnderConcurrency(t *testing.T) {
	const total = 10000
	ids := make([]string, total)
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		i := i
		go func() {
			defer wg.Done()
			ids[i] = New()
		}()
	}
	wg.Wait()
	seen := make(map[string]struct{}, total)
	for i, id := range ids {
		if id == "" {
			t.Fatalf("ids[%d] is empty", i)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("ids[%d] = %q collided with earlier id", i, id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("distinct ids = %d, want %d", len(seen), total)
	}
}
