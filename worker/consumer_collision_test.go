// worker/consumer_collision_test.go
// Pure unit tests for the registration-time collision precheck. No embedded
// NATS — the precheck enumerates durable names from the in-memory
// (handlers, groups) view and panics on duplicates.
package worker

import (
	"strings"
	"testing"
)

func TestAssertNoConsumerNameCollisions_DefaultBranchCollision(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"render.gpu": func(ctx TaskContext) error { return nil },
		"render-gpu": func(ctx TaskContext) error { return nil },
	}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on collision, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "render.gpu") || !strings.Contains(msg, "render-gpu") {
			t.Fatalf("panic must name both originals, got: %s", msg)
		}
		if !strings.Contains(msg, "workers-render-gpu") {
			t.Fatalf("panic must name the colliding durable, got: %s", msg)
		}
	}()
	assertNoConsumerNameCollisions(handlers, nil)
}

func TestAssertNoConsumerNameCollisions_GroupsBranchCollision(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"render": func(ctx TaskContext) error { return nil },
	}
	groups := []string{"gpu.fast", "gpu-fast"}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on group collision, got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %#v", r)
		}
		if !strings.Contains(msg, "gpu.fast") || !strings.Contains(msg, "gpu-fast") {
			t.Fatalf("panic must name both group originals, got: %s", msg)
		}
		if !strings.Contains(msg, "workers-render-gpu-fast") {
			t.Fatalf("panic must name colliding durable, got: %s", msg)
		}
	}()
	assertNoConsumerNameCollisions(handlers, groups)
}

func TestAssertNoConsumerNameCollisions_CrossProduct_NoCollision(t *testing.T) {
	// 2 task types x 2 groups = 4 distinct durables. Must not panic.
	// Guards the cross-product enumeration logic.
	handlers := map[string]HandlerFunc{
		"render":  func(ctx TaskContext) error { return nil },
		"compile": func(ctx TaskContext) error { return nil },
	}
	groups := []string{"fast", "slow"}
	assertNoConsumerNameCollisions(handlers, groups)
}

func TestAssertNoConsumerNameCollisions_NoCollision_Baseline(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"nasr-ingest":                func(ctx TaskContext) error { return nil },
		"airports-canonical-refresh": func(ctx TaskContext) error { return nil },
	}
	assertNoConsumerNameCollisions(handlers, nil)
}

func TestAssertNoConsumerNameCollisions_EmptyHandlers(t *testing.T) {
	assertNoConsumerNameCollisions(map[string]HandlerFunc{}, nil)
}
