// engine/ratelimit_test.go
// Tests for the KV-backed token bucket rate limiter.
// Methodology: each test starts an embedded NATS server with the rate_limits
// bucket, exercises Allow() with various token states, and verifies both
// allowed/denied outcomes and retryAfter durations.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTokenBucketAllowsWithinLimit(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	rl := NewRateLimiter(jsNew)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}

	// 5 per minute, consume 1 — should be allowed
	allowed, retryAfter, err := rl.Allow(context.Background(),
		"task-a", "_global", 5, time.Minute, 1,
	)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}

	// Positive: request is allowed
	if !allowed {
		t.Fatal("expected request to be allowed")
	}
	// Negative: no retry needed when allowed
	if retryAfter != 0 {
		t.Fatalf("retryAfter = %v, want 0", retryAfter)
	}
}

func TestTokenBucketDeniesWhenExhausted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	rl := NewRateLimiter(jsNew)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}

	// 2 per minute — consume all tokens
	for i := 0; i < 2; i++ {
		allowed, _, err := rl.Allow(context.Background(),
			"task-b", "_global", 2, time.Minute, 1,
		)
		if err != nil {
			t.Fatalf("Allow(%d): %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// 3rd request should be denied
	allowed, retryAfter, err := rl.Allow(context.Background(),
		"task-b", "_global", 2, time.Minute, 1,
	)
	if err != nil {
		t.Fatalf("Allow(3rd): %v", err)
	}

	// Positive: request is denied
	if allowed {
		t.Fatal("3rd request should be denied")
	}
	// Negative: retryAfter must be positive
	if retryAfter <= 0 {
		t.Fatalf("retryAfter = %v, want > 0", retryAfter)
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	rl := NewRateLimiter(jsNew)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}

	// Use a controllable clock for deterministic testing.
	now := time.Now()
	rl.now = func() time.Time { return now }

	// 2 per second — exhaust all tokens
	for i := 0; i < 2; i++ {
		allowed, _, err := rl.Allow(context.Background(),
			"task-c", "_global", 2, time.Second, 1,
		)
		if err != nil {
			t.Fatalf("Allow(%d): %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// Verify exhausted
	allowed, _, err := rl.Allow(context.Background(),
		"task-c", "_global", 2, time.Second, 1,
	)
	if err != nil {
		t.Fatalf("Allow exhausted check: %v", err)
	}
	if allowed {
		t.Fatal("bucket should be exhausted")
	}

	// Advance clock by 600ms — should refill 1 token (2 per 1s)
	now = now.Add(600 * time.Millisecond)

	allowed, retryAfter, err := rl.Allow(context.Background(),
		"task-c", "_global", 2, time.Second, 1,
	)
	if err != nil {
		t.Fatalf("Allow after refill: %v", err)
	}

	// Positive: request is allowed after partial refill
	if !allowed {
		t.Fatalf("expected request to be allowed after refill, "+
			"retryAfter=%v", retryAfter)
	}
	// Negative: retryAfter is zero when allowed
	if retryAfter != 0 {
		t.Fatalf("retryAfter = %v, want 0", retryAfter)
	}
}

func TestTokenBucketNilIsNoOp(t *testing.T) {
	// A nil RateLimiter always allows — graceful degradation.
	var rl *RateLimiter

	// Positive: nil limiter allows
	allowed, retryAfter, err := rl.Allow(context.Background(),
		"task-x", "_global", 1, time.Second, 1,
	)
	if err != nil {
		t.Fatalf("nil Allow: %v", err)
	}
	if !allowed {
		t.Fatal("nil RateLimiter should always allow")
	}
	// Negative: no retry for nil limiter
	if retryAfter != 0 {
		t.Fatalf("retryAfter = %v, want 0", retryAfter)
	}
}

func TestTokenBucketKeyedIsolation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	rl := NewRateLimiter(jsNew)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}

	// Exhaust tokens for key "user-1"
	allowed, _, err := rl.Allow(context.Background(),
		"task-d", "user-1", 1, time.Minute, 1,
	)
	if err != nil {
		t.Fatalf("Allow user-1: %v", err)
	}
	if !allowed {
		t.Fatal("first request for user-1 should be allowed")
	}

	// user-1 is exhausted
	allowed, _, _ = rl.Allow(context.Background(),
		"task-d", "user-1", 1, time.Minute, 1,
	)
	if allowed {
		t.Fatal("user-1 should be exhausted")
	}

	// Positive: different key has its own bucket
	allowed, _, err = rl.Allow(context.Background(),
		"task-d", "user-2", 1, time.Minute, 1,
	)
	if err != nil {
		t.Fatalf("Allow user-2: %v", err)
	}
	if !allowed {
		t.Fatal("user-2 should have its own bucket and be allowed")
	}
}
