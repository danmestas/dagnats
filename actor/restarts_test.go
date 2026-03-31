package actor

// Methodology: test restart tracker in isolation. Verifies
// that restart limits within a time window are enforced.

import (
	"testing"
	"time"
)

func TestRestartTrackerAllowsWithinLimit(t *testing.T) {
	tr := NewRestartTracker(3, 1*time.Minute)

	// Positive: first three restarts allowed
	for i := 0; i < 3; i++ {
		if !tr.Allow() {
			t.Fatalf("restart %d should be allowed", i+1)
		}
	}

	// Negative: fourth exceeds limit
	if tr.Allow() {
		t.Fatalf("restart 4 should be denied (limit 3)")
	}
}

func TestRestartTrackerResetsAfterWindow(t *testing.T) {
	// Use a tiny window so we can test expiry
	tr := NewRestartTracker(2, 50*time.Millisecond)

	// Positive: two allowed
	if !tr.Allow() {
		t.Fatalf("restart 1 should be allowed")
	}
	if !tr.Allow() {
		t.Fatalf("restart 2 should be allowed")
	}

	// Negative: third denied
	if tr.Allow() {
		t.Fatalf("restart 3 should be denied")
	}

	// Wait for window to expire
	time.Sleep(60 * time.Millisecond)

	// Positive: allowed again after window expires
	if !tr.Allow() {
		t.Fatalf("restart after window should be allowed")
	}
}

func TestNewRestartTrackerPanicsOnBadArgs(t *testing.T) {
	// Negative: zero limit panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for zero limit")
		}
	}()
	NewRestartTracker(0, time.Minute)
}
