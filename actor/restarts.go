package actor

import "time"

// RestartTracker enforces a bounded number of restarts within a
// sliding time window. If the limit is exceeded, the tracker
// signals that the actor should be stopped or escalated.
type RestartTracker struct {
	limit    int
	window   time.Duration
	restarts []time.Time
}

// NewRestartTracker creates a tracker allowing limit restarts
// within the given window. Panics if limit < 1.
func NewRestartTracker(limit int, window time.Duration) *RestartTracker {
	if limit < 1 {
		panic("actor: restart limit must be >= 1")
	}
	if window <= 0 {
		panic("actor: restart window must be positive")
	}
	return &RestartTracker{
		limit:    limit,
		window:   window,
		restarts: make([]time.Time, 0, limit),
	}
}

// Allow returns true if a restart is permitted. It prunes expired
// entries, then checks the count against the limit. If allowed,
// it records the restart time.
func (tr *RestartTracker) Allow() bool {
	now := time.Now()
	cutoff := now.Add(-tr.window)

	// Prune expired restarts (iterative, no recursion)
	valid := 0
	for _, t := range tr.restarts {
		if t.After(cutoff) {
			tr.restarts[valid] = t
			valid++
		}
	}
	tr.restarts = tr.restarts[:valid]

	if len(tr.restarts) >= tr.limit {
		return false
	}

	tr.restarts = append(tr.restarts, now)
	return true
}
