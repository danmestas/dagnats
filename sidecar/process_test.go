// Methodology: integration tests using real subprocesses (sleep, sh)
// to verify child process lifecycle management. All tests use bounded
// timeouts to prevent hangs.

package sidecar

import (
	"errors"
	"testing"
	"time"
)

func TestProcess_StartStop(t *testing.T) {
	t.Parallel()

	p := &Process{
		Name: "test-sleep",
		Bin:  "sleep",
		Args: []string{"60"},
	}

	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Positive: process should be running after start.
	if !p.IsRunning() {
		t.Fatal("expected process to be running after Start")
	}

	if err := p.Stop(5 * time.Second); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Negative: process should not be running after stop.
	if p.IsRunning() {
		t.Fatal("expected process to be stopped after Stop")
	}
}

func TestProcess_StopTimeout(t *testing.T) {
	t.Parallel()

	// This process traps SIGTERM and ignores it, forcing
	// the stop logic to escalate to SIGKILL.
	p := &Process{
		Name: "test-trap",
		Bin:  "sh",
		Args: []string{
			"-c", `trap "" TERM; sleep 60`,
		},
	}

	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the shell time to install the SIGTERM trap.
	time.Sleep(200 * time.Millisecond)

	// Positive: process is running.
	if !p.IsRunning() {
		t.Fatal("expected process to be running")
	}

	start := time.Now()
	if err := p.Stop(500 * time.Millisecond); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	elapsed := time.Since(start)

	// Negative: process was killed despite trapping SIGTERM.
	if p.IsRunning() {
		t.Fatal("expected process to be killed after timeout")
	}

	// Stop should have waited ~500ms for SIGTERM then
	// escalated to SIGKILL. Verify it took at least the
	// timeout (SIGTERM was ignored) but not too long.
	if elapsed < 400*time.Millisecond {
		t.Fatalf(
			"Stop too fast (%v), SIGTERM may not be trapped",
			elapsed,
		)
	}
	if elapsed > 10*time.Second {
		t.Fatalf(
			"Stop took %v, expected around 500ms", elapsed,
		)
	}
}

func TestProcess_HealthCheck(t *testing.T) {
	t.Parallel()

	healthCalled := false
	healthErr := errors.New("unhealthy")

	p := &Process{
		Name: "test-health",
		Bin:  "sleep",
		Args: []string{"60"},
		Health: func() error {
			healthCalled = true
			return healthErr
		},
	}

	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = p.Stop(5 * time.Second)
	}()

	err := p.Healthy()

	// Positive: Health func was called.
	if !healthCalled {
		t.Fatal("expected Health func to be called")
	}

	// Negative: Healthy returns the error from Health func.
	if err != healthErr {
		t.Fatalf(
			"expected healthErr, got %v", err,
		)
	}
}

func TestProcess_HealthCheck_DefaultAlive(t *testing.T) {
	t.Parallel()

	p := &Process{
		Name: "test-health-default",
		Bin:  "sleep",
		Args: []string{"60"},
	}

	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = p.Stop(5 * time.Second)
	}()

	// Positive: healthy when running and no Health func.
	if err := p.Healthy(); err != nil {
		t.Fatalf("expected healthy, got %v", err)
	}

	_ = p.Stop(5 * time.Second)

	// Negative: unhealthy when not running.
	if err := p.Healthy(); err == nil {
		t.Fatal("expected error when process not running")
	}
}

func TestProcess_RestartBackoff(t *testing.T) {
	t.Parallel()

	// "true" exits immediately with code 0, causing the
	// process to not be running when restart checks.
	p := &Process{
		Name: "test-backoff",
		Bin:  "sh",
		Args: []string{"-c", "exit 1"},
	}

	// First restart: backoff should be ~1s.
	start := time.Now()
	err := p.RestartWithBackoff()
	elapsed := time.Since(start)

	// The process exits immediately so Start succeeds but
	// the process won't stay running. That's fine — we're
	// testing the backoff timing.
	if err != nil {
		t.Fatalf("first restart failed: %v", err)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf(
			"first backoff too short: %v", elapsed,
		)
	}

	// Second restart: backoff should be ~2s.
	start = time.Now()
	err = p.RestartWithBackoff()
	elapsed = time.Since(start)

	if err != nil {
		t.Fatalf("second restart failed: %v", err)
	}

	// Positive: backoff increased.
	if elapsed < 1900*time.Millisecond {
		t.Fatalf(
			"second backoff too short: %v", elapsed,
		)
	}

	// Negative: backoff didn't exceed expected cap.
	if elapsed > 5*time.Second {
		t.Fatalf(
			"second backoff too long: %v", elapsed,
		)
	}
}

func TestProcess_MaxFailures(t *testing.T) {
	t.Parallel()

	p := &Process{
		Name: "test-maxfail",
		Bin:  "sh",
		Args: []string{"-c", "exit 1"},
	}

	// Exhaust all 5 allowed failures.
	for i := range maxConsecutiveFailures {
		err := p.RestartWithBackoff()
		if err != nil {
			t.Fatalf(
				"restart %d should succeed, got: %v",
				i+1, err,
			)
		}
	}

	// The 6th attempt should be rejected.
	err := p.RestartWithBackoff()

	// Positive: error returned after max failures.
	if err == nil {
		t.Fatal(
			"expected error after max consecutive failures",
		)
	}

	// Negative: error message mentions the process name.
	if !containsSubstring(err.Error(), "test-maxfail") {
		t.Fatalf(
			"error should mention process name, got: %v", err,
		)
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) &&
		findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
