// subprocess_retry_test.go provides a bounded-retry runner for the
// browser-driven smoke tests in this package, plus its unit tests.
//
// Why this exists (issue #573): the print smoke test drives a real
// headless-Chrome subprocess. Under full-suite `-p 4` load the Chrome
// process is intermittently killed (`signal: killed`) — either the OOM
// killer reaps it under memory pressure, or the per-attempt deadline
// fires because Chrome's startup+render latency inflates ~5x under
// contention (measured 3s isolated -> 14s+ under load). Both failures
// are transient: the contention spike passes. A single blind timeout
// bump can't rescue an OOM-killed process (it's already gone) and would
// slow the isolated case; a bounded retry with backoff absorbs the
// transient without masking a genuinely-broken Chrome — after
// attemptsMax failures we still surface the last error.
//
// Methodology for the unit tests below:
//   - Drive the retry runner with `/usr/bin/true` and `/usr/bin/false`
//     (resolved via PATH) so no browser is required. A closure counter
//     asserts the exact attempt count for success-first, fail-then-
//     succeed, and always-fail paths.
//   - Bounded: tiny backoff + attempt caps keep every case sub-second.
//   - Min 2 assertions per test (invocation count + error/nil).
package console

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"
)

// runSubprocessWithRetry runs newCmd() up to attemptsMax times, giving each
// attempt its own perAttemptTimeout derived from parent, and sleeping backoff
// between failed attempts. It returns the first successful attempt's captured
// stdout/stderr with a nil error, or — after all attempts fail — the last
// attempt's output and error wrapped with the attempt count. The retry
// absorbs the transient `signal: killed` failures the headless-Chrome smoke
// tests hit under full-suite parallel load (OOM reap or deadline blown by
// contention-inflated startup latency), both of which clear once the spike
// passes. newCmd must NOT set Stdout/Stderr — the runner owns fresh buffers
// each attempt so the caller can read them.
//
// Bounded (TigerStyle): attemptsMax is a hard cap on iterations; each attempt
// is bounded by perAttemptTimeout; the parent context is an absolute ceiling
// that short-circuits the backoff wait — no unbounded loop or wait exists.
func runSubprocessWithRetry(
	parent context.Context,
	attemptsMax int,
	perAttemptTimeout time.Duration,
	backoff time.Duration,
	newCmd func(ctx context.Context) *exec.Cmd,
) (stdout string, stderr string, err error) {
	if attemptsMax < 1 {
		panic("runSubprocessWithRetry: attemptsMax must be >= 1")
	}
	if perAttemptTimeout <= 0 {
		panic("runSubprocessWithRetry: perAttemptTimeout must be > 0")
	}
	if newCmd == nil {
		panic("runSubprocessWithRetry: newCmd must not be nil")
	}

	var lastOut, lastErr bytes.Buffer
	var lastRunErr error
	for attempt := 1; attempt <= attemptsMax; attempt++ {
		attemptCtx, cancel := context.WithTimeout(parent, perAttemptTimeout)
		lastOut.Reset()
		lastErr.Reset()
		cmd := newCmd(attemptCtx)
		cmd.Stdout = &lastOut
		cmd.Stderr = &lastErr
		lastRunErr = cmd.Run()
		cancel()
		if lastRunErr == nil {
			return lastOut.String(), lastErr.String(), nil
		}
		if attempt == attemptsMax {
			break
		}
		// Wait out the contention spike, but let a cancelled parent
		// abandon the retry loop immediately rather than burn attempts.
		select {
		case <-parent.Done():
			return lastOut.String(), lastErr.String(),
				fmt.Errorf("attempt %d/%d: %w (parent: %v)",
					attempt, attemptsMax, lastRunErr, parent.Err())
		case <-time.After(backoff):
		}
	}
	return lastOut.String(), lastErr.String(),
		fmt.Errorf("all %d attempts failed: %w", attemptsMax, lastRunErr)
}

func TestRunSubprocessWithRetry_succeedsFirstAttempt(t *testing.T) {
	var calls int
	newCmd := func(ctx context.Context) *exec.Cmd {
		calls++
		return exec.CommandContext(ctx, "true")
	}
	_, _, err := runSubprocessWithRetry(
		context.Background(), 3, time.Second, 10*time.Millisecond, newCmd,
	)
	if err != nil {
		t.Fatalf("expected success, got err: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 invocation, got %d", calls)
	}
}

func TestRunSubprocessWithRetry_failThenSucceed(t *testing.T) {
	var calls int
	newCmd := func(ctx context.Context) *exec.Cmd {
		calls++
		if calls < 3 {
			return exec.CommandContext(ctx, "false")
		}
		return exec.CommandContext(ctx, "true")
	}
	start := time.Now()
	_, _, err := runSubprocessWithRetry(
		context.Background(), 3, time.Second, 20*time.Millisecond, newCmd,
	)
	if err != nil {
		t.Fatalf("expected eventual success, got err: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 invocations, got %d", calls)
	}
	// Two backoffs of 20ms separated the three attempts.
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("expected >=40ms elapsed from 2 backoffs, got %v", elapsed)
	}
}

func TestRunSubprocessWithRetry_exhaustsAttempts(t *testing.T) {
	var calls int
	newCmd := func(ctx context.Context) *exec.Cmd {
		calls++
		return exec.CommandContext(ctx, "false")
	}
	_, _, err := runSubprocessWithRetry(
		context.Background(), 3, time.Second, 10*time.Millisecond, newCmd,
	)
	if err == nil {
		t.Fatalf("expected error after exhausting attempts, got nil")
	}
	if calls != 3 {
		t.Fatalf("expected 3 invocations before giving up, got %d", calls)
	}
}

func TestRunSubprocessWithRetry_parentCancelStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	newCmd := func(c context.Context) *exec.Cmd {
		calls++
		cancel() // cancel after the first attempt starts
		return exec.CommandContext(c, "false")
	}
	_, _, err := runSubprocessWithRetry(
		ctx, 5, time.Second, time.Second, newCmd,
	)
	if err == nil {
		t.Fatalf("expected error when parent ctx cancelled, got nil")
	}
	// Parent cancellation must stop the retry loop early rather than
	// burning all 5 attempts against a dead context.
	if calls >= 5 {
		t.Fatalf("expected early stop on cancel, got %d invocations", calls)
	}
}
