// Methodology: pure unit tests for the parent-death watcher (#476).
// The watcher is factored as a pure function driven by an injected
// getppid func and a done channel, so these tests assert its loop
// behavior (fires once / never / returns promptly) with no NATS and
// no real process tree. The start-guard decision is tested separately.
package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWatchParentDeath_FiresOnceOnChange drives getppid so it first
// reports the captured startPpid and then a changed value; onGone must
// fire exactly once and the watcher must return.
func TestWatchParentDeath_FiresOnceOnChange(t *testing.T) {
	const startPpid = 4242
	var calls int64
	var ppid atomic.Int64
	ppid.Store(startPpid)

	getppid := func() int { return int(ppid.Load()) }
	onGone := func() { atomic.AddInt64(&calls, 1) }
	done := make(chan struct{})

	finished := make(chan struct{})
	go func() {
		watchParentDeath(getppid, startPpid, 5*time.Millisecond, onGone, done)
		close(finished)
	}()

	// Let it tick a few times with the parent still present.
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("onGone fired %d times before parent changed; want 0", got)
	}

	// Simulate reparenting: parent pid changes.
	ppid.Store(1)

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not return after parent changed")
	}

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("onGone fired %d times; want exactly 1", got)
	}
}

// TestWatchParentDeath_DoneBeforeChange closes done before the parent
// pid ever changes; onGone must never fire and the watcher must return
// promptly.
func TestWatchParentDeath_DoneBeforeChange(t *testing.T) {
	const startPpid = 99
	var calls int64

	getppid := func() int { return startPpid } // never changes
	onGone := func() { atomic.AddInt64(&calls, 1) }
	done := make(chan struct{})
	close(done)

	finished := make(chan struct{})
	go func() {
		watchParentDeath(getppid, startPpid, 5*time.Millisecond, onGone, done)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not return promptly when done closed")
	}

	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("onGone fired %d times after done closed; want 0", got)
	}
}

// TestWatchParentDeath_ReturnsPromptlyOnDoneAfterStart confirms a
// watcher already looping exits when done closes mid-flight without
// ever firing onGone (parent stays put the whole time).
func TestWatchParentDeath_ReturnsPromptlyOnDoneAfterStart(t *testing.T) {
	const startPpid = 7
	var calls int64
	getppid := func() int { return startPpid }
	onGone := func() { atomic.AddInt64(&calls, 1) }
	done := make(chan struct{})

	finished := make(chan struct{})
	go func() {
		watchParentDeath(getppid, startPpid, 5*time.Millisecond, onGone, done)
		close(finished)
	}()

	time.Sleep(20 * time.Millisecond)
	close(done)

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not return after done closed mid-loop")
	}
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("onGone fired %d times; want 0", got)
	}
}

// TestWatchParentDeath_PanicsOnNilOnGone asserts the input contract:
// a nil onGone is a programmer error.
func TestWatchParentDeath_PanicsOnNilOnGone(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil onGone, got none")
		}
	}()
	watchParentDeath(func() int { return 1 }, 1, time.Millisecond, nil, make(chan struct{}))
}

// TestShouldWatchParent_GuardsAgainstNoRealParent verifies the
// start-decision helper: a startPpid <= 1 means the process has no real
// parent (launched by init/launchd), so the watcher is a no-op and must
// not start. A real parent pid (> 1) enables it.
func TestShouldWatchParent_GuardsAgainstNoRealParent(t *testing.T) {
	if shouldWatchParent(0) {
		t.Error("shouldWatchParent(0) = true; want false (no real parent)")
	}
	if shouldWatchParent(1) {
		t.Error("shouldWatchParent(1) = true; want false (parent is init)")
	}
	if !shouldWatchParent(2) {
		t.Error("shouldWatchParent(2) = false; want true (real parent)")
	}
	if !shouldWatchParent(54321) {
		t.Error("shouldWatchParent(54321) = false; want true (real parent)")
	}
}

// guard against an unused import if the file evolves.
var _ = sync.Once{}
