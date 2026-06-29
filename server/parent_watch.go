package server

import (
	"log/slog"
	"os"
	"time"
)

// parentWatchInterval is how often the die-with-parent watcher polls
// getppid. ~500ms is responsive enough for a sidecar reaper while
// costing essentially nothing — macOS has no PR_SET_PDEATHSIG, so a
// poll is the portable mechanism (#476).
const parentWatchInterval = 500 * time.Millisecond

// shouldWatchParent decides whether the die-with-parent watcher should
// run given the process's parent PID captured at startup. A startPpid
// <= 1 means there is no real parent to outlive (the process was
// launched directly by init/launchd, or getppid is unavailable), so the
// flag is meaningless and the watcher must not start — otherwise it
// would treat the absent parent as "already dead" and exit immediately.
func shouldWatchParent(startPpid int) bool {
	return startPpid > 1
}

// watchParentDeath polls getppid on a bounded ticker and calls onGone
// exactly once when the parent PID no longer matches startPpid (the
// parent died and we were reparented to init or a subreaper), then
// returns. Using "!= startPpid" rather than "== 1" catches reparenting
// to a Linux subreaper as well as to PID 1. Returns immediately if done
// is already closed, and stops promptly when done closes mid-loop
// without firing onGone. Pure and injectable for testing: the Server
// wires getppid = os.Getppid, onGone = s.Stop, done = s.stopCh.
func watchParentDeath(
	getppid func() int, startPpid int, interval time.Duration,
	onGone func(), done <-chan struct{},
) {
	if getppid == nil {
		panic("watchParentDeath: getppid must not be nil")
	}
	if onGone == nil {
		panic("watchParentDeath: onGone must not be nil")
	}
	if interval <= 0 {
		panic("watchParentDeath: interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if getppid() != startPpid {
				onGone()
				return
			}
		}
	}
}

// startParentWatch launches the die-with-parent watcher goroutine when
// the server is configured for it (#476). It captures the parent PID
// once, applies the no-real-parent guard, and — when armed — wires the
// pure watcher to os.Getppid / s.Stop / s.stopCh so the goroutine exits
// cleanly on normal shutdown (when stopCh closes) instead of leaking.
func (s *Server) startParentWatch() {
	if s == nil {
		panic("startParentWatch: s is nil")
	}
	if s.stopCh == nil {
		panic("startParentWatch: stopCh is nil")
	}

	startPpid := os.Getppid()
	if !shouldWatchParent(startPpid) {
		slog.Warn(
			"die-with-parent requested but the process has no real "+
				"parent to outlive; watcher disabled",
			"start_ppid", startPpid)
		return
	}

	go watchParentDeath(
		os.Getppid, startPpid, parentWatchInterval, s.Stop, s.stopCh)
}
