package program

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// AuthRetryWindowDefault is how long a program's connection rides out the
// server refusing its key. A node serves clients before its watch of the
// programs bucket has loaded its program users (the bucket needs
// JetStream, which starts with the node), so a node that restarts refuses
// every program key for a while: about 22 seconds on a 3-node mesh.
const AuthRetryWindowDefault = 2 * time.Minute

// authStreak tracks one connection's run of refusals of its key. nats.go
// gives up after the same authorization error twice; the connection sets
// IgnoreAuthErrorAbort instead and closes itself here once a streak
// outlasts window, so a key that was really removed still ends the
// program. A successful reconnect ends the streak.
type authStreak struct {
	name   string
	window time.Duration

	mu    sync.Mutex
	since time.Time // zero: no streak
}

func newAuthStreak(name string, window time.Duration) *authStreak {
	if name == "" {
		panic("program.newAuthStreak: name must not be empty")
	}
	if window <= 0 {
		window = AuthRetryWindowDefault
	}
	return &authStreak{name: name, window: window}
}

// errorHandler counts a refusal toward the streak, then calls the
// caller's own ErrorHandler, if any.
func (s *authStreak) errorHandler(caller nats.ErrHandler) nats.ErrHandler {
	return func(nc *nats.Conn, sub *nats.Subscription, err error) {
		if sub == nil && errors.Is(err, nats.ErrAuthorization) {
			s.refused(nc, err)
		}
		if caller != nil {
			caller(nc, sub, err)
		}
	}
}

// reconnectedHandler ends the streak, then calls the caller's own
// ReconnectHandler, if any.
func (s *authStreak) reconnectedHandler(caller nats.ConnHandler) nats.ConnHandler {
	return func(nc *nats.Conn) {
		s.mu.Lock()
		s.since = time.Time{}
		s.mu.Unlock()
		if caller != nil {
			caller(nc)
		}
	}
}

func (s *authStreak) refused(nc *nats.Conn, err error) {
	if nc == nil || err == nil {
		panic("program.authStreak.refused: nc and err must be set")
	}
	now := time.Now()
	s.mu.Lock()
	first := s.since.IsZero()
	if first {
		s.since = now
	}
	lasted := now.Sub(s.since)
	s.mu.Unlock()
	if first {
		slog.Warn("program: the server refuses this program's key; retrying, as a "+
			"restarting node does until it loads its program users",
			"program", s.name, "retry_window", s.window, "error", err)
	}
	if lasted < s.window {
		return
	}
	slog.Error("program: the server kept refusing this program's key; closing",
		"program", s.name, "refused_for", lasted, "error", err)
	// Not inline: this runs on the connection's callback goroutine.
	go nc.Close()
}
