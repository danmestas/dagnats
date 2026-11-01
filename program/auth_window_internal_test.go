// program/auth_window_internal_test.go
// Tests for riding out a node's program-user gap: a restarting node
// serves before its programs bucket loads, so for a while it refuses the
// program's key. The program connection keeps reconnecting through
// that, and closes only once refusals outlast Options.AuthRetryWindow.
// Methodology: a real embedded server with NKey auth (a placeholder
// user keeps auth on), the program's user removed and restored by
// ReloadOptions, as the node's user set does. The caller's own
// ErrorHandler counts refusals, so each case proves the server refused
// the key at least twice, past nats.go's default abort.
package program

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type authWindowFixture struct {
	server   *natsserver.Server
	opts     *natsserver.Options
	key      Key
	refusals atomic.Int32
}

func startAuthWindowFixture(t *testing.T) *authWindowFixture {
	t.Helper()
	key, err := LoadOrCreateKey(filepath.Join(t.TempDir(), "seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	placeholder, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	placeholderKey, err := placeholder.PublicKey()
	if err != nil {
		t.Fatalf("PublicKey: %v", err)
	}
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, Nkeys: []*natsserver.NkeyUser{
		{Nkey: placeholderKey}, {Nkey: key.PublicKey()},
	}}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	return &authWindowFixture{server: ns, opts: opts, key: key}
}

// setUser reloads the server with the program's user present or not.
func (f *authWindowFixture) setUser(t *testing.T, present bool) {
	t.Helper()
	next := f.opts.Clone()
	next.Nkeys = next.Nkeys[:1]
	if present {
		next.Nkeys = append(next.Nkeys, &natsserver.NkeyUser{Nkey: f.key.PublicKey()})
	}
	if err := f.server.ReloadOptions(next); err != nil {
		t.Fatalf("ReloadOptions: %v", err)
	}
}

func (f *authWindowFixture) dial(t *testing.T, window time.Duration) (*nats.Conn, *Running) {
	t.Helper()
	running := newRunning("toy")
	opts := Options{URL: f.server.ClientURL(), AuthRetryWindow: window,
		NATSOptions: []nats.Option{nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrAuthorization) {
				f.refusals.Add(1)
			}
		})}}
	nc, err := dialAsProgram(opts, "toy", f.key, running)
	if err != nil {
		t.Fatalf("dialAsProgram: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc, running
}

func waitUntil(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s within %s", what, within)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestProgramConnectionRidesOutAnAuthGap(t *testing.T) {
	f := startAuthWindowFixture(t)
	nc, running := f.dial(t, time.Minute)
	f.setUser(t, false)
	waitUntil(t, 15*time.Second, "three refusals of the removed key", func() bool {
		return f.refusals.Load() >= 3
	})
	f.setUser(t, true)
	waitUntil(t, 15*time.Second, "reconnected once the key is back", nc.IsConnected)
	// Negative: the refusals, past nats.go's two-strike abort, never
	// closed the connection.
	select {
	case err := <-running.Done():
		t.Fatalf("connection closed during the gap: %v", err)
	default:
	}
	if nc.Reconnects == 0 {
		t.Fatal("connection never reconnected")
	}
}

func TestProgramConnectionClosesWhenTheGapOutlastsTheWindow(t *testing.T) {
	f := startAuthWindowFixture(t)
	const window = 4 * time.Second
	_, running := f.dial(t, window)
	removed := time.Now()
	f.setUser(t, false)
	select {
	case err := <-running.Done():
		// Positive: it closed for good, naming the refusal.
		if !errors.Is(err, nats.ErrAuthorization) {
			t.Fatalf("Done = %v, want an authorization error", err)
		}
		// Negative: not before the window ran out.
		if elapsed := time.Since(removed); elapsed < window {
			t.Fatalf("closed after %s, before the %s window", elapsed, window)
		}
	case <-time.After(window + 15*time.Second):
		t.Fatal("connection still open long after the window")
	}
	if f.refusals.Load() < 2 {
		t.Fatalf("server refused the key %d times, want at least 2", f.refusals.Load())
	}
}

// TestAuthStreakResetsOnReconnect: a reconnect ends a streak, so two
// short gaps never add up to the window; a streak is measured from its
// first refusal.
func TestAuthStreakResetsOnReconnect(t *testing.T) {
	f := startAuthWindowFixture(t)
	nc, _ := f.dial(t, time.Minute)
	streak := newAuthStreak("toy", time.Minute)
	refuse := streak.errorHandler(nil)
	refuse(nc, nil, nats.ErrAuthorization)
	streak.mu.Lock()
	started := streak.since
	streak.mu.Unlock()
	refuse(nc, nil, nats.ErrAuthorization)
	streak.mu.Lock()
	again := streak.since
	streak.mu.Unlock()
	// Positive: the first refusal starts the streak, a second keeps it.
	if started.IsZero() || !again.Equal(started) {
		t.Fatalf("streak since %v then %v, want started once", started, again)
	}
	streak.reconnectedHandler(nil)(nc)
	streak.mu.Lock()
	defer streak.mu.Unlock()
	// Negative: a reconnect ends it.
	if !streak.since.IsZero() {
		t.Fatalf("streak since %v after a reconnect, want none", streak.since)
	}
}
