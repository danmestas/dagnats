// Methodology: The restart decision is a pure function, tested over its
// truth table. The monitor's JetStream check runs against a faked disk
// (availableDiskBytesFn) and a faked JetStream probe, with time passed
// into check(), so it needs no NATS. The integration tests boot a real
// Server with a short disk check interval, turn JetStream off the way
// nats-server's out-of-space path does (ShutdownJetStream), and assert
// Run returns ErrJetStreamDisabled within a bounded wait; the negative
// case keeps JetStream on and asserts Run keeps running until Stop.

package server

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestShouldRestartForJetStream(t *testing.T) {
	cases := []struct {
		name                        string
		configured, enabled, diskOK bool
		want                        bool
	}{
		{"enabled, disk ok", true, true, true, false},
		{"enabled, disk low", true, true, false, false},
		{"disabled, disk low", true, false, false, false},
		{"disabled, disk ok", true, false, true, true},
		{"not configured", false, false, true, false},
	}
	for _, c := range cases {
		got := shouldRestartForJetStream(c.configured, c.enabled, c.diskOK)
		if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func newJetStreamMonitor(
	t *testing.T, free *int64, enabled *bool,
) (*diskMonitor, *bytes.Buffer, *int) {
	t.Helper()
	m, buf := newTestMonitor(t, free)
	lost := 0
	m.watchJetStream(true, func() bool { return *enabled }, func() { lost++ })
	return m, buf, &lost
}

func TestDiskMonitor_JetStreamDisabledWaitsForDisk(t *testing.T) {
	free, enabled := 3*gib, false
	m, buf, lost := newJetStreamMonitor(t, &free, &enabled)

	m.check(time.Unix(1, 0))
	m.check(time.Unix(61, 0))
	if *lost != 0 {
		t.Fatalf("restart requested while the disk is low: %d", *lost)
	}
	if n := strings.Count(buf.String(), "JetStream was disabled"); n != 2 {
		t.Fatalf("disabled logged %d times on a low disk, want every tick (2): %q",
			n, buf.String())
	}

	free = 100 * gib
	m.check(time.Unix(121, 0))
	if *lost != 1 {
		t.Fatalf("restart requests after disk recovery = %d, want 1", *lost)
	}
	if !strings.Contains(buf.String(), "exiting so the supervisor restarts it") {
		t.Errorf("exit not logged: %q", buf.String())
	}
}

func TestDiskMonitor_JetStreamEnabledNeverRestarts(t *testing.T) {
	free, enabled := 100*gib, true
	m, buf, lost := newJetStreamMonitor(t, &free, &enabled)
	m.check(time.Unix(1, 0))
	free = 1 * gib
	m.check(time.Unix(61, 0))
	if *lost != 0 {
		t.Fatalf("restart requested with JetStream enabled: %d", *lost)
	}
	if strings.Contains(buf.String(), "JetStream was disabled") {
		t.Errorf("enabled JetStream logged as disabled: %q", buf.String())
	}
}

func TestDiskMonitor_ProbeErrorSkipsJetStreamCheck(t *testing.T) {
	free, enabled := 100*gib, false
	m, _, lost := newJetStreamMonitor(t, &free, &enabled)
	availableDiskBytesFn = func(string) (int64, error) { return 0, errors.New("boom") }
	m.check(time.Unix(1, 0))
	if *lost != 0 {
		t.Fatalf("restart requested without a disk reading: %d", *lost)
	}
}

// runRecoveryServer boots a Server on a roomy fake disk with a short
// check interval and returns it with the channel Run reports into.
func runRecoveryServer(t *testing.T) (*Server, chan error) {
	t.Helper()
	origProbe, origInterval := availableDiskBytesFn, diskCheckInterval
	availableDiskBytesFn = func(string) (int64, error) { return 1 << 40, nil }
	diskCheckInterval = 50 * time.Millisecond
	t.Cleanup(func() {
		availableDiskBytesFn, diskCheckInterval = origProbe, origInterval
	})
	srv := New(testConfig(t))
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	waitHTTPAddr(t, srv)
	return srv, errCh
}

func TestServer_RunReturnsErrJetStreamDisabled(t *testing.T) {
	srv, errCh := runRecoveryServer(t)
	defer srv.Stop()

	if err := srv.ns.ShutdownJetStream(); err != nil {
		t.Fatalf("shutdown jetstream: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrJetStreamDisabled) {
			t.Fatalf("Run() = %v, want ErrJetStreamDisabled", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s of JetStream turning off")
	}
	if srv.ns.Running() {
		t.Errorf("embedded NATS still running after Run returned")
	}
}

func TestServer_RunKeepsRunningWithJetStreamEnabled(t *testing.T) {
	srv, errCh := runRecoveryServer(t)
	select {
	case err := <-errCh:
		t.Fatalf("Run() returned with JetStream enabled: %v", err)
	case <-time.After(10 * diskCheckInterval):
	}
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() after Stop = %v, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s of Stop")
	}
}
