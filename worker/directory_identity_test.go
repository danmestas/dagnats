// worker/directory_identity_test.go
// Tests that WorkerRegistration carries identity + heartbeat fields:
// LastSeen, Pid, Hostname, Version. These let `dagnats workers list`
// distinguish processes on the same host and surface heartbeat freshness
// without a separate worker_heartbeats bucket (#289).
//
// Methodology: integration test with real embedded NATS. We register
// via the public Worker.Start path (so production wiring populates the
// new fields) and read the bucket back through Directory.List.
package worker

import (
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestWorkerRegistrationCarriesIdentity asserts the four new fields
// are populated and non-zero after a Worker registers itself.
func TestWorkerRegistrationCarriesIdentity(t *testing.T) {
	start := time.Now()

	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	w := NewWorker(nc)
	w.Handle("identity-task", func(ctx TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Start()
	t.Cleanup(w.Stop)

	// Give the directory write a moment to land. Register is synchronous
	// from inside registerDirectory, so the entry should already be
	// visible, but ListKeys can briefly lag the Put.
	dir := NewDirectory(js)
	var got WorkerRegistration
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		workers, err := dir.List()
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(workers) == 1 {
			got = workers[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.WorkerID == "" {
		t.Fatalf("worker did not register within 2s")
	}

	// Positive assertions: all four identity/heartbeat fields populated.
	if got.Hostname == "" {
		t.Errorf("Hostname is empty; want non-empty")
	}
	if got.Pid <= 0 {
		t.Errorf("Pid = %d; want > 0", got.Pid)
	}
	if got.Version == "" {
		t.Errorf("Version is empty; want non-empty")
	}
	if !got.LastSeen.After(start) {
		t.Errorf("LastSeen = %v; want after start %v",
			got.LastSeen, start)
	}

	// Negative-space cross-check: Pid matches our test process.
	// (Worker.Start runs in-process, so the recorded Pid must be ours.)
	if got.Pid != os.Getpid() {
		t.Errorf("Pid = %d; want %d (current process)",
			got.Pid, os.Getpid())
	}
	hostname, _ := os.Hostname()
	if hostname != "" && got.Hostname != hostname {
		t.Errorf("Hostname = %q; want %q", got.Hostname, hostname)
	}
}

// TestWorkerRegistrationLastSeenUpdates asserts that a subsequent
// Register call (the heartbeat shape) advances LastSeen. We exercise
// Directory.Register directly rather than waiting for the production
// 30s heartbeat tick — same code path, no production seam needed.
func TestWorkerRegistrationLastSeenUpdates(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)
	reg := WorkerRegistration{
		WorkerID:  "worker-lastseen",
		TaskTypes: []string{"t"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
		Hostname:  "host-a",
		Pid:       1234,
		Version:   "test",
	}

	if err := dir.Register(reg); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	first := mustRead(t, dir, reg.WorkerID).LastSeen
	if first.IsZero() {
		t.Fatalf("first LastSeen is zero; want stamped")
	}

	// Ensure the clock advances enough for a fresh stamp to differ.
	time.Sleep(10 * time.Millisecond)

	if err := dir.Register(reg); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	second := mustRead(t, dir, reg.WorkerID).LastSeen

	if !second.After(first) {
		t.Fatalf("LastSeen did not advance: first=%v second=%v",
			first, second)
	}
}

// mustRead pulls the single registration for workerID out of the
// directory; fails the test if it's missing or ambiguous.
func mustRead(
	t *testing.T, dir *Directory, workerID string,
) WorkerRegistration {
	t.Helper()
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, w := range workers {
		if w.WorkerID == workerID {
			return w
		}
	}
	t.Fatalf("worker %q not found among %d entries",
		workerID, len(workers))
	return WorkerRegistration{}
}
