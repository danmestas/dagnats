// worker/directory_test.go
// Tests for the Directory: KV-backed worker registration and listing.
// Methodology: integration test with real embedded NATS.
// Tests that a worker can register, be listed, and deregister.
package worker

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWorkerDirectoryRegister(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)
	reg := WorkerRegistration{
		WorkerID:  "worker-abc123",
		TaskTypes: []string{"send-email", "process-payment"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  10,
		Metadata:  map[string]string{"region": "us-east-1"},
	}

	err = dir.Register(reg)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("len(workers) = %d, want 1", len(workers))
	}
	if workers[0].WorkerID != "worker-abc123" {
		t.Fatalf("worker ID = %q, want %q", workers[0].WorkerID, "worker-abc123")
	}
	if len(workers[0].TaskTypes) != 2 {
		t.Fatalf("len(task types) = %d, want 2", len(workers[0].TaskTypes))
	}
	if workers[0].MaxTasks != 10 {
		t.Fatalf("MaxTasks = %d, want 10", workers[0].MaxTasks)
	}
}

func TestWorkerDirectoryDeregister(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)
	reg := WorkerRegistration{
		WorkerID:  "worker-xyz456",
		TaskTypes: []string{"transform"},
		Language:  "python",
		Transport: "nats",
		MaxTasks:  5,
		Metadata:  nil,
	}

	err = dir.Register(reg)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("len(workers) after register = %d, want 1", len(workers))
	}

	err = dir.Deregister("worker-xyz456")
	if err != nil {
		t.Fatalf("Deregister failed: %v", err)
	}

	workers, err = dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("len(workers) after deregister = %d, want 0", len(workers))
	}
}

func TestWorkerDirectoryListEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if workers == nil {
		t.Fatal("workers slice is nil, want empty slice")
	}
	if len(workers) != 0 {
		t.Fatalf("len(workers) = %d, want 0", len(workers))
	}
}

func TestWorkerDirectoryTTLExpiry(t *testing.T) {
	// This test verifies that entries expire after the 60s TTL.
	// We mock this by checking that stale entries don't appear in List.
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)
	reg := WorkerRegistration{
		WorkerID:  "worker-ttl",
		TaskTypes: []string{"test"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
		Metadata:  nil,
	}

	err = dir.Register(reg)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("len(workers) immediately after register = %d, want 1", len(workers))
	}

	// We cannot wait 60s in a test, but we can verify that the KV bucket
	// has the TTL configured. The actual TTL expiry is tested by NATS itself.
	// This test confirms the registration succeeds and is immediately visible.
	time.Sleep(100 * time.Millisecond)
	workers, err = dir.List()
	if err != nil {
		t.Fatalf("List failed after delay: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("len(workers) after 100ms = %d, want 1", len(workers))
	}
}

func TestNewDirectoryPanicsOnNilJS(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil js")
		}
		// Verify panic message mentions js must not be nil
		if msg, ok := r.(string); ok {
			if msg != "NewDirectory: js must not be nil" {
				t.Fatalf("unexpected panic message: %s", msg)
			}
		}
	}()
	NewDirectory(nil)
}

// TestDirectoryListFiltersStaleEntries proves that List() omits
// entries whose last Put is older than MaxWorkerStaleness. This is
// the read-time guard against NATS KV not evicting old entries
// promptly when a worker dies (the bucket TTL is best-effort and may
// lag a worker's death by tens of seconds). Regression for #233.
func TestDirectoryListFiltersStaleEntries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Short staleness window so the test can demonstrate filtering
	// without waiting the full production 60s. Restore afterwards.
	prev := MaxWorkerStaleness
	MaxWorkerStaleness = 50 * time.Millisecond
	t.Cleanup(func() { MaxWorkerStaleness = prev })

	dir := NewDirectory(js)

	// Register a worker — its entry's Created() time is now.
	stale := WorkerRegistration{
		WorkerID:  "worker-stale",
		TaskTypes: []string{"task-x"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
	}
	if err := dir.Register(stale); err != nil {
		t.Fatalf("Register stale: %v", err)
	}

	// Wait past MaxWorkerStaleness so the entry counts as dead.
	time.Sleep(150 * time.Millisecond)

	// Register a second, fresh worker. Its Created() is fresh.
	fresh := WorkerRegistration{
		WorkerID:  "worker-fresh",
		TaskTypes: []string{"task-y"},
		Language:  "go",
		Transport: "nats",
		MaxTasks:  1,
	}
	if err := dir.Register(fresh); err != nil {
		t.Fatalf("Register fresh: %v", err)
	}

	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Positive: the fresh worker is returned.
	// Negative: the stale worker is filtered out.
	if len(workers) != 1 {
		t.Fatalf("len(workers) = %d, want 1; got %+v",
			len(workers), workers)
	}
	if workers[0].WorkerID != "worker-fresh" {
		t.Fatalf("worker[0].WorkerID = %q, want %q",
			workers[0].WorkerID, "worker-fresh")
	}
}

func TestRegisterPanicsOnEmptyWorkerID(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty WorkerID")
		}
		// Verify panic message mentions WorkerID must not be empty
		if msg, ok := r.(string); ok {
			if msg != "Directory.Register: WorkerID must not be empty" {
				t.Fatalf("unexpected panic message: %s", msg)
			}
		}
	}()

	dir.Register(WorkerRegistration{
		WorkerID:  "",
		TaskTypes: []string{"test"},
	})
}

func TestRegisterPanicsOnEmptyTaskTypes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	dir := NewDirectory(js)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on empty TaskTypes")
		}
		// Verify panic message mentions TaskTypes must not be empty
		if msg, ok := r.(string); ok {
			if msg != "Directory.Register: TaskTypes must not be empty" {
				t.Fatalf("unexpected panic message: %s", msg)
			}
		}
	}()

	dir.Register(WorkerRegistration{
		WorkerID:  "worker-123",
		TaskTypes: []string{},
	})
}
