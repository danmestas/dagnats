// directory_list_concurrency_test.go
// Tests that Directory.List() enumerates keys consistently while the
// bucket is being written. Methodology: integration test against a
// real embedded NATS server, with hot concurrent writers replaying a
// heartbeat-shaped Put on one key while the main goroutine lists.
//
// Regression cover for the enumeration race behind the intermittent
// failure of bridge.TestHeartbeatStopsAfterOwnershipTakeover: List()
// used kv.ListKeys, whose watcher-built snapshot could omit a key
// whose only revision (history=1) was replaced inside the watcher's
// setup window, so a live worker was reported absent roughly once
// per 2000 calls. This probe is statistical by nature -- it cannot
// fail against correct code, but it only catches a reintroduction
// with high probability, not certainty.
package worker

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// listConcurrencyReads bounds the probe. 1500 reads reproduced the
// pre-fix miss on every one of 10 attempts; the fixed path runs them
// in a few seconds because it no longer creates a consumer per call.
const listConcurrencyReads = 1500

// listConcurrencyWriters is the number of goroutines replaying the
// heartbeat Put. Extra writers past a handful do not raise the miss
// rate -- the window is the listing's, not the writer's.
const listConcurrencyWriters = 4

func TestDirectoryListNeverMissesLiveKey(t *testing.T) {
	dir := newListTestDirectory(t)
	reg := WorkerRegistration{
		WorkerID:  "w1",
		TaskTypes: []string{"echo"},
		TokenID:   "admin",
	}
	if err := dir.RegisterOwned(reg, "admin", true); err != nil {
		t.Fatalf("seed RegisterOwned: %v", err)
	}

	stopWriters := startHeartbeatWriters(dir, reg)
	misses, empties := 0, 0
	for range listConcurrencyReads {
		workers, err := dir.List()
		if err != nil {
			stopWriters()
			t.Fatalf("List: %v", err)
		}
		if len(workers) == 0 {
			empties++
		}
		if !containsWorker(workers, "w1") {
			misses++
		}
	}
	stopWriters()

	// Positive space: the key is registered and never deleted, so
	// every List must report it. Negative space: a List that returned
	// nothing at all is the same defect seen from the other side.
	if misses != 0 {
		t.Fatalf(
			"List() missed live key w1 %d/%d times",
			misses, listConcurrencyReads,
		)
	}
	if empties != 0 {
		t.Fatalf(
			"List() returned an empty directory %d/%d times",
			empties, listConcurrencyReads,
		)
	}
}

// TestDirectoryListEnumeratesDottedKeys pins the key-name handling of
// the subject-derived enumeration: a worker_id containing dots spans
// several subject tokens, so recovering the key means stripping the
// bucket prefix, not taking the last token.
func TestDirectoryListEnumeratesDottedKeys(t *testing.T) {
	dir := newListTestDirectory(t)
	ids := []string{"plain", "host.example.com", "a.b.c.d"}
	for _, id := range ids {
		reg := WorkerRegistration{
			WorkerID:  id,
			TaskTypes: []string{"echo"},
			TokenID:   "admin",
		}
		if err := dir.RegisterOwned(reg, "admin", true); err != nil {
			t.Fatalf("RegisterOwned(%q): %v", id, err)
		}
	}
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, id := range ids {
		if !containsWorker(workers, id) {
			t.Fatalf("List() missing worker %q, got %+v", id, workers)
		}
	}
	// Negative space: no key may come back still carrying the bucket
	// subject prefix, which is what a missing TrimPrefix would leave.
	for _, w := range workers {
		if strings.HasPrefix(w.WorkerID, "$KV.") {
			t.Fatalf("List() returned un-stripped subject %q", w.WorkerID)
		}
	}
	if len(workers) != len(ids) {
		t.Fatalf("List() = %d workers, want %d", len(workers), len(ids))
	}
}

// TestDirectoryListSkipsDeletedKeys pins that a deregistered key --
// whose subject still holds a delete marker, and so still appears in
// the stream's subject state -- is not reported as a live worker.
func TestDirectoryListSkipsDeletedKeys(t *testing.T) {
	dir := newListTestDirectory(t)
	reg := WorkerRegistration{
		WorkerID:  "gone",
		TaskTypes: []string{"echo"},
		TokenID:   "admin",
	}
	if err := dir.RegisterOwned(reg, "admin", true); err != nil {
		t.Fatalf("RegisterOwned: %v", err)
	}
	before, err := dir.List()
	if err != nil {
		t.Fatalf("List before: %v", err)
	}
	if !containsWorker(before, "gone") {
		t.Fatalf("List before deregister missing %q, got %+v", "gone", before)
	}
	if err := dir.DeregisterOwned("gone", "admin", true); err != nil {
		t.Fatalf("DeregisterOwned: %v", err)
	}
	after, err := dir.List()
	if err != nil {
		t.Fatalf("List after: %v", err)
	}
	if containsWorker(after, "gone") {
		t.Fatalf("List after deregister still reports %q: %+v", "gone", after)
	}
}

// newListTestDirectory starts an embedded NATS server and returns a
// Directory backed by its workers bucket.
func newListTestDirectory(t *testing.T) *Directory {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if js == nil {
		t.Fatal("jetstream.New returned nil")
	}
	return NewDirectory(js)
}

// startHeartbeatWriters replays reg's registration from several
// goroutines until the returned stop function is called, which waits
// for every writer to exit before returning.
func startHeartbeatWriters(
	dir *Directory, reg WorkerRegistration,
) func() {
	if dir == nil {
		panic("startHeartbeatWriters: dir must not be nil")
	}
	if reg.WorkerID == "" {
		panic("startHeartbeatWriters: reg.WorkerID must not be empty")
	}
	var stop atomic.Bool
	done := make(chan struct{}, listConcurrencyWriters)
	for range listConcurrencyWriters {
		go func() {
			defer func() { done <- struct{}{} }()
			for !stop.Load() {
				// Errors are the point of the race, not the subject
				// of this test: the reader's view is what is asserted.
				_ = dir.RegisterOwned(reg, "admin", true)
			}
		}()
	}
	return func() {
		stop.Store(true)
		for range listConcurrencyWriters {
			<-done
		}
	}
}

// containsWorker reports whether workers holds an entry for workerID.
func containsWorker(workers []WorkerRegistration, workerID string) bool {
	if workerID == "" {
		panic("containsWorker: workerID must not be empty")
	}
	for _, w := range workers {
		if w.WorkerID == workerID {
			return true
		}
	}
	return false
}
