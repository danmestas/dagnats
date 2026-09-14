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
	"context"
	"errors"
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
	hot := WorkerRegistration{
		WorkerID:  "w1",
		TaskTypes: []string{"echo"},
		TokenID:   "admin",
	}
	if err := dir.RegisterOwned(hot, "admin", true); err != nil {
		t.Fatalf("seed RegisterOwned hot: %v", err)
	}
	// A second worker registered once and never written again. It
	// makes the empty-listing and quiet-key counters independent of
	// the hot key: with only one registration, "list came back empty"
	// and "list missed w1" would be the same event counted twice.
	quiet := WorkerRegistration{
		WorkerID:  "w2",
		TaskTypes: []string{"echo"},
		TokenID:   "admin",
	}
	if err := dir.RegisterOwned(quiet, "admin", true); err != nil {
		t.Fatalf("seed RegisterOwned quiet: %v", err)
	}

	stopWriters := startHeartbeatWriters(dir, hot)
	hotMisses, quietMisses, empties := 0, 0, 0
	for range listConcurrencyReads {
		workers, err := dir.List()
		if err != nil {
			if stopErr := stopWriters(); stopErr != nil {
				t.Errorf("writer: %v", stopErr)
			}
			t.Fatalf("List: %v", err)
		}
		if len(workers) == 0 {
			empties++
		}
		if !containsWorker(workers, "w1") {
			hotMisses++
		}
		if !containsWorker(workers, "w2") {
			quietMisses++
		}
	}
	if err := stopWriters(); err != nil {
		t.Fatalf("writer RegisterOwned: %v", err)
	}

	// Positive space: both keys are registered and never deleted, so
	// every List must report both. Negative space: the enumeration
	// snapshot covers the whole bucket, so a racing write to w1 must
	// not drop the untouched w2 either, and no List may come back
	// empty while two workers are registered.
	if hotMisses != 0 {
		t.Fatalf(
			"List() missed live key w1 %d/%d times",
			hotMisses, listConcurrencyReads,
		)
	}
	if quietMisses != 0 {
		t.Fatalf(
			"List() missed untouched key w2 %d/%d times",
			quietMisses, listConcurrencyReads,
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

// errGetBroke stands in for a transient KV Get failure (timeout,
// disconnect) that is not a missing key.
var errGetBroke = errors.New("kv get broke")

// stubKV serves listKeys' bucket name and fails every Get. Embedding
// the interface leaves every other method unimplemented on purpose:
// List() must not call them, and a nil-panic says so loudly if it
// ever starts.
type stubKV struct {
	jetstream.KeyValue
	getErr error
}

func (s stubKV) Bucket() string { return "workers" }

func (s stubKV) Get(
	_ context.Context, _ string,
) (jetstream.KeyValueEntry, error) {
	return nil, s.getErr
}

// stubStream reports one subject so List() has exactly one key to Get.
type stubStream struct {
	jetstream.Stream
}

func (s stubStream) Info(
	_ context.Context, _ ...jetstream.StreamInfoOpt,
) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{
		State: jetstream.StreamState{
			Subjects: map[string]uint64{"$KV.workers.w1": 1},
		},
	}, nil
}

// TestDirectoryListPropagatesGetError pins that a Get failure which
// is NOT ErrKeyNotFound fails the call instead of being swallowed.
// Silently skipping it would drop a live worker from the listing --
// indistinguishable, to the caller, from the enumeration bug this
// file exists to cover.
func TestDirectoryListPropagatesGetError(t *testing.T) {
	dir := &Directory{
		kv:     stubKV{getErr: errGetBroke},
		stream: stubStream{},
	}
	workers, err := dir.List()
	if !errors.Is(err, errGetBroke) {
		t.Fatalf("List() error = %v, want %v", err, errGetBroke)
	}
	// Negative space: a propagated failure must not also hand back a
	// partial list a caller might use.
	if workers != nil {
		t.Fatalf("List() workers = %+v, want nil on error", workers)
	}
}

// TestDirectoryListSkipsNotFoundKeys pins the other side of that
// branch: a delete-marker subject yields ErrKeyNotFound from Get and
// is skipped silently, without failing the whole listing.
func TestDirectoryListSkipsNotFoundKeys(t *testing.T) {
	dir := &Directory{
		kv:     stubKV{getErr: jetstream.ErrKeyNotFound},
		stream: stubStream{},
	}
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List() error = %v, want nil", err)
	}
	if len(workers) != 0 {
		t.Fatalf("List() = %+v, want no workers", workers)
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
// for every writer to exit and then reports the first write error
// that was not plain contention. ErrWorkerIDContended is expected
// here by construction -- these writers deliberately collide on one
// key -- so it is the one error the caller may ignore.
func startHeartbeatWriters(
	dir *Directory, reg WorkerRegistration,
) func() error {
	if dir == nil {
		panic("startHeartbeatWriters: dir must not be nil")
	}
	if reg.WorkerID == "" {
		panic("startHeartbeatWriters: reg.WorkerID must not be empty")
	}
	var stop atomic.Bool
	var firstErr atomic.Value
	done := make(chan struct{}, listConcurrencyWriters)
	for range listConcurrencyWriters {
		go func() {
			defer func() { done <- struct{}{} }()
			for !stop.Load() {
				err := dir.RegisterOwned(reg, "admin", true)
				if err != nil &&
					!errors.Is(err, ErrWorkerIDContended) {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	return func() error {
		stop.Store(true)
		for range listConcurrencyWriters {
			<-done
		}
		if err, ok := firstErr.Load().(error); ok {
			return err
		}
		return nil
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
