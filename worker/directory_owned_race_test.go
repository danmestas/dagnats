// directory_owned_race_test.go
// Tests for the revision-conflict-vs-ownership race in RegisterOwned
// and DeregisterOwned: a revision conflict caused by a write from the
// SAME owner (a concurrent heartbeat re-register, or this
// connection's own deregister on disconnect) is not an ownership
// decision and must not surface as ErrWorkerIDOwned. Methodology:
// real embedded NATS server via natsutil.StartTestServer; the
// package-private *TestHook func vars let a test inject a write
// between RegisterOwned/DeregisterOwned's Get and its guarded write,
// deterministically reproducing the race instead of relying on
// timing. The injected write goes straight through the raw KV
// handle (not RegisterOwned/DeregisterOwned) so it never re-triggers
// the hook it was called from. Each test opens its own server/bucket
// (no sharing).
package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// rawReregister performs a same-shape re-registration write directly
// through the KV handle, bypassing RegisterOwned entirely so it can
// be used inside registerOwnedTestHook/deregisterOwnedTestHook
// without re-triggering the hook it is called from. Simulates
// whatever concurrent writer (heartbeat, admin takeover, etc.) is
// landing in the Get-to-write window under test.
func rawReregister(
	t *testing.T, kv jetstream.KeyValue, workerID, tokenID string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reg := WorkerRegistration{
		WorkerID:  workerID,
		TaskTypes: []string{"echo"},
		TokenID:   tokenID,
		LastSeen:  time.Now(),
	}
	data, err := json.Marshal(reg)
	if err != nil {
		t.Fatalf("marshal raw reregister: %v", err)
	}
	if _, err := kv.Put(ctx, workerID, data); err != nil {
		t.Fatalf("raw reregister Put: %v", err)
	}
}

// TestRegisterOwnedSameOwnerRaceRetries pins the fix: a revision
// conflict from a concurrent write by the SAME owner (e.g. this
// connection's own heartbeat re-register landing between our Get and
// our Update) must not be treated as an ownership violation --
// RegisterOwned must re-Get, see the fresh entry is still owned by
// the same caller, and retry the write to success.
func TestRegisterOwnedSameOwnerRaceRetries(t *testing.T) {
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
		WorkerID:  "race-w1",
		TaskTypes: []string{"echo"},
		TokenID:   "tok-a",
	}
	if err := dir.RegisterOwned(reg, "tok-a", false); err != nil {
		t.Fatalf("initial RegisterOwned: %v", err)
	}

	raceFired := false
	registerOwnedTestHook = func() {
		if raceFired {
			return
		}
		raceFired = true
		// Same owner ("tok-a") writes concurrently -- simulates a
		// heartbeat re-register landing between our Get and Update.
		rawReregister(t, dir.kv, "race-w1", "tok-a")
	}
	t.Cleanup(func() { registerOwnedTestHook = nil })

	err = dir.RegisterOwned(reg, "tok-a", false)
	if err != nil {
		t.Fatalf(
			"RegisterOwned after same-owner race = %v, want nil (same"+
				" owner revision conflict must not be ErrWorkerIDOwned)",
			err,
		)
	}
	if !raceFired {
		t.Fatalf("test hook never fired -- race was not exercised")
	}
}

// TestRegisterOwnedDifferentOwnerRaceRejects pins the flip side: a
// revision conflict caused by a DIFFERENT owner's write (a genuine
// takeover landing in the window) must still be rejected with
// ErrWorkerIDOwned once the retry re-Gets and sees the fresh owner.
func TestRegisterOwnedDifferentOwnerRaceRejects(t *testing.T) {
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
		WorkerID:  "race-w2",
		TaskTypes: []string{"echo"},
		TokenID:   "tok-a",
	}
	if err := dir.RegisterOwned(reg, "tok-a", false); err != nil {
		t.Fatalf("initial RegisterOwned: %v", err)
	}

	raceFired := false
	registerOwnedTestHook = func() {
		if raceFired {
			return
		}
		raceFired = true
		// Admin takeover lands in the window -- a genuine different
		// owner, must still be enforced after the retry.
		rawReregister(t, dir.kv, "race-w2", AdminTokenID)
	}
	t.Cleanup(func() { registerOwnedTestHook = nil })

	err = dir.RegisterOwned(reg, "tok-a", false)
	if err == nil {
		t.Fatalf(
			"RegisterOwned after different-owner race = nil, want" +
				" ErrWorkerIDOwned",
		)
	}
	if err != ErrWorkerIDOwned {
		t.Fatalf(
			"RegisterOwned after different-owner race = %v, want"+
				" ErrWorkerIDOwned", err,
		)
	}
	if !raceFired {
		t.Fatalf("test hook never fired -- race was not exercised")
	}
}

// TestRegisterOwnedCreateConflictDifferentOwnerRejects pins the
// Create-side (not Update-side) counterpart of the different-owner
// case: workerID has never been registered, so the first attempt
// takes the Create branch of registerOwnedAttempt; a different owner
// wins the race to create it first, our Create gets ErrKeyExists,
// and the retry's fresh Get must see that owner and reject with
// ErrWorkerIDOwned -- not silently succeed or misreport contention.
func TestRegisterOwnedCreateConflictDifferentOwnerRejects(t *testing.T) {
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
		WorkerID:  "race-w5",
		TaskTypes: []string{"echo"},
		TokenID:   "tok-a",
	}

	raceFired := false
	registerOwnedTestHook = func() {
		if raceFired {
			return
		}
		raceFired = true
		// race-w5 has never been registered -- our own attempt is
		// about to take the Create branch. A different token wins
		// the race to create it first.
		rawReregister(t, dir.kv, "race-w5", "tok-other")
	}
	t.Cleanup(func() { registerOwnedTestHook = nil })

	err = dir.RegisterOwned(reg, "tok-a", false)
	if err != ErrWorkerIDOwned {
		t.Fatalf(
			"RegisterOwned after create-conflict with a different"+
				" owner = %v, want ErrWorkerIDOwned", err,
		)
	}
	if !raceFired {
		t.Fatalf("test hook never fired -- race was not exercised")
	}
}

// TestRegisterOwnedExhaustsToContended pins the retry bound: when
// every attempt loses the revision race (a pathological, persistent
// contender), RegisterOwned gives up after ownedWriteRetriesMax
// attempts and returns the distinct ErrWorkerIDContended rather than
// looping forever or misreporting ErrWorkerIDOwned.
func TestRegisterOwnedExhaustsToContended(t *testing.T) {
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
		WorkerID:  "race-w3",
		TaskTypes: []string{"echo"},
		TokenID:   "tok-a",
	}
	if err := dir.RegisterOwned(reg, "tok-a", false); err != nil {
		t.Fatalf("initial RegisterOwned: %v", err)
	}

	fireCount := 0
	registerOwnedTestHook = func() {
		fireCount++
		// A same-owner writer that never stops contending -- every
		// attempt's Update loses the race to this raw write.
		rawReregister(t, dir.kv, "race-w3", "tok-a")
	}
	t.Cleanup(func() { registerOwnedTestHook = nil })

	err = dir.RegisterOwned(reg, "tok-a", false)
	if err != ErrWorkerIDContended {
		t.Fatalf(
			"RegisterOwned under persistent contention = %v, want"+
				" ErrWorkerIDContended", err,
		)
	}
	if fireCount != ownedWriteRetriesMax {
		t.Fatalf(
			"hook fired %d times, want %d (ownedWriteRetriesMax)",
			fireCount, ownedWriteRetriesMax,
		)
	}
}

// TestDeregisterOwnedSameOwnerRaceRetries pins the delete-side
// counterpart: a revision conflict from our own concurrent
// re-register (e.g. a heartbeat firing right as we disconnect) must
// not skip a legitimate deregister.
func TestDeregisterOwnedSameOwnerRaceRetries(t *testing.T) {
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
		WorkerID:  "race-w4",
		TaskTypes: []string{"echo"},
		TokenID:   "tok-a",
	}
	if err := dir.RegisterOwned(reg, "tok-a", false); err != nil {
		t.Fatalf("initial RegisterOwned: %v", err)
	}

	raceFired := false
	deregisterOwnedTestHook = func() {
		if raceFired {
			return
		}
		raceFired = true
		// Same owner re-registers concurrently -- simulates a
		// heartbeat tick landing between our Get and Delete.
		rawReregister(t, dir.kv, "race-w4", "tok-a")
	}
	t.Cleanup(func() { deregisterOwnedTestHook = nil })

	err = dir.DeregisterOwned("race-w4", "tok-a", false)
	if err != nil {
		t.Fatalf(
			"DeregisterOwned after same-owner race = %v, want nil",
			err,
		)
	}
	if !raceFired {
		t.Fatalf("test hook never fired -- race was not exercised")
	}
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, w := range workers {
		if w.WorkerID == "race-w4" {
			t.Fatalf("race-w4 still present after DeregisterOwned")
		}
	}
}
