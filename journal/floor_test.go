// journal/floor_test.go
// The protected floor: the lowest AGENT_JOURNAL sequence a later Open
// of a live step still reads. For a step on iteration N that is the
// commit of iteration N-1 (the state Open starts N from) or N's first
// record, whichever is lower; earlier records are below it. A step
// whose iteration cannot be told, or that has no earlier commit, is
// protected from its first record; a fork also protects its parent
// chain. The floor over several runs is the minimum; with no live
// journal it is the stream's LastSeq+1.
//
// Methodology: one embedded server per case, journals written through
// the public Open/Append/Commit/WriteFork surface. The decisive check
// purges the stream below the floor the way an archiver would and
// opens a redelivery of the live iteration: it must still find its
// state and records. A purge one record higher must not. Every wait is
// bounded; every case has a positive and a negative assertion.
package journal

import (
	"context"
	"errors"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// runIDs is a listRuns that answers ids.
func runIDs(ids ...string) func(context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) { return ids, nil }
}

// threeIterations writes iterations 0 and 1 (each a claim, an entry
// and a continue commit with state "s0"/"s1") and leaves iteration 2
// in flight after its claim and one entry. It returns the commit of
// iteration 1 and the claim of iteration 2.
func threeIterations(t *testing.T, c client, runID string) (commit1, claim2 uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	for iteration, state := range []string{`"s0"`, `"s1"`} {
		j := open(t, c, task(runID, iteration, 1, 1))
		if _, err := j.Append(ctx, TypeEntry, []byte(`"work"`)); err != nil {
			t.Fatalf("append %d: %v", iteration, err)
		}
		seq, err := j.Commit(ctx, Commit{Action: ActionContinue, State: []byte(state)})
		if err != nil {
			t.Fatalf("commit %d: %v", iteration, err)
		}
		commit1 = seq
	}
	j := open(t, c, task(runID, 2, 1, 1))
	claim2 = j.Seq()
	if _, err := j.Append(ctx, TypeEntry, []byte(`"in flight"`)); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	return commit1, claim2
}

func protectedFloor(t *testing.T, c client, ids ...string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	floor, err := ProtectedFloor(ctx, c.js, runIDs(ids...))
	if err != nil {
		t.Fatalf("ProtectedFloor: %v", err)
	}
	return floor
}

// purgeBelow removes every record below seq, as an archiver would.
func purgeBelow(t *testing.T, c client, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	stream, err := c.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(seq)); err != nil {
		t.Fatalf("purge below %d: %v", seq, err)
	}
}

func lastSeq(t *testing.T, c client) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	stream, err := c.js.Stream(ctx, natsutil.AgentJournalStream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return stream.CachedInfo().State.LastSeq
}

func TestProtectedFloor_ThirdIterationKeepsItsStateAndRecords(t *testing.T) {
	c := forkClient(t)
	commit1, claim2 := threeIterations(t, c, "r")
	floor := protectedFloor(t, c, "r")
	if floor != commit1 {
		t.Fatalf("floor = %d, want iteration 1's commit %d", floor, commit1)
	}
	if floor >= claim2 || floor <= 1 {
		t.Fatalf("floor %d must sit below iteration 2's claim %d and above iteration 0", floor, claim2)
	}
	purgeBelow(t, c, floor)
	j := open(t, c, task("r", 2, 1, 2))
	if string(j.State()) != `"s1"` {
		t.Fatalf("state after purge = %q, want \"s1\"", j.State())
	}
	if countType(j.Records(), TypeEntry) != 1 {
		t.Fatalf("iteration 2's records after purge = %+v, want its entry", j.Records())
	}
}

func TestProtectedFloor_PurgeAboveTheFloorLosesTheState(t *testing.T) {
	c := forkClient(t)
	commit1, _ := threeIterations(t, c, "r")
	if got := protectedFloor(t, c, "r"); got != commit1 {
		t.Fatalf("floor = %d, want %d", got, commit1)
	}
	purgeBelow(t, c, commit1+1)
	j := open(t, c, task("r", 2, 1, 2))
	if j.State() != nil {
		t.Fatalf("state after over-purge = %q, want nil: the floor is tight", j.State())
	}
	if countType(j.Records(), TypeEntry) != 1 {
		t.Fatalf("iteration 2's entry must survive, got %+v", j.Records())
	}
}

func TestProtectedFloor_CommittedIterationKeepsItsCommit(t *testing.T) {
	c := forkClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	j := open(t, c, task("r", 0, 1, 1))
	commit0, err := j.Commit(ctx, Commit{Action: ActionContinue, State: []byte(`"s0"`)})
	if err != nil {
		t.Fatalf("commit 0: %v", err)
	}
	j = open(t, c, task("r", 1, 1, 1))
	commit1, err := j.Commit(ctx, Commit{Action: ActionContinue, State: []byte(`"s1"`)})
	if err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	// Iteration 2 is dispatched but not yet claimed: its Open starts
	// from iteration 1's commit. A redelivery of iteration 1 that finds
	// its own commit, or a retry of it, still starts from iteration 0's
	// commit, so the floor stays there and goes no lower.
	floor := protectedFloor(t, c, "r")
	if floor != commit0 {
		t.Fatalf("floor = %d, want iteration 0's commit %d", floor, commit0)
	}
	if floor <= 1 || floor >= commit1 {
		t.Fatalf("floor = %d, must drop iteration 0's claim and stay below %d", floor, commit1)
	}
}

func TestProtectedFloor_IsTheMinimumOverRuns(t *testing.T) {
	c := forkClient(t)
	early, _ := threeIterations(t, c, "a")
	late, _ := threeIterations(t, c, "b")
	if early >= late {
		t.Fatalf("run a's floor %d must be below run b's %d", early, late)
	}
	if got := protectedFloor(t, c, "a", "b"); got != early {
		t.Fatalf("floor over a and b = %d, want %d", got, early)
	}
	if got := protectedFloor(t, c, "b"); got != late {
		t.Fatalf("floor over b alone = %d, want %d", got, late)
	}
}

func TestProtectedFloor_NothingLiveIsLastSeqPlusOne(t *testing.T) {
	c := forkClient(t)
	threeIterations(t, c, "done")
	want := lastSeq(t, c) + 1
	if got := protectedFloor(t, c); got != want {
		t.Fatalf("floor with no runs = %d, want LastSeq+1 = %d", got, want)
	}
	if got := protectedFloor(t, c, "no-journal"); got != want {
		t.Fatalf("floor for a run without a journal = %d, want %d", got, want)
	}
}

func TestProtectedFloor_FirstIterationIsProtectedFromItsFirstRecord(t *testing.T) {
	c := forkClient(t)
	threeIterations(t, c, "before")
	j := open(t, c, task("r", 0, 1, 1))
	claim := j.Seq()
	if got := protectedFloor(t, c, "r"); got != claim {
		t.Fatalf("floor = %d, want the first record %d", got, claim)
	}
	if claim <= 1 {
		t.Fatalf("claim %d must sit above the other run's records", claim)
	}
}

func TestProtectedFloor_ForkProtectsItsParent(t *testing.T) {
	c := forkClient(t)
	seqs := parentRun(t, c, "parent")
	writeFork(t, c, "child", "parent", seqs["search"])
	open(t, c, task("child", 0, 1, 1))
	got := protectedFloor(t, c, "child")
	if got != 1 {
		t.Fatalf("floor for a live fork = %d, want the parent's first record 1", got)
	}
	purgeBelow(t, c, got)
	if j := open(t, c, task("child", 0, 1, 2)); countType(j.Records(), TypeToolResult) == 0 {
		t.Fatalf("fork lost its inherited records: %+v", j.Records())
	}
}

func TestProtectedFloor_OverBudgetIsAnError(t *testing.T) {
	c := forkClient(t)
	threeIterations(t, c, "r")
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	_, err := protectedFloorWithin(ctx, c.js, runIDs("r"), floorBudget{runs: 10, records: 3})
	if !errors.Is(err, ErrFloorOverBudget) {
		t.Fatalf("err = %v, want ErrFloorOverBudget", err)
	}
	_, err = protectedFloorWithin(ctx, c.js, runIDs("r", "s"), floorBudget{runs: 1, records: 100})
	if !errors.Is(err, ErrFloorOverBudget) {
		t.Fatalf("err for too many runs = %v, want ErrFloorOverBudget", err)
	}
}
