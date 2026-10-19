package dag

import "sync/atomic"

// SnapshotRevision records the stored revision a WorkflowRun was read
// at, so the next write of that run can be a compare-and-set against
// it. Several engines (one per cluster node) advance the same run; a
// write that does not prove it saw the latest snapshot would erase
// whatever another engine wrote in between.
//
// One SnapshotRevision belongs to one load: copies of the run share
// it by pointer, while a separate load gets its own. That is what lets
// one handler save the same run twice while two handlers that loaded
// the same revision still conflict.
type SnapshotRevision struct {
	seq atomic.Uint64
}

// NewSnapshotRevision returns a handle at seq. seq 0 means the run's
// key does not exist yet.
func NewSnapshotRevision(seq uint64) *SnapshotRevision {
	revision := &SnapshotRevision{}
	revision.seq.Store(seq)
	return revision
}

// Seq returns the revision the next write must expect.
func (r *SnapshotRevision) Seq() uint64 {
	if r == nil {
		panic("SnapshotRevision.Seq: nil revision")
	}
	return r.seq.Load()
}

// Advance records a successful write at seq. Revisions only grow, so
// a seq at or below the current one is a programmer error.
func (r *SnapshotRevision) Advance(seq uint64) {
	if r == nil {
		panic("SnapshotRevision.Advance: nil revision")
	}
	if seq <= r.seq.Load() {
		panic("SnapshotRevision.Advance: revision must increase")
	}
	r.seq.Store(seq)
}
