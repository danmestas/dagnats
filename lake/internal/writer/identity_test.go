// Methodology: a part's name is its identity (stream, date, sequence range, schema version), so
// these tests vary what must not matter (row order, wall clock, a retry) and assert the name, the
// commit and the number of committed parts stay put.
package writer

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/lake"
)

func flushRows(t *testing.T, w *Writer, rows ...Row) []lake.Commit {
	t.Helper()
	for _, row := range rows {
		if err := w.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	commits, err := w.Flush()
	if err != nil {
		t.Fatal(err)
	}
	return commits
}

func TestFlush_BatchOrderDoesNotChangeNameOrBytes(t *testing.T) {
	forward := newTestWriter(t, t.TempDir())
	backward := newTestWriter(t, t.TempDir())
	backward.now = func() time.Time { return time.Unix(5, 0) }
	a := flushRows(t, forward, testRow(1, 1), testRow(2, 1), testRow(3, 1))
	b := flushRows(t, backward, testRow(3, 1), testRow(1, 1), testRow(2, 1))
	if a[0].Part != b[0].Part || a[0].Bytes != b[0].Bytes || a[0].SeqMin != 1 || b[0].SeqMax != 3 {
		t.Fatalf("forward %+v, backward %+v", a[0], b[0])
	}
}

func TestFlush_NameFollowsContent(t *testing.T) {
	first := newTestWriter(t, t.TempDir())
	second := newTestWriter(t, t.TempDir())
	changed := testRow(2, 1)
	changed.Payload = `{"id":"a different event"}`
	a := flushRows(t, first, testRow(1, 1), testRow(2, 1))
	b := flushRows(t, second, testRow(1, 1), changed)
	if a[0].Part == b[0].Part || a[0].Digest == b[0].Digest || a[0].Digest == "" {
		t.Fatalf("different content must not share a name: %+v vs %+v", a[0], b[0])
	}
	if a[0].SeqMin != b[0].SeqMin || a[0].SeqMax != b[0].SeqMax {
		t.Fatal("test premise: both batches cover the same seq range")
	}
}

func TestFlush_RetryOfCommittedIdentityIsNoOp(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	writer.now = func() time.Time { return time.Unix(100, 0) }
	first := flushRows(t, writer, testRow(1, 1), testRow(2, 1))
	writer.now = func() time.Time { return time.Unix(200, 0) }
	retry := flushRows(t, writer, testRow(2, 1), testRow(1, 1))
	if retry[0].Part != first[0].Part || !retry[0].WrittenAt.Equal(first[0].WrittenAt) {
		t.Fatalf("retry rewrote the commit: %+v vs %+v", retry[0], first[0])
	}
	listed, err := listCommits(root)
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed %d commits, %v; want exactly 1", len(listed), err)
	}
	if !listed[0].WrittenAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("committed record changed: %+v", listed[0])
	}
}

func TestFlush_DifferentBatchOverSameRangeIsKeptAndReported(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	flushRows(t, writer, testRow(1, 1), testRow(3, 1))
	reset := testRow(2, 1)
	reset.EventID = "after-reset"
	flushRows(t, writer, testRow(1, 1), reset, testRow(3, 1))
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || len(listing.Commits) != 2 {
		t.Fatalf("listing = %+v, %v; want both batches committed", listing, err)
	}
	if len(listing.Overlaps) != 1 {
		t.Fatalf("overlaps = %+v, want 1", listing.Overlaps)
	}
	if listing.Commits[0].Rows+listing.Commits[1].Rows != 5 {
		t.Fatalf("rows %d+%d: a batch was dropped", listing.Commits[0].Rows, listing.Commits[1].Rows)
	}
}

func TestFlush_RetryRewritesAMissingPart(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	first := flushRows(t, writer, testRow(1, 1), testRow(2, 1))
	if err := os.Remove(first[0].Path); err != nil {
		t.Fatal(err)
	}
	retry := flushRows(t, writer, testRow(1, 1), testRow(2, 1))
	if retry[0].Part != first[0].Part {
		t.Fatalf("retry changed the name: %s vs %s", retry[0].Part, first[0].Part)
	}
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || len(listing.Commits) != 1 || len(listing.Rejected) != 0 {
		t.Fatalf("listing = %+v, %v; want one healthy commit", listing, err)
	}
}

// failingDir makes MkdirAll fail for one date directory, as a full disk would.
type failingDir struct {
	osOps
	fail string
}

func (f *failingDir) MkdirAll(path string) error {
	if f.fail != "" && strings.HasSuffix(path, f.fail) {
		return errors.New("injected mkdir failure")
	}
	return f.osOps.MkdirAll(path)
}

func TestFlush_FailedDateDoesNotPoisonTheBuffer(t *testing.T) {
	root := t.TempDir()
	ops := &failingDir{fail: "dt=2026-10-01"}
	writer, err := newWriter(testConfig(root), ops)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []Row{testRow(1, 1), testRow(2, 2)} {
		if err := writer.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	commits, err := writer.Flush()
	if err == nil || len(commits) != 1 || commits[0].Dt != "2026-10-02" {
		t.Fatalf("Flush = %+v, %v; want the healthy date committed and an error", commits, err)
	}
	ops.fail = ""
	commits, err = writer.Flush()
	if err != nil || len(commits) != 1 || commits[0].Dt != "2026-10-01" {
		t.Fatalf("retry = %+v, %v; want the failed date committed", commits, err)
	}
	if writer.buffered != 0 {
		t.Fatalf("buffered = %d after both flushed", writer.buffered)
	}
}

func TestAppend_RefusesWrongTypeNamingTheColumn(t *testing.T) {
	writer := newTestWriter(t, t.TempDir())
	bad := testRow(1, 1)
	bad.Values["attempts"] = "three"
	err := writer.Append(bad)
	if err == nil || !strings.Contains(err.Error(), `"attempts"`) {
		t.Fatalf("err = %v, want the column named", err)
	}
	zero := testRow(1, 1)
	zero.StreamSeq = 0
	if err := writer.Append(zero); err == nil {
		t.Fatal("StreamSeq 0 accepted")
	}
	nilOK := testRow(1, 1)
	nilOK.Values["attempts"] = nil
	if err := writer.Append(nilOK); err != nil {
		t.Fatalf("nil is NULL and must be accepted: %v", err)
	}
}
