// Methodology: a part's name is its identity (stream, date, sequence range, schema version), so
// these tests vary what must not matter (row order, wall clock, a retry) and assert the name, the
// commit and the number of committed parts stay put.
package writer

import (
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

func TestFlush_NameDoesNotDependOnEncodedBytes(t *testing.T) {
	first := newTestWriter(t, t.TempDir())
	second := newTestWriter(t, t.TempDir())
	changed := testRow(2, 1)
	changed.Payload = `{"id":"a much longer payload"}`
	a := flushRows(t, first, testRow(1, 1), testRow(2, 1))
	b := flushRows(t, second, testRow(1, 1), changed)
	if a[0].Part != b[0].Part || a[0].Bytes == b[0].Bytes {
		t.Fatalf("same identity must keep its name whatever the bytes: %+v vs %+v", a[0], b[0])
	}
	third := newTestWriter(t, t.TempDir())
	c := flushRows(t, third, testRow(1, 1), testRow(3, 1))
	if c[0].Part == a[0].Part {
		t.Fatal("a different sequence range must be a different part")
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

func TestFlush_SameIdentityWithOtherRowCountIsRefused(t *testing.T) {
	writer := newTestWriter(t, t.TempDir())
	flushRows(t, writer, testRow(1, 1), testRow(3, 1))
	for _, row := range []Row{testRow(1, 1), testRow(2, 1), testRow(3, 1)} {
		if err := writer.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	_, err := writer.Flush()
	if err == nil || !strings.Contains(err.Error(), "committed with 2 rows") {
		t.Fatalf("err = %v, want a row-count refusal", err)
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
