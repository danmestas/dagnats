// Methodology: an object store caps one object, so a date whose rows encode past partBytesMax
// is split. The ceiling is lowered through the unexported field so a handful of rows splits;
// the test asserts every part fits, no row is lost or doubled, and a retry lands on the same
// names.
package writer

import (
	"errors"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/lake"
)

func bigRow(seq int64) Row {
	row := testRow(seq, 1)
	// Distinct per row and incompressible enough that zstd cannot fold the rows together.
	var payload strings.Builder
	state := uint64(seq) * 0x9e3779b97f4a7c15
	for payload.Len() < 4096 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload.WriteByte("0123456789abcdef"[state%16])
	}
	row.Payload = payload.String()
	return row
}

func TestFlush_SplitsADatePastThePartByteCeiling(t *testing.T) {
	rows := make([]Row, 8)
	for i := range rows {
		rows[i] = bigRow(int64(i + 1))
	}
	// A part has a large fixed cost (footer, column statistics), so the ceiling sits a quarter
	// of the way from a one-row part to the whole batch: every row fits, the batch does not.
	single := flushRows(t, newTestWriter(t, t.TempDir()), rows[1])
	whole := flushRows(t, newTestWriter(t, t.TempDir()), rows...)
	root := t.TempDir()
	writer := newTestWriter(t, root)
	writer.partBytesMax = single[0].Bytes + (whole[0].Bytes-single[0].Bytes)/4
	commits := flushRows(t, writer, rows...)
	if len(commits) < 2 {
		t.Fatalf("commits = %d, want the date split", len(commits))
	}
	total, next := 0, int64(1)
	for _, commit := range commits {
		if commit.Bytes > writer.partBytesMax || commit.Dt != "2026-10-01" {
			t.Fatalf("part %+v is over the %d-byte ceiling", commit, writer.partBytesMax)
		}
		if commit.SeqMin != next {
			t.Fatalf("parts out of order or with a gap at %+v", commit)
		}
		total, next = total+commit.Rows, commit.SeqMax+1
	}
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || total != 8 || len(listing.Commits) != len(commits) || len(listing.Overlaps) != 0 {
		t.Fatalf("total %d, listing %+v, %v", total, listing, err)
	}
	retry := flushRows(t, writer, rows...)
	for i := range retry {
		if retry[i].Part != commits[i].Part {
			t.Fatalf("retry part %d is %s, was %s", i, retry[i].Part, commits[i].Part)
		}
	}
}

func TestFlush_ARowPastTheCeilingIsAnErrorAndStaysBuffered(t *testing.T) {
	writer := newTestWriter(t, t.TempDir())
	writer.partBytesMax = 1024
	if err := writer.Append(bigRow(1)); err != nil {
		t.Fatal(err)
	}
	commits, err := writer.Flush(t.Context())
	if err == nil || len(commits) != 0 || !strings.Contains(err.Error(), "evt-b") {
		t.Fatalf("Flush = %+v, %v; want an error naming the row", commits, err)
	}
	if writer.buffered != 1 || errors.Is(err, ErrBufferFull) {
		t.Fatalf("buffered = %d after a failed flush, want 1", writer.buffered)
	}
}
