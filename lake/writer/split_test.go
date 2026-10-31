// Methodology: an object store caps one object, so a date whose rows encode past partBytesMax
// is split. The ceiling is lowered through the unexported field so a handful of rows splits;
// the test asserts every part fits, no row is lost or doubled, and a retry lands on the same
// names.
package writer

import (
	"errors"
	"fmt"
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

func TestAppend_RefusesARowPastTheCeilingWhileItsNeighboursFlush(t *testing.T) {
	small := flushRows(t, newTestWriter(t, t.TempDir()), testRow(1, 1))
	writer := newTestWriter(t, t.TempDir())
	writer.partBytesMax = 2 * small[0].Bytes
	oversize := bigRow(2)
	err := writer.Append(oversize)
	if err == nil || !strings.Contains(err.Error(), oversize.EventID) {
		t.Fatalf("Append of an oversize row = %v, want an error naming it", err)
	}
	if writer.buffered != 0 || errors.Is(err, ErrBufferFull) {
		t.Fatalf("buffered = %d, want the oversize row kept out", writer.buffered)
	}
	commits := flushRows(t, writer, testRow(1, 1), testRow(3, 2))
	if len(commits) != 2 || writer.buffered != 0 {
		t.Fatalf("neighbours = %+v, buffered %d; want both dates committed", commits, writer.buffered)
	}
}

// sameSeqRows is bigRows whose seqs repeat: rows sharing a seq are one stream message's rows.
func sameSeqRows(seqs ...int64) []Row {
	rows := make([]Row, len(seqs))
	for i, seq := range seqs {
		rows[i] = bigRow(int64(i + 1))
		rows[i].StreamSeq = seq
		rows[i].EventID = fmt.Sprintf("evt-%d", i)
	}
	return rows
}

func TestFlush_SplitKeepsRowsOfOneSeqInOnePart(t *testing.T) {
	rows := sameSeqRows(1, 2, 3, 4, 4, 4, 5, 6) // indexes 3..5 straddle the midpoint, 4
	single := flushRows(t, newTestWriter(t, t.TempDir()), rows[0])
	whole := flushRows(t, newTestWriter(t, t.TempDir()), rows...)
	root := t.TempDir()
	writer := newTestWriter(t, root)
	writer.partBytesMax = single[0].Bytes + (whole[0].Bytes-single[0].Bytes)/2
	commits := flushRows(t, writer, rows...)
	if len(commits) < 2 {
		t.Fatalf("commits = %d, want the date split", len(commits))
	}
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || len(listing.Commits) != len(commits) {
		t.Fatalf("listing = %+v, %v", listing, err)
	}
	if len(listing.Overlaps) != 0 {
		t.Fatalf("a seq run was split across parts: %+v", listing.Overlaps)
	}
}

func TestFlush_ASeqRunPastTheCeilingIsAnError(t *testing.T) {
	rows := sameSeqRows(7, 7, 7, 7)
	single := flushRows(t, newTestWriter(t, t.TempDir()), rows[0])
	writer := newTestWriter(t, t.TempDir())
	writer.partBytesMax = single[0].Bytes + 512
	for _, row := range rows {
		if err := writer.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	commits, err := writer.Flush(t.Context())
	if err == nil || len(commits) != 0 || !strings.Contains(err.Error(), "stream_seq 7") {
		t.Fatalf("Flush = %+v, %v; want an error naming the seq", commits, err)
	}
	if writer.buffered != len(rows) {
		t.Fatalf("buffered = %d, want the run kept", writer.buffered)
	}
}
