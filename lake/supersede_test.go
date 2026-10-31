// Methodology: real parts and records from lake/writer over a LocalStore, then Supersede and
// ListCommitted, as lode's compaction and retention will run them. Each crash point of the
// rewrite design is a listing asserted on both sides: what is live, what is superseded, and
// whether an Overlap or a rejection shows.
package lake_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/lake"
	"github.com/danmestas/dagnats/lake/writer"
)

func writeRows(t *testing.T, store writer.Store, seqs ...int64) lake.Commit {
	t.Helper()
	w, err := writer.New(store, "p", writer.Config{
		Table: "t", Stream: "S", Schema: lake.Schema{Version: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range seqs {
		row := writer.Row{
			EventID: "evt-" + string(rune('a'+seq)), StreamSeq: seq, Payload: `{}`,
			EventTime: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		}
		if err := w.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	commits, err := w.Flush(t.Context())
	if err != nil || len(commits) != 1 {
		t.Fatalf("Flush = %+v, %v", commits, err)
	}
	return commits[0]
}

func stem(commit lake.Commit) string {
	return strings.TrimSuffix(strings.TrimPrefix(commit.Part, "part-"), ".parquet")
}

func list(t *testing.T, root string) lake.Listing {
	t.Helper()
	listing, err := lake.ListCommitted(root, "p", "t")
	if err != nil {
		t.Fatal(err)
	}
	return listing
}

func supersede(t *testing.T, store writer.Store, commit lake.Commit, by string) lake.Commit {
	t.Helper()
	marked, err := lake.Supersede(t.Context(), store, commit, by, time.Unix(500, 0))
	if err != nil {
		t.Fatalf("Supersede %s: %v", commit.Part, err)
	}
	return marked
}

func TestSupersede_ThenListHidesThePart(t *testing.T) {
	root := t.TempDir()
	store := writer.NewLocalStore(root)
	old := writeRows(t, store, 1, 2)
	kept := writeRows(t, store, 3)
	marked := supersede(t, store, old, "0123456789abcdef")
	if marked.SupersededBy != "0123456789abcdef" || !marked.WrittenAt.Equal(time.Unix(500, 0)) {
		t.Fatalf("marked = %+v", marked)
	}
	listing := list(t, root)
	if len(listing.Commits) != 1 || listing.Commits[0].Part != kept.Part {
		t.Fatalf("commits = %+v, want only the unsuperseded part", listing.Commits)
	}
	if len(listing.Superseded) != 1 || listing.Superseded[0].Part != old.Part ||
		listing.Superseded[0].SupersededBy != "0123456789abcdef" || listing.Superseded[0].Key != old.Key {
		t.Fatalf("superseded = %+v", listing.Superseded)
	}
	if len(listing.Rejected) != 0 || len(listing.Overlaps) != 0 {
		t.Fatalf("listing = %+v, want nothing rejected or overlapping", listing)
	}
}

func TestSupersede_PartDeletedIsTheExpectedEndState(t *testing.T) {
	root := t.TempDir()
	store := writer.NewLocalStore(root)
	old := writeRows(t, store, 1, 2)
	supersede(t, store, old, "0123456789abcdef")
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(old.Key))); err != nil {
		t.Fatal(err)
	}
	listing := list(t, root)
	if len(listing.Rejected) != 0 || len(listing.Commits) != 0 {
		t.Fatalf("listing = %+v, want the deleted superseded part neither live nor rejected", listing)
	}
	if len(listing.Superseded) != 1 || listing.Superseded[0].Part != old.Part {
		t.Fatalf("superseded = %+v", listing.Superseded)
	}
}

func TestSupersede_CompactionOverlapsUntilEveryInputIsSuperseded(t *testing.T) {
	root := t.TempDir()
	store := writer.NewLocalStore(root)
	first := writeRows(t, store, 1, 2)
	second := writeRows(t, store, 3, 4)
	compacted := writeRows(t, store, 1, 2, 3, 4)
	if got := len(list(t, root).Overlaps); got != 2 {
		t.Fatalf("overlaps before supersede = %d, want the compacted part against both", got)
	}
	supersede(t, store, first, stem(compacted))
	partial := list(t, root)
	if len(partial.Overlaps) != 1 || partial.Overlaps[0].First.Part == first.Part ||
		partial.Overlaps[0].Second.Part == first.Part {
		t.Fatalf("partial supersede overlaps = %+v, want only the second input's", partial.Overlaps)
	}
	supersede(t, store, second, stem(compacted))
	done := list(t, root)
	if len(done.Overlaps) != 0 || len(done.Commits) != 1 || done.Commits[0].Part != compacted.Part {
		t.Fatalf("after compaction = %+v, want only the compacted part, no overlap", done)
	}
	if len(done.Superseded) != 2 {
		t.Fatalf("superseded = %+v, want both inputs", done.Superseded)
	}
}

func TestSupersede_RetentionHasNoSuccessor(t *testing.T) {
	root := t.TempDir()
	store := writer.NewLocalStore(root)
	dropped := writeRows(t, store, 1, 2)
	marked := supersede(t, store, dropped, "")
	if marked.SupersededBy != "" {
		t.Fatalf("retention named a successor: %+v", marked)
	}
	record, err := os.ReadFile(filepath.Join(root, "p", "t", "dt=2026-10-01",
		"part-"+stem(dropped)+".json.superseded"))
	if err != nil || strings.Contains(string(record), "superseded_by") {
		t.Fatalf("record %s, %v; want no superseded_by key", record, err)
	}
	listing := list(t, root)
	if len(listing.Commits) != 0 || len(listing.Superseded) != 1 ||
		listing.Superseded[0].SupersededBy != "" {
		t.Fatalf("listing = %+v, want the part superseded with no successor", listing)
	}
}

func TestSupersede_IsIdempotentAndRefusesAnotherSuccessor(t *testing.T) {
	root := t.TempDir()
	store := writer.NewLocalStore(root)
	old := writeRows(t, store, 1, 2)
	first := supersede(t, store, old, "0123456789abcdef")
	again, err := lake.Supersede(t.Context(), store, old, "0123456789abcdef", time.Unix(900, 0))
	if err != nil || !again.WrittenAt.Equal(first.WrittenAt) {
		t.Fatalf("re-supersede = %+v, %v; want the existing record back", again, err)
	}
	_, err = lake.Supersede(t.Context(), store, old, "fedcba9876543210", time.Unix(900, 0))
	if err == nil || errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "fedcba9876543210") {
		t.Fatalf("supersede by another part = %v, want a conflict naming it", err)
	}
	if got := len(list(t, root).Superseded); got != 1 {
		t.Fatalf("superseded = %d, want 1", got)
	}
}

func TestSupersede_RefusesBadInput(t *testing.T) {
	store := writer.NewLocalStore(t.TempDir())
	old := writeRows(t, store, 1)
	for name, by := range map[string]string{"itself": stem(old), "a path": "../x", "dotted": "a.b"} {
		if _, err := lake.Supersede(context.Background(), store, old, by, time.Unix(1, 0)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	noKey := old
	noKey.Key = ""
	if _, err := lake.Supersede(t.Context(), store, noKey, "", time.Unix(1, 0)); err == nil {
		t.Error("a commit with no key: accepted")
	}
}
