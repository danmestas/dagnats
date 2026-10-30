// Methodology: ListCommitted is exercised on hand-written commit records, so the reader's
// verdicts (reject, overlap) do not depend on the writer being correct.
package lake

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func plant(t *testing.T, root, dt, stem, record string, withPart bool) {
	t.Helper()
	dir := PartitionDir(root, "p", "t", dt)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if withPart {
		if err := os.WriteFile(filepath.Join(dir, stem+".parquet"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, stem+".json"), []byte(record), 0o644); err != nil {
		t.Fatal(err)
	}
}

func record(stem string, min, max int) string {
	return `{"part":"` + stem + `.parquet","rows":2,"bytes":1,"stream":"S","seq_min":` +
		strconv.Itoa(min) + `,"seq_max":` + strconv.Itoa(max) + `,"schema_version":1,"written_at":"2026-10-01T00:00:00Z"}`
}

func TestListCommitted_OneBadRecordDoesNotHideTheRest(t *testing.T) {
	root := t.TempDir()
	plant(t, root, "2026-10-01", "part-good", record("part-good", 1, 2), true)
	plant(t, root, "2026-10-01", "part-garbled", "{not json", true)
	plant(t, root, "2026-10-01", "part-nopart", record("part-nopart", 3, 4), false)
	plant(t, root, "2026-10-01", "part-wrong", record("part-other", 5, 6), true)
	listing, err := ListCommitted(root, "p", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Commits) != 1 || listing.Commits[0].Part != "part-good.parquet" {
		t.Fatalf("commits = %+v", listing.Commits)
	}
	if len(listing.Rejected) != 3 {
		t.Fatalf("rejected = %+v, want 3", listing.Rejected)
	}
	for _, rejected := range listing.Rejected {
		if rejected.Reason == "" || !strings.HasSuffix(rejected.Record, ".json") {
			t.Fatalf("rejected without reason or record: %+v", rejected)
		}
	}
}

func TestListCommitted_ReportsOverlappingSequenceRanges(t *testing.T) {
	root := t.TempDir()
	plant(t, root, "2026-10-01", "part-a", record("part-a", 1, 3), true)
	plant(t, root, "2026-10-01", "part-b", record("part-b", 3, 5), true)
	plant(t, root, "2026-10-02", "part-c", record("part-c", 6, 7), true)
	listing, err := ListCommitted(root, "p", "t")
	if err != nil || len(listing.Commits) != 3 {
		t.Fatalf("listing = %+v, %v", listing, err)
	}
	if len(listing.Overlaps) != 1 || listing.Overlaps[0].First.Part != "part-a.parquet" ||
		listing.Overlaps[0].Second.Part != "part-b.parquet" {
		t.Fatalf("overlaps = %+v, want only a/b", listing.Overlaps)
	}
}
