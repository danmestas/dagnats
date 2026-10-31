// Methodology: integration tests over a real temp directory. Ordering is proven by crashing the
// writer between part and commit through the unexported beforeCommit hook and listing what a
// reader would see. Name stability is proven by writing the same rows into two separate roots.
package writer

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/lake"
)

func testConfig(root string) Config {
	return Config{
		Root: root, Program: "notify", Table: "deliveries", Stream: "NOTIFY_DELIVERY",
		Schema: lake.Schema{Version: 1, Columns: []lake.Column{
			{Name: "org_id", Type: lake.TypeString}, {Name: "attempts", Type: lake.TypeInt64},
			{Name: "score", Type: lake.TypeFloat64},
			{Name: "ok", Type: lake.TypeBool}, {Name: "sent_at", Type: lake.TypeTimestamp},
		}},
	}
}

func testRow(seq int64, day int) Row {
	at := time.Date(2026, 10, day, 12, 30, 0, 0, time.UTC)
	return Row{
		EventID: "evt-" + string(rune('a'+seq)), EventTime: at, StreamSeq: seq,
		Payload: `{"id":"x"}`,
		Values: map[string]any{
			"org_id": "o1", "attempts": int64(seq), "score": 1.5, "ok": true, "sent_at": at,
		},
	}
}

func listCommits(root string) ([]lake.Commit, error) {
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	return listing.Commits, err
}

func newTestWriter(t *testing.T, root string) *Writer {
	t.Helper()
	writer, err := NewWriter(testConfig(root))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return writer
}

func TestFlush_CommitsOnePartPerEventDate(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	for _, row := range []Row{testRow(1, 1), testRow(2, 1), testRow(3, 2)} {
		if err := writer.Append(row); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	flushed, err := writer.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	listed, err := listCommits(root)
	if err != nil {
		t.Fatalf("ListCommitted: %v", err)
	}
	if len(flushed) != 2 || len(listed) != 2 {
		t.Fatalf("flushed %d, listed %d, want 2 and 2", len(flushed), len(listed))
	}
	first := listed[0]
	if first.Dt != "2026-10-01" || first.Rows != 2 || first.SeqMin != 1 || first.SeqMax != 2 {
		t.Fatalf("first commit = %+v", first)
	}
	if first.Stream != "NOTIFY_DELIVERY" || first.SchemaVersion != 1 || first.WrittenAt.IsZero() {
		t.Fatalf("first commit metadata = %+v", first)
	}
	if listed[1].Dt != "2026-10-02" || listed[1].Rows != 1 {
		t.Fatalf("second commit = %+v", listed[1])
	}
	again, err := writer.Flush()
	if err != nil || len(again) != 0 {
		t.Fatalf("second Flush = %v, %v; want nothing buffered", again, err)
	}
}

func TestFlush_CrashBetweenPartAndCommitLeavesUncommittedPart(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	if err := writer.Append(testRow(1, 1)); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash")
	writer.beforeCommit = func() error { return crash }
	if _, err := writer.Flush(); !errors.Is(err, crash) {
		t.Fatalf("Flush err = %v, want crash", err)
	}
	parts, err := filepath.Glob(filepath.Join(root, "notify", "deliveries", "dt=*", "part-*.parquet"))
	if err != nil || len(parts) != 1 {
		t.Fatalf("parts on disk = %v, %v; want the orphan", parts, err)
	}
	listed, err := listCommits(root)
	if err != nil || len(listed) != 0 {
		t.Fatalf("ListCommitted = %v, %v; want no committed parts", listed, err)
	}
	// The rows are still buffered: a retry commits the same content-addressed part.
	writer.beforeCommit = nil
	if _, err := writer.Flush(); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	listed, err = listCommits(root)
	if err != nil || len(listed) != 1 {
		t.Fatalf("after retry: %v, %v", listed, err)
	}
	parts, _ = filepath.Glob(filepath.Join(root, "notify", "deliveries", "dt=*", "part-*.parquet"))
	if len(parts) != 1 {
		t.Fatalf("retry wrote a second part: %v", parts)
	}
}

func TestFlush_PartIsDurableBeforeCommitExists(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	if err := writer.Append(testRow(1, 1)); err != nil {
		t.Fatal(err)
	}
	writer.beforeCommit = func() error {
		parts, _ := filepath.Glob(filepath.Join(root, "notify", "deliveries", "dt=*", "part-*.parquet"))
		commits, _ := filepath.Glob(filepath.Join(root, "notify", "deliveries", "dt=*", "part-*.json"))
		if len(parts) != 1 || len(commits) != 0 {
			t.Errorf("at commit time: %d parts, %d commits; want 1 and 0", len(parts), len(commits))
		}
		return nil
	}
	if _, err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestFlush_ContentAddressedNameIsStable(t *testing.T) {
	names := make([]string, 2)
	for i := range names {
		writer := newTestWriter(t, t.TempDir())
		for _, row := range []Row{testRow(1, 1), testRow(2, 1)} {
			if err := writer.Append(row); err != nil {
				t.Fatal(err)
			}
		}
		commits, err := writer.Flush()
		if err != nil || len(commits) != 1 {
			t.Fatalf("Flush = %v, %v", commits, err)
		}
		names[i] = commits[0].Part
	}
	if names[0] != names[1] {
		t.Fatalf("same rows, different names: %s vs %s", names[0], names[1])
	}
	if !strings.HasPrefix(names[0], "part-") || !strings.HasSuffix(names[0], ".parquet") ||
		len(names[0]) != len("part-")+16+len(".parquet") {
		t.Fatalf("name %q is not part-<16 hex>.parquet", names[0])
	}
	other := newTestWriter(t, t.TempDir())
	if err := other.Append(testRow(9, 1)); err != nil {
		t.Fatal(err)
	}
	commits, _ := other.Flush()
	if commits[0].Part == names[0] {
		t.Fatal("different rows produced the same name")
	}
}

func TestAppend_RefusesBadRows(t *testing.T) {
	writer := newTestWriter(t, t.TempDir())
	noID := testRow(1, 1)
	noID.EventID = ""
	noTime := testRow(1, 1)
	noTime.EventTime = time.Time{}
	unknown := testRow(1, 1)
	unknown.Values["nope"] = "x"
	bad := map[string]Row{"no id": noID, "no time": noTime, "unknown column": unknown}
	for name, row := range bad {
		if err := writer.Append(row); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAppend_BufferIsBounded(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.BufferedRowsMax = 2
	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 2; i++ {
		if err := writer.Append(testRow(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Append(testRow(3, 1)); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("third Append err = %v, want ErrBufferFull", err)
	}
	if _, err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(testRow(3, 1)); err != nil {
		t.Fatalf("Append after Flush: %v", err)
	}
}

func TestNewWriter_RefusesBadConfig(t *testing.T) {
	bad := []func(*Config){
		func(c *Config) { c.Root = "" },
		func(c *Config) { c.Program = "../x" },
		func(c *Config) { c.Table = "A" },
		func(c *Config) { c.Schema.Version = 0 },
		func(c *Config) { c.Stream = "" },
		func(c *Config) { c.BufferedRowsMax = PartRowsMax + 1 },
		func(c *Config) { c.BufferedRowsMax = -1 },
		func(c *Config) { c.Schema.Columns = []lake.Column{{Name: "payload", Type: lake.TypeString}} },
	}
	for i, mutate := range bad {
		cfg := testConfig(t.TempDir())
		mutate(&cfg)
		if _, err := NewWriter(cfg); err == nil {
			t.Errorf("case %d: accepted", i)
		}
	}
}

func TestListCommitted_RejectsCommitWithoutPart(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	if err := writer.Append(testRow(1, 1)); err != nil {
		t.Fatal(err)
	}
	commits, err := writer.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(commits[0].Path); err != nil {
		t.Fatal(err)
	}
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || len(listing.Commits) != 0 || len(listing.Rejected) != 1 {
		t.Fatalf("listing = %+v, %v; want the commit rejected", listing, err)
	}
	if !strings.Contains(listing.Rejected[0].Reason, "missing part") {
		t.Fatalf("reason = %q", listing.Rejected[0].Reason)
	}
	absent, err := lake.ListCommitted(root, "notify", "absent")
	if err != nil || len(absent.Commits) != 0 {
		t.Fatalf("absent table = %+v, %v; want empty", absent, err)
	}
}
