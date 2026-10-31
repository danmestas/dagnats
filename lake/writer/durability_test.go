// Methodology: durability is an ordering of effects, so the LocalStore's filesystem is swapped
// for a recorder that wraps the real one and logs each call. The test asserts the order a
// crash-safe commit needs; dropping any sync fails it, and a rename anywhere fails it too.
package writer

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recorder struct {
	osOps
	log []string
}

func (r *recorder) note(op, path string) { r.log = append(r.log, op+" "+filepath.Base(path)) }

func (r *recorder) CreateTemp(dir, pattern string) (tempFile, error) {
	file, err := r.osOps.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return &recordedFile{tempFile: file, rec: r}, nil
}

func (r *recorder) Link(from, to string) error {
	r.note("link", to)
	return r.osOps.Link(from, to)
}

func (r *recorder) Remove(path string) error {
	r.note("remove", path)
	return r.osOps.Remove(path)
}

func (r *recorder) SyncDir(dir string) error {
	r.note("syncdir", dir)
	return r.osOps.SyncDir(dir)
}

type recordedFile struct {
	tempFile
	rec *recorder
}

// final is the name the temp file will be linked to: .tmp-<name>-<random>.
func (f *recordedFile) final() string {
	name := strings.TrimPrefix(filepath.Base(f.Name()), tempPrefix)
	return name[:strings.LastIndex(name, "-")]
}

func (f *recordedFile) Write(data []byte) (int, error) {
	f.rec.note("write", f.final())
	return f.tempFile.Write(data)
}

func (f *recordedFile) Sync() error {
	f.rec.note("sync", f.final())
	return f.tempFile.Sync()
}

func indexOf(t *testing.T, log []string, want string, from int) int {
	t.Helper()
	for i := from; i < len(log); i++ {
		if strings.HasPrefix(log[i], want) {
			return i
		}
	}
	t.Fatalf("%q not found after index %d in %v", want, from, log)
	return -1
}

func TestFlush_DurabilityOrdering(t *testing.T) {
	root := t.TempDir()
	rec := &recorder{}
	writer, err := New(newLocalStore(root, rec), "notify", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	commits := flushRows(t, writer, testRow(1, 1))
	part := strings.TrimSuffix(commits[0].Part, ".parquet")
	dt := "dt=2026-10-01"
	// Each Put makes every directory on its way down durable in its parent, then writes and
	// syncs a temp file, links it to the key, removes the temp and syncs the directory. The
	// part's Put finishes before the commit record's starts.
	parents := []string{
		"syncdir deliveries", "syncdir notify", "syncdir " + filepath.Base(root),
		"syncdir " + filepath.Base(filepath.Dir(root)),
	}
	var steps []string
	for _, name := range []string{part + ".parquet", part + ".json"} {
		steps = append(steps, parents...)
		steps = append(steps, "write "+name, "sync "+name, "link "+name, "remove .tmp-"+name,
			"syncdir "+dt)
	}
	cursor := 0
	for _, step := range steps {
		cursor = indexOf(t, rec.log, step, cursor) + 1
	}
	if got := len(rec.log); got != len(steps) {
		t.Fatalf("unexpected extra effects: %v", rec.log)
	}
	for _, entry := range rec.log {
		if strings.HasPrefix(entry, "rename") {
			t.Fatalf("a commit renamed: %v", rec.log)
		}
	}
}

func TestLocalStore_FirstPutSweepsTempFilesNeverCommitted(t *testing.T) {
	root := t.TempDir()
	commits := flushRows(t, newTestWriter(t, root), testRow(1, 1))
	dir := filepath.Dir(partPath(root, commits[0]))
	stray := filepath.Join(dir, tempPrefix+"part-dead.parquet-123")
	if err := os.WriteFile(stray, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	flushRows(t, newTestWriter(t, root), testRow(2, 2))
	if _, err := os.Stat(stray); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp file survived the first Put to its table: %v", err)
	}
	if _, err := os.Stat(partPath(root, commits[0])); err != nil {
		t.Fatalf("sweep removed a committed part: %v", err)
	}
}
