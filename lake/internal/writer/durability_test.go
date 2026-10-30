// Methodology: durability is an ordering of effects, so the writer's filesystem is swapped for a
// recorder that wraps the real one and logs each call. The test asserts the order a crash-safe
// commit needs; dropping any sync from the writer fails it.
package writer

import (
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

func (r *recorder) Rename(from, to string) error {
	r.note("rename", to)
	return r.osOps.Rename(from, to)
}

func (r *recorder) SyncDir(dir string) error {
	r.note("syncdir", dir)
	return r.osOps.SyncDir(dir)
}

type recordedFile struct {
	tempFile
	rec *recorder
}

// final is the name the temp file will be renamed to: .tmp-<name>-<random>.
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
		if strings.HasSuffix(log[i], want) || log[i] == want {
			return i
		}
	}
	t.Fatalf("%q not found after index %d in %v", want, from, log)
	return -1
}

func TestFlush_DurabilityOrdering(t *testing.T) {
	root := t.TempDir()
	rec := &recorder{}
	writer, err := newWriter(testConfig(root), rec)
	if err != nil {
		t.Fatal(err)
	}
	commits := flushRows(t, writer, testRow(1, 1))
	part := strings.TrimSuffix(commits[0].Part, ".parquet")
	log := rec.log
	dt := "dt=2026-10-01"
	// Each step must come after the previous: new directories are durable in their parents
	// before the part is renamed into them; the part is synced, renamed and its directory
	// synced before the commit record is written; the commit is synced the same way.
	steps := []string{
		"syncdir deliveries", "syncdir notify", "syncdir " + filepath.Base(root),
		"write " + part + ".parquet", "sync " + part + ".parquet", "rename " + part + ".parquet",
		"syncdir " + dt,
		"write " + part + ".json", "sync " + part + ".json", "rename " + part + ".json",
		"syncdir " + dt,
	}
	cursor := 0
	for _, step := range steps {
		cursor = indexOf(t, log, step, cursor) + 1
	}
	if got := len(log); got != len(steps) {
		t.Fatalf("unexpected extra effects: %v", log)
	}
}

func TestNewWriter_SweepsTempFilesNeverCommitted(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	commits := flushRows(t, writer, testRow(1, 1))
	dir := filepath.Dir(commits[0].Path)
	stray := filepath.Join(dir, tempPrefix+"part-dead.parquet-123")
	if err := os.WriteFile(stray, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWriter(testConfig(root)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatalf("temp file survived NewWriter: %v", err)
	}
	if _, err := os.Stat(commits[0].Path); err != nil {
		t.Fatalf("sweep removed a committed part: %v", err)
	}
}
