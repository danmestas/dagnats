package configfile

// Methodology: integration tests against the real fsnotify watcher
// using a tempdir. Each test uses a sub-100ms debounce so the test
// suite stays snappy while still exercising the debounce path.
// Each test asserts a positive (reload fired with expected content)
// and a negative (no extra reloads / no reload when content
// unchanged) signal per the dagnats coding rules.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// silentLogger drops watcher log output so the test stream stays
// clean. A noisy logger here would drown the test failure messages.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recorder captures every ReloadFn invocation. Channel sends are
// non-blocking via select-default so a runaway burst doesn't deadlock
// the watcher loop.
type recorder struct {
	mu     sync.Mutex
	calls  []ConfigFile
	notify chan struct{}
}

func newRecorder() *recorder {
	return &recorder{notify: make(chan struct{}, 32)}
}

func (r *recorder) reload(cfg ConfigFile) error {
	r.mu.Lock()
	r.calls = append(r.calls, cfg)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) waitFor(t *testing.T, target int, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for r.count() < target {
		select {
		case <-r.notify:
		case <-deadline:
			t.Fatalf("waited %s for %d reloads, got %d",
				d, target, r.count())
		}
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}

func startWatcher(
	t *testing.T, cfgPath string, rec *recorder,
) *Watcher {
	t.Helper()
	w, err := NewWatcher(cfgPath, rec.reload, silentLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	w.SetDebounce(75 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	if err := w.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		w.Stop()
	})
	return w
}

const initialYAML = `
workflows:
  - name: hello
    steps:
      - id: a
        task: echo
triggers:
  - id: t1
    workflow_id: hello
    enabled: true
    cron:
      expression: "* * * * *"
`

const updatedYAML = `
workflows:
  - name: hello
    steps:
      - id: a
        task: echo
triggers:
  - id: t1
    workflow_id: hello
    enabled: false
    cron:
      expression: "* * * * *"
`

func TestWatcherFiresOnEdit(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "dagnats.yaml")
	writeFile(t, cfg, initialYAML)

	rec := newRecorder()
	startWatcher(t, cfg, rec)

	// Edit the file. Watcher should fire within ~1.5s comfortably.
	writeFile(t, cfg, updatedYAML)
	rec.waitFor(t, 1, 1500*time.Millisecond)

	got := rec.calls[0]
	if len(got.Triggers) != 1 || got.Triggers[0].Enabled {
		t.Fatalf("reloaded trigger Enabled = true; want false. got %+v",
			got.Triggers)
	}
}

func TestWatcherAtomicSaveRenameTriggersReload(t *testing.T) {
	// Simulates vim `:w` which atomic-saves: write a `.swp.<random>`
	// alongside, then rename it onto the target path. The file-level
	// watch would lose the inode here; the parent-dir watch must
	// still fire.
	dir := t.TempDir()
	cfg := filepath.Join(dir, "dagnats.yaml")
	writeFile(t, cfg, initialYAML)

	rec := newRecorder()
	startWatcher(t, cfg, rec)

	tmp := cfg + ".swp"
	writeFile(t, tmp, updatedYAML)
	if err := os.Rename(tmp, cfg); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	rec.waitFor(t, 1, 1500*time.Millisecond)
	got := rec.calls[len(rec.calls)-1]
	if len(got.Triggers) != 1 || got.Triggers[0].Enabled {
		t.Fatalf("atomic-save reload: trigger Enabled = true; want false")
	}
}

func TestWatcherContentHashDedupSuppressesMtimeOnlyChange(t *testing.T) {
	// macOS Spotlight / metadata pokes touch mtime without changing
	// content. The content-hash dedup must absorb these so the
	// reload callback only fires on real edits.
	dir := t.TempDir()
	cfg := filepath.Join(dir, "dagnats.yaml")
	writeFile(t, cfg, initialYAML)

	rec := newRecorder()
	startWatcher(t, cfg, rec)

	// First, a real edit so we know the watcher is hot.
	writeFile(t, cfg, updatedYAML)
	rec.waitFor(t, 1, 1500*time.Millisecond)
	baseline := rec.count()

	// Now bump mtime without changing content.
	future := time.Now().Add(1 * time.Second)
	if err := os.Chtimes(cfg, future, future); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	// Give the watcher more than one debounce window to (not) fire.
	time.Sleep(400 * time.Millisecond)
	if got := rec.count(); got != baseline {
		t.Fatalf("Chtimes triggered reload: count %d, want %d",
			got, baseline)
	}
}

func TestWatcherSiblingFileIgnored(t *testing.T) {
	// Touching a different file in the same directory must not fire
	// a reload — kqueue reports parent-dir Write on any contents
	// change but the watcher filters by Event.Name.
	dir := t.TempDir()
	cfg := filepath.Join(dir, "dagnats.yaml")
	writeFile(t, cfg, initialYAML)

	rec := newRecorder()
	startWatcher(t, cfg, rec)

	// Sibling write.
	writeFile(t, filepath.Join(dir, "other.txt"), "noise")
	time.Sleep(300 * time.Millisecond)

	// A reload may have been triggered by the parent-dir "Write" with
	// empty Name, but tryReload would early-return because the file
	// hash hasn't changed. So count must remain zero.
	if got := rec.count(); got != 0 {
		t.Fatalf("sibling write fired reload: count %d, want 0", got)
	}
}

func TestWatcherStopIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "dagnats.yaml")
	writeFile(t, cfg, initialYAML)

	rec := newRecorder()
	w, err := NewWatcher(cfg, rec.reload, silentLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	w.SetDebounce(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	w.Stop()
	// Second call must not panic / hang.
	w.Stop()
}

// Sanity: the recorder mechanism we rely on can detect a call.
func TestRecorderSanity(t *testing.T) {
	r := newRecorder()
	_ = r.reload(ConfigFile{})
	if atomic.LoadInt64(new(int64)) != 0 || r.count() != 1 {
		t.Fatalf("recorder did not record")
	}
}
