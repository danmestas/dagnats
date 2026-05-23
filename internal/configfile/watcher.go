// internal/configfile/watcher.go
// fsnotify-based hot-reload watcher. Watches the PARENT directory of
// the config file (not the file itself) because kqueue on macOS ties
// the watch to the file's inode — and editors that atomic-save
// (vim `:w`, VSCode, JetBrains) replace the inode on every save,
// invalidating a file-level watch. The fsnotify README recommends
// the parent-dir pattern explicitly for exactly this case (audit
// confirmed on v1.10.1).
//
// On every event the watcher filters by `Event.Name == cfgPath` so
// sibling-file writes in the same directory don't fire reloads. A
// 500ms debounce coalesces editor save bursts, and a content-hash
// dedup absorbs Spotlight / metadata noise on macOS where the
// debounce alone is not enough (also called out in the audit).
package configfile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// defaultDebounce is the editor-save coalescing window. 500ms is
// well above any single atomic-save sequence (rename → write) and
// short enough that operators don't feel a perceptible lag.
const defaultDebounce = 500 * time.Millisecond

// defaultMaxReadBytes bounds the file read on every reload so a
// pathological file growth cannot OOM the engine. Matches
// load.go's maxConfigBytes.
const defaultMaxReadBytes = 1 << 20

// ReloadFn is called on every debounced, content-changed event.
// Implementations should be quick — the watcher loop holds no lock
// while invoking it, but a slow ReloadFn delays the next debounce.
// The returned error is logged but does not stop the watcher; the
// next valid file write should reset state.
type ReloadFn func(cfg ConfigFile) error

// Watcher coordinates one fsnotify watcher on the parent directory
// of cfgPath and a debounce timer. Stop() is idempotent.
type Watcher struct {
	cfgPath  string
	parent   string
	debounce time.Duration
	reload   ReloadFn
	logger   *slog.Logger

	fsw      *fsnotify.Watcher
	cancel   context.CancelFunc
	done     chan struct{}
	lastHash [32]byte
	mu       sync.Mutex // guards lastHash
}

// NewWatcher constructs a Watcher for the given config path.
// The parent directory of cfgPath must exist; the file itself need
// not exist yet at Start time. Panics on programmer error.
func NewWatcher(
	cfgPath string, reload ReloadFn, logger *slog.Logger,
) (*Watcher, error) {
	if cfgPath == "" {
		panic("NewWatcher: cfgPath must not be empty")
	}
	if reload == nil {
		panic("NewWatcher: reload must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	abs, err := filepath.Abs(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("abs %q: %w", cfgPath, err)
	}
	parent := filepath.Dir(abs)
	info, err := os.Stat(parent)
	if err != nil {
		return nil, fmt.Errorf("stat parent %q: %w", parent, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("parent %q is not a directory", parent)
	}

	return &Watcher{
		cfgPath:  abs,
		parent:   parent,
		debounce: defaultDebounce,
		reload:   reload,
		logger:   logger,
	}, nil
}

// SetDebounce overrides the default 500ms debounce. Intended for
// tests; production callers should leave the default.
func (w *Watcher) SetDebounce(d time.Duration) {
	if d <= 0 {
		panic("SetDebounce: duration must be positive")
	}
	w.debounce = d
}

// Start begins watching. Returns immediately; the watch runs in a
// background goroutine until Stop is called or ctx is cancelled.
// Also seeds lastHash with the file's current contents so the first
// real event isn't silently dropped as "no change".
func (w *Watcher) Start(ctx context.Context) error {
	if ctx == nil {
		panic("Watcher.Start: ctx must not be nil")
	}
	if w.fsw != nil {
		panic("Watcher.Start: already started")
	}

	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify.NewWatcher: %w", err)
	}
	if err := fsw.Add(w.parent); err != nil {
		_ = fsw.Close()
		return fmt.Errorf("fsnotify add parent %q: %w",
			w.parent, err)
	}
	w.fsw = fsw

	if hash, ok := w.readHash(); ok {
		w.lastHash = hash
	}

	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.done = make(chan struct{})

	go w.loop(runCtx)
	return nil
}

// Stop terminates the watcher loop and closes the fsnotify watcher.
// Idempotent — safe to call from Server shutdown even if Start
// errored or was never called.
func (w *Watcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.done != nil {
		<-w.done
	}
	if w.fsw != nil {
		_ = w.fsw.Close()
		w.fsw = nil
	}
}

// loop drains fsnotify events, filters to cfgPath, debounces, and
// fires the reload only on content-hash change. Exits on ctx done.
//
// Single-goroutine ownership of the debounce state: the timer
// channel is drained inside the same select that owns pending, so
// no second goroutine ever touches w-local variables.
func (w *Watcher) loop(ctx context.Context) {
	defer close(w.done)

	// Stopped, drained timer so the C channel is empty and ready
	// for the first Reset to start the debounce window.
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	pending := false

	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			w.logger.Warn("configfile watcher error", "err", err)
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			if !w.isOurFile(ev) {
				continue
			}
			// Reset the debounce window. Drain any pending
			// channel value first to keep semantics deterministic.
			if !timer.Stop() && pending {
				<-timer.C
			}
			pending = true
			timer.Reset(w.debounce)
		case <-timer.C:
			pending = false
			w.tryReload()
		}
	}
}

// isOurFile filters the parent-dir event stream down to events that
// concern cfgPath. kqueue may report parent-dir "Write" with empty
// Name when the directory's mtime updates; we treat empty Name as
// "directory changed, check the file" and let the hash check decide.
func (w *Watcher) isOurFile(ev fsnotify.Event) bool {
	if ev.Name == "" {
		return true
	}
	abs, err := filepath.Abs(ev.Name)
	if err != nil {
		return false
	}
	return abs == w.cfgPath
}

// tryReload reads the file, compares the content hash, and calls
// reload only when the hash differs. Read / parse errors are logged
// at warn but do not stop the watcher.
func (w *Watcher) tryReload() {
	hash, ok := w.readHash()
	if !ok {
		// File missing / unreadable. Treat as "no reload" and
		// wait for the next event — likely a transient
		// rename-in-progress on atomic save.
		return
	}
	w.mu.Lock()
	if hash == w.lastHash {
		w.mu.Unlock()
		return
	}
	w.lastHash = hash
	w.mu.Unlock()

	f, err := os.Open(w.cfgPath)
	if err != nil {
		w.logger.Warn("configfile reload open",
			"path", w.cfgPath, "err", err)
		return
	}
	defer f.Close()

	cfg, err := Load(io.LimitReader(f, defaultMaxReadBytes+1))
	if err != nil {
		w.logger.Warn("configfile reload parse",
			"path", w.cfgPath, "err", err)
		return
	}
	if err := Validate(cfg); err != nil {
		w.logger.Warn("configfile reload validate",
			"path", w.cfgPath, "err", err)
		return
	}
	if err := w.reload(cfg); err != nil {
		w.logger.Warn("configfile reload apply",
			"path", w.cfgPath, "err", err)
		return
	}
	w.logger.Info("configfile reloaded",
		"path", w.cfgPath,
		"workflows", len(cfg.Workflows),
		"triggers", len(cfg.Triggers))
}

// readHash returns a sha256 of the file contents. Bool false means
// the file could not be read (yet) — caller treats that as "skip".
func (w *Watcher) readHash() ([32]byte, bool) {
	f, err := os.Open(w.cfgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return [32]byte{}, false
		}
		w.logger.Warn("configfile read hash",
			"path", w.cfgPath, "err", err)
		return [32]byte{}, false
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, defaultMaxReadBytes+1)); err != nil {
		w.logger.Warn("configfile hash copy",
			"path", w.cfgPath, "err", err)
		return [32]byte{}, false
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, true
}
