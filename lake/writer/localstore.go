package writer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// tempPrefix marks files that are never part of a table; a crash can leave them behind.
const tempPrefix = ".tmp-"

// KeyDepthMax bounds a key's segments, and so the directory syncs one Put makes.
const KeyDepthMax = 16

// sweptTablesMax bounds the tables one LocalStore remembers sweeping.
const sweptTablesMax = 1 << 12

// LocalStore is a Store over a local directory, the lake root. Put is atomic and create-only:
// it writes a dot-prefixed temp file beside the key, fsyncs it, links it to the key with
// link(2), which fails if the key exists, removes the temp, and fsyncs the directory. Rename
// would replace an existing key, so it is never used. Every directory on the way down is fsynced
// in its parent first, unconditionally, because one created by a crashed run may never have
// been. Readers match only part-*, so temp files are invisible.
//
// The first Put under a "<program>/<table>/" prefix removes temp files a crashed process left in
// that table's date directories. That relies on one process writing a table at a time: a second
// process's in-flight temp would be removed and its Put fail, never commit wrong data. Puts are
// serialized, so the sweep never meets a temp of this store. Safe for concurrent use.
//
// Use one LocalStore per root per process: two instances can sweep each other's temp files. The
// root's own entry is made durable in its parent, but no directory above that. The filesystem
// must support hard links; there is no fallback. Put can fail after the link succeeded, when the
// directory sync fails: the object is then in place but maybe not durable, and a retry gets
// fs.ErrExist, which the writer takes as the same content already stored.
type LocalStore struct {
	root string
	ops  fileOps

	mu    sync.Mutex
	swept map[string]bool // "<program>/<table>" prefixes already swept
}

// NewLocalStore returns a store rooted at dir, which is created on the first Put.
func NewLocalStore(dir string) *LocalStore {
	return newLocalStore(dir, osOps{})
}

func newLocalStore(dir string, ops fileOps) *LocalStore {
	if dir == "" || ops == nil {
		panic("lake: NewLocalStore needs a directory")
	}
	return &LocalStore{root: dir, ops: ops, swept: map[string]bool{}}
}

// Put creates key with data; an existing key is an error matching fs.ErrExist.
func (s *LocalStore) Put(ctx context.Context, key string, data []byte) error {
	segments := checkKey(key)
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sweepOnce(segments); err != nil {
		return err
	}
	dir := filepath.Join(s.root, filepath.FromSlash(path.Dir(key)))
	if err := s.ensureDir(dir, len(segments)-1); err != nil {
		return err
	}
	return s.createFile(dir, path.Base(key), data)
}

// Get returns the object at key; a missing key is an error matching fs.ErrNotExist.
func (s *LocalStore) Get(ctx context.Context, key string) ([]byte, error) {
	checkKey(key)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(s.root, filepath.FromSlash(key)))
	if err != nil {
		return nil, fmt.Errorf("lake: get %s: %w", key, err)
	}
	return data, nil
}

// checkKey panics on a key no writer builds: keys are relative, clean and slash-separated.
func checkKey(key string) []string {
	if key == "" || path.Clean(key) != key || strings.HasPrefix(key, "/") ||
		strings.HasPrefix(key, "../") || key == ".." || strings.Contains(key, "\\") {
		panic(fmt.Sprintf("lake: store key %q is not a clean relative key", key))
	}
	segments := strings.Split(key, "/")
	if len(segments) > KeyDepthMax || strings.HasPrefix(segments[len(segments)-1], tempPrefix) {
		panic(fmt.Sprintf("lake: store key %q is too deep or names a temp file", key))
	}
	return segments
}

// ensureDir creates dir, depth levels below the root, and makes every entry on the way down
// durable in its parent, up to and including the root's own entry.
func (s *LocalStore) ensureDir(dir string, depth int) error {
	if depth < 0 || depth >= KeyDepthMax {
		panic("lake: ensureDir depth out of range")
	}
	if err := s.ops.MkdirAll(dir); err != nil {
		return fmt.Errorf("lake: mkdir %s: %w", dir, err)
	}
	current := dir
	for range depth + 1 {
		parent := filepath.Dir(current)
		if err := s.ops.SyncDir(parent); err != nil {
			return fmt.Errorf("lake: sync dir %s: %w", parent, err)
		}
		current = parent
	}
	return nil
}

// createFile makes name appear whole or not at all, and never over an existing file.
func (s *LocalStore) createFile(dir, name string, data []byte) (err error) {
	if dir == "" || name == "" {
		panic("lake: createFile needs a directory and a final name")
	}
	temp, err := s.ops.CreateTemp(dir, tempPrefix+name+"-*")
	if err != nil {
		return fmt.Errorf("lake: create temp in %s: %w", dir, err)
	}
	defer func() {
		if removeErr := s.ops.Remove(temp.Name()); removeErr != nil && err == nil {
			err = fmt.Errorf("lake: remove temp of %s: %w", name, removeErr)
		}
		if err == nil {
			err = s.ops.SyncDir(dir)
		}
	}()
	if _, err = temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("lake: write %s: %w", name, err)
	}
	if err = temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("lake: sync %s: %w", name, err)
	}
	if err = temp.Close(); err != nil {
		return fmt.Errorf("lake: close %s: %w", name, err)
	}
	if err = s.ops.Link(temp.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("lake: put %s: %w", name, err)
	}
	return nil
}

// sweepOnce removes, once per store, the temp files a crash left in a table's date directories.
func (s *LocalStore) sweepOnce(segments []string) error {
	if len(segments) < 3 {
		return nil
	}
	table := segments[0] + "/" + segments[1]
	if s.swept[table] {
		return nil
	}
	if len(s.swept) >= sweptTablesMax {
		return fmt.Errorf("lake: local store has swept %d tables; use another store", sweptTablesMax)
	}
	if err := s.sweepTable(filepath.Join(s.root, filepath.FromSlash(table))); err != nil {
		return err
	}
	s.swept[table] = true
	return nil
}

func (s *LocalStore) sweepTable(table string) error {
	dirs, err := s.ops.ReadDir(table)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lake: sweep %s: %w", table, err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() || !strings.HasPrefix(dir.Name(), "dt=") {
			continue
		}
		partition := filepath.Join(table, dir.Name())
		files, err := s.ops.ReadDir(partition)
		if err != nil {
			return fmt.Errorf("lake: sweep %s: %w", partition, err)
		}
		for _, file := range files {
			if !strings.HasPrefix(file.Name(), tempPrefix) {
				continue
			}
			if err := s.ops.Remove(filepath.Join(partition, file.Name())); err != nil {
				return fmt.Errorf("lake: sweep %s: %w", file.Name(), err)
			}
		}
	}
	return nil
}

// fileOps is every filesystem effect the LocalStore has. It is an interface so a test can record
// the order of writes, syncs and links: durability is an ordering property no output reveals.
type fileOps interface {
	MkdirAll(path string) error
	CreateTemp(dir, pattern string) (tempFile, error)
	Link(from, to string) error
	SyncDir(dir string) error
	ReadDir(dir string) ([]fs.DirEntry, error)
	Remove(path string) error
}

type tempFile interface {
	Write(data []byte) (int, error)
	Sync() error
	Close() error
	Name() string
}

type osOps struct{}

func (osOps) MkdirAll(path string) error { return os.MkdirAll(path, 0o755) }

func (osOps) CreateTemp(dir, pattern string) (tempFile, error) {
	return os.CreateTemp(dir, pattern)
}

func (osOps) Link(from, to string) error { return os.Link(from, to) }

func (osOps) ReadDir(dir string) ([]fs.DirEntry, error) { return os.ReadDir(dir) }

func (osOps) Remove(path string) error { return os.Remove(path) }

func (osOps) SyncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
