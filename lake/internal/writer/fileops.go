package writer

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// tempPrefix marks files that are never part of a table; a crash can leave them behind.
const tempPrefix = ".tmp-"

// fileOps is every filesystem effect the writer has. It is an interface so a test can record the
// order of writes, syncs and renames: durability is an ordering property no output reveals.
type fileOps interface {
	MkdirAll(path string) error
	CreateTemp(dir, pattern string) (tempFile, error)
	Rename(from, to string) error
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

func (osOps) Rename(from, to string) error { return os.Rename(from, to) }

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

// ensurePartition creates a date directory and makes every new entry on the way down durable
// in its parent, so a crash cannot lose a directory that holds a committed part. The parent
// syncs are unconditional: a directory created by an earlier, crashed run may never have been
// synced.
func ensurePartition(ops fileOps, partition string) error {
	if partition == "" || !strings.Contains(filepath.Base(partition), "dt=") {
		panic("lake: ensurePartition needs a dt= directory")
	}
	if err := ops.MkdirAll(partition); err != nil {
		return fmt.Errorf("lake: mkdir %s: %w", partition, err)
	}
	table := filepath.Dir(partition)
	program := filepath.Dir(table)
	for _, dir := range []string{table, program, filepath.Dir(program)} {
		if err := ops.SyncDir(dir); err != nil {
			return fmt.Errorf("lake: sync dir %s: %w", dir, err)
		}
	}
	return nil
}

// writeFileAtomic makes name appear whole or not at all: write a dot-prefixed temp file, fsync it,
// rename, fsync the directory. Readers only match part-*, so temp files are invisible.
func writeFileAtomic(ops fileOps, dir, name string, data []byte) (err error) {
	if dir == "" || name == "" || strings.HasPrefix(name, tempPrefix) {
		panic("lake: writeFileAtomic needs a directory and a final name")
	}
	temp, err := ops.CreateTemp(dir, tempPrefix+name+"-*")
	if err != nil {
		return fmt.Errorf("lake: create temp in %s: %w", dir, err)
	}
	defer func() {
		if err != nil {
			_ = ops.Remove(temp.Name())
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
	if err = ops.Rename(temp.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("lake: rename %s: %w", name, err)
	}
	if err = ops.SyncDir(dir); err != nil {
		return fmt.Errorf("lake: sync dir %s: %w", dir, err)
	}
	return nil
}

// sweepTemp removes the temp files a crash left in a table's date directories. They are never
// committed, and the table has one writer, so none is in use.
func sweepTemp(ops fileOps, table string) error {
	dirs, err := ops.ReadDir(table)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lake: sweep %s: %w", table, err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() || !strings.HasPrefix(dir.Name(), "dt=") {
			continue
		}
		path := filepath.Join(table, dir.Name())
		files, err := ops.ReadDir(path)
		if err != nil {
			return fmt.Errorf("lake: sweep %s: %w", path, err)
		}
		for _, file := range files {
			if !strings.HasPrefix(file.Name(), tempPrefix) {
				continue
			}
			if err := ops.Remove(filepath.Join(path, file.Name())); err != nil {
				return fmt.Errorf("lake: sweep %s: %w", file.Name(), err)
			}
		}
	}
	return nil
}
