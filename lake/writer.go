package lake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// BufferedRowsMaxDefault is the Append ceiling when Config.BufferedRowsMax is zero.
const BufferedRowsMaxDefault = 1 << 20

// ErrBufferFull is returned by Append when BufferedRowsMax rows await a Flush.
var ErrBufferFull = errors.New("lake: writer buffer full; Flush first")

// Config names one table and the schema its parts are written under.
type Config struct {
	Root    string
	Program string
	Table   string
	Stream  string // recorded in each commit record
	Schema  Schema

	BufferedRowsMax int
}

// Row is one event. Values maps declared column names to Go values; a value of the wrong type is
// stored as NULL, an undeclared name is an error.
type Row struct {
	EventID   string
	EventTime time.Time // UTC date of this time picks the dt partition
	StreamSeq int64
	Payload   string // the full event as JSON
	Values    map[string]any
}

// Writer buffers rows per event date and writes them as content-addressed parts. It is not safe
// for concurrent use: the archive owns one writer per table.
type Writer struct {
	cfg      Config
	enc      *encoder
	declared map[string]bool
	buffers  map[string][]Row // dt -> rows
	buffered int
	now      func() time.Time

	// beforeCommit runs after a part is durable and before its commit record is written; tests
	// use it to crash there.
	beforeCommit func() error
}

// NewWriter validates the config. It touches no files.
func NewWriter(cfg Config) (*Writer, error) {
	if cfg.Root == "" || !validName(cfg.Program) || !validName(cfg.Table) {
		return nil, fmt.Errorf("lake: bad root/program/table %q/%q/%q", cfg.Root, cfg.Program, cfg.Table)
	}
	if err := cfg.Schema.Validate(); err != nil {
		return nil, err
	}
	if cfg.BufferedRowsMax < 0 {
		return nil, fmt.Errorf("lake: BufferedRowsMax %d is negative", cfg.BufferedRowsMax)
	}
	if cfg.BufferedRowsMax == 0 {
		cfg.BufferedRowsMax = BufferedRowsMaxDefault
	}
	declared := make(map[string]bool, len(cfg.Schema.Columns))
	for _, column := range cfg.Schema.Columns {
		declared[column.Name] = true
	}
	return &Writer{
		cfg: cfg, enc: newEncoder(cfg.Schema.Columns), declared: declared,
		buffers: map[string][]Row{}, now: time.Now,
	}, nil
}

// Append buffers one row under its event date.
func (w *Writer) Append(row Row) error {
	if row.EventID == "" || row.EventTime.IsZero() {
		return fmt.Errorf("lake: row needs EventID and EventTime")
	}
	for name := range row.Values {
		if !w.declared[name] {
			return fmt.Errorf("lake: row %s has undeclared column %q", row.EventID, name)
		}
	}
	if w.buffered >= w.cfg.BufferedRowsMax {
		return ErrBufferFull
	}
	dt := row.EventTime.UTC().Format(time.DateOnly)
	w.buffers[dt] = append(w.buffers[dt], row)
	w.buffered++
	return nil
}

// Flush writes every buffered date as a part, then its commit record. A date whose write fails
// stays buffered, as do later ones, so a retry produces the same parts; the commits made before
// the failure are returned with the error.
func (w *Writer) Flush() ([]Commit, error) {
	dts := make([]string, 0, len(w.buffers))
	for dt := range w.buffers {
		dts = append(dts, dt)
	}
	sort.Strings(dts)
	var commits []Commit
	for _, dt := range dts {
		commit, err := w.flushPartition(dt, w.buffers[dt])
		if err != nil {
			return commits, err
		}
		commits = append(commits, commit)
		w.buffered -= len(w.buffers[dt])
		delete(w.buffers, dt)
	}
	return commits, nil
}

func (w *Writer) flushPartition(dt string, rows []Row) (Commit, error) {
	if len(rows) == 0 || dt == "" {
		panic("lake: flushPartition needs a date and rows")
	}
	data, err := w.enc.encode(rows, w.cfg.Schema.Version)
	if err != nil {
		return Commit{}, err
	}
	sum := sha256.Sum256(data)
	stem := "part-" + hex.EncodeToString(sum[:8])
	dir := partitionDir(w.cfg.Root, w.cfg.Program, w.cfg.Table, dt)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Commit{}, fmt.Errorf("lake: mkdir %s: %w", dir, err)
	}
	if err := writeFileAtomic(dir, stem+".parquet", data); err != nil {
		return Commit{}, err
	}
	if w.beforeCommit != nil {
		if err := w.beforeCommit(); err != nil {
			return Commit{}, err
		}
	}
	commit := Commit{
		Part: stem + ".parquet", Rows: len(rows), Bytes: len(data), Stream: w.cfg.Stream,
		SeqMin: rows[0].StreamSeq, SeqMax: rows[0].StreamSeq,
		SchemaVersion: w.cfg.Schema.Version, WrittenAt: w.now().UTC(),
		Dt: dt, Path: filepath.Join(dir, stem+".parquet"),
	}
	for _, row := range rows {
		commit.SeqMin = min(commit.SeqMin, row.StreamSeq)
		commit.SeqMax = max(commit.SeqMax, row.StreamSeq)
	}
	record, err := json.Marshal(commit)
	if err != nil {
		return Commit{}, fmt.Errorf("lake: encode commit: %w", err)
	}
	if err := writeFileAtomic(dir, stem+".json", record); err != nil {
		return Commit{}, err
	}
	return commit, nil
}

// writeFileAtomic makes name appear whole or not at all: write a dot-prefixed temp file, fsync,
// rename, fsync the directory. Readers only match part-*, so temp files are invisible.
func writeFileAtomic(dir, name string, data []byte) (err error) {
	temp, err := os.CreateTemp(dir, ".tmp-"+name+"-*")
	if err != nil {
		return fmt.Errorf("lake: create temp in %s: %w", dir, err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(temp.Name())
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
	if err = os.Rename(temp.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("lake: rename %s: %w", name, err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("lake: open dir %s: %w", dir, err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("lake: sync dir %s: %w", dir, err)
	}
	return nil
}
