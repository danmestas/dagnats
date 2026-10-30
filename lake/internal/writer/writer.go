// Package writer is the only way to put data into the lake. It is internal to the lake module on
// purpose: exporting a writer is how a second sync grows back, so the archive program, which lives
// in this module as cmd/dagnats-archive, is the one writer, with one credential, watermark,
// compactor and sweep. Readers use the public lake package.
//
// One table has exactly one Writer. The archive guarantees that with a single durable consumer
// per table, so the writer takes no lock; two writers on one table would race on temp files and
// on the sweep in NewWriter.
package writer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/danmestas/dagnats/lake"
)

// PartRowsMax is the most rows one part holds, and so the most one Flush may encode for a date.
const PartRowsMax = 1 << 20

// ErrBufferFull is returned by Append when BufferedRowsMax rows await a Flush.
var ErrBufferFull = errors.New("lake: writer buffer full; Flush first")

// Config names one table and the schema its parts are written under.
type Config struct {
	Root    string
	Program string
	Table   string
	Stream  string // identity of every part; required
	Schema  lake.Schema

	// BufferedRowsMax bounds Append; zero means PartRowsMax. It may not exceed PartRowsMax, so a
	// full buffer is always flushable.
	BufferedRowsMax int
}

// Row is one event. Values maps declared column names to Go values of the column's type (nil is
// NULL, an absent name is NULL); a value of another type, or an undeclared name, is an error.
// StreamSeq is the event's position in its stream, at least 1: a part is named from the sequence
// range it covers.
type Row struct {
	EventID   string
	EventTime time.Time // UTC date of this time picks the dt partition
	StreamSeq int64
	Payload   string // the full event as JSON
	Values    map[string]any
}

// Writer buffers rows per event date and writes them as parts named by identity, not by content:
// a part is (program, table, stream, date, first seq, last seq, schema version), so a retry of a
// batch lands on the same name whatever the encoder produced. Not safe for concurrent use.
type Writer struct {
	cfg      Config
	ops      fileOps
	enc      *encoder
	declared map[string]lake.ColumnType
	buffers  map[string][]Row // dt -> rows
	buffered int
	now      func() time.Time

	// beforeCommit runs after a part is durable and before its commit record is written; tests
	// use it to crash there.
	beforeCommit func() error
}

// NewWriter validates the config and removes temp files a crashed predecessor left in the
// table's directories.
func NewWriter(cfg Config) (*Writer, error) {
	return newWriter(cfg, osOps{})
}

func newWriter(cfg Config, ops fileOps) (*Writer, error) {
	if cfg.Root == "" || !lake.ValidName(cfg.Program) || !lake.ValidName(cfg.Table) {
		return nil, fmt.Errorf("lake: bad root/program/table %q/%q/%q", cfg.Root, cfg.Program, cfg.Table)
	}
	if cfg.Stream == "" {
		return nil, fmt.Errorf("lake: Stream is required: it is part of every part's identity")
	}
	if err := cfg.Schema.Validate(); err != nil {
		return nil, err
	}
	if cfg.BufferedRowsMax < 0 || cfg.BufferedRowsMax > PartRowsMax {
		return nil, fmt.Errorf("lake: BufferedRowsMax %d: want 0..%d", cfg.BufferedRowsMax, PartRowsMax)
	}
	if cfg.BufferedRowsMax == 0 {
		cfg.BufferedRowsMax = PartRowsMax
	}
	declared := make(map[string]lake.ColumnType, len(cfg.Schema.Columns))
	for _, column := range cfg.Schema.Columns {
		declared[column.Name] = column.Type
	}
	if err := sweepTemp(ops, filepath.Join(cfg.Root, cfg.Program, cfg.Table)); err != nil {
		return nil, err
	}
	return &Writer{
		cfg: cfg, ops: ops, enc: newEncoder(cfg.Schema.Columns), declared: declared,
		buffers: map[string][]Row{}, now: time.Now,
	}, nil
}

// Append buffers one row under its event date.
func (w *Writer) Append(row Row) error {
	if row.EventID == "" || row.EventTime.IsZero() || row.StreamSeq < 1 {
		return fmt.Errorf("lake: row needs EventID, EventTime and StreamSeq >= 1")
	}
	for name, value := range row.Values {
		columnType, found := w.declared[name]
		if !found {
			return fmt.Errorf("lake: row %s has undeclared column %q", row.EventID, name)
		}
		if _, ok := projectValue(columnType, value); !ok {
			return fmt.Errorf("lake: row %s column %q (%s): got %T", row.EventID, name, columnType, value)
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
// the failure are returned with the error. A batch whose identity is already committed with the
// same row count is returned as is and not rewritten.
func (w *Writer) Flush() ([]lake.Commit, error) {
	dts := make([]string, 0, len(w.buffers))
	for dt := range w.buffers {
		dts = append(dts, dt)
	}
	sort.Strings(dts)
	var commits []lake.Commit
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

// partStem names a part from its identity.
func (w *Writer) partStem(dt string, seqMin, seqMax int64) string {
	hash := sha256.New()
	for _, field := range []string{
		w.cfg.Program, w.cfg.Table, w.cfg.Stream, dt, strconv.FormatInt(seqMin, 10),
		strconv.FormatInt(seqMax, 10), strconv.Itoa(w.cfg.Schema.Version),
	} {
		hash.Write([]byte(field))
		hash.Write([]byte{0})
	}
	return "part-" + hex.EncodeToString(hash.Sum(nil)[:8])
}

func (w *Writer) flushPartition(dt string, rows []Row) (lake.Commit, error) {
	if len(rows) == 0 || len(rows) > PartRowsMax {
		panic("lake: flushPartition needs 1.." + strconv.Itoa(PartRowsMax) + " rows")
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].StreamSeq < rows[j].StreamSeq })
	seqMin, seqMax := rows[0].StreamSeq, rows[len(rows)-1].StreamSeq
	stem := w.partStem(dt, seqMin, seqMax)
	dir := lake.PartitionDir(w.cfg.Root, w.cfg.Program, w.cfg.Table, dt)
	existing, err := lake.ReadCommit(filepath.Join(dir, stem+".json"))
	switch {
	case err == nil && existing.Rows == len(rows):
		existing.Dt, existing.Path = dt, filepath.Join(dir, existing.Part)
		return existing, nil
	case err == nil:
		return lake.Commit{}, fmt.Errorf("lake: %s is committed with %d rows, batch has %d",
			stem, existing.Rows, len(rows))
	case !errors.Is(err, fs.ErrNotExist):
		return lake.Commit{}, err
	}
	data, err := w.enc.encode(rows, w.cfg.Schema.Version)
	if err != nil {
		return lake.Commit{}, err
	}
	return w.writeParts(dir, stem, dt, rows, data)
}

func (w *Writer) writeParts(dir, stem, dt string, rows []Row, data []byte) (lake.Commit, error) {
	if err := ensurePartition(w.ops, dir); err != nil {
		return lake.Commit{}, err
	}
	if err := writeFileAtomic(w.ops, dir, stem+".parquet", data); err != nil {
		return lake.Commit{}, err
	}
	if w.beforeCommit != nil {
		if err := w.beforeCommit(); err != nil {
			return lake.Commit{}, err
		}
	}
	commit := lake.Commit{
		Part: stem + ".parquet", Rows: len(rows), Bytes: len(data), Stream: w.cfg.Stream,
		SeqMin: rows[0].StreamSeq, SeqMax: rows[len(rows)-1].StreamSeq,
		SchemaVersion: w.cfg.Schema.Version, WrittenAt: w.now().UTC(),
		Dt: dt, Path: filepath.Join(dir, stem+".parquet"),
	}
	record, err := json.Marshal(commit)
	if err != nil {
		return lake.Commit{}, fmt.Errorf("lake: encode commit: %w", err)
	}
	if err := writeFileAtomic(w.ops, dir, stem+".json", record); err != nil {
		return lake.Commit{}, err
	}
	return commit, nil
}
