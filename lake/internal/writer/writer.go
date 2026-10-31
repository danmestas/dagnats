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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	Stream  string // recorded in every commit and hashed into part names; required
	Schema  lake.Schema

	// BufferedRowsMax bounds Append; zero means PartRowsMax. It may not exceed PartRowsMax, so a
	// full buffer is always flushable.
	BufferedRowsMax int
}

// Row is one event. Values maps declared column names to Go values of the column's type (nil is
// NULL, an absent name is NULL); a value of another type, or an undeclared name, is an error.
// StreamSeq is the event's position in its stream, at least 1: commit records carry the sequence
// range so readers can detect two parts covering the same messages.
type Row struct {
	EventID   string
	EventTime time.Time // UTC date of this time picks the dt partition
	StreamSeq int64
	Payload   string // the full event as JSON
	Values    map[string]any
}

// Writer buffers rows per event date and writes them as parts named by a digest of the batch's
// content (not of the encoded Parquet bytes, so an encoder upgrade keeps names), so a true retry
// lands on the same name and different content never replaces committed data. Not safe for
// concurrent use.
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
// stays buffered and does not stop the other dates; the commits made are returned with the joined
// errors, and a later Flush retries what is left. A batch whose digest is already committed is a
// retry of the same content: its commit is returned, and its part is rewritten only if missing.
func (w *Writer) Flush() ([]lake.Commit, error) {
	dts := make([]string, 0, len(w.buffers))
	for dt := range w.buffers {
		dts = append(dts, dt)
	}
	sort.Strings(dts)
	var commits []lake.Commit
	var failures []error
	for _, dt := range dts {
		commit, err := w.flushPartition(dt, w.buffers[dt])
		if err != nil {
			failures = append(failures, fmt.Errorf("dt=%s: %w", dt, err))
			continue
		}
		commits = append(commits, commit)
		w.buffered -= len(w.buffers[dt])
		delete(w.buffers, dt)
	}
	return commits, errors.Join(failures...)
}

// batchDigest hashes what makes a batch the same batch: where it is written and, for each row in
// sequence order, its seq, event id and payload, length-prefixed so fields cannot run together.
func (w *Writer) batchDigest(dt string, rows []Row) [sha256.Size]byte {
	hash := sha256.New()
	put := func(field string) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		hash.Write(length[:])
		hash.Write([]byte(field))
	}
	for _, field := range []string{
		w.cfg.Program, w.cfg.Table, w.cfg.Stream, dt, strconv.Itoa(w.cfg.Schema.Version),
	} {
		put(field)
	}
	for _, row := range rows {
		put(strconv.FormatInt(row.StreamSeq, 10))
		put(row.EventID)
		put(row.Payload)
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}

func (w *Writer) flushPartition(dt string, rows []Row) (lake.Commit, error) {
	if len(rows) == 0 || len(rows) > PartRowsMax {
		panic("lake: flushPartition needs 1.." + strconv.Itoa(PartRowsMax) + " rows")
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].StreamSeq != rows[j].StreamSeq {
			return rows[i].StreamSeq < rows[j].StreamSeq
		}
		return rows[i].EventID < rows[j].EventID
	})
	digest := w.batchDigest(dt, rows)
	stem := "part-" + hex.EncodeToString(digest[:8])
	dir := lake.PartitionDir(w.cfg.Root, w.cfg.Program, w.cfg.Table, dt)
	existing, err := lake.ReadCommit(filepath.Join(dir, stem+".json"))
	fullDigest := hex.EncodeToString(digest[:])
	if err == nil && existing.Digest != fullDigest {
		return lake.Commit{}, fmt.Errorf("lake: %s is committed with digest %s, batch has %s: "+
			"stem collision", stem, existing.Digest, fullDigest)
	}
	if err == nil {
		if _, statErr := os.Stat(filepath.Join(dir, existing.Part)); statErr == nil {
			existing.Dt, existing.Path = dt, filepath.Join(dir, existing.Part)
			return existing, nil
		}
	}
	data, err := w.enc.encode(rows, w.cfg.Schema.Version)
	if err != nil {
		return lake.Commit{}, err
	}
	return w.writeParts(dir, stem, fullDigest, dt, rows, data)
}

func (w *Writer) writeParts(
	dir, stem, digest, dt string, rows []Row, data []byte,
) (lake.Commit, error) {
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
		Part: stem + ".parquet", Digest: digest, Rows: len(rows), Bytes: len(data), Stream: w.cfg.Stream,
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
