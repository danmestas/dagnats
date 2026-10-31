// Package writer writes a program's tables into the lake: Parquet parts named by a digest of
// their content, each followed by a commit record that makes it visible to lake.ListCommitted.
// The layout, reserved columns and commit format are the lake package's; this is the one
// implementation of them, so every program's data reads the same way.
//
// A Writer is scoped to one program and one table. It writes only under "<program>/<table>/" in
// its Store; building any other key is a programmer error and panics. Committed parts are
// immutable: the writer only creates keys, never replaces, renames or deletes them. Rewriting
// partitions for compaction or retention is a separate design.
//
// One table has one Writer at a time; that is what makes a part's sequence range the writer's to
// claim. Two writers that do meet on one key are committing the same batch (see Store), so the
// writer takes no lock.
//
// A part's name, and a date's split into parts, follow the exact rows flushed. A retry after a
// partial failure that also carries newly appended rows is a different batch: its parts overlap
// the ones already committed, and ListCommitted reports them as Overlaps, as it would for any
// re-flush of one stream's messages under different content.
//
// LocalStore is the local implementation, used by dagnats' archive. lode
// (github.com/danmestas/lode) holds the S3-compatible one. Its conditional-PUT path is tested by
// lode against Garage and SigV4 vectors, not yet live on R2.
package writer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/lake"
)

// Store is where a Writer's bytes go. Keys are slash-separated and relative to the store's root,
// e.g. "lode/events/dt=2026-10-31/part-0123456789abcdef.parquet". A Store owns its root:
// LocalStore takes a directory, an S3 store a bucket and prefix.
//
// The writer's correctness does not depend on Put being atomic, only on Put being create-only in
// the absence of a race:
//   - parts are named by a digest of their content, so two writers putting one part key put the
//     same rows, and either copy is correct;
//   - a commit record's key is derived from the same digest, so two writers racing on one record
//     key are committing the same batch, and the records differ only in WrittenAt.
//
// The record is the visibility point, not a mutual-exclusion point.
type Store interface {
	// Put creates key with data. When key already exists it returns an error matching
	// fs.ErrExist via errors.Is, and does not replace the object. When it returns nil the object
	// is durable: a later Get, from any process, returns data.
	//
	// A store with conditional writes (S3 If-None-Match: *, R2, Tigris, MinIO; link(2) locally)
	// is atomic. A store without them (Garage v2.1.0) may check first and then write; it must
	// document that two concurrent Puts of one key can both succeed, last writer wins.
	Put(ctx context.Context, key string, data []byte) error

	// Get returns the whole object at key, or an error matching fs.ErrNotExist via errors.Is.
	// The writer only Gets commit records, never parts.
	Get(ctx context.Context, key string) ([]byte, error)
}

// PartRowsMax is the most rows one part holds, and so the most one Flush may encode for a date.
const PartRowsMax = 1 << 20

// PartBytesMax is the most bytes one encoded part holds. A date whose rows encode past it is
// split across several parts, each with its own commit. It keeps a part well under the 128 MiB
// object cap of lode's stores.
const PartBytesMax = 64 << 20

// ErrBufferFull is returned by Append when BufferedRowsMax rows await a Flush.
var ErrBufferFull = errors.New("lake: writer buffer full; Flush first")

// Config names one table of the writer's program and the schema its parts are written under.
type Config struct {
	Table  string
	Stream string // recorded in every commit and hashed into part names; required
	Schema lake.Schema

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
	store    Store
	program  string
	cfg      Config
	enc      *encoder
	declared map[string]lake.ColumnType
	buffers  map[string][]Row // dt -> rows
	buffered int
	now      func() time.Time

	// partBytesMax is PartBytesMax; tests lower it to split small batches.
	partBytesMax int
	// beforeCommit runs after a part is stored and before its commit record is; tests use it
	// to crash there.
	beforeCommit func() error
}

// New returns a writer for one table of program in store. Names come from callers' config, so
// a bad one is an error, not a panic. It does no I/O.
func New(store Store, program string, cfg Config) (*Writer, error) {
	if store == nil {
		return nil, fmt.Errorf("lake: New needs a Store")
	}
	if !lake.ValidName(program) || !lake.ValidName(cfg.Table) {
		return nil, fmt.Errorf("lake: bad program/table %q/%q", program, cfg.Table)
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
	return &Writer{
		store: store, program: program, cfg: cfg, enc: newEncoder(cfg.Schema.Columns),
		declared: declared, buffers: map[string][]Row{}, now: time.Now,
		partBytesMax: PartBytesMax,
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
	if err := w.checkRowFits(row); err != nil {
		return err
	}
	dt := row.EventTime.UTC().Format(time.DateOnly)
	w.buffers[dt] = append(w.buffers[dt], row)
	w.buffered++
	return nil
}

// Flush writes every buffered date as one or more parts, each followed by its commit record:
// Put the part, then Put the record. fs.ErrExist on the part means the same content is there;
// on the record it means the same batch is committed, which a Get and a digest compare confirm
// (a different digest is a stem collision, an error). There is no rename, delete or listing. A
// crash between the two Puts leaves an orphan part that ListCommitted ignores.
//
// A date whose write fails stays buffered and does not stop the other dates; the commits made
// are returned with the joined errors, and a later Flush retries what is left. A retry of a
// committed batch returns its existing commit.
func (w *Writer) Flush(ctx context.Context) ([]lake.Commit, error) {
	dts := make([]string, 0, len(w.buffers))
	for dt := range w.buffers {
		dts = append(dts, dt)
	}
	sort.Strings(dts)
	var commits []lake.Commit
	var failures []error
	for _, dt := range dts {
		made, err := w.flushPartition(ctx, dt, w.buffers[dt])
		commits = append(commits, made...)
		if err != nil {
			failures = append(failures, fmt.Errorf("dt=%s: %w", dt, err))
			continue
		}
		w.buffered -= len(w.buffers[dt])
		delete(w.buffers, dt)
	}
	return commits, errors.Join(failures...)
}

// A value's bytes land in a part at most in a dictionary or data page and as min and max in the
// page header, the column chunk's statistics and the column index: 7 copies, and measured at
// about 7x. zstd grows incompressible data by well under 1%, and the footer of ColumnCountMax
// columns is a few KiB. So 16 copies plus 1 MiB bounds a one-row part with a 2x margin.
const (
	rowCopiesMax         = 16
	partOverheadBytesMax = 1 << 20
)

// checkRowFits refuses a row that alone would encode past partBytesMax, so it never enters a
// buffer it could not leave. Only a row the bound cannot clear is encoded, since an encode costs
// tens of microseconds.
func (w *Writer) checkRowFits(row Row) error {
	if w.partBytesMax < 1 || row.EventID == "" {
		panic("lake: checkRowFits on an unvalidated writer or row")
	}
	raw := len(row.EventID) + len(row.Payload) + 3*8 // event_time, stream_seq, schema_version
	for _, value := range row.Values {
		if text, ok := value.(string); ok {
			raw += len(text)
		} else {
			raw += 8
		}
	}
	if partOverheadBytesMax+rowCopiesMax*raw <= w.partBytesMax {
		return nil
	}
	data, err := w.enc.encode([]Row{row}, w.cfg.Schema.Version)
	if err != nil {
		return err
	}
	if len(data) > w.partBytesMax {
		return fmt.Errorf("lake: row %s encodes to %d bytes, over the %d-byte part ceiling",
			row.EventID, len(data), w.partBytesMax)
	}
	return nil
}

// key is the only way the writer names an object, so it is where the program scope holds.
func (w *Writer) key(dt, name string) string {
	key := lake.PartitionKey(w.program, w.cfg.Table, dt) + "/" + name
	if !strings.HasPrefix(key, w.program+"/"+w.cfg.Table+"/dt=") || path.Clean(key) != key ||
		strings.Count(key, "/") != 3 || !strings.HasPrefix(name, "part-") {
		panic(fmt.Sprintf("lake: key %q is outside %s/%s", key, w.program, w.cfg.Table))
	}
	return key
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
		w.program, w.cfg.Table, w.cfg.Stream, dt, strconv.Itoa(w.cfg.Schema.Version),
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

// flushPartition encodes a date's rows and halves any span whose part would pass partBytesMax,
// at the StreamSeq change nearest its middle, writing each span that fits in sequence order.
// Rows of one seq stay in one part, since parts of one stream whose ranges share a seq are what
// ListCommitted reports as Overlaps. The split depends only on the rows and the
// encoder, so a retry of the same rows lands on the same parts.
func (w *Writer) flushPartition(ctx context.Context, dt string, rows []Row) ([]lake.Commit, error) {
	if len(rows) == 0 || len(rows) > PartRowsMax || w.partBytesMax < 1 {
		panic("lake: flushPartition needs 1.." + strconv.Itoa(PartRowsMax) + " rows")
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].StreamSeq != rows[j].StreamSeq {
			return rows[i].StreamSeq < rows[j].StreamSeq
		}
		return rows[i].EventID < rows[j].EventID
	})
	type span struct{ low, high int }
	pending := []span{{0, len(rows)}}
	var commits []lake.Commit
	// Each pop either writes a span or splits it in two, so 2*len(rows) pops cover every case.
	for steps := 0; len(pending) > 0; steps++ {
		if steps >= 2*len(rows) {
			panic("lake: part split did not converge")
		}
		next := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		data, err := w.enc.encode(rows[next.low:next.high], w.cfg.Schema.Version)
		if err != nil {
			return commits, err
		}
		if len(data) > w.partBytesMax {
			middle, found := seqBoundary(rows, next.low, next.high)
			if !found {
				return commits, fmt.Errorf("lake: the %d rows of stream_seq %d encode to %d bytes, "+
					"over the %d-byte part ceiling", next.high-next.low, rows[next.low].StreamSeq,
					len(data), w.partBytesMax)
			}
			pending = append(pending, span{middle, next.high}, span{next.low, middle})
			continue
		}
		commit, err := w.writePart(ctx, dt, rows[next.low:next.high], data)
		if err != nil {
			return commits, err
		}
		commits = append(commits, commit)
	}
	return commits, nil
}

// seqBoundary finds the index in (low, high) nearest the middle where StreamSeq changes; rows are
// sorted by seq. It reports false when the whole span is one seq.
func seqBoundary(rows []Row, low, high int) (int, bool) {
	if low < 0 || high > len(rows) || high-low < 1 {
		panic("lake: seqBoundary span out of range")
	}
	middle := low + (high-low)/2
	for offset := 0; offset <= high-low; offset++ {
		for _, at := range []int{middle - offset, middle + offset} {
			if at > low && at < high && rows[at-1].StreamSeq != rows[at].StreamSeq {
				return at, true
			}
		}
	}
	return 0, false
}

func (w *Writer) writePart(
	ctx context.Context, dt string, rows []Row, data []byte,
) (lake.Commit, error) {
	digest := w.batchDigest(dt, rows)
	stem := "part-" + hex.EncodeToString(digest[:8])
	partKey, recordKey := w.key(dt, stem+".parquet"), w.key(dt, stem+".json")
	if err := w.store.Put(ctx, partKey, data); err != nil && !errors.Is(err, fs.ErrExist) {
		return lake.Commit{}, fmt.Errorf("lake: put %s: %w", partKey, err)
	}
	if w.beforeCommit != nil {
		if err := w.beforeCommit(); err != nil {
			return lake.Commit{}, err
		}
	}
	commit := lake.Commit{
		Part: stem + ".parquet", Digest: hex.EncodeToString(digest[:]), Rows: len(rows),
		Bytes: len(data), Stream: w.cfg.Stream,
		SeqMin: rows[0].StreamSeq, SeqMax: rows[len(rows)-1].StreamSeq,
		SchemaVersion: w.cfg.Schema.Version, WrittenAt: w.now().UTC(), Dt: dt, Key: partKey,
	}
	record, err := json.Marshal(commit)
	if err != nil {
		return lake.Commit{}, fmt.Errorf("lake: encode commit: %w", err)
	}
	err = w.store.Put(ctx, recordKey, record)
	if err == nil {
		return commit, nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return lake.Commit{}, fmt.Errorf("lake: put %s: %w", recordKey, err)
	}
	return w.existingCommit(ctx, recordKey, commit)
}

// existingCommit returns the commit already at recordKey when it is the same batch's.
func (w *Writer) existingCommit(ctx context.Context, recordKey string, ours lake.Commit) (
	lake.Commit, error,
) {
	data, err := w.store.Get(ctx, recordKey)
	if err != nil {
		return lake.Commit{}, fmt.Errorf("lake: get %s: %w", recordKey, err)
	}
	existing, err := lake.DecodeCommit(recordKey, data)
	if err != nil {
		return lake.Commit{}, err
	}
	if existing.Digest != ours.Digest {
		return lake.Commit{}, fmt.Errorf("lake: %s is committed with digest %s, batch has %s: "+
			"stem collision", existing.Part, existing.Digest, ours.Digest)
	}
	existing.Dt, existing.Key = ours.Dt, ours.Key
	return existing, nil
}
