# lake

`github.com/danmestas/dagnats/lake` is the shared analytics store's layout, schema rules and
commit records. It is its own Go module, pure Go; DuckDB is a test-only dependency.

```
<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.parquet
<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.json    (commit record)
<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.json.superseded    (supersede record)
```

A reader trusts a part only through its commit record. `lake.ListCommitted` lists a local
table's commits and reports records it will not trust (`Rejected`) and commits of one stream
with overlapping sequence ranges (`Overlaps`).

## Superseding parts

Committed parts are immutable. Compaction and retention (lode) hide a part instead of rewriting
it. `lake.Supersede` writes a create-only `part-<stem>.json.superseded` record beside the commit.
The record holds the commit with `SupersededBy` set to the stem of the part that now holds the
rows, or left empty when retention dropped them. Its `WrittenAt` is when the part stopped being
live.

`ListCommitted` lists a superseded part in `Superseded` instead of `Commits`, and leaves it out
of `Overlaps`. A supersede record whose part has been deleted is not rejected: deleting the part
is what follows. A compacted part therefore overlaps its inputs until the last of them is
superseded. A second `Supersede` to the same successor returns the stored record. One naming
another successor is an error.

`Supersede` writes through a `lake.RecordStore` (Put and Get), which every `writer.Store`
satisfies. The rewrite design is lode's `docs/design/lake-rewrite.md`.

## Writing

`lake/writer` is the one writer of this layout. Every program writes through it, so one reader
works over every program's data.

```go
store := writer.NewLocalStore("/var/lib/dagnats/lake")
w, err := writer.New(store, "notify", writer.Config{
	Table: "deliveries", Stream: "NOTIFY_DELIVERY", Schema: schema,
})
// ...
err = w.Append(writer.Row{EventID: id, EventTime: at, StreamSeq: seq, Payload: raw})
commits, err := w.Flush(ctx)
```

- A writer is scoped to one program and one table, and writes only under
  `<program>/<table>/`.
- A part is named by a digest of its batch's content, so a retry lands on the same name.
  Committed parts are never replaced, renamed or deleted.
- A date whose rows encode past `writer.PartBytesMax` (64 MiB) is split into several parts,
  each with its own commit, at stream sequence boundaries. `Append` refuses a row that alone
  would encode past the ceiling; `Flush` fails on one sequence's rows that would.
- A retry after a partial failure that also carries newly appended rows is a different batch.
  Its parts overlap the committed ones, and `ListCommitted` reports them as `Overlaps`.

### The Store seam

```go
type Store interface {
	Put(ctx context.Context, key string, data []byte) error // create-only: fs.ErrExist if present
	Get(ctx context.Context, key string) ([]byte, error)    // fs.ErrNotExist if absent
}
```

Keys are slash-separated and relative to the store's root. A commit is two Puts, the part and
then its record. Nothing renames.

`Put` must not replace an existing key. A store with conditional writes (S3 `If-None-Match: *`,
R2, Tigris, MinIO) is atomic. A store without them (Garage v2.1.0) may check first and then
write, and must document that two racing Puts of one key can both succeed. The writer stays
correct either way. Two writers can only meet on one key when they hold the same batch, so
either copy of the part, and either commit record, is right.

`writer.LocalStore` is the local implementation, used by dagnats' archive. It writes a temp
file, fsyncs it, and links it into place with link(2), which fails if the key exists. It needs a
filesystem with hard links, and one instance per root per process, since an instance sweeps
crashed runs' temp files on its first Put to a table. lode
(`github.com/danmestas/lode`) holds the S3-compatible store. Its conditional-PUT path is tested
live against Cloudflare R2; Garage v2.1.0 ignores `If-None-Match` and is check-first.
