# lake

`github.com/danmestas/dagnats/lake` is the shared analytics store's layout, schema rules and
commit records. It is its own Go module, pure Go; DuckDB is a test-only dependency.

```
<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.parquet
<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.json    (commit record)
```

A reader trusts a part only through its commit record. `lake.ListCommitted` lists a local
table's commits and reports records it will not trust (`Rejected`) and commits of one stream
with overlapping sequence ranges (`Overlaps`).

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
  each with its own commit.

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
file, fsyncs it, and links it into place with link(2), which fails if the key exists. lode
(`github.com/danmestas/lode`) holds the S3-compatible store. Its conditional-PUT path is tested
by lode against Garage and SigV4 vectors, not yet live on R2.
