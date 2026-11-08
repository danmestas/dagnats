// Methodology: the bytes additions (Schema.PayloadBytes and the TypeBytes column) proven end to
// end. Rows go through Writer into a LocalStore and DuckDB reads the committed parts back, since
// "BLOB, not VARCHAR" is a claim about the reader. Identity tests compare batch digests across
// writers, and a golden check pins that a string table's part name did not move.
package writer

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/lake"
)

func bytesConfig() Config {
	return Config{
		Table: "deliveries", Stream: "SEALED",
		Schema: lake.Schema{Version: 1, PayloadBytes: true, Columns: []lake.Column{
			{Name: "blob", Type: lake.TypeBytes}, {Name: "org_id", Type: lake.TypeString},
		}},
	}
}

func bytesRow(seq int64, payload []byte, blob any) Row {
	row := testRow(seq, 1)
	row.Payload = ""
	row.PayloadBytes = payload
	row.Values = map[string]any{"org_id": "o1", "blob": blob}
	return row
}

func TestBytes_DuckDBReadsBlobs(t *testing.T) {
	root := t.TempDir()
	w, err := New(NewLocalStore(root), "notify", bytesConfig())
	if err != nil {
		t.Fatal(err)
	}
	invalidUTF8 := []byte{0xff, 0xfe, 0x00, 0x80, 'a'}
	flushRows(t, w, bytesRow(1, invalidUTF8, []byte{0, 1, 2}), bytesRow(2, []byte{}, nil))
	listing, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil || len(listing.Commits) != 1 {
		t.Fatalf("commits = %d, %v", len(listing.Commits), err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := "SELECT typeof(payload), payload, typeof(blob), blob FROM read_parquet('" +
		listing.Commits[0].Path + "') ORDER BY stream_seq"
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var payloadTypes, blobTypes []string
	var payloads, blobs [][]byte
	for rows.Next() {
		var pt, bt string
		var payload, blob []byte
		if err := rows.Scan(&pt, &payload, &bt, &blob); err != nil {
			t.Fatal(err)
		}
		payloadTypes, blobTypes = append(payloadTypes, pt), append(blobTypes, bt)
		payloads, blobs = append(payloads, payload), append(blobs, blob)
	}
	if err := rows.Err(); err != nil || len(payloads) != 2 {
		t.Fatalf("got %d rows, err %v", len(payloads), err)
	}
	if payloadTypes[0] != "BLOB" || blobTypes[0] != "BLOB" {
		t.Fatalf("types = %v / %v; want BLOB", payloadTypes, blobTypes)
	}
	if !bytes.Equal(payloads[0], invalidUTF8) || !bytes.Equal(blobs[0], []byte{0, 1, 2}) {
		t.Fatalf("row 1 bytes = %x / %x", payloads[0], blobs[0])
	}
	if len(payloads[1]) != 0 || blobs[1] != nil {
		t.Fatalf("row 2: empty payload %x, blob should be NULL: %x", payloads[1], blobs[1])
	}
}

func TestBytes_AppendRefusesMismatchedPayloadKind(t *testing.T) {
	bytesWriter, err := New(NewLocalStore(t.TempDir()), "notify", bytesConfig())
	if err != nil {
		t.Fatal(err)
	}
	stringRow := testRow(1, 1)
	stringRow.Values = nil
	if err := bytesWriter.Append(stringRow); err == nil ||
		!strings.Contains(err.Error(), "payload kind") {
		t.Fatalf("string payload into a bytes table = %v", err)
	}
	stringWriter := newTestWriter(t, t.TempDir())
	mixed := testRow(1, 1)
	mixed.PayloadBytes = []byte("x")
	if err := stringWriter.Append(mixed); err == nil {
		t.Fatal("PayloadBytes into a string table was accepted")
	}
	if err := bytesWriter.Append(bytesRow(1, []byte("x"), "not bytes")); err == nil {
		t.Fatal("a string value in a bytes column was accepted")
	}
}

func TestBytes_DigestCoversPayloadBytes(t *testing.T) {
	commitOf := func(payload []byte) lake.Commit {
		w, err := New(NewLocalStore(t.TempDir()), "notify", bytesConfig())
		if err != nil {
			t.Fatal(err)
		}
		return flushRows(t, w, bytesRow(1, payload, nil))[0]
	}
	a, again, b := commitOf([]byte{1, 2, 3}), commitOf([]byte{1, 2, 3}), commitOf([]byte{1, 2, 4})
	if a.Digest != again.Digest || a.Part != again.Part {
		t.Fatalf("same bytes, different identity: %+v vs %+v", a, again)
	}
	if a.Digest == b.Digest || a.Part == b.Part {
		t.Fatalf("different bytes share identity: %+v vs %+v", a, b)
	}
}

func TestBytes_StringTableIdentityUnchanged(t *testing.T) {
	// Pinned from the writer before PayloadBytes existed: existing parts keep their names.
	commit := flushRows(t, newTestWriter(t, t.TempDir()), testRow(1, 1))[0]
	if commit.Part != "part-"+commit.Digest[:16]+".parquet" {
		t.Fatalf("part %s does not follow digest %s", commit.Part, commit.Digest)
	}
	if commit.Part != "part-5cc421e3f90611c7.parquet" {
		t.Fatalf("string table part name moved: %s", commit.Part)
	}
}
