// Methodology: the reader contract, proven with the real reader. Rows go through Writer, and
// DuckDB reads the committed files back with hive partitioning. DuckDB is a test-only dependency
// (it needs CGO); the package itself is pure Go.
package writer

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/danmestas/dagnats/lake"
)

func TestRoundTrip_DuckDBReadsCommittedParts(t *testing.T) {
	root := t.TempDir()
	writer := newTestWriter(t, root)
	missing := testRow(2, 1)
	missing.Values = map[string]any{"org_id": "o2", "attempts": "not a number"}
	for _, row := range []Row{testRow(1, 1), missing, testRow(3, 2)} {
		if err := writer.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	commits, err := lake.ListCommitted(root, "notify", "deliveries")
	if err != nil {
		t.Fatal(err)
	}
	files := make([]string, len(commits))
	for i, commit := range commits {
		files[i] = "'" + commit.Path + "'"
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := "SELECT dt::VARCHAR, event_id, stream_seq, schema_version, payload, org_id, " +
		"attempts, score, ok, sent_at FROM read_parquet([" + strings.Join(files, ",") +
		"], hive_partitioning = true) ORDER BY stream_seq"
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	type result struct {
		dt, id, payload, org string
		seq, version         int64
		attempts             sql.NullInt64
		score                sql.NullFloat64
		ok                   sql.NullBool
		sent                 sql.NullTime
	}
	var got []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.dt, &r.id, &r.seq, &r.version, &r.payload, &r.org,
			&r.attempts, &r.score, &r.ok, &r.sent); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil || len(got) != 3 {
		t.Fatalf("got %d rows, err %v; want 3", len(got), err)
	}
	first := got[0]
	wantAt := time.Date(2026, 10, 1, 12, 30, 0, 0, time.UTC)
	if first.dt != "2026-10-01" || first.id != "evt-b" || first.seq != 1 || first.version != 1 ||
		first.payload != `{"id":"x"}` || first.org != "o1" || first.attempts.Int64 != 1 ||
		first.score.Float64 != 1.5 || !first.ok.Bool || !first.sent.Time.Equal(wantAt) {
		t.Fatalf("first row = %+v", first)
	}
	// A mistyped or absent value is NULL in that row; the row itself survives.
	if got[1].attempts.Valid || got[1].score.Valid || got[1].ok.Valid || got[1].sent.Valid {
		t.Fatalf("second row should project NULLs: %+v", got[1])
	}
	if got[2].dt != "2026-10-02" {
		t.Fatalf("third row dt = %s", got[2].dt)
	}
}
