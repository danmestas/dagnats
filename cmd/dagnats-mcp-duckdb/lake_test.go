// Tests for the record-driven lake views.
// Methodology: real DuckDB over lake-shaped directories built by hand: Parquet parts written by
// DuckDB, commit and supersede records as empty files, since a view's file list follows record
// names alone. Each crash state of lode's compaction (an orphan, a superseded part whose bytes
// remain, a compacted part) is asserted as the rows a query sees.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writePart writes ids as a one-column Parquet part under lake/<program>/<table>/dt=<dt>/.
func writePart(t *testing.T, lake, program, table, dt, stem string, ids ...int) string {
	t.Helper()
	dir := filepath.Join(lake, program, table, "dt="+dt)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	values := ""
	for i, id := range ids {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("(%d::BIGINT, 'row-%d')", id, id)
	}
	file := filepath.Join(dir, "part-"+stem+".parquet")
	query := fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) AS v(event_id, payload)) TO %s "+
		"(FORMAT PARQUET)", values, sqlString(file))
	if _, err := conn.Exec(query); err != nil {
		t.Fatalf("write part: %v", err)
	}
	return file
}

// mark creates an empty record file beside a part: ".json" commits it, ".json.superseded"
// supersedes it.
func mark(t *testing.T, part, suffix string) {
	t.Helper()
	record := part[:len(part)-len(".parquet")] + suffix
	if err := os.WriteFile(record, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func queryCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// compactedLake is lode's compaction mid-flight on one table: part a live, an orphan part b with
// no record, part c superseded by the compacted part d that holds c's rows, c's bytes still
// present.
func compactedLake(t *testing.T) string {
	lake := t.TempDir()
	mark(t, writePart(t, lake, "lode", "events", "2026-10-01", "a", 1, 2), ".json")
	writePart(t, lake, "lode", "events", "2026-10-01", "b", 3)
	c := writePart(t, lake, "lode", "events", "2026-10-01", "c", 4, 5)
	mark(t, c, ".json")
	mark(t, c, ".json.superseded")
	mark(t, writePart(t, lake, "lode", "events", "2026-10-02", "d", 4, 5), ".json")
	return lake
}

func TestLakeView_ReadsLivePartsOnly(t *testing.T) {
	data := t.TempDir()
	createTestTraces(t, data)
	db, err := OpenDB(data, WithLakeDir(compactedLake(t)))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(queryCtx(t),
		"SELECT event_id, count(*) AS n FROM lode_events GROUP BY event_id ORDER BY event_id", 100)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := map[int64]int64{}
	for _, row := range rows {
		got[row["event_id"].(int64)] = row["n"].(int64)
	}
	want := map[int64]int64{1: 1, 2: 1, 4: 1, 5: 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows = %v, want %v: orphan 3 hidden, compacted 4 and 5 once", got, want)
	}
	dts, err := db.Query(queryCtx(t), "SELECT DISTINCT dt FROM lode_events ORDER BY dt", 10)
	if err != nil || len(dts) != 2 {
		t.Fatalf("hive dt = %v, %v; want two dates", dts, err)
	}
}

func TestLakeView_RefreshesPerQuery(t *testing.T) {
	lake := t.TempDir()
	first := writePart(t, lake, "lode", "events", "2026-10-01", "a", 1, 2)
	mark(t, first, ".json")
	db, err := OpenDB(t.TempDir(), WithLakeDir(lake))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	count := func() int64 {
		rows, err := db.Query(queryCtx(t), "SELECT count(*) AS n FROM lode_events", 1)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		return rows[0]["n"].(int64)
	}
	if got := count(); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	mark(t, writePart(t, lake, "lode", "events", "2026-10-01", "z", 1, 2, 3), ".json")
	mark(t, first, ".json.superseded")
	if got := count(); got != 3 {
		t.Fatalf("count after compaction = %d, want 3: the new part, not the superseded one", got)
	}
	mark(t, writePart(t, lake, "lode", "audit", "2026-10-01", "x", 9), ".json")
	if _, err := db.Query(queryCtx(t), "SELECT * FROM lode_audit", 10); err != nil {
		t.Fatalf("a table committed after startup has no view: %v", err)
	}
}

func TestLakeView_UnsetMeansNoLakeViews(t *testing.T) {
	data := t.TempDir()
	createTestTraces(t, data)
	db, err := OpenDB(data)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(queryCtx(t), "SELECT table_name FROM information_schema.tables "+
		"ORDER BY table_name", 100)
	if err != nil || len(rows) != len(signals) {
		t.Fatalf("tables = %v, %v; want only the %d signals", rows, err, len(signals))
	}
	traces, err := db.Query(queryCtx(t), "SELECT TraceId FROM traces", 10)
	if err != nil || len(traces) != 1 || traces[0]["TraceId"] != "abc123" {
		t.Fatalf("traces = %v, %v", traces, err)
	}
}

func TestLakeView_SignalViewsUnchangedAndNamesRefused(t *testing.T) {
	data := t.TempDir()
	createTestTraces(t, data)
	lake := compactedLake(t)
	// metrics/sum would be named metrics_sum, a signal view; Bad/x fails the lake name rules.
	mark(t, writePart(t, lake, "metrics", "sum", "2026-10-01", "m", 7), ".json")
	mark(t, writePart(t, lake, "Bad", "x", "2026-10-01", "n", 8), ".json")
	db, err := OpenDB(data, WithLakeDir(lake))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	traces, err := db.Query(queryCtx(t), "SELECT TraceId FROM traces", 10)
	if err != nil || len(traces) != 1 || traces[0]["TraceId"] != "abc123" {
		t.Fatalf("traces = %v, %v", traces, err)
	}
	if rows, err := db.Query(queryCtx(t), "SELECT * FROM metrics_sum", 10); err != nil ||
		len(rows) != 0 {
		t.Fatalf("metrics_sum = %v, %v; want the empty signal placeholder, not the lake table",
			rows, err)
	}
	rows, err := db.Query(queryCtx(t), "SELECT table_name FROM information_schema.tables "+
		"WHERE table_name NOT IN ('traces','logs','metrics_gauge','metrics_sum',"+
		"'metrics_histogram') ORDER BY table_name", 100)
	if err != nil || len(rows) != 1 || rows[0]["table_name"] != "lode_events" {
		t.Fatalf("lake views = %v, %v; want only lode_events", rows, err)
	}
}

func TestLakeView_LockCoversTheLakeDirOnly(t *testing.T) {
	lake := compactedLake(t)
	db, err := OpenDB(t.TempDir(), WithLakeDir(lake))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	inside := "SELECT * FROM read_parquet('" + lake + "/lode/events/*/part-a.parquet')"
	if _, err := db.Query(queryCtx(t), inside, 10); err != nil {
		t.Fatalf("a read inside the lake dir was refused: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "x.parquet")
	writePart(t, filepath.Dir(outside), "p", "t", "2026-10-01", "o", 1)
	escape := "SELECT * FROM read_parquet('" + filepath.Dir(outside) + "/**/*.parquet')"
	if _, err := db.Query(queryCtx(t), escape, 10); err == nil {
		t.Fatal("a read outside both dirs was allowed")
	}
}

func TestLakeView_ReadsAnS3LakeDir(t *testing.T) {
	local := compactedLake(t)
	objects := map[string][]byte{}
	err := filepath.Walk(local, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		rel, _ := filepath.Rel(local, path)
		objects["lake/"+filepath.ToSlash(rel)] = body
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	server := newFakeS3Server(t, fakeS3{"bucket": objects})
	db, err := OpenDB(t.TempDir(), WithLakeDir("s3://bucket/lake"), WithS3Endpoint(server))
	if err != nil {
		t.Fatalf("OpenDB over s3: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(queryCtx(t), "SELECT count(*) AS n FROM lode_events", 1)
	if err != nil || rows[0]["n"].(int64) != 4 {
		t.Fatalf("s3 lake view = %v, %v; want 4 live rows", rows, err)
	}
}
