// Tests for DuckDB connection and Parquet view layer.
// Methodology: create real Parquet files with DuckDB, then query through views.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestOpenDB(t *testing.T) {
	dir := t.TempDir()
	createTestTraces(t, dir)

	db, err := OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	results, err := db.Query(ctx, "SELECT * FROM traces", 100)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["TraceId"] != "abc123" {
		t.Errorf(
			"expected TraceId=abc123, got %v", results[0]["TraceId"],
		)
	}
}

func TestOpenDB_EmptyDataDir(t *testing.T) {
	_, err := OpenDB("")
	if err == nil {
		t.Fatal("expected error for empty dataDir")
	}
	if err.Error() != "dataDir must not be empty" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestQuery_EnforcesLimit(t *testing.T) {
	dir := t.TempDir()
	createTestTracesMultiple(t, dir, 50)

	db, err := OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	results, err := db.Query(ctx, "SELECT * FROM traces", 10)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results (limit), got %d", len(results))
	}
	// Verify we actually had more data available.
	allResults, err := db.Query(ctx, "SELECT * FROM traces", 100)
	if err != nil {
		t.Fatalf("Query all failed: %v", err)
	}
	if len(allResults) <= 10 {
		t.Errorf(
			"expected more than 10 total results, got %d",
			len(allResults),
		)
	}
}

func TestQuery_ExistingLimit(t *testing.T) {
	dir := t.TempDir()
	createTestTracesMultiple(t, dir, 20)

	db, err := OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	results, err := db.Query(
		ctx, "SELECT * FROM traces LIMIT 5", 100,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	// The user-specified LIMIT 5 should be preserved.
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

// createTestTraces writes a single-row Parquet file for traces.
func createTestTraces(t *testing.T, dir string) {
	t.Helper()
	tracesDir := fmt.Sprintf("%s/traces", dir)
	if err := os.MkdirAll(tracesDir, 0o755); err != nil {
		t.Fatalf("mkdir traces: %v", err)
	}
	tmpDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open tmpdb: %v", err)
	}
	defer tmpDB.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT 'abc123' AS TraceId,
			       'test.operation' AS SpanName,
			       100 AS Duration,
			       0 AS StatusCode,
			       current_timestamp AS Timestamp
		) TO '%s/test.parquet' (FORMAT PARQUET)`,
		tracesDir,
	)
	if _, err := tmpDB.Exec(query); err != nil {
		t.Fatalf("create test parquet: %v", err)
	}
}

// createTestTracesMultiple writes N rows to a Parquet file.
func createTestTracesMultiple(t *testing.T, dir string, n int) {
	t.Helper()
	tracesDir := fmt.Sprintf("%s/traces", dir)
	if err := os.MkdirAll(tracesDir, 0o755); err != nil {
		t.Fatalf("mkdir traces: %v", err)
	}
	tmpDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open tmpdb: %v", err)
	}
	defer tmpDB.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT 'trace-' || i AS TraceId,
			       'op-' || i AS SpanName,
			       i * 10 AS Duration,
			       0 AS StatusCode,
			       current_timestamp AS Timestamp
			FROM generate_series(1, %d) AS t(i)
		) TO '%s/test.parquet' (FORMAT PARQUET)`,
		n, tracesDir,
	)
	if _, err := tmpDB.Exec(query); err != nil {
		t.Fatalf("create test parquet: %v", err)
	}
}
