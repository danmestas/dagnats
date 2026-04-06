// Tests for MCP tool query functions.
// Methodology: create real Parquet data, then test each tool function.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	createToolTestData(t, dir)

	db, err := OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestQueryTraces(t *testing.T) {
	db := setupTestDB(t)
	result, err := queryTraces(db, map[string]any{})
	if err != nil {
		t.Fatalf("queryTraces failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least 1 trace row")
	}
	if _, ok := rows[0]["TraceId"]; !ok {
		t.Error("expected TraceId column in results")
	}
}

func TestGetTrace(t *testing.T) {
	db := setupTestDB(t)
	result, err := getTrace(db, map[string]any{
		"trace_id": "trace-1",
	})
	if err != nil {
		t.Fatalf("getTrace failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for trace-1, got %d", len(rows))
	}
	if rows[0]["TraceId"] != "trace-1" {
		t.Errorf("expected TraceId=trace-1, got %v", rows[0]["TraceId"])
	}
}

func TestGetTrace_MissingID(t *testing.T) {
	db := setupTestDB(t)
	_, err := getTrace(db, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing trace_id")
	}
	if err.Error() != "trace_id is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestQueryLogs(t *testing.T) {
	db := setupTestDB(t)
	result, err := queryLogs(db, map[string]any{
		"severity": "ERROR",
	})
	if err != nil {
		t.Fatalf("queryLogs failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 ERROR log, got %d", len(rows))
	}
	if rows[0]["SeverityText"] != "ERROR" {
		t.Errorf(
			"expected SeverityText=ERROR, got %v",
			rows[0]["SeverityText"],
		)
	}
}

func TestGetErrors(t *testing.T) {
	db := setupTestDB(t)
	result, err := getErrors(db, map[string]any{})
	if err != nil {
		t.Fatalf("getErrors failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 error trace, got %d", len(rows))
	}
	if rows[0]["TraceId"] != "trace-err" {
		t.Errorf(
			"expected TraceId=trace-err, got %v",
			rows[0]["TraceId"],
		)
	}
}

func TestLatencyPercentiles(t *testing.T) {
	db := setupTestDB(t)
	result, err := latencyPercentiles(db, map[string]any{})
	if err != nil {
		t.Fatalf("latencyPercentiles failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 percentile row, got %d", len(rows))
	}
	if _, ok := rows[0]["p50"]; !ok {
		t.Error("expected p50 in results")
	}
	if _, ok := rows[0]["total_spans"]; !ok {
		t.Error("expected total_spans in results")
	}
}

func TestQuerySql(t *testing.T) {
	db := setupTestDB(t)
	result, err := querySql(db, map[string]any{
		"sql": "SELECT count(*) AS cnt FROM traces",
	})
	if err != nil {
		t.Fatalf("querySql failed: %v", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(result), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 count row, got %d", len(rows))
	}
}

func TestQuerySql_MissingSQL(t *testing.T) {
	db := setupTestDB(t)
	_, err := querySql(db, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing sql")
	}
	if err.Error() != "sql is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

// createToolTestData writes Parquet files for traces and logs.
func createToolTestData(t *testing.T, dir string) {
	t.Helper()
	for _, sub := range []string{"traces", "logs"} {
		if err := os.MkdirAll(
			fmt.Sprintf("%s/%s", dir, sub), 0o755,
		); err != nil {
			t.Fatalf("mkdir %s: %v", sub, err)
		}
	}

	tmpDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open tmpdb: %v", err)
	}
	defer tmpDB.Close()

	tracesSQL := fmt.Sprintf(
		`COPY (
			SELECT * FROM (VALUES
				('trace-1', 'svc-a', 'op.get', 50, 0,
				 '', current_timestamp),
				('trace-2', 'svc-a', 'op.put', 200, 0,
				 '', current_timestamp),
				('trace-err', 'svc-b', 'op.fail', 500, 2,
				 'something broke', current_timestamp)
			) AS t(TraceId, ServiceName, SpanName, Duration,
			       StatusCode, StatusMessage, Timestamp)
		) TO '%s/traces/data.parquet' (FORMAT PARQUET)`, dir,
	)
	if _, err := tmpDB.Exec(tracesSQL); err != nil {
		t.Fatalf("create traces parquet: %v", err)
	}

	logsSQL := fmt.Sprintf(
		`COPY (
			SELECT * FROM (VALUES
				('svc-a', 'INFO', 'all good', current_timestamp),
				('svc-b', 'ERROR', 'disk full', current_timestamp)
			) AS t(ServiceName, SeverityText, Body, Timestamp)
		) TO '%s/logs/data.parquet' (FORMAT PARQUET)`, dir,
	)
	if _, err := tmpDB.Exec(logsSQL); err != nil {
		t.Fatalf("create logs parquet: %v", err)
	}
}
