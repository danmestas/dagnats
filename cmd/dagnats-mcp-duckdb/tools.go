package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const queryTimeout = 30 * time.Second

// queryTraces searches traces with optional filters on service, operation,
// status, and time range.
func queryTraces(db *DB, args map[string]any) (string, error) {
	var qb queryBuilder
	addTraceFilters(&qb, args)
	query := fmt.Sprintf(
		`SELECT TraceId, SpanName, Duration, StatusCode, `+
			`Timestamp FROM traces%s ORDER BY Timestamp DESC`,
		qb.where(),
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, qb.args)
}

// getTrace returns all spans for a specific trace ID.
func getTrace(db *DB, args map[string]any) (string, error) {
	traceID, ok := args["trace_id"].(string)
	if !ok || traceID == "" {
		return "", fmt.Errorf("trace_id is required")
	}
	var qb queryBuilder
	qb.add("TraceId = "+qb.placeholder(), traceID)
	query := fmt.Sprintf(
		`SELECT * FROM traces%s ORDER BY Timestamp ASC`,
		qb.where(),
	)
	return executeAndMarshal(db, query, maxLimit, qb.args)
}

// queryLogs searches logs with optional filters on service, severity,
// and body content.
func queryLogs(db *DB, args map[string]any) (string, error) {
	var qb queryBuilder
	if svc, ok := args["service"].(string); ok && svc != "" {
		qb.add("ServiceName = "+qb.placeholder(), svc)
	}
	if sev, ok := args["severity"].(string); ok && sev != "" {
		qb.add("SeverityText = "+qb.placeholder(), sev)
	}
	if body, ok := args["body"].(string); ok && body != "" {
		qb.add(
			"Body ILIKE '%' || "+qb.placeholder()+" || '%'",
			body,
		)
	}
	query := fmt.Sprintf(
		`SELECT * FROM logs%s ORDER BY Timestamp DESC`,
		qb.where(),
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, qb.args)
}

// queryMetrics searches across all metric views.
func queryMetrics(db *DB, args map[string]any) (string, error) {
	var qb queryBuilder
	if name, ok := args["name"].(string); ok && name != "" {
		qb.add("MetricName = "+qb.placeholder(), name)
	}
	if svc, ok := args["service"].(string); ok && svc != "" {
		qb.add("ServiceName = "+qb.placeholder(), svc)
	}
	w := qb.where()
	limit := intArg(args, "limit", defaultLimit)

	// Each UNION arm uses the same WHERE clause. DuckDB binds
	// positional parameters once, so the same $1/$2 apply to all.
	query := fmt.Sprintf(
		`SELECT 'gauge' AS type, * FROM metrics_gauge%s `+
			`UNION ALL `+
			`SELECT 'sum' AS type, * FROM metrics_sum%s `+
			`UNION ALL `+
			`SELECT 'histogram' AS type, * `+
			`FROM metrics_histogram%s`,
		w, w, w,
	)
	return executeAndMarshal(db, query, limit, qb.args)
}

// getErrors returns traces with error status (StatusCode = 2).
func getErrors(db *DB, args map[string]any) (string, error) {
	var qb queryBuilder
	qb.addClause("StatusCode = 2")
	if svc, ok := args["service"].(string); ok && svc != "" {
		qb.add("ServiceName = "+qb.placeholder(), svc)
	}
	addTimeRange(&qb, args)
	query := fmt.Sprintf(
		`SELECT TraceId, SpanName, Duration, StatusMessage, `+
			`Timestamp FROM traces%s ORDER BY Timestamp DESC`,
		qb.where(),
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, qb.args)
}

// latencyPercentiles computes p50, p90, p95, p99 for a span name.
func latencyPercentiles(
	db *DB, args map[string]any,
) (string, error) {
	var qb queryBuilder
	if op, ok := args["operation"].(string); ok && op != "" {
		qb.add("SpanName = "+qb.placeholder(), op)
	}
	if svc, ok := args["service"].(string); ok && svc != "" {
		qb.add("ServiceName = "+qb.placeholder(), svc)
	}
	addTimeRange(&qb, args)
	query := fmt.Sprintf(
		`SELECT `+
			`percentile_disc(0.5) WITHIN GROUP `+
			`(ORDER BY Duration) AS p50, `+
			`percentile_disc(0.9) WITHIN GROUP `+
			`(ORDER BY Duration) AS p90, `+
			`percentile_disc(0.95) WITHIN GROUP `+
			`(ORDER BY Duration) AS p95, `+
			`percentile_disc(0.99) WITHIN GROUP `+
			`(ORDER BY Duration) AS p99, `+
			`count(*) AS total_spans `+
			`FROM traces%s`, qb.where(),
	)
	return executeAndMarshal(db, query, 1, qb.args)
}

// querySql executes raw SQL with an enforced limit.
// The query itself cannot be parameterized since it is user-provided
// SQL. Safety relies on the database being opened read-only.
func querySql(db *DB, args map[string]any) (string, error) {
	query, ok := args["sql"].(string)
	if !ok || query == "" {
		return "", fmt.Errorf("sql is required")
	}
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, nil)
}

// executeAndMarshal runs the query and returns JSON.
func executeAndMarshal(
	db *DB, query string, limit int, args []any,
) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), queryTimeout,
	)
	defer cancel()

	results, err := db.Query(ctx, query, limit, args...)
	if err != nil {
		return "", fmt.Errorf("execute query: %w", err)
	}
	if results == nil {
		results = []map[string]any{}
	}
	data, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("marshal results: %w", err)
	}
	return string(data), nil
}

// queryBuilder accumulates WHERE clauses with positional parameters.
type queryBuilder struct {
	clauses []string
	args    []any
	idx     int
}

// placeholder returns the next $N placeholder string.
func (qb *queryBuilder) placeholder() string {
	qb.idx++
	return fmt.Sprintf("$%d", qb.idx)
}

// add appends a clause (which should contain a $N placeholder from a
// prior call to placeholder()) and its corresponding argument.
func (qb *queryBuilder) add(clause string, arg any) {
	qb.clauses = append(qb.clauses, clause)
	qb.args = append(qb.args, arg)
}

// addClause appends a clause with no parameter (e.g. static conditions).
func (qb *queryBuilder) addClause(clause string) {
	qb.clauses = append(qb.clauses, clause)
}

// where returns the assembled WHERE string, or empty if no clauses.
func (qb *queryBuilder) where() string {
	if len(qb.clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(qb.clauses, " AND ")
}

// addTraceFilters builds parameterized trace query filters.
func addTraceFilters(qb *queryBuilder, args map[string]any) {
	if svc, ok := args["service"].(string); ok && svc != "" {
		qb.add("ServiceName = "+qb.placeholder(), svc)
	}
	if op, ok := args["operation"].(string); ok && op != "" {
		qb.add("SpanName = "+qb.placeholder(), op)
	}
	if status, ok := args["status"].(string); ok && status != "" {
		qb.add("StatusCode = "+qb.placeholder(), status)
	}
	addTimeRange(qb, args)
}

// addTimeRange appends parameterized time-range clauses.
func addTimeRange(qb *queryBuilder, args map[string]any) {
	if since, ok := args["since"].(string); ok && since != "" {
		qb.add("Timestamp >= "+qb.placeholder(), since)
	}
	if until, ok := args["until"].(string); ok && until != "" {
		qb.add("Timestamp <= "+qb.placeholder(), until)
	}
}

// intArg extracts an integer argument with a default.
func intArg(args map[string]any, key string, def int) int {
	if v, ok := args[key].(float64); ok && v > 0 {
		return int(v)
	}
	return def
}
