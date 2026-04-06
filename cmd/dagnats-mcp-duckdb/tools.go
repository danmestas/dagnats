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
	where, params := buildTraceFilters(args)
	query := fmt.Sprintf(
		`SELECT TraceId, SpanName, Duration, StatusCode, Timestamp `+
			`FROM traces%s ORDER BY Timestamp DESC`, where,
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, params)
}

// getTrace returns all spans for a specific trace ID.
func getTrace(db *DB, args map[string]any) (string, error) {
	traceID, ok := args["trace_id"].(string)
	if !ok || traceID == "" {
		return "", fmt.Errorf("trace_id is required")
	}
	query := fmt.Sprintf(
		`SELECT * FROM traces WHERE TraceId = '%s' `+
			`ORDER BY Timestamp ASC`,
		sanitize(traceID),
	)
	return executeAndMarshal(db, query, maxLimit, nil)
}

// queryLogs searches logs with optional filters on service, severity,
// and body content.
func queryLogs(db *DB, args map[string]any) (string, error) {
	var clauses []string
	if svc, ok := args["service"].(string); ok && svc != "" {
		clauses = append(clauses,
			fmt.Sprintf("ServiceName = '%s'", sanitize(svc)),
		)
	}
	if sev, ok := args["severity"].(string); ok && sev != "" {
		clauses = append(clauses,
			fmt.Sprintf("SeverityText = '%s'", sanitize(sev)),
		)
	}
	if body, ok := args["body"].(string); ok && body != "" {
		clauses = append(clauses,
			fmt.Sprintf("Body ILIKE '%%%s%%'", sanitize(body)),
		)
	}
	where := buildWhere(clauses)
	query := fmt.Sprintf(
		`SELECT * FROM logs%s ORDER BY Timestamp DESC`, where,
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, nil)
}

// queryMetrics searches across all metric views.
func queryMetrics(db *DB, args map[string]any) (string, error) {
	var clauses []string
	if name, ok := args["name"].(string); ok && name != "" {
		clauses = append(clauses,
			fmt.Sprintf("MetricName = '%s'", sanitize(name)),
		)
	}
	if svc, ok := args["service"].(string); ok && svc != "" {
		clauses = append(clauses,
			fmt.Sprintf("ServiceName = '%s'", sanitize(svc)),
		)
	}
	where := buildWhere(clauses)
	limit := intArg(args, "limit", defaultLimit)

	query := fmt.Sprintf(
		`SELECT 'gauge' AS type, * FROM metrics_gauge%s `+
			`UNION ALL `+
			`SELECT 'sum' AS type, * FROM metrics_sum%s `+
			`UNION ALL `+
			`SELECT 'histogram' AS type, * `+
			`FROM metrics_histogram%s`,
		where, where, where,
	)
	return executeAndMarshal(db, query, limit, nil)
}

// getErrors returns traces with error status (StatusCode = 2).
func getErrors(db *DB, args map[string]any) (string, error) {
	var clauses []string
	clauses = append(clauses, "StatusCode = 2")
	if svc, ok := args["service"].(string); ok && svc != "" {
		clauses = append(clauses,
			fmt.Sprintf("ServiceName = '%s'", sanitize(svc)),
		)
	}
	addTimeRange(&clauses, args)
	where := buildWhere(clauses)
	query := fmt.Sprintf(
		`SELECT TraceId, SpanName, Duration, StatusMessage, `+
			`Timestamp FROM traces%s ORDER BY Timestamp DESC`,
		where,
	)
	limit := intArg(args, "limit", defaultLimit)
	return executeAndMarshal(db, query, limit, nil)
}

// latencyPercentiles computes p50, p90, p95, p99 for a span name.
func latencyPercentiles(
	db *DB, args map[string]any,
) (string, error) {
	var clauses []string
	if op, ok := args["operation"].(string); ok && op != "" {
		clauses = append(clauses,
			fmt.Sprintf("SpanName = '%s'", sanitize(op)),
		)
	}
	if svc, ok := args["service"].(string); ok && svc != "" {
		clauses = append(clauses,
			fmt.Sprintf("ServiceName = '%s'", sanitize(svc)),
		)
	}
	addTimeRange(&clauses, args)
	where := buildWhere(clauses)
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
			`FROM traces%s`, where,
	)
	return executeAndMarshal(db, query, 1, nil)
}

// querySql executes raw SQL with an enforced limit.
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
	db *DB, query string, limit int, _ map[string]any,
) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), queryTimeout,
	)
	defer cancel()

	results, err := db.Query(ctx, query, limit)
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

// buildTraceFilters builds WHERE clauses from trace query args.
func buildTraceFilters(
	args map[string]any,
) (string, map[string]any) {
	var clauses []string
	if svc, ok := args["service"].(string); ok && svc != "" {
		clauses = append(clauses,
			fmt.Sprintf("ServiceName = '%s'", sanitize(svc)),
		)
	}
	if op, ok := args["operation"].(string); ok && op != "" {
		clauses = append(clauses,
			fmt.Sprintf("SpanName = '%s'", sanitize(op)),
		)
	}
	if status, ok := args["status"].(string); ok && status != "" {
		clauses = append(clauses,
			fmt.Sprintf("StatusCode = %s", sanitize(status)),
		)
	}
	addTimeRange(&clauses, args)
	return buildWhere(clauses), nil
}

// addTimeRange appends time-range clauses if since/until are present.
func addTimeRange(clauses *[]string, args map[string]any) {
	if since, ok := args["since"].(string); ok && since != "" {
		*clauses = append(*clauses,
			fmt.Sprintf("Timestamp >= '%s'", sanitize(since)),
		)
	}
	if until, ok := args["until"].(string); ok && until != "" {
		*clauses = append(*clauses,
			fmt.Sprintf("Timestamp <= '%s'", sanitize(until)),
		)
	}
}

// buildWhere joins clauses into a WHERE string.
func buildWhere(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

// sanitize removes single quotes to prevent SQL injection.
func sanitize(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// intArg extracts an integer argument with a default.
func intArg(args map[string]any, key string, def int) int {
	if v, ok := args[key].(float64); ok && v > 0 {
		return int(v)
	}
	return def
}
