package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

const (
	defaultLimit = 100
	maxLimit     = 10000
)

// DB wraps a DuckDB connection with Parquet views over telemetry data.
type DB struct {
	conn    *sql.DB
	dataDir string
}

// OpenDB opens an in-memory DuckDB database and creates read-only views
// over Parquet files in dataDir. Views are created for traces, logs,
// metrics_gauge, metrics_sum, and metrics_histogram.
func OpenDB(dataDir string) (*DB, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("dataDir must not be empty")
	}

	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	db := &DB{conn: conn, dataDir: dataDir}
	if err := db.createViews(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("create views: %w", err)
	}

	return db, nil
}

// createViews sets up Parquet-backed views for each telemetry signal.
// Views that have no Parquet files yet are created as empty tables.
func (db *DB) createViews() error {
	views := []string{
		"traces", "logs",
		"metrics_gauge", "metrics_sum", "metrics_histogram",
	}
	for _, name := range views {
		if err := db.createView(name); err != nil {
			return fmt.Errorf("create view %s: %w", name, err)
		}
	}
	return nil
}

// createView creates a single Parquet-backed view, or an empty
// placeholder table when no Parquet files exist yet.
func (db *DB) createView(name string) error {
	if !hasParquetFiles(db.dataDir, name) {
		query := fmt.Sprintf(
			`CREATE OR REPLACE TABLE %s `+
				`AS SELECT 1 WHERE false`,
			name,
		)
		_, err := db.conn.Exec(query)
		return err
	}
	glob := fmt.Sprintf(
		"%s/%s/**/*.parquet", db.dataDir, name,
	)
	query := fmt.Sprintf(
		`CREATE OR REPLACE VIEW %s AS SELECT * FROM `+
			`read_parquet('%s', `+
			`union_by_name=true, hive_partitioning=true)`,
		name, glob,
	)
	_, err := db.conn.Exec(query)
	return err
}

// hasParquetFiles checks whether any .parquet files exist under
// dataDir/name.
func hasParquetFiles(dataDir string, name string) bool {
	pattern := filepath.Join(dataDir, name, "**", "*.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) > 0 {
		return len(matches) > 0
	}
	// filepath.Glob doesn't recurse with **, try one level.
	pattern = filepath.Join(dataDir, name, "*.parquet")
	matches, _ = filepath.Glob(pattern)
	if len(matches) > 0 {
		return true
	}
	// Walk to find any parquet file.
	dir := filepath.Join(dataDir, name)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	}
	found := false
	filepath.Walk(dir, func(
		path string, info os.FileInfo, err error,
	) error {
		if err != nil {
			return err
		}
		if !info.IsDir() &&
			strings.HasSuffix(path, ".parquet") {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	return found
}

// Close closes the underlying DuckDB connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Query executes a SQL query and returns results as a slice of maps.
// A LIMIT clause is enforced if not already present.
func (db *DB) Query(
	ctx context.Context, query string, limit int,
) ([]map[string]any, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	query = enforceLimit(query, limit)

	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// enforceLimit appends a LIMIT clause if one is not already present.
func enforceLimit(query string, limit int) string {
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "LIMIT") {
		return query
	}
	return fmt.Sprintf("%s LIMIT %d", strings.TrimRight(query, " ;"), limit)
}

// scanRows converts sql.Rows into a slice of column-name-keyed maps.
func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}
	return results, nil
}
