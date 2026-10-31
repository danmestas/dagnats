package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	defaultLimit = 100
	maxLimit     = 10000
)

// DB wraps a DuckDB connection with Parquet views over telemetry data, and over the lake when
// one is configured.
type DB struct {
	conn    *sql.DB
	dataDir string
	lakeDir string // empty: no lake views

	lakeMu     sync.Mutex
	lakeViews  map[string]string // view name -> its part list, to skip unchanged views
	lakeWarned map[string]bool   // program/table names already reported as having no view
}

// Option adjusts OpenDB.
type Option func(*options)

type options struct {
	s3Endpoint string
	lakeDir    string
}

// WithS3Endpoint points an s3:// data dir at an S3-compatible endpoint
// (R2, Garage, Tigris, MinIO) instead of AWS. "http://" turns TLS off;
// a bare host or "https://" keeps it on. Credentials then come from
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION.
func WithS3Endpoint(endpoint string) Option {
	return func(o *options) { o.s3Endpoint = endpoint }
}

// signals are the views OpenDB creates, one per telemetry signal.
var signals = []string{
	"traces", "logs",
	"metrics_gauge", "metrics_sum", "metrics_histogram",
}

// OpenDB opens an in-memory DuckDB database with a read-only view over
// the Parquet files of each telemetry signal in dataDir, a local path or
// an s3:// prefix. Once the views exist, file access is locked to
// dataDir for the life of the connection: queries cannot read, write or
// attach anything else, and cannot undo the lock.
func OpenDB(dataDir string, opts ...Option) (*DB, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("dataDir must not be empty")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	root, err := dataRoot(dataDir)
	if err != nil {
		return nil, err
	}
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db := &DB{conn: conn, dataDir: root}
	if o.lakeDir != "" {
		if db.lakeDir, err = dataRoot(o.lakeDir); err != nil {
			conn.Close()
			return nil, err
		}
		db.lakeViews, db.lakeWarned = map[string]string{}, map[string]bool{}
	}
	if err := db.setup(o); err != nil {
		conn.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) setup(o options) error {
	if isS3(db.dataDir) || isS3(db.lakeDir) {
		if err := db.configureS3(o.s3Endpoint); err != nil {
			return fmt.Errorf("configure s3: %w", err)
		}
	}
	for _, name := range signals {
		if err := db.createView(name); err != nil {
			return fmt.Errorf("create view %s: %w", name, err)
		}
	}
	if err := db.lockToDataDir(); err != nil {
		return fmt.Errorf("lock file access: %w", err)
	}
	if db.lakeDir != "" {
		// After the lock, as every later refresh runs.
		if err := db.refreshLakeViews(context.Background()); err != nil {
			return fmt.Errorf("lake views: %w", err)
		}
	}
	return nil
}

// dataRoot is dataDir in the form the lock compares against: an
// absolute local path, or the s3:// prefix, without a trailing slash.
func dataRoot(dataDir string) (string, error) {
	if isS3(dataDir) {
		root := strings.TrimRight(dataDir, "/")
		if strings.Count(root, "/") < 2 || root == "s3:/" {
			return "", fmt.Errorf("s3 data dir %q needs a bucket", dataDir)
		}
		return root, nil
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("data dir %q: %w", dataDir, err)
	}
	return abs, nil
}

func isS3(dataDir string) bool { return strings.HasPrefix(dataDir, "s3://") }

// configureS3 loads httpfs and creates the S3 secret. httpfs is not
// bundled with the driver, so the first use downloads DuckDB's signed
// extension into the user's DuckDB extension cache.
func (db *DB) configureS3(endpoint string) error {
	for _, stmt := range []string{"INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.conn.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	secret := "CREATE SECRET dagnats_s3 (TYPE s3, PROVIDER credential_chain)"
	if endpoint != "" {
		host, useSSL := parseS3Endpoint(endpoint)
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "auto"
		}
		secret = fmt.Sprintf("CREATE SECRET dagnats_s3 (TYPE s3, KEY_ID %s, "+
			"SECRET %s, REGION %s, ENDPOINT %s, URL_STYLE 'path', USE_SSL %t)",
			sqlString(os.Getenv("AWS_ACCESS_KEY_ID")),
			sqlString(os.Getenv("AWS_SECRET_ACCESS_KEY")),
			sqlString(region), sqlString(host), useSSL)
	}
	if _, err := db.conn.Exec(secret); err != nil {
		return fmt.Errorf("create s3 secret: %w", err)
	}
	return nil
}

// parseS3Endpoint splits an endpoint flag into the host DuckDB wants and
// whether to use TLS.
func parseS3Endpoint(endpoint string) (host string, useSSL bool) {
	host, useSSL = endpoint, true
	if rest, ok := strings.CutPrefix(endpoint, "http://"); ok {
		host, useSSL = rest, false
	} else if rest, ok := strings.CutPrefix(endpoint, "https://"); ok {
		host = rest
	}
	return strings.TrimRight(host, "/"), useSSL
}

// createView creates a Parquet-backed view for one signal, or an empty
// placeholder table when the signal has no Parquet files yet.
func (db *DB) createView(name string) error {
	glob := sqlString(db.dataDir + "/" + name + "/**/*.parquet")
	var count int
	err := db.conn.QueryRow("SELECT count(*) FROM glob(" + glob + ")").Scan(&count)
	if err != nil {
		return fmt.Errorf("list %s: %w", name, err)
	}
	query := fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS SELECT 1 WHERE false`, name)
	if count > 0 {
		query = fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS SELECT * FROM `+
			`read_parquet(%s, union_by_name=true, hive_partitioning=true)`,
			name, glob)
	}
	_, err = db.conn.Exec(query)
	return err
}

// lockToDataDir allows file access under the data dir (and the lake dir)
// only, turns all other access off, and locks the configuration so no
// query can change it back.
func (db *DB) lockToDataDir() error {
	allowed := sqlString(db.dataDir + "/")
	if db.lakeDir != "" {
		allowed += ", " + sqlString(db.lakeDir+"/")
	}
	for _, stmt := range []string{
		"SET allowed_directories = [" + allowed + "]",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	} {
		if _, err := db.conn.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// sqlString quotes s as a SQL string literal.
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Close closes the underlying DuckDB connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Query executes a SQL query and returns results as a slice of maps.
// At most limit rows come back, whatever the query says. Use $1, $2, etc.
// placeholders in the query and pass corresponding values as args.
func (db *DB) Query(
	ctx context.Context, query string, limit int,
	args ...any,
) ([]map[string]any, error) {
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	query = enforceLimit(query, limit)
	if db.lakeDir != "" {
		if err := db.refreshLakeViews(ctx); err != nil {
			return nil, fmt.Errorf("lake views: %w", err)
		}
	}

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// enforceLimit wraps query in a subquery capped at limit rows. Wrapping,
// rather than looking for a LIMIT in the text, means a larger LIMIT, the
// word in a comment or an alias cannot lift the cap, and a second
// statement becomes a syntax error. The newline before the closing
// parenthesis keeps a trailing line comment from swallowing it.
func enforceLimit(query string, limit int) string {
	if limit <= 0 {
		panic("enforceLimit: limit must be positive")
	}
	inner := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	return fmt.Sprintf("SELECT * FROM (\n%s\n) AS capped LIMIT %d", inner, limit)
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
