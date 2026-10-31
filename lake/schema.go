// Package lake is the read side of dagnats' shared analytics store: its layout, schema rules and
// commit records. Data is Hive-partitioned Parquet under one root, each part trusted only once its
// commit record exists.
//
//	<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.parquet
//	<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.json   (commit record)
//	<root>/<program>/<table>/dt=YYYY-MM-DD/part-<sha256-16>.json.superseded   (see Supersede)
//
// Every program writes through lake/writer, the one implementation of this layout, into a
// writer.Store: a local directory (writer.LocalStore) or an S3-compatible store (lode). The
// package is pure Go; DuckDB is imported only by tests.
package lake

import (
	"fmt"
	"regexp"
)

// ColumnType is the Go-facing type of a declared column. Each maps to one nullable Parquet type.
type ColumnType string

const (
	TypeString    ColumnType = "string"    // UTF8
	TypeInt64     ColumnType = "int64"     // INT64
	TypeFloat64   ColumnType = "float64"   // DOUBLE
	TypeBool      ColumnType = "bool"      // BOOLEAN
	TypeTimestamp ColumnType = "timestamp" // TIMESTAMP(us, UTC)
)

// ColumnCountMax bounds a declared schema so a part's footer and the writer's row slice stay small.
const ColumnCountMax = 64

// reservedColumns are written on every row (or, for dt, derived from the path), so a declared
// column may not take the name.
var reservedColumns = []string{
	"dt", "event_id", "event_time", "stream_seq", "payload", "schema_version",
}

var nameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// Column is a typed projection of one value of an event.
type Column struct {
	Name string
	Type ColumnType
}

// Schema is the declared columns of one table. Reserved columns are implied.
type Schema struct {
	Version int
	Columns []Column
}

// Validate enforces the naming, reserved-column and type rules.
func (s Schema) Validate() error {
	if s.Version < 1 {
		return fmt.Errorf("lake: schema version %d: must be >= 1", s.Version)
	}
	if len(s.Columns) > ColumnCountMax {
		return fmt.Errorf("lake: %d columns: at most %d", len(s.Columns), ColumnCountMax)
	}
	seen := make(map[string]bool, len(s.Columns))
	for _, column := range s.Columns {
		if !nameRE.MatchString(column.Name) {
			return fmt.Errorf("lake: column %q: must match %s", column.Name, nameRE)
		}
		if isReserved(column.Name) {
			return fmt.Errorf("lake: column %q is reserved", column.Name)
		}
		if seen[column.Name] {
			return fmt.Errorf("lake: column %q declared twice", column.Name)
		}
		seen[column.Name] = true
		switch column.Type {
		case TypeString, TypeInt64, TypeFloat64, TypeBool, TypeTimestamp:
		default:
			return fmt.Errorf("lake: column %q: unknown type %q", column.Name, column.Type)
		}
	}
	return nil
}

func isReserved(name string) bool {
	for _, reserved := range reservedColumns {
		if name == reserved {
			return true
		}
	}
	return false
}

// ValidName reports whether a program or table name is a legal path segment.
func ValidName(name string) bool {
	return nameRE.MatchString(name)
}
