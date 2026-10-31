package writer

import (
	"bytes"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/lake"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// RowGroupRowsMax keeps column statistics tight enough that WHERE org_id = ? prunes row groups.
const RowGroupRowsMax = 64 << 10

// encoder turns rows into the bytes of one Parquet part. Every column is nullable; payload is
// ZSTD, the rest take the library default.
type encoder struct {
	schema  *parquet.Schema
	columns []lake.Column
	// index maps a column name to its leaf position, because parquet.Group orders fields by name.
	index map[string]int
}

func newEncoder(declared []lake.Column) *encoder {
	group := parquet.Group{
		"event_id":       parquet.Optional(parquet.String()),
		"event_time":     parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
		"stream_seq":     parquet.Optional(parquet.Int(64)),
		"schema_version": parquet.Optional(parquet.Int(64)),
		"payload":        parquet.Optional(parquet.Compressed(parquet.String(), &zstd.Codec{})),
	}
	for _, column := range declared {
		group[column.Name] = parquet.Optional(columnNode(column.Type))
	}
	schema := parquet.NewSchema("lake", group)
	index := make(map[string]int, len(group))
	for name := range group {
		leaf, found := schema.Lookup(name)
		if !found {
			panic("lake: schema lost column " + name)
		}
		index[name] = leaf.ColumnIndex
	}
	return &encoder{schema: schema, columns: declared, index: index}
}

func columnNode(columnType lake.ColumnType) parquet.Node {
	switch columnType {
	case lake.TypeString:
		return parquet.String()
	case lake.TypeInt64:
		return parquet.Int(64)
	case lake.TypeFloat64:
		return parquet.Leaf(parquet.DoubleType)
	case lake.TypeBool:
		return parquet.Leaf(parquet.BooleanType)
	case lake.TypeTimestamp:
		return parquet.Timestamp(parquet.Microsecond)
	}
	panic("lake: unvalidated column type " + string(columnType))
}

// encode builds one part. The same rows always yield the same bytes: no clock, no randomness.
func (e *encoder) encode(rows []Row, schemaVersion int) ([]byte, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("lake: encode: no rows")
	}
	if len(rows) > PartRowsMax {
		return nil, fmt.Errorf("lake: encode: %d rows exceeds one part", len(rows))
	}
	var buffer bytes.Buffer
	writer := parquet.NewWriter(&buffer, e.schema,
		parquet.MaxRowsPerRowGroup(RowGroupRowsMax),
		parquet.CreatedBy("dagnats-lake", "1", ""),
	)
	encoded := make([]parquet.Row, len(rows))
	for i, row := range rows {
		encoded[i] = e.encodeRow(row, schemaVersion)
	}
	if _, err := writer.WriteRows(encoded); err != nil {
		return nil, fmt.Errorf("lake: write rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("lake: close part: %w", err)
	}
	return buffer.Bytes(), nil
}

func (e *encoder) encodeRow(row Row, schemaVersion int) parquet.Row {
	if schemaVersion < 1 || len(e.index) != len(e.columns)+5 {
		panic("lake: encodeRow on an unvalidated schema")
	}
	out := make(parquet.Row, len(e.index))
	set := func(name string, value parquet.Value) {
		position := e.index[name]
		out[position] = value.Level(0, 1, position)
	}
	set("event_id", parquet.ByteArrayValue([]byte(row.EventID)))
	set("event_time", parquet.Int64Value(row.EventTime.UnixMicro()))
	set("stream_seq", parquet.Int64Value(row.StreamSeq))
	set("schema_version", parquet.Int64Value(int64(schemaVersion)))
	set("payload", parquet.ByteArrayValue([]byte(row.Payload)))
	for _, column := range e.columns {
		position := e.index[column.Name]
		value, ok := projectValue(column.Type, row.Values[column.Name])
		if !ok {
			panic("lake: encodeRow got a value Append should have refused: " + column.Name)
		}
		if value.IsNull() {
			out[position] = value.Level(0, 0, position)
		} else {
			out[position] = value.Level(0, 1, position)
		}
	}
	return out
}

// projectValue converts a Go value to a Parquet value. nil is NULL; a value of the wrong type
// reports false, so Append can refuse it with the column's name. The archive turns a mistyped or
// missing JSON value into nil before appending, because payload still holds the event whole.
func projectValue(columnType lake.ColumnType, value any) (parquet.Value, bool) {
	switch typed := value.(type) {
	case nil:
		return parquet.Value{}, true
	case string:
		if columnType == lake.TypeString {
			return parquet.ByteArrayValue([]byte(typed)), true
		}
	case int:
		if columnType == lake.TypeInt64 {
			return parquet.Int64Value(int64(typed)), true
		}
	case int64:
		if columnType == lake.TypeInt64 {
			return parquet.Int64Value(typed), true
		}
	case float64:
		if columnType == lake.TypeFloat64 {
			return parquet.DoubleValue(typed), true
		}
	case bool:
		if columnType == lake.TypeBool {
			return parquet.BooleanValue(typed), true
		}
	case time.Time:
		if columnType == lake.TypeTimestamp {
			return parquet.Int64Value(typed.UnixMicro()), true
		}
	}
	return parquet.Value{}, false
}
