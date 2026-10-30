// Methodology: pure unit tests of Schema.Validate. The reserved-column rule is asserted with a
// literal name list, so renaming a reserved column (a wire-format break for every reader) fails
// here rather than silently in a downstream query.
package lake

import (
	"strings"
	"testing"
)

func TestSchemaValidate_RefusesReservedColumnNames(t *testing.T) {
	for _, name := range []string{
		"dt", "event_id", "event_time", "stream_seq", "payload", "schema_version",
	} {
		schema := Schema{Version: 1, Columns: []Column{{Name: name, Type: TypeString}}}
		err := schema.Validate()
		if err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Fatalf("column %q: err = %v, want reserved", name, err)
		}
	}
	ok := Schema{Version: 1, Columns: []Column{{Name: "org_id", Type: TypeString}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("org_id: %v", err)
	}
}

func TestSchemaValidate_RefusesBadShapes(t *testing.T) {
	cases := map[string]Schema{
		"version zero":   {Version: 0},
		"duplicate":      {Version: 1, Columns: []Column{{"a", TypeBool}, {"a", TypeBool}}},
		"uppercase":      {Version: 1, Columns: []Column{{"Org", TypeString}}},
		"leading digit":  {Version: 1, Columns: []Column{{"1a", TypeString}}},
		"unknown type":   {Version: 1, Columns: []Column{{"a", "decimal"}}},
		"too many":       {Version: 1, Columns: tooManyColumns()},
		"empty name":     {Version: 1, Columns: []Column{{"", TypeString}}},
		"name too long":  {Version: 1, Columns: []Column{{strings.Repeat("a", 33), TypeString}}},
		"path separator": {Version: 1, Columns: []Column{{"a/b", TypeString}}},
	}
	for name, schema := range cases {
		if err := schema.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func tooManyColumns() []Column {
	columns := make([]Column, ColumnCountMax+1)
	for i := range columns {
		name := "c" + strings.Repeat("a", i%20) + string(rune('a'+i/20))
		columns[i] = Column{Name: name, Type: TypeInt64}
	}
	return columns
}
