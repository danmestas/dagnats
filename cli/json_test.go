// cli/json_test.go
// Methodology: direct unit tests for JSON output helpers. Covers flag
// detection, flag stripping, and JSON formatting with both positive
// and negative cases for each function.
package cli

import (
	"bytes"
	"math"
	"testing"
)

func TestHasJSONFlagPresent(t *testing.T) {
	cases := [][]string{
		{"--json"},
		{"other", "--json"},
		{"--json", "other"},
		{"--verbose", "--json", "run"},
	}
	for _, args := range cases {
		if !HasJSONFlag(args) {
			t.Errorf("HasJSONFlag(%v) = false, want true", args)
		}
	}
}

func TestHasJSONFlagAbsent(t *testing.T) {
	cases := [][]string{
		{},
		{"--jsonl"},
		{"--JSON"},
		{"-json"},
		{"json"},
		{"run", "start"},
	}
	for _, args := range cases {
		if HasJSONFlag(args) {
			t.Errorf("HasJSONFlag(%v) = true, want false", args)
		}
	}
}

func TestHasJSONFlagNil(t *testing.T) {
	if HasJSONFlag(nil) {
		t.Error("HasJSONFlag(nil) = true, want false")
	}
}

func TestStripJSONFlag(t *testing.T) {
	input := []string{"run", "--json", "--verbose"}
	got := StripJSONFlag(input)

	if len(got) != 2 {
		t.Fatalf("StripJSONFlag: got len %d, want 2", len(got))
	}
	if got[0] != "run" || got[1] != "--verbose" {
		t.Errorf(
			"StripJSONFlag: got %v, want [run --verbose]",
			got,
		)
	}
}

func TestStripJSONFlagNoMatch(t *testing.T) {
	input := []string{"run", "--verbose"}
	got := StripJSONFlag(input)

	if len(got) != 2 {
		t.Fatalf("StripJSONFlag: got len %d, want 2", len(got))
	}
	if got[0] != "run" || got[1] != "--verbose" {
		t.Errorf(
			"StripJSONFlag: got %v, want [run --verbose]",
			got,
		)
	}
}

func TestStripJSONFlagPanicsOnNil(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("StripJSONFlag(nil) did not panic")
		}
	}()
	StripJSONFlag(nil)
}

func TestFormatJSON(t *testing.T) {
	var buf bytes.Buffer
	data := map[string]string{"key": "value"}
	err := FormatJSON(&buf, data)

	if err != nil {
		t.Fatalf("FormatJSON returned error: %v", err)
	}

	want := "{\n  \"key\": \"value\"\n}\n"
	if buf.String() != want {
		t.Errorf(
			"FormatJSON output = %q, want %q",
			buf.String(), want,
		)
	}
}

func TestFormatJSONError(t *testing.T) {
	var buf bytes.Buffer
	// math.Inf cannot be marshaled to JSON.
	err := FormatJSON(&buf, math.Inf(1))

	if err == nil {
		t.Fatal("FormatJSON(Inf) should return error")
	}
	if buf.Len() != 0 {
		t.Errorf(
			"FormatJSON(Inf) wrote %d bytes, want 0",
			buf.Len(),
		)
	}
}

func TestFormatJSONPanicsOnNilWriter(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("FormatJSON(nil, ...) did not panic")
		}
	}()
	_ = FormatJSON(nil, "test")
}
