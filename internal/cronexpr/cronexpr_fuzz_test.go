// cronexpr/cronexpr_fuzz_test.go
// Tests that malformed expressions are input errors, never panics (#740).
// Methodology: table of known crashers plus a fuzz target seeded with them;
// ParseCron must return either an expression or an error for any string.
package cronexpr

import (
	"strings"
	"testing"
)

var emptyItemExprs = []string{
	"0,,30 3 * * *",
	",5 * * * *",
	"5, * * * *",
	"0 3 * * 1,",
}

func TestParseCron_EmptyItemIsError(t *testing.T) {
	for _, s := range emptyItemExprs {
		expr, err := ParseCron(s)
		// Negative: an empty comma item is rejected.
		if err == nil {
			t.Fatalf("ParseCron(%q): want error, got %+v", s, expr)
		}
	}
	// Negative: an overlong expression is an error too, not a panic.
	if _, err := ParseCron(strings.Repeat("0", 300)); err == nil {
		t.Fatalf("ParseCron overlong: want error")
	}
	// Positive: the same lists without the empty item still parse.
	if _, err := ParseCron("0,30 3 * * 1"); err != nil {
		t.Fatalf("ParseCron valid list: %v", err)
	}
}

func FuzzParseCron(f *testing.F) {
	for _, s := range emptyItemExprs {
		f.Add(s)
	}
	f.Add("*/15 9-17 * * 1-5")
	f.Fuzz(func(t *testing.T, s string) {
		expr, err := ParseCron(s)
		if err == nil && expr == nil {
			t.Fatalf("ParseCron(%q): nil expression without error", s)
		}
	})
}
