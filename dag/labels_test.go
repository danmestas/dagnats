// dag/labels_test.go

// Tests for run label validation: count/key-charset/key-length/value-length
// bounds. Methodology: table-driven, each case exercises exactly one bound
// at its boundary (valid at the limit, invalid one past it) plus an
// invalid-charset case. Positive + negative space checked per case.
package dag

import (
	"strings"
	"testing"
)

func TestValidateLabels(t *testing.T) {
	repeat := func(r byte, n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = r
		}
		return string(b)
	}
	labelsOfCount := func(n int) map[string]string {
		m := make(map[string]string, n)
		for i := 0; i < n; i++ {
			m[strings.Repeat("k", 1)+string(rune('a'+i%26))+string(rune('0'+i/26))] = "v"
		}
		return m
	}

	cases := []struct {
		name    string
		labels  map[string]string
		wantErr bool
	}{
		{"nil is valid", nil, false},
		{"empty is valid", map[string]string{}, false},
		{"16 labels ok", labelsOfCount(LabelsCountMax), false},
		{"17 labels rejected", labelsOfCount(LabelsCountMax + 1), true},
		{"64-char key ok", map[string]string{repeat('a', LabelKeyLengthMax): "v"}, false},
		{"65-char key rejected", map[string]string{repeat('a', LabelKeyLengthMax+1): "v"}, true},
		{"256-char value ok", map[string]string{"k": repeat('v', LabelValueLengthMax)}, false},
		{"257-char value rejected", map[string]string{"k": repeat('v', LabelValueLengthMax+1)}, true},
		{"invalid charset rejected", map[string]string{"Bad Key!": "v"}, true},
		{"valid charset ok", map[string]string{"env.name-1_2": "v"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLabels(tc.labels)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error for %s, got: %v", tc.name, err)
			}
		})
	}
}

func TestValidateLabelsErrorNamesOffendingKey(t *testing.T) {
	err := ValidateLabels(map[string]string{"bad key!": "v"})
	if err == nil {
		t.Fatal("expected error for invalid charset key, got nil")
	}
	if !strings.Contains(err.Error(), "bad key!") {
		t.Fatalf("error should name the offending key, got: %v", err)
	}
}

// TestValidateLabelsErrorIsDeterministicAcrossViolations proves that with
// two invalid keys present, ValidateLabels always names the lexically
// first one -- map iteration order is not stable across calls, so
// without sorting this test would flake. Run repeatedly to make a
// regression to map-order iteration visible rather than a coin flip.
func TestValidateLabelsErrorIsDeterministicAcrossViolations(t *testing.T) {
	labels := map[string]string{
		"zzz bad": "v",
		"aaa bad": "v",
	}
	for i := 0; i < 20; i++ {
		err := ValidateLabels(labels)
		if err == nil {
			t.Fatal("expected error for two invalid keys, got nil")
		}
		// Positive: the lexically-first offending key is always named.
		if !strings.Contains(err.Error(), "aaa bad") {
			t.Fatalf("iteration %d: error = %v, want it to name %q",
				i, err, "aaa bad")
		}
		// Negative: the other invalid key is never named instead.
		if strings.Contains(err.Error(), "zzz bad") {
			t.Fatalf("iteration %d: error = %v, must not name %q",
				i, err, "zzz bad")
		}
	}
}

func TestLabelsMatch(t *testing.T) {
	have := map[string]string{"tenant": "a", "region": "us"}
	// Positive: nil/empty want matches anything; a subset want matches.
	if !LabelsMatch(nil, have) {
		t.Fatal("nil want should match any run")
	}
	if !LabelsMatch(map[string]string{"tenant": "a"}, have) {
		t.Fatal("matching subset should match")
	}
	// Negative: a wrong value, or a key absent from have, must not match.
	if LabelsMatch(map[string]string{"tenant": "b"}, have) {
		t.Fatal("mismatched value must not match")
	}
	if LabelsMatch(map[string]string{"stage": "prod"}, have) {
		t.Fatal("key absent from have must not match")
	}
}
