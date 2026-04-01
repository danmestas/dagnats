// cli/color_test.go
// Methodology: direct unit tests for color helpers. Verify ANSI wrapping
// when enabled and plain passthrough when NO_COLOR is set.
package cli

import "testing"

func TestColorizeWrapsString(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	got := colorize(colorRed, "error")
	if got != colorRed+"error"+colorReset {
		t.Fatalf("expected ANSI-wrapped string, got %q", got)
	}
	if got == "error" {
		t.Fatal("color should be applied when NO_COLOR is unset")
	}
}

func TestColorizeRespectsNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	got := colorize(colorRed, "error")
	if got != "error" {
		t.Fatalf("expected plain string with NO_COLOR, got %q", got)
	}
}

func TestColorizeEmptyStringReturnsEmpty(t *testing.T) {
	got := colorize(colorRed, "")
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestColorStatusMapsCorrectly(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	cases := []struct {
		status string
		color  string
	}{
		{"completed", colorGreen},
		{"skipped", colorGreen},
		{"failed", colorRed},
		{"running", colorYellow},
		{"queued", colorYellow},
		{"pending", colorGray},
		{"cancelled", colorGray},
	}
	for _, tc := range cases {
		got := ColorStatus(tc.status)
		want := tc.color + tc.status + colorReset
		if got != want {
			t.Errorf("ColorStatus(%q) = %q, want %q",
				tc.status, got, want)
		}
	}
}

func TestColorStatusUnknownPassthrough(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	got := ColorStatus("unknown_status")
	if got != "unknown_status" {
		t.Errorf("expected passthrough for unknown status, got %q", got)
	}
}

func TestColorStatusNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	got := ColorStatus("failed")
	if got != "failed" {
		t.Errorf("expected plain string with NO_COLOR, got %q", got)
	}
}
