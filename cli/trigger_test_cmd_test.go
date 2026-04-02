// cli/trigger_test_cmd_test.go
// Tests for the trigger test command output formatting.
// Methodology: unit test the cron validation and next-fire formatting.
package cli

import (
	"strings"
	"testing"
)

func TestFormatCronTest_ValidExpr(t *testing.T) {
	output := FormatCronTest("*/15 * * * *", "UTC", 5)
	// Positive: contains the expression
	if !strings.Contains(output, "*/15 * * * *") {
		t.Fatal("output should contain the cron expression")
	}
	// Positive: contains "Valid"
	if !strings.Contains(output, "Valid") {
		t.Fatal("output should indicate expression is valid")
	}
	// Positive: shows next fire times
	if !strings.Contains(output, "Next") {
		t.Fatal("output should show next fire times")
	}
	// Negative: does not contain "Error"
	if strings.Contains(output, "Error") {
		t.Fatal("valid expression should not show Error")
	}
}

func TestFormatCronTest_InvalidExpr(t *testing.T) {
	output := FormatCronTest("bad cron", "UTC", 5)
	// Positive: contains "Invalid"
	if !strings.Contains(output, "Invalid") {
		t.Fatal("output should indicate expression is invalid")
	}
	// Negative: does not show fire times
	if strings.Contains(output, "Next") {
		t.Fatal("invalid expression should not show fire times")
	}
}

func TestFormatCronTest_BadTimezone(t *testing.T) {
	output := FormatCronTest("* * * * *", "Bad/Zone", 5)
	// Positive: contains timezone-related error text
	lower := strings.ToLower(output)
	if !strings.Contains(lower, "timezone") {
		t.Fatal("output should mention timezone error")
	}
	// Negative: does not show fire times
	if strings.Contains(output, "Next") {
		t.Fatal("bad timezone should not show fire times")
	}
}
