// cli/trigger_test_cmd_test.go
// Tests for the trigger test command output formatting.
// Methodology: unit test the cron validation and next-fire formatting.
package cli

import (
	"bytes"
	"encoding/json"
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

func TestFormatCronTestJSON_ValidExpr(t *testing.T) {
	var buf bytes.Buffer
	FormatCronTestJSON(&buf, "*/15 * * * *", "UTC", 5)

	var result cronTestResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}

	// Positive: expression valid with next times
	if !result.Valid {
		t.Fatal("expected valid=true for valid expression")
	}
	if len(result.NextTimes) != 5 {
		t.Fatalf("expected 5 next_times, got %d",
			len(result.NextTimes))
	}

	// Negative: no error field
	if result.Error != "" {
		t.Fatal("valid expression should have empty error")
	}
}

func TestFormatCronTestJSON_InvalidExpr(t *testing.T) {
	var buf bytes.Buffer
	FormatCronTestJSON(&buf, "bad cron", "UTC", 5)

	var result cronTestResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}

	// Positive: invalid with error message
	if result.Valid {
		t.Fatal("expected valid=false for invalid expression")
	}
	if result.Error == "" {
		t.Fatal("invalid expression should have error message")
	}

	// Negative: no next_times
	if len(result.NextTimes) != 0 {
		t.Fatal("invalid expression should have no next_times")
	}
}

func TestFormatCronTestJSON_BadTimezone(t *testing.T) {
	var buf bytes.Buffer
	FormatCronTestJSON(&buf, "* * * * *", "Bad/Zone", 5)

	var result cronTestResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}

	// Positive: invalid with timezone error
	if result.Valid {
		t.Fatal("expected valid=false for bad timezone")
	}
	if result.Error == "" {
		t.Fatal("bad timezone should have error message")
	}

	// Negative: no next_times
	if len(result.NextTimes) != 0 {
		t.Fatal("bad timezone should have no next_times")
	}
}
