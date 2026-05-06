// cli/trigger_test_cmd.go
// Command for testing cron expressions: validates syntax and shows
// next N fire times in the specified timezone.
package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/trigger"
)

// cronTestResult is the JSON response for trigger test.
type cronTestResult struct {
	Expression string   `json:"expression"`
	Valid      bool     `json:"valid"`
	Timezone   string   `json:"timezone,omitempty"`
	Error      string   `json:"error,omitempty"`
	NextTimes  []string `json:"next_times,omitempty"`
}

// runTriggerTestCmd validates a cron expression and shows fire times.
func runTriggerTestCmd(args []string) {
	if args == nil {
		panic("runTriggerTestCmd: args must not be nil")
	}

	// Strip --json BEFORE fs.Parse to avoid ExitOnError.
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	fs := flag.NewFlagSet("trigger test", flag.ExitOnError)
	tz := fs.String("tz", "UTC", "Timezone")
	count := fs.Int("count", 5, "Number of fire times to show")
	cronFlag := fs.String("cron", "",
		"Cron expression (alternative to positional argument)")
	fs.Parse(args)

	expr, err := resolveTriggerTestExpr(*cronFlag, fs.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger test "+
				"(<cron-expr> | --cron=EXPR) "+
				"[--tz=TZ] [--count=N] [--json]")
		os.Exit(1)
	}

	if jsonOutput {
		FormatCronTestJSON(os.Stdout, expr, *tz, *count)
		return
	}
	fmt.Print(FormatCronTest(expr, *tz, *count))
}

// resolveTriggerTestExpr picks the cron expression to test, accepting
// either a --cron flag value or a single positional argument. Errors
// when both forms or neither are supplied.
//
// Why both forms: trigger create takes --cron=, so operators expect
// trigger test to accept the same shape (issue #175).
func resolveTriggerTestExpr(
	cronFlag string, posArgs []string,
) (string, error) {
	if posArgs == nil {
		panic("resolveTriggerTestExpr: posArgs must not be nil")
	}
	const maxArgs = 1000
	if len(posArgs) > maxArgs {
		panic("resolveTriggerTestExpr: posArgs exceeds max bound")
	}

	hasFlag := cronFlag != ""
	posCount := len(posArgs)

	if hasFlag && posCount > 0 {
		return "", fmt.Errorf(
			"specify cron expression as positional " +
				"OR --cron=, not both")
	}
	if !hasFlag && posCount != 1 {
		return "", fmt.Errorf(
			"cron expression required: " +
				"dagnats trigger test " +
				"(<cron-expr> | --cron=EXPR)")
	}
	if hasFlag {
		return cronFlag, nil
	}
	return posArgs[0], nil
}

// FormatCronTestJSON writes a JSON cronTestResult to w.
func FormatCronTestJSON(
	w io.Writer, expr, tz string, count int,
) {
	if w == nil {
		panic("FormatCronTestJSON: w must not be nil")
	}
	if expr == "" {
		panic("FormatCronTestJSON: expr must not be empty")
	}

	const maxCount = 100
	if count > maxCount {
		count = maxCount
	}

	result := cronTestResult{Expression: expr}

	loc, err := time.LoadLocation(tz)
	if err != nil {
		result.Error = fmt.Sprintf(
			"%q is not a valid timezone", tz)
		FormatJSON(w, result)
		return
	}
	result.Timezone = tz

	parsed, parseErr := trigger.ParseCron(expr)
	if parseErr != nil {
		result.Error = parseErr.Error()
		FormatJSON(w, result)
		return
	}

	result.Valid = true
	now := time.Now().In(loc)
	times := parsed.NextN(now, count)
	result.NextTimes = make([]string, len(times))
	for i, t := range times {
		result.NextTimes[i] = t.In(loc).Format(time.RFC3339)
	}
	FormatJSON(w, result)
}

// FormatCronTest validates a cron expression and returns a formatted
// string showing validity and next N fire times.
func FormatCronTest(expr, tz string, count int) string {
	if expr == "" {
		panic("FormatCronTest: expr must not be empty")
	}
	if count < 0 {
		panic("FormatCronTest: count must not be negative")
	}

	const maxCount = 100
	if count > maxCount {
		count = maxCount
	}

	var b strings.Builder

	loc, err := time.LoadLocation(tz)
	if err != nil {
		fmt.Fprintf(&b,
			"Timezone error: %q is not a valid timezone\n", tz)
		return b.String()
	}

	parsed, err := trigger.ParseCron(expr)
	if err != nil {
		fmt.Fprintf(&b, "Invalid: %s\n  %v\n", expr, err)
		return b.String()
	}

	fmt.Fprintf(&b, "Valid: %s\n", expr)

	now := time.Now().In(loc)
	times := parsed.NextN(now, count)
	if len(times) > 0 {
		fmt.Fprintf(&b, "\nNext %d fire times (%s):\n",
			len(times), tz)
		for i, t := range times {
			fmt.Fprintf(&b, "  %d. %s\n",
				i+1,
				t.In(loc).Format("2006-01-02 15:04 MST"))
		}
	}
	return b.String()
}
