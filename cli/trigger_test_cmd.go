// cli/trigger_test_cmd.go
// Command for testing cron expressions: validates syntax and shows
// next N fire times in the specified timezone.
package cli

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/danmestas/dagnats/trigger"
)

// runTriggerTestCmd validates a cron expression and shows fire times.
func runTriggerTestCmd(args []string) {
	if args == nil {
		panic("runTriggerTestCmd: args must not be nil")
	}
	fs := flag.NewFlagSet("trigger test", flag.ExitOnError)
	tz := fs.String("tz", "UTC", "Timezone")
	count := fs.Int("count", 5, "Number of fire times to show")
	fs.Parse(args)

	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger test <cron-expr> "+
				"[--tz=TZ] [--count=N]")
		os.Exit(1)
	}
	expr := fs.Arg(0)

	fmt.Print(FormatCronTest(expr, *tz, *count))
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
