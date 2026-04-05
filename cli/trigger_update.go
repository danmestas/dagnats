// cli/trigger_update.go
// Update an existing trigger's configuration without delete/recreate.
package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/danmestas/dagnats/internal/api"
)

// runTriggerUpdateCmd updates an existing trigger in-place.
func runTriggerUpdateCmd(args []string) {
	runTriggerUpdateCmdWithWriter(args, os.Stdout)
}

// runTriggerUpdateCmdWithWriter updates a trigger, writing to w.
func runTriggerUpdateCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic(
			"runTriggerUpdateCmdWithWriter: w must not be nil",
		)
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger update <trigger-id> "+
				"[--cron=EXPR] [--tz=TZ] [--backfill] "+
				"[--subject=SUB] [--webhook=PATH] "+
				"[--secret=SEC] [--json]")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic(
			"runTriggerUpdateCmdWithWriter: empty triggerID",
		)
	}

	updates := parseTriggerUpdateFlags(args[1:])

	svc, nc := connectService()
	defer nc.Close()

	err := svc.UpdateTrigger(
		context.Background(), triggerID, updates,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "update trigger: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, triggerActionResult{
			TriggerID: triggerID, Action: "updated",
		})
		return
	}
	fmt.Fprintf(w, "Trigger updated: %s\n", triggerID)
}

// parseTriggerUpdateFlags extracts update fields from args.
// Uses strings.HasPrefix to match the manual flag parsing pattern.
func parseTriggerUpdateFlags(
	args []string,
) api.TriggerUpdates {
	if args == nil {
		panic("parseTriggerUpdateFlags: args must not be nil")
	}

	var updates api.TriggerUpdates
	const maxArgs = 100
	for i, arg := range args {
		if i >= maxArgs {
			break
		}
		switch {
		case strings.HasPrefix(arg, "--cron="):
			v := arg[len("--cron="):]
			updates.CronExpr = &v
		case strings.HasPrefix(arg, "--tz="):
			v := arg[len("--tz="):]
			updates.Timezone = &v
		case arg == "--backfill":
			v := true
			updates.Backfill = &v
		case strings.HasPrefix(arg, "--subject="):
			v := arg[len("--subject="):]
			updates.Subject = &v
		case strings.HasPrefix(arg, "--webhook="):
			v := arg[len("--webhook="):]
			updates.Webhook = &v
		case strings.HasPrefix(arg, "--secret="):
			v := arg[len("--secret="):]
			updates.Secret = &v
		}
	}
	return updates
}
