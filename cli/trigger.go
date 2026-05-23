// cli/trigger.go
// Commands for managing workflow triggers: create, list, delete.
package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/internal/trigger"
)

// triggerActionResult is the JSON response for delete/enable/disable.
type triggerActionResult struct {
	TriggerID string `json:"trigger_id"`
	Action    string `json:"action"`
}

// runTriggerCmd dispatches trigger subcommands.
func runTriggerCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats trigger <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  create   create a new trigger")
		fmt.Println("  list     list all triggers")
		fmt.Println("  update   update an existing trigger")
		fmt.Println("  delete   delete a trigger")
		fmt.Println("  enable   enable a trigger")
		fmt.Println("  disable  disable a trigger")
		fmt.Println("  fire     fire a cron/webhook trigger now")
		fmt.Println("  test     validate a cron expression and show fire times")
		fmt.Println("  history  show trigger fire history")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats trigger " +
			"<create|list|update|delete|enable|disable|fire|test|history>" +
			" [--json]")
		return
	}
	switch args[0] {
	case "create":
		runTriggerCreateCmd(args[1:])
	case "list":
		runTriggerListCmd(args[1:])
	case "update":
		runTriggerUpdateCmd(args[1:])
	case "delete":
		runTriggerDeleteCmd(args[1:])
	case "enable":
		runTriggerEnableCmd(args[1:])
	case "disable":
		runTriggerDisableCmd(args[1:])
	case "fire":
		runTriggerFireCmd(args[1:])
	case "test":
		runTriggerTestCmd(args[1:])
	case "history":
		runTriggerHistoryCmd(args[1:])
	default:
		fmt.Printf("unknown trigger subcommand: %s\n", args[0])
	}
}

// parseTriggerCreateFlags parses command-line flags for trigger create.
// Returns nil if parsing fails or required args are missing.
func parseTriggerCreateFlags(args []string) *trigger.TriggerDef {
	if len(args) < 1 {
		return nil
	}

	fs := flag.NewFlagSet("trigger create", flag.ExitOnError)
	cronExpr := fs.String("cron", "", "Cron expression")
	subject := fs.String("subject", "", "NATS subject pattern")
	webhook := fs.String("webhook", "", "Webhook path")
	timezone := fs.String("tz", "UTC", "Timezone for cron")
	backfill := fs.Bool("backfill", false, "Enable backfill for cron")
	secret := fs.String("secret", "", "Webhook secret")

	fs.Parse(args[1:])

	// Validate exactly one trigger type
	typeCount := 0
	if *cronExpr != "" {
		typeCount++
	}
	if *subject != "" {
		typeCount++
	}
	if *webhook != "" {
		typeCount++
	}
	if typeCount != 1 {
		return nil
	}

	def := &trigger.TriggerDef{
		ID:         generateTriggerID(),
		WorkflowID: args[0],
		Enabled:    true,
	}

	if *cronExpr != "" {
		def.Cron = &trigger.CronConfig{
			Expression: *cronExpr,
			Timezone:   *timezone,
			Backfill:   *backfill,
		}
	}
	if *subject != "" {
		def.Subject = &trigger.SubjectConfig{
			Subject: *subject,
		}
	}
	if *webhook != "" {
		webhookSecret := *secret
		if webhookSecret == "" {
			webhookSecret = os.Getenv("DAGNATS_WEBHOOK_SECRET")
		}
		def.Webhook = &trigger.WebhookConfig{
			Path:   *webhook,
			Secret: webhookSecret,
		}
	}

	return def
}

// runTriggerCreateCmd creates a new trigger and stores it via api.Service.
func runTriggerCreateCmd(args []string) {
	runTriggerCreateCmdWithWriter(args, os.Stdout)
}

// runTriggerCreateCmdWithWriter creates a trigger, writing to w.
func runTriggerCreateCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerCreateCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	def := parseTriggerCreateFlags(args)
	if def == nil {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger create <workflow-id> "+
				"[--cron=EXPR] [--subject=SUB] "+
				"[--webhook=PATH] [--tz=TZ] "+
				"[--backfill] [--secret=SEC] [--json]")
		fmt.Fprintln(os.Stderr,
			"error: exactly one of --cron, --subject, "+
				"or --webhook must be specified")
		os.Exit(1)
	}
	if def.WorkflowID == "" {
		panic("runTriggerCreateCmdWithWriter: empty workflowID")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.CreateTrigger(context.Background(), *def)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create trigger: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		result := map[string]string{"trigger_id": def.ID}
		FormatJSON(w, result)
		return
	}
	fmt.Fprintf(w, "Trigger created: %s\n", def.ID)
}

// runTriggerListCmd lists all triggers via api.Service.
func runTriggerListCmd(args []string) {
	runTriggerListCmdWithWriter(args, os.Stdout)
}

// runTriggerListCmdWithWriter lists triggers, writing to w.
func runTriggerListCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerListCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)

	svc, nc := connectService()
	defer nc.Close()

	defs, err := svc.ListTriggers(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "list triggers: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, defs)
		return
	}

	if len(defs) == 0 {
		fmt.Fprintln(w, "No triggers found.")
		return
	}

	printTriggerTable(w, defs)
}

// printTriggerTable writes a formatted trigger table to w.
func printTriggerTable(w io.Writer, defs []trigger.TriggerDef) {
	if w == nil {
		panic("printTriggerTable: w must not be nil")
	}
	if defs == nil {
		panic("printTriggerTable: defs must not be nil")
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tWORKFLOW\tTYPE\tCONFIG\tENABLED")

	const maxDefs = 10000
	for i, def := range defs {
		if i >= maxDefs {
			break
		}
		trigType, config := triggerTypeConfig(def)
		enabled := "no"
		if def.Enabled {
			enabled = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			def.ID, def.WorkflowID, trigType, config, enabled)
	}

	tw.Flush()
}

// triggerTypeConfig returns the type and config string for a trigger.
func triggerTypeConfig(
	def trigger.TriggerDef,
) (string, string) {
	if def.Cron != nil {
		return "cron", def.Cron.Expression
	}
	if def.Subject != nil {
		return "subject", def.Subject.Subject
	}
	if def.Webhook != nil {
		return "webhook", def.Webhook.Path
	}
	if def.HTTP != nil {
		return "http",
			fmt.Sprintf("%s %s", def.HTTP.Method, def.HTTP.Path)
	}
	return "unknown", ""
}

// runTriggerDeleteCmd deletes a trigger via api.Service.
func runTriggerDeleteCmd(args []string) {
	runTriggerDeleteCmdWithWriter(args, os.Stdout)
}

// runTriggerDeleteCmdWithWriter deletes a trigger, writing to w.
func runTriggerDeleteCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerDeleteCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger delete "+
				"<trigger-id> [--json]")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerDeleteCmdWithWriter: empty triggerID")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.DeleteTrigger(context.Background(), triggerID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete trigger: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, triggerActionResult{
			TriggerID: triggerID, Action: "deleted",
		})
		return
	}
	fmt.Fprintf(w, "Trigger deleted: %s\n", triggerID)
}

// runTriggerEnableCmd enables a trigger via api.Service.
func runTriggerEnableCmd(args []string) {
	runTriggerEnableCmdWithWriter(args, os.Stdout)
}

// runTriggerEnableCmdWithWriter enables a trigger, writing to w.
func runTriggerEnableCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerEnableCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger enable "+
				"<trigger-id> [--json]")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerEnableCmdWithWriter: empty triggerID")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.SetTriggerEnabled(
		context.Background(), triggerID, true,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "enable trigger: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, triggerActionResult{
			TriggerID: triggerID, Action: "enabled",
		})
		return
	}
	fmt.Fprintf(w, "Trigger enabled: %s\n", triggerID)
}

// runTriggerDisableCmd disables a trigger via api.Service.
func runTriggerDisableCmd(args []string) {
	runTriggerDisableCmdWithWriter(args, os.Stdout)
}

// runTriggerDisableCmdWithWriter disables a trigger, writing to w.
func runTriggerDisableCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerDisableCmdWithWriter: w must not be nil")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger disable "+
				"<trigger-id> [--json]")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerDisableCmdWithWriter: empty triggerID")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.SetTriggerEnabled(
		context.Background(), triggerID, false,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "disable trigger: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		FormatJSON(w, triggerActionResult{
			TriggerID: triggerID, Action: "disabled",
		})
		return
	}
	fmt.Fprintf(w, "Trigger disabled: %s\n", triggerID)
}

// triggerFireResult is the JSON response for `dagnats trigger fire`.
// Action is always "fired" on the success path; the run id surfaces
// the new run so the caller can deep-link to the dashboard.
type triggerFireResult struct {
	TriggerID string `json:"trigger_id"`
	Action    string `json:"action"`
	RunID     string `json:"run_id"`
}

// runTriggerFireCmd forces one manual fire of the given trigger via
// api.Service.FireTrigger (#352). Cron + webhook triggers are
// supported; subject + http return the not-fireable error path.
func runTriggerFireCmd(args []string) {
	runTriggerFireCmdWithWriter(args, os.Stdout)
}

// runTriggerFireCmdWithWriter is the testable variant; writes its
// human-mode line to w. JSON mode emits triggerFireResult through
// FormatJSON. Both modes exit(1) on error after printing to stderr.
func runTriggerFireCmdWithWriter(args []string, w io.Writer) {
	if w == nil {
		panic("runTriggerFireCmdWithWriter: w must not be nil")
	}
	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger fire <trigger-id> [--json]")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerFireCmdWithWriter: empty triggerID")
	}
	svc, nc := connectService()
	defer nc.Close()
	runID, err := svc.FireTrigger(
		context.Background(), triggerID,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fire trigger: %v\n", err)
		os.Exit(1)
	}
	if jsonOutput {
		FormatJSON(w, triggerFireResult{
			TriggerID: triggerID, Action: "fired", RunID: runID,
		})
		return
	}
	fmt.Fprintf(w, "Trigger fired: %s (run %s)\n", triggerID, runID)
}

// generateTriggerID creates a unique ID for a new trigger using crypto/rand.
func generateTriggerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("generateTriggerID: crypto/rand failed: " + err.Error())
	}
	return "trig-" + hex.EncodeToString(b)
}
