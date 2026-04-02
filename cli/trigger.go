// cli/trigger.go
// Commands for managing workflow triggers: create, list, delete.
package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/danmestas/dagnats/trigger"
)

// runTriggerCmd dispatches trigger subcommands.
func runTriggerCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats trigger <command>")
		fmt.Println("Commands:")
		fmt.Println("  create   create a new trigger")
		fmt.Println("  list     list all triggers")
		fmt.Println("  delete   delete a trigger")
		fmt.Println("  enable   enable a trigger")
		fmt.Println("  disable  disable a trigger")
		fmt.Println("  test     validate a cron expression and show fire times")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats trigger " +
			"<create|list|delete|enable|disable|test>")
		return
	}
	switch args[0] {
	case "create":
		runTriggerCreateCmd(args[1:])
	case "list":
		runTriggerListCmd(args[1:])
	case "delete":
		runTriggerDeleteCmd(args[1:])
	case "enable":
		runTriggerEnableCmd(args[1:])
	case "disable":
		runTriggerDisableCmd(args[1:])
	case "test":
		runTriggerTestCmd(args[1:])
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
		def.Webhook = &trigger.WebhookConfig{
			Path:   *webhook,
			Secret: *secret,
		}
	}

	return def
}

// runTriggerCreateCmd creates a new trigger and stores it via api.Service.
func runTriggerCreateCmd(args []string) {
	def := parseTriggerCreateFlags(args)
	if def == nil {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger create <workflow-id> "+
				"[--cron=EXPR] [--subject=SUB] [--webhook=PATH] "+
				"[--tz=TZ] [--backfill] [--secret=SEC]")
		fmt.Fprintln(os.Stderr,
			"error: exactly one of --cron, --subject, "+
				"or --webhook must be specified")
		os.Exit(1)
	}
	if def.WorkflowID == "" {
		panic("runTriggerCreateCmd: workflowID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.CreateTrigger(context.Background(), *def)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create trigger: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Trigger created: %s\n", def.ID)
}

// runTriggerListCmd lists all triggers via api.Service.
func runTriggerListCmd(args []string) {
	svc, nc := connectService()
	defer nc.Close()

	defs, err := svc.ListTriggers(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "list triggers: %v\n", err)
		os.Exit(1)
	}

	if len(defs) == 0 {
		fmt.Println("No triggers found.")
		return
	}

	// Print table header
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tWORKFLOW\tTYPE\tCONFIG\tENABLED")

	for _, def := range defs {
		trigType := "unknown"
		config := ""
		if def.Cron != nil {
			trigType = "cron"
			config = def.Cron.Expression
		} else if def.Subject != nil {
			trigType = "subject"
			config = def.Subject.Subject
		} else if def.Webhook != nil {
			trigType = "webhook"
			config = def.Webhook.Path
		}

		enabled := "no"
		if def.Enabled {
			enabled = "yes"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			def.ID, def.WorkflowID, trigType, config, enabled)
	}

	w.Flush()
}

// runTriggerDeleteCmd deletes a trigger via api.Service.
func runTriggerDeleteCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger delete <trigger-id>")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerDeleteCmd: triggerID must not be empty")
	}

	svc, nc := connectService()
	defer nc.Close()

	err := svc.DeleteTrigger(context.Background(), triggerID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete trigger: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Trigger deleted: %s\n", triggerID)
}

// runTriggerEnableCmd enables a trigger via api.Service.
func runTriggerEnableCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger enable <trigger-id>")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerEnableCmd: triggerID must not be empty")
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

	fmt.Printf("Trigger enabled: %s\n", triggerID)
}

// runTriggerDisableCmd disables a trigger via api.Service.
func runTriggerDisableCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger disable <trigger-id>")
		os.Exit(1)
	}
	triggerID := args[0]
	if triggerID == "" {
		panic("runTriggerDisableCmd: triggerID must not be empty")
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

	fmt.Printf("Trigger disabled: %s\n", triggerID)
}

// generateTriggerID creates a unique ID for a new trigger using crypto/rand.
func generateTriggerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("generateTriggerID: crypto/rand failed: " + err.Error())
	}
	return "trig-" + hex.EncodeToString(b)
}
