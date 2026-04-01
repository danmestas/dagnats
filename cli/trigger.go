// cli/trigger.go
// Commands for managing workflow triggers: create, list, delete.
// Triggers are stored in the NATS KV "triggers" bucket.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/trigger"
	"github.com/nats-io/nats.go"
)

// runTriggerCmd dispatches trigger subcommands.
func runTriggerCmd(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: dagnats trigger <create|list|delete>")
		return
	}
	switch args[0] {
	case "create":
		runTriggerCreateCmd(args[1:])
	case "list":
		runTriggerListCmd(args[1:])
	case "delete":
		runTriggerDeleteCmd(args[1:])
	default:
		fmt.Printf("unknown trigger subcommand: %s\n", args[0])
	}
}

// TriggerCreateFlags holds parsed flags for trigger create command.
type TriggerCreateFlags struct {
	WorkflowID string
	Cron       string
	Subject    string
	Webhook    string
	Timezone   string
	Backfill   bool
	Secret     string
}

// parseTriggerCreateFlags parses command-line flags for trigger create.
// Returns nil if parsing fails or required args are missing.
func parseTriggerCreateFlags(args []string) *TriggerCreateFlags {
	if len(args) < 1 {
		return nil
	}
	flags := &TriggerCreateFlags{
		WorkflowID: args[0],
		Timezone:   "UTC",
	}
	// Parse flags in format --flag=value or --flag value
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if len(arg) < 3 || arg[0:2] != "--" {
			continue
		}
		var key, value string
		// Check for --flag=value format
		for j := 2; j < len(arg); j++ {
			if arg[j] == '=' {
				key = arg[2:j]
				value = arg[j+1:]
				break
			}
		}
		// --flag value format
		if key == "" {
			key = arg[2:]
			if i+1 < len(args) && args[i+1][0] != '-' {
				value = args[i+1]
				i++
			}
		}
		switch key {
		case "cron":
			flags.Cron = value
		case "subject":
			flags.Subject = value
		case "webhook":
			flags.Webhook = value
		case "tz":
			flags.Timezone = value
		case "backfill":
			flags.Backfill = (value == "true" || value == "")
		case "secret":
			flags.Secret = value
		}
	}
	return flags
}

// runTriggerCreateCmd creates a new trigger and stores it in KV.
func runTriggerCreateCmd(args []string) {
	flags := parseTriggerCreateFlags(args)
	if flags == nil {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats trigger create <workflow-id> "+
				"[--cron=EXPR] [--subject=SUB] [--webhook=PATH] "+
				"[--tz=TZ] [--backfill] [--secret=SEC]")
		os.Exit(1)
	}
	if flags.WorkflowID == "" {
		panic("runTriggerCreateCmd: workflowID must not be empty")
	}

	// Validate exactly one trigger type
	typeCount := 0
	if flags.Cron != "" {
		typeCount++
	}
	if flags.Subject != "" {
		typeCount++
	}
	if flags.Webhook != "" {
		typeCount++
	}
	if typeCount != 1 {
		fmt.Fprintln(os.Stderr,
			"error: exactly one of --cron, --subject, "+
				"or --webhook must be specified")
		os.Exit(1)
	}

	// Build TriggerDef
	def := trigger.TriggerDef{
		ID:         generateTriggerID(),
		WorkflowID: flags.WorkflowID,
		Enabled:    true,
	}
	if flags.Cron != "" {
		def.Cron = &trigger.CronConfig{
			Expression: flags.Cron,
			Timezone:   flags.Timezone,
			Backfill:   flags.Backfill,
		}
	}
	if flags.Subject != "" {
		def.Subject = &trigger.SubjectConfig{
			Subject: flags.Subject,
		}
	}
	if flags.Webhook != "" {
		def.Webhook = &trigger.WebhookConfig{
			Path:   flags.Webhook,
			Secret: flags.Secret,
		}
	}

	// Connect to NATS
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get JetStream context: %v\n", err)
		os.Exit(1)
	}

	// Store in triggers KV bucket
	trigKV, err := js.KeyValue("triggers")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get triggers bucket: %v\n", err)
		os.Exit(1)
	}

	data, err := json.Marshal(def)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal trigger: %v\n", err)
		os.Exit(1)
	}

	_, err = trigKV.Put(def.ID, data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store trigger: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Trigger created: %s\n", def.ID)
}

// runTriggerListCmd lists all triggers from the KV bucket.
func runTriggerListCmd(args []string) {
	// Connect to NATS
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get JetStream context: %v\n", err)
		os.Exit(1)
	}

	trigKV, err := js.KeyValue("triggers")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get triggers bucket: %v\n", err)
		os.Exit(1)
	}

	// List all keys
	keys, err := trigKV.Keys()
	if err != nil {
		fmt.Fprintf(os.Stderr, "list trigger keys: %v\n", err)
		os.Exit(1)
	}

	if len(keys) == 0 {
		fmt.Println("No triggers found.")
		return
	}

	// Print table header
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tWORKFLOW\tTYPE\tCONFIG\tENABLED")

	for _, key := range keys {
		entry, err := trigKV.Get(key)
		if err != nil {
			continue
		}
		var def trigger.TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}

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

// runTriggerDeleteCmd deletes a trigger from the KV bucket.
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

	// Connect to NATS
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "get JetStream context: %v\n", err)
		os.Exit(1)
	}

	trigKV, err := js.KeyValue("triggers")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get triggers bucket: %v\n", err)
		os.Exit(1)
	}

	err = trigKV.Delete(triggerID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete trigger: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Trigger deleted: %s\n", triggerID)
}

// generateTriggerID creates a unique ID for a new trigger.
// Uses a simple timestamp-based ID for now.
func generateTriggerID() string {
	return fmt.Sprintf("trig-%d", timeNowUnix())
}

// timeNowUnix returns current Unix timestamp. Extracted for testing.
var timeNowUnix = func() int64 {
	return time.Now().Unix()
}
