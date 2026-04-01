package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// runRunCmd dispatches run subcommands. Stubs are placeholders until HTTP
// client integration is added in a later task.
func runRunCmd(args []string) {
	if len(args) == 0 {
		fmt.Println(
			"Usage: dagnats run <start|status|history|retry|cancel>",
		)
		return
	}
	switch args[0] {
	case "start":
		fmt.Println("(run start not yet implemented)")
	case "status":
		fmt.Println("(run status not yet implemented)")
	case "history":
		fmt.Println("(run history not yet implemented)")
	case "retry":
		fmt.Println("(run retry not yet implemented)")
	case "cancel":
		runCancelCmd(args[1:])
	default:
		fmt.Printf("unknown run subcommand: %s\n", args[0])
	}
}

// runCancelCmd publishes a workflow.cancelled event to cancel a running workflow.
func runCancelCmd(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Usage: dagnats run cancel <run-id>")
		os.Exit(1)
	}
	runID := args[0]
	if runID == "" {
		panic("runCancelCmd: runID must not be empty")
	}

	// Connect to NATS using default URL
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

	// Publish workflow.cancelled event
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, runID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal cancel event: %v\n", err)
		os.Exit(1)
	}

	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	_, err = js.PublishMsg(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish cancel event: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cancellation requested for run: %s\n", runID)
}

// FormatRunStatus renders a WorkflowRun as a human-readable string. Steps are
// rendered individually to avoid exposing raw Go map syntax in terminal output.
func FormatRunStatus(run dag.WorkflowRun) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Run:      %s\n", run.RunID)
	fmt.Fprintf(&b, "Workflow: %s\n", run.WorkflowID)
	fmt.Fprintf(&b, "Status:   %s\n", run.Status.String())
	fmt.Fprintf(&b, "Created:  %s\n", run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&b, "\nSteps:\n")
	for id, state := range run.Steps {
		fmt.Fprintf(&b, "  %-20s %s (attempts: %d)\n", id, state.Status.String(), state.Attempts)
	}
	return b.String()
}
