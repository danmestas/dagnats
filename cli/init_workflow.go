// cli/init_workflow.go
// Scaffold command for generating workflow JSON definitions with
// step chaining and handler registration code snippets.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// workflowStep represents a single step in the workflow JSON output.
type workflowStep struct {
	ID        string   `json:"id"`
	Task      string   `json:"task"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// workflowTriggerStub mirrors the trigger.TriggerDef JSON shape so
// the scaffold can emit a sample disabled trigger without importing
// internal/trigger. The init package stays a pure file generator.
type workflowTriggerStub struct {
	ID      string   `json:"id"`
	Enabled bool     `json:"enabled"`
	Cron    cronStub `json:"cron"`
}

// cronStub mirrors trigger.CronConfig for the scaffold output.
type cronStub struct {
	Expression string `json:"expression"`
	Timezone   string `json:"timezone"`
	Backfill   bool   `json:"backfill"`
}

// workflowDef represents the complete workflow JSON document.
type workflowDef struct {
	Schema   string                `json:"$schema"`
	Name     string                `json:"name"`
	Version  string                `json:"version"`
	Triggers []workflowTriggerStub `json:"triggers"`
	Steps    []workflowStep        `json:"steps"`
}

// scaffoldWorkflow generates a workflow JSON file in dir named
// <name>.json. When steps is nil, a single "process" step is used.
// Steps are chained linearly — each depends on the previous.
func scaffoldWorkflow(
	dir string, name string, steps []string,
) error {
	if dir == "" {
		panic("scaffoldWorkflow: dir must not be empty")
	}
	if name == "" {
		panic("scaffoldWorkflow: name must not be empty")
	}

	if steps == nil {
		steps = []string{"process"}
	}

	const maxSteps = 20
	if len(steps) > maxSteps {
		return fmt.Errorf(
			"too many steps: %d (max %d)", len(steps), maxSteps,
		)
	}

	path := dir + "/" + name + ".json"
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("file %q already exists", path)
	}

	wf := buildWorkflowDef(name, steps)
	return writeWorkflowFile(path, wf)
}

// buildWorkflowDef constructs the workflow definition struct
// with linearly chained steps.
func buildWorkflowDef(
	name string, steps []string,
) workflowDef {
	if name == "" {
		panic("buildWorkflowDef: name must not be empty")
	}
	if len(steps) == 0 {
		panic("buildWorkflowDef: steps must not be empty")
	}

	schemaURL := "https://raw.githubusercontent.com/" +
		"danmestas/dagnats/main/docs/workflow-schema.json"

	wfSteps := make([]workflowStep, 0, len(steps))
	for i, step := range steps {
		ws := workflowStep{
			ID:   step,
			Task: name + "-" + step,
		}
		if i > 0 {
			ws.DependsOn = []string{steps[i-1]}
		}
		wfSteps = append(wfSteps, ws)
	}

	// Emit a disabled cron trigger stub so the embedded-triggers
	// pattern (issue #171) is discoverable in the file the operator
	// will be editing. Disabled means the workflow registers cleanly
	// without firing — flip enabled:true to activate.
	triggerStubs := []workflowTriggerStub{{
		ID:      name + "-cron",
		Enabled: false,
		Cron: cronStub{
			Expression: "*/5 * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}}

	return workflowDef{
		Schema:   schemaURL,
		Name:     name,
		Version:  "1.0",
		Triggers: triggerStubs,
		Steps:    wfSteps,
	}
}

// writeWorkflowFile marshals the workflow to indented JSON and
// writes it to the given path.
func writeWorkflowFile(path string, wf workflowDef) error {
	if path == "" {
		panic("writeWorkflowFile: path must not be empty")
	}
	if wf.Name == "" {
		panic("writeWorkflowFile: wf.Name must not be empty")
	}

	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal workflow: %w", err)
	}
	data = append(data, '\n')

	return os.WriteFile(path, data, 0644)
}

// printWorkflowSnippet prints handler registration code for
// the scaffolded workflow. Single-step shows inline body;
// multi-step shows one w.Handle per step.
func printWorkflowSnippet(name string, steps []string) {
	if name == "" {
		panic("printWorkflowSnippet: name must not be empty")
	}
	if len(steps) == 0 {
		panic("printWorkflowSnippet: steps must not be empty")
	}

	fmt.Println("\nRegister handlers in your worker:")
	fmt.Println()

	if len(steps) == 1 {
		printSingleStepSnippet(name, steps[0])
		return
	}

	for _, step := range steps {
		funcName := "handle" + toPascalCase(step)
		taskName := name + "-" + step
		fmt.Printf(
			"w.Handle(%q, %s)\n", taskName, funcName,
		)
	}
	fmt.Println()
}

// printSingleStepSnippet prints an inline handler for one step.
func printSingleStepSnippet(name string, step string) {
	if name == "" {
		panic("printSingleStepSnippet: name must not be empty")
	}
	if step == "" {
		panic("printSingleStepSnippet: step must not be empty")
	}

	taskName := name + "-" + step
	fmt.Printf(`w.Handle(%q, func(ctx worker.TaskContext) error {
	input := ctx.Input()
	// TODO: implement %s logic
	return ctx.Complete(input)
})
`, taskName, step)
	fmt.Println()
}

// toPascalCase converts a hyphen-separated string to PascalCase.
// "image-pipeline" becomes "ImagePipeline".
func toPascalCase(s string) string {
	if s == "" {
		panic("toPascalCase: input must not be empty")
	}
	const maxLen = 256
	if len(s) > maxLen {
		panic("toPascalCase: input exceeds max length")
	}

	parts := strings.Split(s, "-")
	result := strings.Builder{}
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		result.WriteString(
			strings.ToUpper(part[:1]) + part[1:],
		)
	}
	return result.String()
}

// runInitWorkflowCmd is the CLI entry point for
// "dagnats init workflow <name> [--steps=a,b,c]".
func runInitWorkflowCmd(args []string) {
	if args == nil {
		panic("runInitWorkflowCmd: args must not be nil")
	}
	if len(args) > 1000 {
		panic("runInitWorkflowCmd: args exceeds max bound")
	}

	parsed := parseWorkflowArgs(args)
	if parsed.err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", parsed.err)
		os.Exit(1)
	}

	if err := scaffoldWorkflow(
		".", parsed.name, parsed.steps,
	); err != nil {
		fmt.Fprintf(os.Stderr, "init workflow: %v\n", err)
		os.Exit(1)
	}

	steps := parsed.steps
	if steps == nil {
		steps = []string{"process"}
	}
	fmt.Printf("Created %s.json\n", parsed.name)
	printWorkflowSnippet(parsed.name, steps)

	printHint(false,
		"Next steps:",
		fmt.Sprintf(
			"  dagnats workflow register %s.json",
			parsed.name,
		),
		"  # Add handler code to your main.go "+
			"(see snippet above)",
	)
}

// workflowArgResult holds the parsed arguments for init workflow.
type workflowArgResult struct {
	name  string
	steps []string
	err   error
}

// parseWorkflowArgs extracts name and optional --steps flag.
func parseWorkflowArgs(args []string) workflowArgResult {
	if args == nil {
		panic("parseWorkflowArgs: args must not be nil")
	}
	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("parseWorkflowArgs: args exceeds max bound")
	}

	var name string
	var steps []string

	for _, arg := range args {
		if strings.HasPrefix(arg, "--steps=") {
			raw := strings.TrimPrefix(arg, "--steps=")
			steps = strings.Split(raw, ",")
			continue
		}
		if name == "" {
			name = arg
			continue
		}
		return workflowArgResult{
			err: fmt.Errorf(
				"unexpected argument: %s", arg,
			),
		}
	}

	if name == "" {
		return workflowArgResult{
			err: fmt.Errorf(
				"usage: dagnats init workflow <name> " +
					"[--steps=a,b,c]",
			),
		}
	}

	if err := validateWorkflowName(name); err != nil {
		return workflowArgResult{err: err}
	}

	return workflowArgResult{name: name, steps: steps}
}

// validateWorkflowName checks that name is valid: alphanumeric
// and hyphens, 2-256 characters.
func validateWorkflowName(name string) error {
	if len(name) > 256 {
		panic("validateWorkflowName: name exceeds max length")
	}
	if len(name) < 2 {
		return fmt.Errorf(
			"workflow name must be at least 2 characters",
		)
	}

	if !namePattern.MatchString(name) {
		return fmt.Errorf(
			"workflow name must be alphanumeric and " +
				"hyphens only",
		)
	}
	return nil
}
