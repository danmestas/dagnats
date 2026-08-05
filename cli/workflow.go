// cli/workflow.go
// Commands for managing workflow definitions: list, register.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// workflowFile is the on-disk format for `workflow register`. It
// embeds dag.WorkflowDef and adds an optional `triggers` array so a
// workflow JSON can declare its cron/subject/webhook triggers in one
// place. The dag package stays free of trigger types — extracting
// triggers here lets us call svc.CreateTrigger after a successful
// workflow register without polluting the core domain (issue #171).
type workflowFile struct {
	dag.WorkflowDef
	Triggers []trigger.TriggerDef `json:"triggers,omitempty"`
}

// runWorkflowCmd dispatches workflow subcommands.
func runWorkflowCmd(args []string) {
	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats workflow <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list       list registered workflows")
		fmt.Println("  register   register a workflow from a JSON file")
		fmt.Println("  show       show details of a registered workflow")
		fmt.Println("  validate   validate a workflow JSON file")
		fmt.Println("  delete     delete a registered workflow")
		return
	}
	if len(args) == 0 {
		fmt.Println("Usage: dagnats workflow <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  list       list registered workflows")
		fmt.Println("  register   register a workflow from a JSON file")
		fmt.Println("  show       show details of a registered workflow")
		fmt.Println("  validate   validate a workflow JSON file")
		fmt.Println("  delete     delete a registered workflow")
		return
	}
	switch args[0] {
	case "list":
		runWorkflowListCmd(args[1:])
	case "register":
		runWorkflowRegisterCmd(args[1:])
	case "show":
		runWorkflowShowCmd(args[1:])
	case "validate":
		runWorkflowValidateCmd(args[1:])
	case "delete":
		runWorkflowDeleteCmd(args[1:])
	default:
		fmt.Printf("unknown workflow subcommand: %s\n", args[0])
	}
}

// runWorkflowListCmd retrieves and prints all registered workflows.
func runWorkflowListCmd(args []string) {
	jsonOutput := HasJSONFlag(args)

	svc, nc := connectService()
	defer nc.Close()

	defs, err := svc.ListWorkflows(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "list workflows: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, defs); err != nil {
			fmt.Fprintf(
				os.Stderr, "format json: %v\n", err,
			)
			os.Exit(1)
		}
		return
	}

	if len(defs) == 0 {
		fmt.Println("No workflows registered.")
		return
	}

	printWorkflowListTable(defs)
}

// printWorkflowListTable renders workflows as a human-readable table.
func printWorkflowListTable(defs []dag.WorkflowDef) {
	if len(defs) == 0 {
		panic("printWorkflowListTable: defs must not be empty")
	}
	if len(defs) > 100000 {
		panic("printWorkflowListTable: defs exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tSTEPS\tTIMEOUT")

	for _, def := range defs {
		timeout := "none"
		if def.Timeout > 0 {
			timeout = def.Timeout.String()
		}
		fmt.Fprintf(
			w, "%s\t%d\t%s\n",
			def.Name, len(def.Steps), timeout,
		)
	}

	w.Flush()
}

// workflowRegisterResult is the JSON output for workflow register.
type workflowRegisterResult struct {
	Name     string   `json:"name"`
	Action   string   `json:"action"`
	Steps    int      `json:"steps"`
	Warnings []string `json:"warnings,omitempty"`
}

// runWorkflowRegisterCmd reads a workflow definition file and
// registers it.
func runWorkflowRegisterCmd(args []string) {
	jsonOutput := HasJSONFlag(args)
	if jsonOutput {
		args = StripJSONFlag(args)
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats workflow register <file> [--json]")
		os.Exit(1)
	}
	filePath := args[0]
	if filePath == "" {
		panic(
			"runWorkflowRegisterCmd: filePath must not be empty",
		)
	}

	wf := readWorkflowFile(filePath)
	def := wf.WorkflowDef

	svc, nc := connectService()
	defer nc.Close()

	// Check whether this workflow already exists to distinguish
	// create from update in user feedback.
	_, getErr := svc.GetWorkflow(def.Name)
	isUpdate := getErr == nil

	err := registerWorkflowWithTriggers(
		context.Background(), svc, wf,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	action := "created"
	if isUpdate {
		action = "updated"
	}
	warnings := checkMissingWorkers(nc, def)
	printRegisterResult(jsonOutput, def, action, warnings)
}

// printRegisterResult formats and writes the post-register output for
// `workflow register`. Splits text and JSON modes. Exits non-zero on
// JSON-encoding failure (matches the prior inline behavior).
func printRegisterResult(
	jsonOutput bool,
	def dag.WorkflowDef,
	action string,
	warnings []string,
) {
	if action == "" {
		panic("printRegisterResult: action must not be empty")
	}
	if def.Name == "" {
		panic("printRegisterResult: def.Name must not be empty")
	}
	if jsonOutput {
		result := workflowRegisterResult{
			Name:     def.Name,
			Action:   action,
			Steps:    len(def.Steps),
			Warnings: warnings,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}
	fmt.Printf("Workflow %s: %s (%d steps)\n",
		action, def.Name, len(def.Steps))
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr,
			"Warning: no active worker for task %q\n", w)
	}
	printHint(false,
		"Hint: start a run with:",
		fmt.Sprintf("  dagnats run start %s '{}' --watch", def.Name),
	)
}

// readWorkflowFile reads and parses a workflow JSON file as a
// workflowFile (workflow def + optional triggers). Exits on error
// since this is a CLI helper.
func readWorkflowFile(filePath string) workflowFile {
	if filePath == "" {
		panic("readWorkflowFile: filePath must not be empty")
	}
	if len(filePath) > 4096 {
		panic("readWorkflowFile: filePath unreasonably long")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}

	wf, perr := parseWorkflowFile(data)
	if perr != nil {
		fmt.Fprintf(os.Stderr, "parse workflow: %v\n", perr)
		os.Exit(1)
	}
	return wf
}

// parseWorkflowFile parses workflow JSON bytes into a workflowFile.
// Pure (no I/O, no exit) so tests can drive it directly.
func parseWorkflowFile(data []byte) (workflowFile, error) {
	if len(data) == 0 {
		return workflowFile{}, fmt.Errorf("workflow file is empty")
	}
	var wf workflowFile
	if err := json.Unmarshal(data, &wf); err != nil {
		return workflowFile{}, err
	}
	if err := rejectUnknownTopLevelFields(data); err != nil {
		return workflowFile{}, err
	}
	return wf, nil
}

// rejectUnknownTopLevelFields fails closed when the workflow file
// carries a top-level key the parser doesn't recognize. Go's default
// json.Unmarshal silently drops such keys, which let a workflow declare
// its trigger under a singular `trigger` object (instead of the
// `triggers` array) and register with the trigger thrown away (#607).
//
// WHY the map-diff instead of json.Decoder.DisallowUnknownFields: the
// standard decoder applies its strictness recursively into every nested
// struct, so it would also reject a legitimately-new field inside a step
// or trigger config that an older CLI simply hasn't learned yet. That
// forward-compatibility risk is real for nested structures but not for
// the outermost envelope, whose small schema IS the parser's contract
// with file authors. Diffing top-level keys against the struct tags
// scopes the check to exactly that envelope and no deeper.
func rejectUnknownTopLevelFields(data []byte) error {
	if len(data) == 0 {
		panic("rejectUnknownTopLevelFields: data must not be empty")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		// A non-object top level is caught by the struct unmarshal in
		// the caller; nothing to diff here.
		return nil
	}
	known := knownWorkflowFileKeys()
	unknown := make([]string, 0)
	for key := range raw {
		// Mirror encoding/json's case-insensitive field matching so we
		// flag exactly the keys it would have silently dropped.
		if _, ok := known[strings.ToLower(key)]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf(
		"unrecognized workflow field(s): %s "+
			"(check for a typo or a singular \"trigger\" that "+
			"should be the \"triggers\" array)",
		strings.Join(unknown, ", "),
	)
}

// knownWorkflowFileKeys returns the lowercased set of JSON keys valid at
// the top level of a workflow file. It reflects over workflowFile so the
// set never drifts from the struct: the embedded dag.WorkflowDef
// contributes its promoted keys and workflowFile adds "triggers".
func knownWorkflowFileKeys() map[string]struct{} {
	keys := make(map[string]struct{}, 16)
	root := reflect.TypeOf(workflowFile{})
	if root.Kind() != reflect.Struct {
		panic("knownWorkflowFileKeys: workflowFile must be a struct")
	}
	// Explicit stack (no recursion) walks embedded structs whose fields
	// JSON promotes to the top level.
	stack := []reflect.Type{root}
	const maxTypes = 100
	for len(stack) > 0 {
		if len(stack) > maxTypes {
			panic("knownWorkflowFileKeys: type stack exceeds bound")
		}
		typ := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		stack = collectTopLevelKeys(typ, keys, stack)
	}
	if len(keys) == 0 {
		panic("knownWorkflowFileKeys: no keys collected")
	}
	return keys
}

// collectTopLevelKeys adds typ's promoted JSON keys to keys and returns
// stack extended with any embedded structs still to visit. An anonymous
// struct field with no json tag is promoted (its fields become top-level
// keys); anything else contributes its own key name.
func collectTopLevelKeys(
	typ reflect.Type, keys map[string]struct{}, stack []reflect.Type,
) []reflect.Type {
	if keys == nil {
		panic("collectTopLevelKeys: keys must not be nil")
	}
	if typ.Kind() != reflect.Struct {
		panic("collectTopLevelKeys: typ must be a struct")
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue // unexported — json ignores it
		}
		if field.Anonymous &&
			field.Type.Kind() == reflect.Struct &&
			field.Tag.Get("json") == "" {
			stack = append(stack, field.Type)
			continue
		}
		name := jsonFieldName(field)
		if name == "" || name == "-" {
			continue
		}
		keys[strings.ToLower(name)] = struct{}{}
	}
	return stack
}

// jsonFieldName resolves the wire key for a struct field: the json tag
// name, or the Go field name when the tag is absent or names only
// options (e.g. `json:",omitempty"`).
func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name := tag
	if idx := strings.IndexByte(tag, ','); idx >= 0 {
		name = tag[:idx]
	}
	if name == "" {
		return field.Name
	}
	return name
}

// validateEmbeddedTriggers auto-fills each embedded trigger's
// workflow_id from the parent workflow's name, rejects explicit
// mismatches (a copy-paste error trap), and runs trigger.Validate.
// Mutates wf.Triggers in place so callers can write them after a
// successful validation. Shared by `workflow register` (#171) and
// `workflow validate` (#180) so both gates apply the same rules.
func validateEmbeddedTriggers(wf *workflowFile) error {
	if wf == nil {
		panic("validateEmbeddedTriggers: wf must not be nil")
	}
	if wf.WorkflowDef.Name == "" {
		panic("validateEmbeddedTriggers: parent workflow name empty")
	}
	for i := range wf.Triggers {
		got := wf.Triggers[i].WorkflowID
		if got != "" && got != wf.WorkflowDef.Name {
			return fmt.Errorf("trigger %q: workflow_id %q "+
				"does not match parent workflow %q",
				wf.Triggers[i].ID, got, wf.WorkflowDef.Name)
		}
		if got == "" {
			wf.Triggers[i].WorkflowID = wf.WorkflowDef.Name
		}
		if err := trigger.Validate(wf.Triggers[i]); err != nil {
			return fmt.Errorf("trigger %q: %w",
				wf.Triggers[i].ID, err)
		}
	}
	return nil
}

// registerWorkflowWithTriggers validates triggers up-front, registers
// the workflow definition, then creates each trigger. Atomicity: a
// validation failure prevents any KV write, so an invalid embedded
// trigger leaves no partial state behind. Idempotency: re-running on
// the same file updates both workflow and triggers in place (KV puts
// are last-write-wins, keyed on workflow Name and trigger ID).
func registerWorkflowWithTriggers(
	ctx context.Context, svc *api.Service, wf workflowFile,
) error {
	if ctx == nil {
		panic("registerWorkflowWithTriggers: ctx must not be nil")
	}
	if svc == nil {
		panic("registerWorkflowWithTriggers: svc must not be nil")
	}
	if err := validateEmbeddedTriggers(&wf); err != nil {
		return err
	}
	if err := svc.RegisterWorkflow(
		ctx, wf.WorkflowDef,
	); err != nil {
		return fmt.Errorf("register workflow: %w", err)
	}
	for _, t := range wf.Triggers {
		if err := svc.CreateTrigger(ctx, t); err != nil {
			return fmt.Errorf("create trigger %q: %w", t.ID, err)
		}
	}
	return nil
}

// checkMissingWorkers queries JetStream for active task consumers
// and returns task types with no worker. Silently returns nil on
// JetStream errors — this is best-effort advisory only.
func checkMissingWorkers(
	nc *nats.Conn, def dag.WorkflowDef,
) []string {
	if nc == nil {
		panic("checkMissingWorkers: nc must not be nil")
	}
	if len(def.Steps) == 0 {
		panic("checkMissingWorkers: def must have steps")
	}

	js, jsErr := jetstream.New(nc)
	if jsErr != nil {
		return nil
	}
	return api.CheckTaskConsumers(js, def)
}
