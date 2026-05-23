// cli/trigger_type.go
// `dagnats trigger-type` command — observes the `trigger_types` KV
// bucket (parent #273 Phase 2.5, #343). Pure read-only — workers
// register trigger types via the worker.RegisterTriggerType SDK
// method (#337). OWNER_STATUS is computed as set-membership against
// worker.NewDirectory(js).List() (already filters via
// MaxWorkerStaleness=60s, per #289 — no separate heartbeats bucket
// exists, audit-locked in the #343 body).
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// triggerTypesBucket is the KV bucket name for trigger type defs.
// Duplicated here as a literal rather than imported so the CLI does
// not depend on internal/trigger constants — the bucket name is the
// contract across the worker SDK, engine, and CLI.
const triggerTypesBucket = "trigger_types"

// ownerStatusLive marks trigger types whose OwnerWorkerID appears in
// the live workers set returned by Directory.List().
const ownerStatusLive = "live"

// ownerStatusStale marks trigger types whose OwnerWorkerID is absent
// from the live workers set (the owning worker has not refreshed its
// directory entry within MaxWorkerStaleness).
const ownerStatusStale = "stale"

// runTriggerTypeCmd dispatches trigger-type subcommands.
func runTriggerTypeCmd(args []string) {
	if args == nil {
		panic("runTriggerTypeCmd: args must not be nil")
	}
	if HasHelpFlag(args) || len(args) == 0 {
		printTriggerTypeUsage()
		return
	}
	switch args[0] {
	case "list":
		runTriggerTypeListCmd(args[1:])
	case "describe":
		runTriggerTypeDescribeCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr,
			"unknown trigger-type subcommand: %s\n", args[0])
		printTriggerTypeUsage()
		exitFunc(1)
	}
}

// printTriggerTypeUsage prints the usage for the trigger-type command.
func printTriggerTypeUsage() {
	fmt.Println("Usage: dagnats trigger-type <command> [--json]")
	fmt.Println("Commands:")
	fmt.Println(
		"  list             list registered trigger types")
	fmt.Println(
		"  describe <name>  show full TriggerTypeDef")
}

// runTriggerTypeListCmd reads the trigger_types KV and prints each
// entry. Empty bucket prints "No trigger types registered." — empty
// is not an error.
func runTriggerTypeListCmd(args []string) {
	if args == nil {
		panic("runTriggerTypeListCmd: args must not be nil")
	}
	jsonOutput := HasJSONFlag(args)

	js, closeFn, ok := openJetStream()
	if !ok {
		return
	}
	defer closeFn()

	defs, err := listTriggerTypes(js)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"list trigger types: %v\n", err)
		exitFunc(1)
		return
	}

	liveIDs, err := liveWorkerIDs(js)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"list workers: %v\n", err)
		exitFunc(1)
		return
	}

	if jsonOutput {
		rendered := renderTriggerTypesJSON(defs, liveIDs)
		if err := FormatJSON(os.Stdout, rendered); err != nil {
			fmt.Fprintf(os.Stderr,
				"format json: %v\n", err)
			exitFunc(1)
			return
		}
		return
	}

	if len(defs) == 0 {
		fmt.Println("No trigger types registered.")
		return
	}

	printTriggerTypesTable(os.Stdout, defs, liveIDs)
}

// runTriggerTypeDescribeCmd reads one trigger type by name and
// pretty-prints every field. Unknown name → exit 1 with a clear
// error.
func runTriggerTypeDescribeCmd(args []string) {
	if args == nil {
		panic("runTriggerTypeDescribeCmd: args must not be nil")
	}
	jsonOutput := HasJSONFlag(args)
	name := firstPositional(args)
	if name == "" {
		fmt.Fprintln(os.Stderr,
			"Error: trigger-type describe requires a name")
		printTriggerTypeUsage()
		exitFunc(1)
		return
	}

	js, closeFn, ok := openJetStream()
	if !ok {
		return
	}
	defer closeFn()

	def, err := getTriggerType(js, name)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: trigger type %q not found: %v\n", name, err)
		exitFunc(1)
		return
	}

	if jsonOutput {
		if err := FormatJSON(os.Stdout, def); err != nil {
			fmt.Fprintf(os.Stderr,
				"format json: %v\n", err)
			exitFunc(1)
			return
		}
		return
	}

	printTriggerTypeDetail(os.Stdout, def)
}

// openJetStream connects to NATS and returns a JetStream context.
// Returns (js, closeFn, true) on success, or (nil, nil, false) after
// calling exitFunc(1). closeFn is always safe to call when ok is
// true.
func openJetStream() (jetstream.JetStream, func(), bool) {
	nc, err := connectNATS()
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Error: cannot connect to NATS: %v\n", err)
		exitFunc(1)
		return nil, nil, false
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		fmt.Fprintf(os.Stderr,
			"Error: jetstream init: %v\n", err)
		exitFunc(1)
		return nil, nil, false
	}
	return js, func() { nc.Close() }, true
}

// firstPositional returns the first non-flag arg, or "" if there is
// none. Flags begin with "-" by convention.
func firstPositional(args []string) string {
	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("firstPositional: args exceeds max bound")
	}
	for _, a := range args {
		if len(a) > 0 && a[0] != '-' {
			return a
		}
	}
	return ""
}

// listTriggerTypes reads every entry from the `trigger_types` KV
// bucket. Returns an empty slice when nothing is registered — the
// bucket is provisioned by SetupAll, so absence of keys is the
// boot state, not an error.
func listTriggerTypes(
	js jetstream.JetStream,
) ([]trigger.TriggerTypeDef, error) {
	if js == nil {
		panic("listTriggerTypes: js must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, triggerTypesBucket)
	if err != nil {
		return nil, fmt.Errorf(
			"%s KV bind: %w", triggerTypesBucket, err)
	}

	keys, err := kv.ListKeys(ctx)
	if err != nil {
		if err == jetstream.ErrNoKeysFound {
			return []trigger.TriggerTypeDef{}, nil
		}
		return nil, err
	}

	defs := make([]trigger.TriggerTypeDef, 0, 16)
	const maxDefs = 10000
	count := 0
	for key := range keys.Keys() {
		if count >= maxDefs {
			break
		}
		count++
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var def trigger.TriggerTypeDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// getTriggerType reads a single trigger type by name. A missing
// entry returns jetstream.ErrKeyNotFound verbatim so callers can
// surface a clear not-found message.
func getTriggerType(
	js jetstream.JetStream, name string,
) (trigger.TriggerTypeDef, error) {
	if js == nil {
		panic("getTriggerType: js must not be nil")
	}
	if name == "" {
		panic("getTriggerType: name must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, triggerTypesBucket)
	if err != nil {
		return trigger.TriggerTypeDef{}, fmt.Errorf(
			"%s KV bind: %w", triggerTypesBucket, err)
	}
	entry, err := kv.Get(ctx, name)
	if err != nil {
		return trigger.TriggerTypeDef{}, err
	}
	var def trigger.TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return trigger.TriggerTypeDef{}, fmt.Errorf(
			"decode trigger type %q: %w", name, err)
	}
	return def, nil
}

// liveWorkerIDs returns the set of WorkerIDs from
// worker.NewDirectory(js).List(). Directory.List already filters via
// MaxWorkerStaleness, so any ID present in the returned map is live
// per the heartbeat contract (#289). Returns an empty set when no
// workers are registered.
func liveWorkerIDs(
	js jetstream.JetStream,
) (map[string]struct{}, error) {
	if js == nil {
		panic("liveWorkerIDs: js must not be nil")
	}
	dir := worker.NewDirectory(js)
	regs, err := dir.List()
	if err != nil {
		return nil, err
	}
	const maxWorkers = 100000
	if len(regs) > maxWorkers {
		panic("liveWorkerIDs: workers exceeds max bound")
	}
	live := make(map[string]struct{}, len(regs))
	for _, r := range regs {
		live[r.WorkerID] = struct{}{}
	}
	return live, nil
}

// ownerStatus returns ownerStatusLive when ownerID is in liveIDs,
// otherwise ownerStatusStale. An empty ownerID is treated as stale
// — a trigger type with no owner cannot be live by definition.
func ownerStatus(
	ownerID string, liveIDs map[string]struct{},
) string {
	if ownerID == "" {
		return ownerStatusStale
	}
	if _, ok := liveIDs[ownerID]; ok {
		return ownerStatusLive
	}
	return ownerStatusStale
}

// printTriggerTypesTable renders the table to w. Panics on empty
// input — callers check len() and print "No trigger types
// registered." instead; an empty table would be a UX regression.
func printTriggerTypesTable(
	w io.Writer,
	defs []trigger.TriggerTypeDef,
	liveIDs map[string]struct{},
) {
	if w == nil {
		panic("printTriggerTypesTable: writer must not be nil")
	}
	if len(defs) == 0 {
		panic("printTriggerTypesTable: defs must not be empty")
	}
	const maxRows = 100000
	if len(defs) > maxRows {
		panic("printTriggerTypesTable: defs exceeds max bound")
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw,
		"NAME\tOWNER_WORKER_ID\tVERSION\tOWNER_STATUS")
	for _, d := range defs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			d.Name,
			d.OwnerWorkerID,
			d.Version,
			ownerStatus(d.OwnerWorkerID, liveIDs),
		)
	}
	if err := tw.Flush(); err != nil {
		// Flush only fails when the underlying writer fails; surface
		// it rather than swallow so a broken stdout is visible.
		fmt.Fprintf(os.Stderr, "tabwriter flush: %v\n", err)
	}
}

// triggerTypeListJSON is the JSON-mode row shape — the raw
// TriggerTypeDef plus the computed OWNER_STATUS so machine
// consumers don't have to re-do the set-membership check.
type triggerTypeListJSON struct {
	trigger.TriggerTypeDef
	OwnerStatus string `json:"owner_status"`
}

// renderTriggerTypesJSON wraps each def with its computed
// OwnerStatus for JSON output. Returns an empty slice (not nil)
// when defs is empty so the JSON output is `[]` rather than `null`.
func renderTriggerTypesJSON(
	defs []trigger.TriggerTypeDef,
	liveIDs map[string]struct{},
) []triggerTypeListJSON {
	const maxRows = 100000
	if len(defs) > maxRows {
		panic("renderTriggerTypesJSON: defs exceeds max bound")
	}
	out := make([]triggerTypeListJSON, 0, len(defs))
	for _, d := range defs {
		out = append(out, triggerTypeListJSON{
			TriggerTypeDef: d,
			OwnerStatus:    ownerStatus(d.OwnerWorkerID, liveIDs),
		})
	}
	return out
}

// printTriggerTypeDetail pretty-prints a single TriggerTypeDef.
// Schemas are re-indented via json.Indent so a single-line stored
// schema is readable on stdout without altering the canonical bytes
// in KV.
func printTriggerTypeDetail(
	w io.Writer, def trigger.TriggerTypeDef,
) {
	if w == nil {
		panic("printTriggerTypeDetail: writer must not be nil")
	}
	fmt.Fprintf(w, "Name:            %s\n", def.Name)
	fmt.Fprintf(w, "Owner Worker ID: %s\n", def.OwnerWorkerID)
	fmt.Fprintf(w, "Version:         %s\n", def.Version)
	fmt.Fprintf(w, "Description:     %s\n", def.Description)
	fmt.Fprintf(w, "Registered At:   %s\n",
		def.RegisteredAt.UTC().Format(time.RFC3339))
	fmt.Fprintln(w, "Config Schema:")
	fmt.Fprintln(w, indentJSONForDisplay(def.ConfigSchema))
	fmt.Fprintln(w, "Payload Schema:")
	fmt.Fprintln(w, indentJSONForDisplay(def.PayloadSchema))
}

// indentJSONForDisplay re-indents raw JSON bytes for human display.
// An empty input returns a sentinel "(none)" so the operator can
// tell apart a missing schema from a syntactically empty one;
// invalid input falls back to the verbatim bytes prefixed with
// "(invalid)" so we never silently hide what's in the bucket.
func indentJSONForDisplay(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "  (none)"
	}
	const maxLen = 1 << 20
	if len(raw) > maxLen {
		return "  (schema exceeds display bound)"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "  ", "  "); err != nil {
		return "  (invalid: " + err.Error() + ")\n  " + string(raw)
	}
	// json.Indent leaves the first line without the prefix — prepend
	// once so every line aligns under the heading.
	return "  " + buf.String()
}
