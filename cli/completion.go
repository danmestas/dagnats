// cli/completion.go
// Shell completion generation and dynamic completion handling.
// Generates bash/zsh scripts that delegate to `dagnats __complete`
// for both static and dynamic completions (workflow names, run IDs).
package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

// topLevelCommands lists every top-level command for static completion.
var topLevelCommands = []string{
	"clean", "completion", "config", "demo", "dev", "dlq", "init",
	"logs", "metrics", "observe", "run", "serve", "sidecar",
	"singleton", "status", "trace", "trigger", "workers",
	"workflow",
}

// subcommandMap maps parent commands to their subcommands.
var subcommandMap = map[string][]string{
	"run": {
		"bulk", "cancel", "cancel-all", "events",
		"inspect", "list", "output", "retry",
		"retry-all", "signal", "start", "status", "watch",
	},
	"workflow": {
		"list", "register", "show", "validate",
	},
	"trigger": {
		"create", "delete", "disable", "enable",
		"history", "list", "test", "update",
	},
	"dlq": {
		"list", "replay", "watch",
	},
	"singleton": {
		"list", "release",
	},
	"completion": {
		"bash", "zsh",
	},
	"demo": {
		"seed",
	},
}

// flagMap maps "command" or "command.subcommand" to available flags.
var flagMap = map[string][]string{
	"run.start":  {"--at=", "--json", "--output", "--watch"},
	"run.status": {"--json", "--last"},
	"run.cancel": {"--json", "--last"},
	"run.signal": {"--json", "--last"},
	"run.list": {
		"--json", "--limit=", "--scheduled",
		"--status=", "--workflow=",
	},
	"run.events": {
		"--full", "--json", "--last",
		"--step=", "--type=",
	},
	"run.inspect":       {"--json", "--last", "--trace"},
	"run.watch":         {"--json", "--last"},
	"run.output":        {"--json", "--last"},
	"run.retry":         {"--json", "--last"},
	"run.cancel-all":    {"--json", "--workflow="},
	"run.retry-all":     {"--json", "--workflow="},
	"run.bulk":          {"--json"},
	"workflow.list":     {"--json"},
	"workflow.register": {"--json"},
	"workflow.show":     {"--json"},
	"workflow.validate": {},
	"trigger.create": {
		"--backfill", "--cron=", "--json",
		"--secret=", "--subject=", "--tz=", "--webhook=",
	},
	"trigger.list":    {"--json"},
	"trigger.update":  {"--json"},
	"trigger.delete":  {"--json"},
	"trigger.enable":  {"--json"},
	"trigger.disable": {"--json"},
	"trigger.test":    {},
	"trigger.history": {"--json"},
	"dlq.list":        {"--json", "--limit=", "--run="},
	"dlq.replay":      {"--json", "--run="},
	"dlq.watch":       {"--json"},
	"demo.seed": {
		"--count=", "--include-failed", "--json", "--timeout=",
	},
}

// dynamicCompletionCommands identifies argument positions that need
// dynamic completion from NATS. Key format: "command.subcommand".
var dynamicCompletionCommands = map[string]string{
	"run.start":      "workflows",
	"run.status":     "runs",
	"run.inspect":    "runs",
	"workflow.show":  "workflows",
	"trigger.enable": "triggers",
}

// completionTimeoutMax is the NATS connection deadline for dynamic
// completions. Completions must be fast or users perceive lag.
const completionTimeoutMax = 500 * time.Millisecond

// runCompletionCmd dispatches the "completion" subcommand to
// generate shell-specific completion scripts.
func runCompletionCmd(args []string) {
	if args == nil {
		panic("runCompletionCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runCompletionCmd: args exceeds max bound")
	}

	if len(args) == 0 || HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats completion <bash|zsh>")
		fmt.Println(
			"\nGenerate shell completion scripts.")
		fmt.Println(
			"\nAdd to your shell profile:")
		fmt.Println(
			`  eval "$(dagnats completion bash)"`)
		fmt.Println(
			`  eval "$(dagnats completion zsh)"`)
		return
	}

	switch args[0] {
	case "bash":
		generateBashCompletion()
	case "zsh":
		generateZshCompletion()
	default:
		fmt.Fprintf(os.Stderr,
			"unknown shell: %s (supported: bash, zsh)\n",
			args[0])
		os.Exit(1)
	}
}

// generateBashCompletion writes a bash completion script to stdout.
// The script defines a _dagnats_completions function that delegates
// to `dagnats __complete` for all completion logic.
func generateBashCompletion() {
	if bashCompletionScript == "" {
		panic("generateBashCompletion: script is empty")
	}
	fmt.Print(bashCompletionScript)
}

// generateZshCompletion writes a zsh completion script to stdout.
// The script defines a _dagnats function that delegates to
// `dagnats __complete` for all completion logic.
func generateZshCompletion() {
	if zshCompletionScript == "" {
		panic("generateZshCompletion: script is empty")
	}
	fmt.Print(zshCompletionScript)
}

// handleCompleteCmd processes the hidden __complete command. It
// parses the partial command line from args and outputs one
// completion candidate per line. Returns silently on errors
// (shell completion convention).
func handleCompleteCmd(args []string) {
	if args == nil {
		panic("handleCompleteCmd: args must not be nil")
	}
	if len(args) > 1000 {
		panic("handleCompleteCmd: args exceeds max bound")
	}

	// Strip the "--" separator if present.
	cleaned := stripSeparator(args)

	// Strip "dagnats" from the front if present (the shell passes
	// the full COMP_WORDS including the program name).
	if len(cleaned) > 0 && cleaned[0] == "dagnats" {
		cleaned = cleaned[1:]
	}

	completions := resolveCompletions(cleaned)
	printCompletions(completions)
}

// stripSeparator removes a leading "--" from the args slice.
// Shells pass `dagnats __complete -- <words>` and the "--"
// is just a separator.
func stripSeparator(args []string) []string {
	if args == nil {
		panic("stripSeparator: args must not be nil")
	}
	if len(args) > 1000 {
		panic("stripSeparator: args exceeds max bound")
	}

	if len(args) > 0 && args[0] == "--" {
		return args[1:]
	}
	return args
}

// resolveCompletions determines the appropriate completions for
// the given partial command line words.
func resolveCompletions(words []string) []string {
	if words == nil {
		panic("resolveCompletions: words must not be nil")
	}
	if len(words) > 1000 {
		panic("resolveCompletions: words exceeds max bound")
	}

	// No words yet: complete top-level commands.
	if len(words) == 0 {
		return topLevelCommands
	}

	command := words[0]
	remaining := words[1:]

	// Typing a partial top-level command: filter matches.
	if len(remaining) == 0 {
		return filterPrefix(topLevelCommands, command)
	}

	return resolveSubcompletions(command, remaining)
}

// resolveSubcompletions handles completion after the top-level
// command has been identified.
func resolveSubcompletions(
	command string, remaining []string,
) []string {
	if command == "" {
		panic(
			"resolveSubcompletions: command must not be empty",
		)
	}
	if remaining == nil {
		panic(
			"resolveSubcompletions: remaining must not be nil",
		)
	}

	subs, hasSubs := subcommandMap[command]
	if !hasSubs {
		return nil
	}

	subcommand := remaining[0]
	afterSub := remaining[1:]

	// Typing partial subcommand: filter matches.
	if len(afterSub) == 0 {
		return filterPrefix(subs, subcommand)
	}

	// After subcommand: check for flag or dynamic completion.
	return resolveArgumentCompletions(
		command, subcommand, afterSub,
	)
}

// resolveArgumentCompletions returns flag or dynamic completions
// for the argument position after a known subcommand.
func resolveArgumentCompletions(
	command, subcommand string, afterSub []string,
) []string {
	if command == "" {
		panic(
			"resolveArgumentCompletions: " +
				"command must not be empty",
		)
	}
	if subcommand == "" {
		panic(
			"resolveArgumentCompletions: " +
				"subcommand must not be empty",
		)
	}

	current := afterSub[len(afterSub)-1]
	key := command + "." + subcommand

	// If typing a flag, complete flags.
	if strings.HasPrefix(current, "-") {
		flags := flagMap[key]
		return filterPrefix(flags, current)
	}

	// Check for dynamic completion at this position.
	return fetchDynamicCompletions(key, current)
}

// fetchDynamicCompletions connects to NATS and retrieves
// completion candidates (workflow names, run IDs, etc.).
// Returns nil silently on any error.
func fetchDynamicCompletions(
	key, prefix string,
) []string {
	if key == "" {
		panic(
			"fetchDynamicCompletions: key must not be empty",
		)
	}
	// prefix may be empty (user just pressed TAB)

	completionType, needsDynamic := dynamicCompletionCommands[key]
	if !needsDynamic {
		return nil
	}

	switch completionType {
	case "workflows":
		return fetchWorkflowNames(prefix)
	case "runs":
		return fetchRunIDs(prefix)
	default:
		return nil
	}
}

// fetchWorkflowNames lists workflow names from NATS KV. Returns
// nil on any error (silent failure for completions).
func fetchWorkflowNames(prefix string) []string {
	if len(prefix) > 256 {
		return nil
	}
	svc, nc := connectForCompletion()
	if svc == nil {
		return nil
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), completionTimeoutMax,
	)
	defer cancel()

	defs, err := svc.ListWorkflows(ctx)
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(defs))
	const maxResults = 50
	for i, def := range defs {
		if i >= maxResults {
			break
		}
		if prefix == "" ||
			strings.HasPrefix(def.Name, prefix) {
			names = append(names, def.Name)
		}
	}
	sort.Strings(names)
	return names
}

// fetchRunIDs lists recent run ID prefixes from NATS KV. Returns
// nil on any error (silent failure for completions).
func fetchRunIDs(prefix string) []string {
	if len(prefix) > 256 {
		return nil
	}
	svc, nc := connectForCompletion()
	if svc == nil {
		return nil
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), completionTimeoutMax,
	)
	defer cancel()

	runs, err := svc.ListRuns(ctx, "")
	if err != nil {
		return nil
	}

	ids := make([]string, 0, len(runs))
	const maxResults = 20
	for i, run := range runs {
		if i >= maxResults {
			break
		}
		if prefix == "" ||
			strings.HasPrefix(run.RunID, prefix) {
			ids = append(ids, run.RunID)
		}
	}
	return ids
}

// connectForCompletion attempts a best-effort NATS connection
// with a short timeout. Returns nil service and nil conn if
// connection fails. Caller must close nc when svc is non-nil.
func connectForCompletion() (
	*api.Service, *nats.Conn,
) {
	natsURL := GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	nc, err := nats.Connect(
		natsURL,
		nats.Timeout(completionTimeoutMax),
	)
	if err != nil {
		return nil, nil
	}
	svc, errMsg := tryNewService(nc)
	if errMsg != "" {
		nc.Close()
		return nil, nil
	}
	return svc, nc
}

// filterPrefix returns all candidates that start with the given
// prefix. Returns the full list when prefix is empty.
func filterPrefix(
	candidates []string, prefix string,
) []string {
	if candidates == nil {
		panic("filterPrefix: candidates must not be nil")
	}
	const maxCandidates = 1000
	if len(candidates) > maxCandidates {
		panic("filterPrefix: candidates exceeds max bound")
	}

	if prefix == "" {
		return candidates
	}

	matched := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if strings.HasPrefix(c, prefix) {
			matched = append(matched, c)
		}
	}
	return matched
}

// printCompletions writes one completion per line to stdout.
func printCompletions(completions []string) {
	const maxCompletions = 1000
	for i, c := range completions {
		if i >= maxCompletions {
			break
		}
		fmt.Println(c)
	}
}

// bashCompletionScript is the static bash completion script.
// It delegates all completion logic to `dagnats __complete`.
const bashCompletionScript = `_dagnats_completions() {
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local completions
    completions=$(dagnats __complete -- "${COMP_WORDS[@]}" 2>/dev/null)
    if [ $? -eq 0 ]; then
        COMPREPLY=($(compgen -W "${completions}" -- "${cur}"))
    fi
}
complete -F _dagnats_completions dagnats
`

// zshCompletionScript is the static zsh completion script.
// It delegates all completion logic to `dagnats __complete`.
const zshCompletionScript = `_dagnats() {
    local -a completions
    completions=("${(@f)$(dagnats __complete -- "${words[@]}" 2>/dev/null)}")
    if [ ${#completions[@]} -gt 0 ]; then
        compadd -- "${completions[@]}"
    fi
}
compdef _dagnats dagnats
`
