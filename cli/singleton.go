// cli/singleton.go
// Commands for managing singleton workflow locks. Provides
// visibility into active locks and an admin escape hatch
// to release stuck locks.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// singletonLock represents a lock entry from the KV bucket.
type singletonLock struct {
	Key   string `json:"key"`
	RunID string `json:"run_id"`
}

// runSingletonCmd dispatches singleton subcommands.
func runSingletonCmd(args []string) {
	if args == nil {
		panic("runSingletonCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runSingletonCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) || len(args) == 0 {
		printSingletonUsage()
		return
	}

	switch args[0] {
	case "list":
		runSingletonListCmd(args[1:])
	case "release":
		runSingletonReleaseCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr,
			"unknown singleton subcommand: %s\n", args[0])
		printSingletonUsage()
		os.Exit(1)
	}
}

// printSingletonUsage prints singleton subcommand help.
func printSingletonUsage() {
	fmt.Println(
		"Usage: dagnats singleton <command> [args]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  list       show active singleton locks")
	fmt.Println("  release    delete a singleton lock")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --workflow=X  filter by workflow name")
	fmt.Println("  --json        output as JSON")
}

// runSingletonListCmd lists active singleton locks.
func runSingletonListCmd(args []string) {
	if args == nil {
		panic("runSingletonListCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic("runSingletonListCmd: args exceeds max bound")
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	var workflowFilter string
	for _, arg := range args {
		if len(arg) > 256 {
			panic("runSingletonListCmd: arg exceeds max length")
		}
		if len(arg) > 11 && arg[:11] == "--workflow=" {
			workflowFilter = arg[11:]
		}
	}

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	locks := listSingletonLocks(js, workflowFilter)

	if jsonOutput {
		if err := FormatJSON(os.Stdout, locks); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if len(locks) == 0 {
		fmt.Println("No active singleton locks.")
		return
	}

	printSingletonTable(locks)
}

// listSingletonLocks reads all entries from singleton_locks KV.
func listSingletonLocks(
	js jetstream.JetStream, workflowFilter string,
) []singletonLock {
	if js == nil {
		panic("listSingletonLocks: js must not be nil")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, "singleton_locks")
	if err != nil {
		return nil
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil
	}

	const maxKeys = 10000
	locks := make([]singletonLock, 0, len(keys))
	for i, key := range keys {
		if i >= maxKeys {
			break
		}
		if workflowFilter != "" && !keyMatchesWorkflow(
			key, workflowFilter,
		) {
			continue
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			continue
		}
		lock := parseLockEntry(key, entry.Value())
		locks = append(locks, lock)
	}
	return locks
}

// keyMatchesWorkflow checks if a lock key belongs to a workflow.
// Keys are "{workflow}" or "{workflow}.{entityKey}".
func keyMatchesWorkflow(key, workflow string) bool {
	if key == "" {
		panic("keyMatchesWorkflow: key must not be empty")
	}
	if workflow == "" {
		panic(
			"keyMatchesWorkflow: workflow must not be empty",
		)
	}
	if key == workflow {
		return true
	}
	prefix := workflow + "."
	return len(key) > len(prefix) && key[:len(prefix)] == prefix
}

// parseLockEntry extracts run_id from lock JSON.
func parseLockEntry(key string, data []byte) singletonLock {
	if key == "" {
		panic("parseLockEntry: key must not be empty")
	}
	lock := singletonLock{Key: key}
	if data != nil {
		var v struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(data, &v); err == nil {
			lock.RunID = v.RunID
		}
	}
	return lock
}

// printSingletonTable displays locks as a formatted table.
func printSingletonTable(locks []singletonLock) {
	if len(locks) == 0 {
		panic("printSingletonTable: locks must not be empty")
	}
	const maxLocks = 10000
	if len(locks) > maxLocks {
		panic("printSingletonTable: locks exceeds max bound")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tRUN ID")
	for _, l := range locks {
		runID := l.RunID
		if len(runID) > 8 {
			runID = runID[:8]
		}
		fmt.Fprintf(w, "%s\t%s\n", l.Key, runID)
	}
	w.Flush()
}

// runSingletonReleaseCmd deletes a singleton lock by key.
func runSingletonReleaseCmd(args []string) {
	if args == nil {
		panic("runSingletonReleaseCmd: args must not be nil")
	}
	if len(args) > 100 {
		panic(
			"runSingletonReleaseCmd: args exceeds max bound",
		)
	}

	jsonOutput := HasJSONFlag(args)
	args = StripJSONFlag(args)

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr,
			"Usage: dagnats singleton release <key> [--json]")
		os.Exit(1)
	}

	key := args[0]
	if key == "" {
		panic(
			"runSingletonReleaseCmd: key must not be empty",
		)
	}
	if len(key) > 256 {
		fmt.Fprintln(os.Stderr,
			"error: key exceeds max length (256)")
		os.Exit(1)
	}

	_, nc := connectService()
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JetStream: %v\n", err)
		os.Exit(1)
	}

	released := releaseSingletonLock(js, key)

	if jsonOutput {
		result := map[string]any{
			"key":      key,
			"released": released,
		}
		if err := FormatJSON(os.Stdout, result); err != nil {
			fmt.Fprintf(os.Stderr, "format json: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if released {
		fmt.Printf("Released singleton lock: %s\n", key)
	} else {
		fmt.Printf("No lock found for key: %s\n", key)
	}
}

// releaseSingletonLock deletes a lock from singleton_locks KV.
func releaseSingletonLock(
	js jetstream.JetStream, key string,
) bool {
	if js == nil {
		panic("releaseSingletonLock: js must not be nil")
	}
	if key == "" {
		panic("releaseSingletonLock: key must not be empty")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	kv, err := js.KeyValue(ctx, "singleton_locks")
	if err != nil {
		return false
	}

	if err := kv.Delete(ctx, key); err != nil {
		return false
	}
	return true
}
