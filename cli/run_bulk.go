// cli/run_bulk.go
// Bulk run CLI command. Starts multiple runs with different inputs.
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/danmestas/dagnats/internal/api"
)

func runBulkCmd(args []string) {
	if args == nil {
		panic("runBulkCmd: args must not be nil")
	}
	fs := flag.NewFlagSet("bulk", flag.ExitOnError)
	workflow := fs.String("workflow", "", "workflow ID (required)")
	fromFile := fs.String("from-file", "", "JSONL file")
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}
	if *workflow == "" {
		fmt.Fprintln(os.Stderr, "--workflow is required")
		fs.Usage()
		os.Exit(1)
	}
	inputs := collectBulkInputs(fs.Args(), *fromFile)
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr,
			"no inputs: use positional args or --from-file")
		os.Exit(1)
	}

	svc, nc := connectService()
	defer nc.Close()
	resp, err := svc.BulkStartRuns(
		context.Background(),
		api.BulkRunRequest{
			WorkflowID: *workflow, Inputs: inputs,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bulk run: %v\n", err)
		os.Exit(1)
	}
	if *jsonOut {
		FormatJSON(os.Stdout, resp)
		return
	}
	fmt.Printf("Started %d runs:\n", resp.Total)
	for _, id := range resp.RunIDs {
		fmt.Printf("  %s\n", id)
	}
}

func collectBulkInputs(
	positional []string, fromFile string,
) []json.RawMessage {
	var inputs []json.RawMessage
	for _, arg := range positional {
		inputs = append(inputs, json.RawMessage(arg))
	}
	if fromFile != "" {
		inputs = append(inputs, readJSONLFile(fromFile)...)
	}
	return inputs
}

func readJSONLFile(path string) []json.RawMessage {
	if path == "" {
		panic("readJSONLFile: path must not be empty")
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", path, err)
		os.Exit(1)
	}
	defer f.Close()
	var inputs []json.RawMessage
	scanner := bufio.NewScanner(f)
	const maxLine = 1024 * 1024
	scanner.Buffer(make([]byte, maxLine), maxLine)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		inputs = append(inputs,
			json.RawMessage(append([]byte{}, line...)),
		)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
		os.Exit(1)
	}
	return inputs
}
