// cli/json.go
// Shared JSON output helpers for the --json flag. Every command that
// supports machine-readable output delegates to these functions for
// consistent flag detection, stripping, and formatting.
package cli

import (
	"encoding/json"
	"io"
)

// HasJSONFlag returns true when args contains the exact string "--json".
// Nil-safe: returns false for nil args.
func HasJSONFlag(args []string) bool {
	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("HasJSONFlag: args exceeds max bound")
	}

	for _, arg := range args {
		if arg == "--json" {
			return true
		}
	}
	return false
}

// StripJSONFlag returns a copy of args with all "--json" entries removed.
// Panics on nil args — callers must provide a valid slice.
func StripJSONFlag(args []string) []string {
	if args == nil {
		panic("StripJSONFlag: args must not be nil")
	}

	const maxArgs = 1000
	if len(args) > maxArgs {
		panic("StripJSONFlag: args exceeds max bound")
	}

	result := make([]string, 0, len(args))
	for _, arg := range args {
		if arg != "--json" {
			result = append(result, arg)
		}
	}
	return result
}

// FormatJSON marshals v as indented JSON and writes it to w with a
// trailing newline. Returns an error if v cannot be marshaled.
// Panics on nil writer.
func FormatJSON(w io.Writer, v any) error {
	if w == nil {
		panic("FormatJSON: writer must not be nil")
	}
	if v == nil {
		panic("FormatJSON: value must not be nil")
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}
