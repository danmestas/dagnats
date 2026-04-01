// cli/color.go
// ANSI color utilities for CLI output. Respects the NO_COLOR env var
// (see https://no-color.org/) so tests and piped output stay clean.
package cli

import "os"

// colorEnabled returns false when the NO_COLOR env var is set,
// disabling all ANSI escape sequences for accessibility and testing.
func colorEnabled() bool {
	_, set := os.LookupEnv("NO_COLOR")
	return !set
}

// ColorRed wraps s in ANSI red (31) if color is enabled.
func ColorRed(s string) string {
	if s == "" {
		panic("ColorRed: input must not be empty")
	}
	if !colorEnabled() {
		return s
	}
	return "\033[31m" + s + "\033[0m"
}

// ColorGreen wraps s in ANSI green (32) if color is enabled.
func ColorGreen(s string) string {
	if s == "" {
		panic("ColorGreen: input must not be empty")
	}
	if !colorEnabled() {
		return s
	}
	return "\033[32m" + s + "\033[0m"
}

// ColorYellow wraps s in ANSI yellow (33) if color is enabled.
func ColorYellow(s string) string {
	if s == "" {
		panic("ColorYellow: input must not be empty")
	}
	if !colorEnabled() {
		return s
	}
	return "\033[33m" + s + "\033[0m"
}

// ColorGray wraps s in ANSI bright black / gray (90) if color is enabled.
func ColorGray(s string) string {
	if s == "" {
		panic("ColorGray: input must not be empty")
	}
	if !colorEnabled() {
		return s
	}
	return "\033[90m" + s + "\033[0m"
}

// ColorStatus maps a workflow or step status string to the
// appropriate color: green for completed, red for failed,
// yellow for running/queued, gray for pending/cancelled/skipped.
func ColorStatus(status string) string {
	if status == "" {
		panic("ColorStatus: status must not be empty")
	}
	if !colorEnabled() {
		return status
	}
	switch status {
	case "completed":
		return ColorGreen(status)
	case "failed":
		return ColorRed(status)
	case "running", "queued":
		return ColorYellow(status)
	case "pending", "cancelled", "skipped":
		return ColorGray(status)
	default:
		return status
	}
}
