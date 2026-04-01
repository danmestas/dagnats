// cli/color.go
// ANSI color helpers for CLI output using the Gruvbox palette.
// Respects the NO_COLOR environment variable — when set, all functions
// return the input string unmodified.
package cli

import "os"

// Gruvbox 24-bit ANSI escape sequences, matching server/banner.go palette.
const (
	colorRed    = "\033[38;2;204;36;29m"  // #cc241d
	colorGreen  = "\033[38;2;142;192;124m" // #8ec07c
	colorYellow = "\033[38;2;254;128;25m"  // #fe8019
	colorGray   = "\033[38;2;146;131;116m" // #928374
	colorBold   = "\033[1m"
	colorReset  = "\033[0m"
)

// colorEnabled returns false when the NO_COLOR environment variable
// is set to any non-empty value.
func colorEnabled() bool {
	return os.Getenv("NO_COLOR") == ""
}

// colorize wraps s in the given ANSI code and a reset suffix.
// Returns s unmodified when color is disabled.
func colorize(code, s string) string {
	if code == "" {
		panic("colorize: code must not be empty")
	}
	if s == "" {
		return s
	}
	if !colorEnabled() {
		return s
	}
	return code + s + colorReset
}

// ColorRed applies Gruvbox red to s.
func ColorRed(s string) string { return colorize(colorRed, s) }

// ColorGreen applies Gruvbox green/aqua to s.
func ColorGreen(s string) string { return colorize(colorGreen, s) }

// ColorYellow applies Gruvbox orange/yellow to s.
func ColorYellow(s string) string { return colorize(colorYellow, s) }

// ColorGray applies Gruvbox gray to s.
func ColorGray(s string) string { return colorize(colorGray, s) }

// ColorBold applies bold formatting to s.
func ColorBold(s string) string { return colorize(colorBold, s) }

// ColorStatus maps a run or step status string to the appropriate
// color: completed/skipped=green, failed=red, running/queued=yellow,
// pending/cancelled=gray.
func ColorStatus(status string) string {
	if status == "" {
		panic("ColorStatus: status must not be empty")
	}
	switch status {
	case "completed", "skipped":
		return ColorGreen(status)
	case "failed":
		return ColorRed(status)
	case "running", "queued":
		return ColorYellow(status)
	case "pending", "cancelled":
		return ColorGray(status)
	default:
		return status
	}
}
