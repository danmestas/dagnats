// cli/config.go
// Commands for viewing effective server configuration.
package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/danmestas/dagnats/server"
)

// runConfigCmd dispatches config subcommands.
func runConfigCmd(args []string) {
	if args == nil {
		panic("runConfigCmd: args must not be nil")
	}
	if len(args) > 1000 {
		panic("runConfigCmd: args exceeds max bound")
	}

	if HasHelpFlag(args) {
		fmt.Println("Usage: dagnats config <command> [--json]")
		fmt.Println("Commands:")
		fmt.Println("  show    show effective configuration")
		return
	}
	if len(args) == 0 {
		// Default to show when no subcommand given
		runConfigShowCmd([]string{})
		return
	}
	switch args[0] {
	case "show":
		runConfigShowCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr,
			"unknown config subcommand: %s\n", args[0])
	}
}

// runConfigShowCmd prints the resolved configuration.
func runConfigShowCmd(args []string) {
	if args == nil {
		panic("runConfigShowCmd: args must not be nil")
	}
	if len(args) > 1000 {
		panic("runConfigShowCmd: args exceeds max bound")
	}

	configPath := extractConfigFlag(args)
	cfg, loadedPath, err := server.ConfigWithPath(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if cfg.DataDir == "" {
		panic("runConfigShowCmd: DataDir resolved to empty")
	}

	if HasJSONFlag(args) {
		if err := FormatJSON(os.Stdout, cfg); err != nil {
			fmt.Fprintf(os.Stderr,
				"json format error: %v\n", err)
		}
		return
	}

	printConfigShowOutput(cfg, loadedPath)
}

// printConfigShowOutput renders the human-readable config display.
func printConfigShowOutput(cfg server.Config, loadedPath string) {
	if cfg.DataDir == "" {
		panic("printConfigShowOutput: DataDir is empty")
	}

	sourceDisplay := "(defaults)"
	if loadedPath != "" {
		sourceDisplay = loadedPath
	}

	remotesDisplay := "(none)"
	if len(cfg.LeafRemotes) > 0 {
		remotesDisplay = strings.Join(
			cfg.LeafRemotes, ", ",
		)
	}

	credsDisplay := "(none)"
	if cfg.LeafCredentials != "" {
		credsDisplay = cfg.LeafCredentials
	}
	monitorDisplay := "(disabled)"
	if cfg.MonitorPort > 0 {
		monitorDisplay = fmt.Sprintf(
			":%d", cfg.MonitorPort,
		)
	}

	fmt.Printf("config_file:      %s\n", sourceDisplay)
	fmt.Printf("data_dir:         %s\n", cfg.DataDir)
	fmt.Printf("http_addr:        %s\n", cfg.HTTPAddr)
	fmt.Printf("nats_port:        %d\n", cfg.NATSPort)
	fmt.Printf("monitor_port:     %s\n", monitorDisplay)
	fmt.Printf("leaf_remotes:     %s\n", remotesDisplay)
	fmt.Printf("leaf_credentials: %s\n", credsDisplay)
	fmt.Printf("max_store_bytes:  %d\n",
		cfg.MaxStoreBytes)
	printConsoleEnvVars()
}

// consoleEnvVars enumerates every console-relevant env var dagnats
// honours at startup. The slice is the single source of truth for
// `dagnats config show` and for docs/console.md. Each entry carries
// the operator-facing description and a default applied when unset.
// Ordering: console-facing first, then metrics-facing.
var consoleEnvVars = []envVarRow{
	{Name: "DAGNATS_HTTP_ADDR",
		Default: "127.0.0.1:8080",
		Desc:    "HTTP listener for /console + /api + /metrics."},
	{Name: "DAGNATS_CONSOLE_PASSWORD", Sensitive: true,
		Default: "(unset → basic-auth disabled)",
		Desc:    "Basic-auth password for the console."},
	{Name: "DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH",
		Default: "false",
		Desc: "When true, trust X-Forwarded-User from " +
			"upstream proxies."},
	{Name: "CONSOLE_READ_ONLY",
		Default: "false",
		Desc: "When true, every console mutation returns 405 " +
			"with a canonical body."},
	{Name: "CONSOLE_CSRF_SECRET", Sensitive: true,
		Default: "(random per-restart)",
		Desc: "HMAC secret for CSRF tokens. Set this in " +
			"production so tokens survive restarts."},
	{Name: "METRICS_AUTH",
		Default: "loopback",
		Desc: "Gate mode for /metrics: loopback | basic | " +
			"forward | none."},
	{Name: "METRICS_BASIC_USER",
		Default: "(unset)",
		Desc:    "Username for METRICS_AUTH=basic."},
	{Name: "METRICS_BASIC_PASS", Sensitive: true,
		Default: "(unset)",
		Desc:    "Password for METRICS_AUTH=basic."},
}

// envVarRow is one row in the env-var table.
type envVarRow struct {
	Name      string
	Default   string
	Desc      string
	Sensitive bool
}

// printConsoleEnvVars renders the console-relevant env-var block as
// a stable table sorted by name. Operators can grep "CONSOLE_" or
// "METRICS_" without scanning the whole output.
func printConsoleEnvVars() {
	if len(consoleEnvVars) > 200 {
		panic("printConsoleEnvVars: too many env vars")
	}
	rows := make([]envVarRow, len(consoleEnvVars))
	copy(rows, consoleEnvVars)
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Name < rows[j].Name
	})
	fmt.Printf("\nconsole env vars:\n")
	for _, row := range rows {
		fmt.Printf("  %-38s %s\n", row.Name, renderEnvVarValue(row))
	}
}

// renderEnvVarValue formats one env var's resolved display value.
// Sensitive vars print "(set)" or the default placeholder so secrets
// never leak into a terminal that's screen-shared or logged.
func renderEnvVarValue(row envVarRow) string {
	if row.Name == "" {
		panic("renderEnvVarValue: row.Name is empty")
	}
	if row.Default == "" {
		panic("renderEnvVarValue: row.Default is empty")
	}
	raw := os.Getenv(row.Name)
	switch {
	case raw == "":
		return fmt.Sprintf("%s (default)", row.Default)
	case row.Sensitive:
		return "(set)"
	default:
		return raw
	}
}
