// cli/sidecar.go
// Sidecar command: manages the local observability sidecar that runs
// an OTel Collector, otlp2parquet writer, and DuckDB MCP server as
// supervised child processes.
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/danmestas/dagnats/sidecar"
)

const (
	sidecarMaxArgs        = 100
	defaultConfigFileName = "dagnats.yaml"
)

// runSidecarCmd dispatches sidecar subcommands.
func runSidecarCmd(args []string) {
	if args == nil {
		panic("runSidecarCmd: args must not be nil")
	}
	if len(args) > sidecarMaxArgs {
		panic("runSidecarCmd: args exceeds max bound")
	}
	if HasHelpFlag(args) {
		printSidecarUsage()
		return
	}
	if len(args) == 0 {
		runSidecarStartCmd(args)
		return
	}
	switch args[0] {
	case "start":
		runSidecarStartCmd(args[1:])
	case "install":
		runSidecarInstallCmd(args[1:])
	case "status":
		runSidecarStatusCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr,
			"unknown sidecar command: %s\n", args[0])
		printSidecarUsage()
		exitFunc(1)
	}
}

// printSidecarUsage prints help for sidecar subcommands.
func printSidecarUsage() {
	fmt.Println(
		"Usage: dagnats sidecar [command] [flags]",
	)
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println(
		"  start    start the sidecar (default)",
	)
	fmt.Println(
		"  install  install/update external binaries",
	)
	fmt.Println(
		"  status   check if required binaries are available",
	)
	fmt.Println()
	fmt.Println("Start flags:")
	fmt.Println(
		"  --config=PATH  config file (default: dagnats.yaml)",
	)
}

// runSidecarStartCmd loads config and runs the supervisor.
func runSidecarStartCmd(args []string) {
	if args == nil {
		panic("runSidecarStartCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		printSidecarUsage()
		return
	}

	configPath := extractConfigFlag(args)
	if configPath == "" {
		configPath = defaultConfigFileName
	}
	cfg := loadSidecarConfig(configPath)
	ensureStorageDir(cfg)
	writeCollectorYAML(cfg)
	checkBinariesAvailable()
	startSupervisor(cfg)
}

// loadSidecarConfig reads and validates the config file.
func loadSidecarConfig(path string) *sidecar.SidecarConfig {
	if path == "" {
		panic("loadSidecarConfig: path must not be empty")
	}

	cfg, err := sidecar.LoadConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"error: load config: %v\n", err)
		exitFunc(1)
		return nil
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: invalid config: %v\n", err)
		exitFunc(1)
		return nil
	}

	return cfg
}

// ensureStorageDir creates the storage directory if needed.
func ensureStorageDir(cfg *sidecar.SidecarConfig) {
	if cfg == nil {
		panic("ensureStorageDir: cfg must not be nil")
	}

	dir := cfg.Storage.LocalPath
	if dir == "" {
		return
	}

	const dirPerms = 0o755
	if err := os.MkdirAll(dir, dirPerms); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: create storage dir: %v\n", err)
		exitFunc(1)
	}
}

// writeCollectorYAML generates the collector config file.
func writeCollectorYAML(cfg *sidecar.SidecarConfig) {
	if cfg == nil {
		panic("writeCollectorYAML: cfg must not be nil")
	}

	path := collectorYAMLPath(cfg)
	dir := filepath.Dir(path)

	const dirPerms = 0o755
	if err := os.MkdirAll(dir, dirPerms); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: create config dir: %v\n", err)
		exitFunc(1)
		return
	}

	if err := sidecar.WriteCollectorConfig(cfg, path); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: write collector config: %v\n", err)
		exitFunc(1)
	}
}

// collectorYAMLPath returns where to write the generated config.
func collectorYAMLPath(
	cfg *sidecar.SidecarConfig,
) string {
	if cfg == nil {
		panic("collectorYAMLPath: cfg must not be nil")
	}
	return cfg.Storage.LocalPath + "/otelcol-config.yaml"
}

// checkBinariesAvailable verifies all required binaries exist.
func checkBinariesAvailable() {
	required := []string{"otelcol", "otlp2parquet", "dagnats-mcp-duckdb"}
	missing := findMissingBinaries(required)

	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr,
			"error: missing binaries: %s\n",
			strings.Join(missing, ", "))
		fmt.Fprintf(os.Stderr,
			"hint: run 'dagnats sidecar install' first\n")
		exitFunc(1)
	}
}

// findMissingBinaries returns names that cannot be found.
func findMissingBinaries(names []string) []string {
	if names == nil {
		panic("findMissingBinaries: names must not be nil")
	}
	if len(names) > 20 {
		panic("findMissingBinaries: names exceeds max bound")
	}

	missing := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := sidecar.FindBinary(name); err != nil {
			missing = append(missing, name)
		}
	}
	return missing
}

// startSupervisor creates the supervisor, prints the banner,
// runs until signal, and prints the shutdown message.
func startSupervisor(cfg *sidecar.SidecarConfig) {
	if cfg == nil {
		panic("startSupervisor: cfg must not be nil")
	}

	sup, err := sidecar.NewSupervisor(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"error: create supervisor: %v\n", err)
		exitFunc(1)
		return
	}

	printStartBanner(cfg)

	if err := sup.Run(); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: supervisor: %v\n", err)
		exitFunc(1)
		return
	}

	fmt.Println("Sidecar stopped.")
}

// printStartBanner displays startup info to the user.
func printStartBanner(cfg *sidecar.SidecarConfig) {
	if cfg == nil {
		panic("printStartBanner: cfg must not be nil")
	}

	mcpTransport := "stdio"
	if cfg.MCP.Listen != "" {
		mcpTransport = cfg.MCP.Listen
	}

	exportAddr := strings.Replace(cfg.Listen, "0.0.0.0", "localhost", 1)

	fmt.Println("Sidecar started:")
	fmt.Printf("  Collector:    http://%s (OTLP/HTTP)\n",
		cfg.Listen)
	fmt.Printf("  Export:       export OTEL_EXPORTER_OTLP_ENDPOINT=http://%s\n",
		exportAddr)
	fmt.Printf("  Storage:      %s (%s)\n",
		cfg.Storage.Type, cfg.Storage.LocalPath)
	fmt.Printf("  DuckDB MCP:   %s\n", mcpTransport)
	if cfg.Backend != nil {
		fmt.Printf("  Backend:      %s (forwarding)\n", cfg.Backend.Endpoint)
	}
}

// runSidecarInstallCmd installs required external binaries.
func runSidecarInstallCmd(args []string) {
	if args == nil {
		panic("runSidecarInstallCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats sidecar install",
		)
		fmt.Println()
		fmt.Println(
			"Installs otelcol, otlp2parquet, and dagnats-mcp-duckdb to ~/.dagnats/bin/.",
		)
		return
	}

	if err := sidecar.InstallAll(os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr,
			"error: install: %v\n", err)
		exitFunc(1)
		return
	}

	fmt.Println("All binaries installed.")
}

// runSidecarStatusCmd checks binary availability and prints.
func runSidecarStatusCmd(args []string) {
	if args == nil {
		panic("runSidecarStatusCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		fmt.Println(
			"Usage: dagnats sidecar status",
		)
		fmt.Println()
		fmt.Println(
			"Checks if required binaries are available.",
		)
		return
	}

	names := []string{"otelcol", "otlp2parquet", "dagnats-mcp-duckdb"}
	allFound := true

	for _, name := range names {
		path, err := sidecar.FindBinary(name)
		if err != nil {
			fmt.Printf("  %-16s not found\n", name)
			allFound = false
			continue
		}
		fmt.Printf("  %-16s %s\n", name, path)
	}

	if !allFound {
		fmt.Println()
		fmt.Println(
			"hint: run 'dagnats sidecar install' " +
				"to download missing binaries",
		)
	}
}
