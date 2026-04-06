// cli/config.go
// Commands for viewing effective server configuration.
package cli

import (
	"fmt"
	"os"
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
}
