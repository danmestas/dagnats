package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/danmestas/dagnats/server"
)

func runServeCmd(args []string) {
	if HasHelpFlag(args) {
		printServeHelp()
		return
	}

	if hasDryRunFlag(args) {
		runServeDryRun()
		return
	}

	configPath := extractConfigFlag(args)

	cfg, loadedPath, err := server.ConfigWithPath(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if err := applyServeFlagOverrides(args, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	printConfigSource(os.Stderr, loadedPath)

	// Phase 4 / ADR-018: surface the resolved config file path so
	// Server.Run can drive a configfile.Watcher against the same
	// dagnats.yaml. Empty when no file was found — Run treats that
	// as "no file-managed declarative section, skip the watcher".
	cfg.ConfigFilePath = loadedPath

	srv := server.New(cfg)

	if len(cfg.Workers) > 0 {
		w := server.EmbeddedWorker(srv)
		for _, wc := range cfg.Workers {
			w.Handle(wc.Task, buildHandler(wc))
		}
	}

	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// printServeHelp prints usage for the serve command.
func printServeHelp() {
	fmt.Println("Usage: dagnats serve [--config=PATH]" +
		" [--dry-run] [--nats-ws-port=N]" +
		" [--nats-ws-no-tls]")
	fmt.Println("Starts embedded NATS server with" +
		" DagNats engine and API.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --config=PATH       path to config file")
	fmt.Println("  --dry-run           validate config" +
		" without starting the server")
	fmt.Println("  --nats-ws-port=N    NATS WebSocket port" +
		" for browser clients (0 = off, default)")
	fmt.Println("  --nats-ws-no-tls    run the WebSocket" +
		" listener without TLS (dev only; ADR-020)")
	fmt.Println()
	fmt.Println("Config search order (when --config" +
		" not specified):")
	fmt.Println("  1. ./dagnats.yaml")
	fmt.Println("  2. $XDG_CONFIG_HOME/dagnats/" +
		"dagnats.yaml")
	fmt.Println("  3. /etc/dagnats/dagnats.yaml" +
		" (Linux only)")
	fmt.Println()
	fmt.Println("Env: DAGNATS_DATA_DIR," +
		" DAGNATS_HTTP_ADDR, DAGNATS_NATS_PORT")
	fmt.Println()
	fmt.Println("Workflows + triggers in the same dagnats.yaml are")
	fmt.Println("hot-reloaded on file edit (ADR-018).")
	fmt.Println()
	fmt.Println("Run 'dagnats config show'" +
		" to see effective configuration.")
}

// applyServeFlagOverrides applies CLI flags that override the
// resolved server.Config. Only flags introduced for the embedded
// WebSocket listener are handled here today (see ADR-020); other
// configuration still flows through file/env. Returns an error
// on malformed flag values so the operator sees a clear message
// instead of a silent default.
func applyServeFlagOverrides(
	args []string, cfg *server.Config,
) error {
	if args == nil {
		panic("applyServeFlagOverrides: args must not be nil")
	}
	if cfg == nil {
		panic("applyServeFlagOverrides: cfg must not be nil")
	}
	if len(args) > 1000 {
		panic("applyServeFlagOverrides: args exceeds max bound")
	}

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--nats-ws-port="):
			val := strings.TrimPrefix(arg, "--nats-ws-port=")
			port, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf(
					"--nats-ws-port: %w", err)
			}
			cfg.NATSWebsocketPort = port
		case arg == "--nats-ws-no-tls":
			cfg.NATSWebsocketNoTLS = true
		case strings.HasPrefix(arg, "--nats-ws-no-tls="):
			val := strings.TrimPrefix(arg, "--nats-ws-no-tls=")
			switch strings.ToLower(val) {
			case "1", "true", "yes", "on":
				cfg.NATSWebsocketNoTLS = true
			case "0", "false", "no", "off":
				cfg.NATSWebsocketNoTLS = false
			default:
				return fmt.Errorf(
					"--nats-ws-no-tls: invalid %q", val)
			}
		}
	}
	return nil
}

// hasDryRunFlag checks if --dry-run is present in args.
func hasDryRunFlag(args []string) bool {
	if args == nil {
		panic("hasDryRunFlag: args is nil")
	}
	if len(args) > 1000 {
		panic("hasDryRunFlag: args exceeds max bound")
	}
	for _, a := range args {
		if a == "--dry-run" {
			return true
		}
	}
	return false
}

// runServeDryRun validates config and prints a report.
// Exits 0 on success, 1 on validation failure.
func runServeDryRun() {
	rc := server.ResolveConfig()
	passed := server.PrintDryRun(os.Stdout, rc)
	if !passed {
		os.Exit(1)
	}
}
