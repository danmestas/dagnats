// cli/dev.go
// Development watch mode: builds and restarts a Go project on file
// changes. Connects to NATS to verify the server is running before
// entering the watch loop.
package cli

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// colorCyan is the Gruvbox-inspired cyan used for [dev] prefix.
const colorCyan = "\033[38;2;131;165;152m" // #83a598

// devPrefix returns the "[dev]" label, colored cyan when color
// is enabled.
func devPrefix() string {
	if !colorEnabled() {
		return "[dev]"
	}
	return colorCyan + "[dev]" + colorReset
}

// runDevCmd is the entry point for `dagnats dev`.
func runDevCmd(args []string) {
	if args == nil {
		panic("runDevCmd: args must not be nil")
	}
	if HasHelpFlag(args) {
		printDevHelp()
		return
	}
	dir, delay := parseDevFlags(args)
	checkNATSReachable()
	watcher := initWatcher(dir, delay)
	runner := newDevRunner(dir)
	runner.ensureGitignore()
	initialBuildAndStart(runner)
	runWatchLoop(runner, watcher, delay)
}

// parseDevFlags extracts --dir and --delay from args.
func parseDevFlags(args []string) (string, time.Duration) {
	if args == nil {
		panic("parseDevFlags: args must not be nil")
	}
	fs := flag.NewFlagSet("dev", flag.ExitOnError)
	dir := fs.String("dir", ".", "Project directory")
	delayMs := fs.Int(
		"delay", 500, "Poll delay in milliseconds",
	)
	fs.Parse(args)
	delay := time.Duration(*delayMs) * time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	return *dir, delay
}

// checkNATSReachable verifies that NATS is available. Exits
// with code 1 and a helpful message if connection fails.
func checkNATSReachable() {
	natsURL := GetEnvWithFallback(
		"DAGNATS_NATS_URL", "NATS_URL", nats.DefaultURL,
	)
	nc, err := nats.Connect(
		natsURL, nats.Timeout(2*time.Second),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s Cannot connect to NATS at %s\n",
			devPrefix(), ColorRed("error:"), natsURL,
		)
		fmt.Fprintf(os.Stderr,
			"%s Hint: run 'dagnats serve' first\n",
			devPrefix(),
		)
		exitFunc(1)
		return
	}
	nc.Close()
}

// initWatcher creates and snapshots a file watcher. Exits
// with code 1 if no .go files are found.
func initWatcher(
	dir string, delay time.Duration,
) *fileWatcher {
	if dir == "" {
		panic("initWatcher: dir must not be empty")
	}
	watcher := newFileWatcher(dir, delay)
	if err := watcher.snapshot(); err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(), ColorRed("error:"), err,
		)
		exitFunc(1)
		return nil
	}
	if watcher.fileCount() == 0 {
		fmt.Fprintf(os.Stderr,
			"%s %s no .go files found in %s\n",
			devPrefix(), ColorRed("error:"), dir,
		)
		exitFunc(1)
		return nil
	}
	fmt.Fprintf(os.Stdout,
		"%s watching %d files in %s\n",
		devPrefix(), watcher.fileCount(), dir,
	)
	return watcher
}

// initialBuildAndStart performs the first build+start cycle.
// Exits with code 1 if the initial build fails.
func initialBuildAndStart(runner *devRunner) {
	if runner == nil {
		panic("initialBuildAndStart: runner must not be nil")
	}
	fmt.Fprintf(os.Stdout,
		"%s %s\n",
		devPrefix(), ColorYellow("building..."),
	)
	if err := runner.build(); err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(), ColorRed("build failed:"), err,
		)
		exitFunc(1)
		return
	}
	if err := runner.start(); err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(), ColorRed("start failed:"), err,
		)
		exitFunc(1)
		return
	}
	fmt.Fprintf(os.Stdout,
		"%s %s\n",
		devPrefix(), ColorGreen("started"),
	)
}

// runWatchLoop polls for file changes and rebuilds on
// detection. Handles SIGINT/SIGTERM for clean shutdown.
func runWatchLoop(
	runner *devRunner,
	watcher *fileWatcher,
	delay time.Duration,
) {
	if runner == nil {
		panic("runWatchLoop: runner must not be nil")
	}
	if watcher == nil {
		panic("runWatchLoop: watcher must not be nil")
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	const debounceDelay = 200 * time.Millisecond

	for {
		select {
		case <-sigCh:
			fmt.Fprintf(os.Stdout,
				"\n%s shutting down...\n", devPrefix(),
			)
			runner.cleanup()
			return
		case <-ticker.C:
			rebuildIfChanged(runner, watcher, debounceDelay)
		}
	}
}

// rebuildIfChanged checks for changes and triggers a rebuild.
// Keeps the old process running if the new build fails.
func rebuildIfChanged(
	runner *devRunner,
	watcher *fileWatcher,
	debounce time.Duration,
) {
	if runner == nil {
		panic("rebuildIfChanged: runner must not be nil")
	}
	if watcher == nil {
		panic("rebuildIfChanged: watcher must not be nil")
	}
	changed, err := watcher.poll()
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(), ColorRed("poll error:"), err,
		)
		return
	}
	if !changed {
		return
	}
	time.Sleep(debounce)
	fmt.Fprintf(os.Stdout,
		"%s %s\n",
		devPrefix(), ColorYellow("change detected, rebuilding..."),
	)
	if err := runner.build(); err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(),
			ColorRed("build failed (keeping old process):"),
			err,
		)
		return
	}
	runner.stop()
	if err := runner.start(); err != nil {
		fmt.Fprintf(os.Stderr,
			"%s %s %v\n",
			devPrefix(), ColorRed("restart failed:"), err,
		)
		return
	}
	fmt.Fprintf(os.Stdout,
		"%s %s\n",
		devPrefix(), ColorGreen("restarted"),
	)
}

// printDevHelp prints help text for the dev command.
func printDevHelp() {
	fmt.Println("Usage: dagnats dev [--dir=DIR] [--delay=MS]")
	fmt.Println()
	fmt.Println("Watch mode: builds and restarts on .go file changes.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --dir    project directory (default \".\")")
	fmt.Println("  --delay  poll interval in ms (default 500)")
}
