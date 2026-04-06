// server/dry_run.go
// Dry-run validation: loads config, reports sources, checks prerequisites.
// Validates environment without starting any components.
package server

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

const (
	maxValidationChecks = 20
	sourceDefault       = "default"
	sourceFile          = "from file"
	sourceEnv           = "from env"
)

// ConfigEntry holds a resolved config value and its source.
type ConfigEntry struct {
	Key    string
	Value  string
	Source string
}

// ValidationResult holds one check outcome.
type ValidationResult struct {
	Name   string
	Passed bool
	Detail string
}

// ResolvedConfig holds config entries with provenance tracking.
type ResolvedConfig struct {
	Config  Config
	Entries []ConfigEntry
}

// ResolveConfig loads config and tracks the source of each value.
// Returns resolved config with provenance for every key.
func ResolveConfig() ResolvedConfig {
	defaults := DefaultConfig()
	cfg := DefaultConfig()

	// Load config file if present
	cfgPath := "dagnats.yaml"
	if err := loadConfigFile(cfgPath, &cfg); err != nil {
		// Non-fatal: file may be missing
		_ = err // logged inside loadConfigFile
	}

	// Track which keys the config file changed vs defaults
	fileChanged := detectFileOverrides(defaults, cfg)

	// Snapshot after file, before env
	afterFile := cfg

	applyEnvOverrides(&cfg)

	entries := buildEntries(defaults, afterFile, cfg, fileChanged)
	return ResolvedConfig{Config: cfg, Entries: entries}
}

// buildEntries constructs provenance entries by comparing
// defaults, file-applied, and env-applied config values.
func buildEntries(
	defaults, afterFile, final Config,
	fileChanged map[string]bool,
) []ConfigEntry {
	if fileChanged == nil {
		panic("buildEntries: fileChanged is nil")
	}

	entries := make([]ConfigEntry, 0, 8)
	entries = appendEntry(entries, "data_dir",
		final.DataDir, sourceFor(
			defaults.DataDir, afterFile.DataDir,
			final.DataDir, fileChanged["data_dir"],
		))
	entries = appendEntry(entries, "http_addr",
		final.HTTPAddr, sourceFor(
			defaults.HTTPAddr, afterFile.HTTPAddr,
			final.HTTPAddr, fileChanged["http_addr"],
		))
	entries = appendEntry(entries, "nats_port",
		strconv.Itoa(final.NATSPort), sourceFor(
			strconv.Itoa(defaults.NATSPort),
			strconv.Itoa(afterFile.NATSPort),
			strconv.Itoa(final.NATSPort),
			fileChanged["nats_port"],
		))
	entries = appendEntry(entries, "otlp_endpoint",
		final.OTLPEndpoint, sourceFor(
			defaults.OTLPEndpoint, afterFile.OTLPEndpoint,
			final.OTLPEndpoint, fileChanged["otlp_endpoint"],
		))
	entries = appendEntry(entries, "workers",
		fmt.Sprintf("%d configured", len(final.Workers)),
		workersSource(defaults, afterFile, final),
	)
	return entries
}

// appendEntry adds a config entry to the slice.
func appendEntry(
	entries []ConfigEntry, key, value, source string,
) []ConfigEntry {
	if key == "" {
		panic("appendEntry: key is empty")
	}
	return append(entries, ConfigEntry{
		Key: key, Value: value, Source: source,
	})
}

// sourceFor determines where a config value originated
// by comparing default, after-file, and final values.
func sourceFor(
	defaultVal, afterFileVal, finalVal string,
	fromFile bool,
) string {
	if finalVal != afterFileVal {
		return sourceEnv
	}
	if fromFile {
		return sourceFile
	}
	return sourceDefault
}

// workersSource determines the source of worker configs.
func workersSource(
	defaults, afterFile, final Config,
) string {
	if len(final.Workers) != len(afterFile.Workers) {
		return sourceEnv
	}
	if len(afterFile.Workers) != len(defaults.Workers) {
		return sourceFile
	}
	return sourceDefault
}

// detectFileOverrides returns keys where file differs from defaults.
func detectFileOverrides(
	defaults, current Config,
) map[string]bool {
	changed := make(map[string]bool, 8)
	if current.DataDir != defaults.DataDir {
		changed["data_dir"] = true
	}
	if current.HTTPAddr != defaults.HTTPAddr {
		changed["http_addr"] = true
	}
	if current.NATSPort != defaults.NATSPort {
		changed["nats_port"] = true
	}
	if current.OTLPEndpoint != defaults.OTLPEndpoint {
		changed["otlp_endpoint"] = true
	}
	return changed
}

// DryRunValidate checks prerequisites without starting components.
// Returns validation results and true if all passed.
func DryRunValidate(cfg Config) ([]ValidationResult, bool) {
	if cfg.DataDir == "" {
		panic("DryRunValidate: DataDir is empty")
	}
	if cfg.NATSPort <= 0 {
		panic("DryRunValidate: NATSPort must be positive")
	}

	results := make([]ValidationResult, 0, maxValidationChecks)
	allPassed := true

	result := validateDataDir(cfg.DataDir)
	results = append(results, result)
	if !result.Passed {
		allPassed = false
	}

	result = validatePort("NATS", cfg.NATSPort)
	results = append(results, result)
	if !result.Passed {
		allPassed = false
	}

	result = validateHTTPAddr(cfg.HTTPAddr)
	results = append(results, result)
	if !result.Passed {
		allPassed = false
	}

	if cfg.LeafCredentials != "" {
		result = validateCredentials(cfg.LeafCredentials)
		results = append(results, result)
		if !result.Passed {
			allPassed = false
		}
	}

	if len(cfg.Workers) > 0 {
		result = validateWorkers(cfg.Workers)
		results = append(results, result)
		if !result.Passed {
			allPassed = false
		}
	}

	return results, allPassed
}

// validateDataDir checks the data directory is writable or creatable.
func validateDataDir(dir string) ValidationResult {
	if dir == "" {
		panic("validateDataDir: dir is empty")
	}

	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return ValidationResult{
				Name:   "data directory writable",
				Passed: false,
				Detail: "path exists but is not a directory",
			}
		}
		// Try writing a temp file to verify writability
		return checkDirWritable(dir)
	}

	// Directory doesn't exist; check parent exists
	if os.IsNotExist(err) {
		return checkParentExists(dir)
	}
	return ValidationResult{
		Name:   "data directory writable",
		Passed: false,
		Detail: err.Error(),
	}
}

// checkParentExists verifies the parent directory exists so
// MkdirAll could succeed during actual startup.
func checkParentExists(dir string) ValidationResult {
	if dir == "" {
		panic("checkParentExists: dir is empty")
	}

	parent := filepath.Dir(dir)
	info, err := os.Stat(parent)
	if err != nil {
		return ValidationResult{
			Name:   "data directory writable",
			Passed: false,
			Detail: "parent does not exist: " + parent,
		}
	}
	if !info.IsDir() {
		return ValidationResult{
			Name:   "data directory writable",
			Passed: false,
			Detail: "parent is not a directory: " + parent,
		}
	}
	return ValidationResult{
		Name:   "data directory writable",
		Passed: true,
		Detail: "directory does not exist, can be created",
	}
}

// checkDirWritable verifies a directory allows file creation.
func checkDirWritable(dir string) ValidationResult {
	if dir == "" {
		panic("checkDirWritable: dir is empty")
	}

	f, err := os.CreateTemp(dir, ".dagnats-check-*")
	if err != nil {
		return ValidationResult{
			Name:   "data directory writable",
			Passed: false,
			Detail: "not writable: " + err.Error(),
		}
	}
	name := f.Name()
	f.Close()
	os.Remove(name)

	return ValidationResult{
		Name:   "data directory writable",
		Passed: true,
	}
}

// validatePort checks if a TCP port is available by briefly listening.
func validatePort(label string, port int) ValidationResult {
	if label == "" {
		panic("validatePort: label is empty")
	}
	if port <= 0 || port > 65535 {
		panic("validatePort: port out of range")
	}

	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return ValidationResult{
			Name:   fmt.Sprintf("%s port %d available", label, port),
			Passed: false,
			Detail: err.Error(),
		}
	}
	ln.Close()

	return ValidationResult{
		Name:   fmt.Sprintf("%s port %d available", label, port),
		Passed: true,
	}
}

// validateHTTPAddr checks if the HTTP address is available.
func validateHTTPAddr(addr string) ValidationResult {
	if addr == "" {
		panic("validateHTTPAddr: addr is empty")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return ValidationResult{
			Name:   fmt.Sprintf("HTTP port %s available", addr),
			Passed: false,
			Detail: err.Error(),
		}
	}
	ln.Close()

	return ValidationResult{
		Name:   fmt.Sprintf("HTTP port %s available", addr),
		Passed: true,
	}
}

// validateCredentials checks that a credentials file exists.
func validateCredentials(path string) ValidationResult {
	if path == "" {
		panic("validateCredentials: path is empty")
	}

	_, err := os.Stat(path)
	if err != nil {
		return ValidationResult{
			Name:   "leaf credentials file exists",
			Passed: false,
			Detail: err.Error(),
		}
	}

	return ValidationResult{
		Name:   "leaf credentials file exists",
		Passed: true,
	}
}

// validateWorkers checks worker config consistency.
func validateWorkers(workers []WorkerConfig) ValidationResult {
	if len(workers) == 0 {
		panic("validateWorkers: workers is empty")
	}
	if len(workers) > maxWorkerConfigs {
		panic("validateWorkers: workers exceeds max bound")
	}

	err := validateWorkerConfigs(workers)
	if err != nil {
		return ValidationResult{
			Name:   "worker configs valid",
			Passed: false,
			Detail: err.Error(),
		}
	}

	return ValidationResult{
		Name:   "worker configs valid",
		Passed: true,
	}
}

// PrintDryRun writes the dry-run report to w.
// Returns true if all validations passed.
func PrintDryRun(w io.Writer, rc ResolvedConfig) bool {
	if w == nil {
		panic("PrintDryRun: writer is nil")
	}

	printConfigSummary(w, rc)

	results, allPassed := DryRunValidate(rc.Config)
	printValidationResults(w, results)

	if allPassed {
		fmt.Fprintln(w, "\nConfig OK")
	} else {
		fmt.Fprintln(w, "\nConfig INVALID")
	}

	return allPassed
}

// printConfigSummary writes the resolved config table.
func printConfigSummary(w io.Writer, rc ResolvedConfig) {
	if w == nil {
		panic("printConfigSummary: writer is nil")
	}

	cfgFile := "dagnats.yaml"
	if _, err := os.Stat(cfgFile); err != nil {
		cfgFile = "(none found)"
	}
	fmt.Fprintf(w, "Config source: %s\n\n", cfgFile)

	for _, e := range rc.Entries {
		fmt.Fprintf(w, "  %-16s %s", e.Key+":", e.Value)
		if e.Source != "" {
			fmt.Fprintf(w, "  (%s)", e.Source)
		}
		fmt.Fprintln(w)
	}
}

// printValidationResults writes check outcomes with marks.
func printValidationResults(
	w io.Writer, results []ValidationResult,
) {
	if w == nil {
		panic("printValidationResults: writer is nil")
	}
	if len(results) > maxValidationChecks {
		panic("printValidationResults: too many results")
	}

	fmt.Fprintln(w, "\nValidation:")
	for _, r := range results {
		mark := "\xe2\x9c\x93" // check mark
		if !r.Passed {
			mark = "\xe2\x9c\x97" // X mark
		}
		fmt.Fprintf(w, "  %s %s", mark, r.Name)
		if r.Detail != "" && !r.Passed {
			fmt.Fprintf(w, ": %s", r.Detail)
		}
		fmt.Fprintln(w)
	}
}
