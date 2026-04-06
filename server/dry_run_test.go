// server/dry_run_test.go
// Methodology: Unit tests for dry-run validation. Each test exercises one
// validation check with both positive (pass) and negative (fail) assertions.
// No NATS or external dependencies needed.
package server

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDryRunValidate_ValidConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		HTTPAddr:      ":0",
		NATSPort:      0,
		MaxStoreBytes: 1 << 20,
	}

	// Use an ephemeral port to avoid conflicts
	cfg.NATSPort = findFreePort(t)
	cfg.HTTPAddr = findFreeAddr(t)

	results, allPassed := DryRunValidate(cfg)

	// Positive: all checks should pass
	if !allPassed {
		for _, r := range results {
			if !r.Passed {
				t.Errorf("check %q failed: %s",
					r.Name, r.Detail)
			}
		}
		t.Fatal("expected all validations to pass")
	}

	// Positive: should have at least data dir and port checks
	if len(results) < 3 {
		t.Fatalf("expected at least 3 results, got %d",
			len(results))
	}
}

func TestDryRunValidate_BadDataDir(t *testing.T) {
	cfg := Config{
		DataDir:       "/nonexistent/deeply/nested/path",
		HTTPAddr:      findFreeAddr(t),
		NATSPort:      findFreePort(t),
		MaxStoreBytes: 1 << 20,
	}

	results, allPassed := DryRunValidate(cfg)

	// Negative: should not all pass
	if allPassed {
		t.Fatal("expected validation to fail for bad dir")
	}

	// Positive: data dir check should be present and failed
	found := false
	for _, r := range results {
		if strings.Contains(r.Name, "data directory") {
			found = true
			if r.Passed {
				t.Error("data dir check should have failed")
			}
		}
	}
	if !found {
		t.Fatal("data directory check not found")
	}
}

func TestDryRunValidate_WithWorkers(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		HTTPAddr:      findFreeAddr(t),
		NATSPort:      findFreePort(t),
		MaxStoreBytes: 1 << 20,
		Workers: []WorkerConfig{
			{Task: "build", Exec: "go build ./..."},
		},
	}

	results, allPassed := DryRunValidate(cfg)

	// Positive: should pass with valid worker config
	if !allPassed {
		for _, r := range results {
			if !r.Passed {
				t.Errorf("check %q failed: %s",
					r.Name, r.Detail)
			}
		}
		t.Fatal("expected all validations to pass")
	}

	// Positive: worker check should be present
	found := false
	for _, r := range results {
		if strings.Contains(r.Name, "worker") {
			found = true
		}
	}
	if !found {
		t.Fatal("worker config check not found")
	}
}

func TestDryRunValidate_InvalidWorkers(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		HTTPAddr:      findFreeAddr(t),
		NATSPort:      findFreePort(t),
		MaxStoreBytes: 1 << 20,
		Workers: []WorkerConfig{
			{Task: "bad"}, // neither exec nor http
		},
	}

	results, allPassed := DryRunValidate(cfg)

	// Negative: should fail
	if allPassed {
		t.Fatal("expected validation to fail for bad workers")
	}

	// Positive: worker check should fail
	for _, r := range results {
		if strings.Contains(r.Name, "worker") {
			if r.Passed {
				t.Error("worker check should have failed")
			}
		}
	}
}

func TestDryRunValidate_MissingCredentials(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:         dir,
		HTTPAddr:        findFreeAddr(t),
		NATSPort:        findFreePort(t),
		MaxStoreBytes:   1 << 20,
		LeafCredentials: "/nonexistent/creds.file",
	}

	results, allPassed := DryRunValidate(cfg)

	// Negative: should fail
	if allPassed {
		t.Fatal("expected validation to fail for missing creds")
	}

	// Positive: credentials check should be present
	found := false
	for _, r := range results {
		if strings.Contains(r.Name, "credentials") {
			found = true
			if r.Passed {
				t.Error("credentials check should have failed")
			}
		}
	}
	if !found {
		t.Fatal("credentials check not found")
	}
}

func TestDryRunValidate_CredentialsExist(t *testing.T) {
	dir := t.TempDir()
	credsPath := filepath.Join(dir, "test.creds")
	if err := os.WriteFile(
		credsPath, []byte("creds"), 0600,
	); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		DataDir:         dir,
		HTTPAddr:        findFreeAddr(t),
		NATSPort:        findFreePort(t),
		MaxStoreBytes:   1 << 20,
		LeafCredentials: credsPath,
	}

	results, allPassed := DryRunValidate(cfg)

	// Positive: should pass
	if !allPassed {
		for _, r := range results {
			if !r.Passed {
				t.Errorf("check %q failed: %s",
					r.Name, r.Detail)
			}
		}
		t.Fatal("expected all validations to pass")
	}

	// Positive: credentials check present and passed
	found := false
	for _, r := range results {
		if strings.Contains(r.Name, "credentials") {
			found = true
			if !r.Passed {
				t.Error("credentials check should pass")
			}
		}
	}
	if !found {
		t.Fatal("credentials check not found")
	}
}

func TestPrintDryRun_OutputFormat(t *testing.T) {
	dir := t.TempDir()
	rc := ResolvedConfig{
		Config: Config{
			DataDir:       dir,
			HTTPAddr:      findFreeAddr(t),
			NATSPort:      findFreePort(t),
			MaxStoreBytes: 1 << 20,
		},
		Entries: []ConfigEntry{
			{Key: "data_dir", Value: dir, Source: sourceDefault},
			{Key: "http_addr", Value: ":0", Source: sourceDefault},
			{Key: "nats_port", Value: "0", Source: sourceDefault},
		},
	}

	var buf bytes.Buffer
	passed := PrintDryRun(&buf, rc)
	output := buf.String()

	// Positive: should contain config source header
	if !strings.Contains(output, "Config source:") {
		t.Fatal("output should contain 'Config source:'")
	}

	// Positive: should contain validation section
	if !strings.Contains(output, "Validation:") {
		t.Fatal("output should contain 'Validation:'")
	}

	// Positive: should end with Config OK when passed
	if passed && !strings.Contains(output, "Config OK") {
		t.Fatal("output should contain 'Config OK' on pass")
	}

	// Negative: should not contain INVALID on pass
	if passed && strings.Contains(output, "Config INVALID") {
		t.Fatal("output should not contain INVALID on pass")
	}
}

func TestPrintDryRun_FailureOutput(t *testing.T) {
	rc := ResolvedConfig{
		Config: Config{
			DataDir:       "/nonexistent/deeply/nested",
			HTTPAddr:      findFreeAddr(t),
			NATSPort:      findFreePort(t),
			MaxStoreBytes: 1 << 20,
		},
		Entries: []ConfigEntry{
			{
				Key:    "data_dir",
				Value:  "/nonexistent/deeply/nested",
				Source: sourceDefault,
			},
		},
	}

	var buf bytes.Buffer
	passed := PrintDryRun(&buf, rc)
	output := buf.String()

	// Negative: should not pass
	if passed {
		t.Fatal("should fail for nonexistent data dir")
	}

	// Positive: should contain INVALID
	if !strings.Contains(output, "Config INVALID") {
		t.Fatal("output should contain 'Config INVALID'")
	}

	// Negative: should not contain Config OK
	if strings.Contains(output, "Config OK") {
		t.Fatal("output should not contain 'Config OK'")
	}
}

func TestSourceFor_DetectsCorrectSource(t *testing.T) {
	// Default: all values same
	src := sourceFor("val", "val", "val", false)
	if src != sourceDefault {
		t.Errorf("expected %q, got %q", sourceDefault, src)
	}

	// File: afterFile differs from default
	src = sourceFor("default", "changed", "changed", true)
	if src != sourceFile {
		t.Errorf("expected %q, got %q", sourceFile, src)
	}

	// Env: final differs from afterFile
	src = sourceFor("default", "file", "env", false)
	if src != sourceEnv {
		t.Errorf("expected %q, got %q", sourceEnv, src)
	}

	// Env overrides file
	src = sourceFor("default", "file", "env", true)
	if src != sourceEnv {
		t.Errorf("expected %q, got %q", sourceEnv, src)
	}
}

// findFreePort returns an available TCP port for testing.
func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := findFreeListener(t)
	if err != nil {
		t.Fatalf("findFreePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// findFreeAddr returns an available TCP address string.
func findFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := findFreeListener(t)
	if err != nil {
		t.Fatalf("findFreeAddr: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// findFreeListener opens a listener on an ephemeral port.
func findFreeListener(t *testing.T) (net.Listener, error) {
	t.Helper()
	return net.Listen("tcp", ":0")
}
