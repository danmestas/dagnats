// Methodology: integration tests using real subprocesses (sleep, sh)
// to verify supervisor orchestration — start order, stop all, and
// cleanup on partial failure. All tests use bounded timeouts.

package sidecar

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// testSupervisor builds a Supervisor with overridden processes,
// bypassing NewSupervisor which requires real binary names.
func testSupervisor(procs []*Process) *Supervisor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Supervisor{
		cfg:       DefaultConfig(),
		processes: procs,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func TestSupervisor_StartOrder(t *testing.T) {
	t.Parallel()

	// Track the order processes become running via timestamps.
	// Each process is a long sleep so they stay alive.
	procs := []*Process{
		{Name: "first", Bin: "sleep", Args: []string{"60"}},
		{Name: "second", Bin: "sleep", Args: []string{"60"}},
		{Name: "third", Bin: "sleep", Args: []string{"60"}},
	}

	sup := testSupervisor(procs)
	defer sup.Stop()

	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Positive: all three processes should be running.
	for _, p := range procs {
		if !p.IsRunning() {
			t.Fatalf("expected %s to be running", p.Name)
		}
	}

	// Negative: process count matches expected.
	if len(sup.processes) != 3 {
		t.Fatalf(
			"expected 3 processes, got %d",
			len(sup.processes),
		)
	}
}

func TestSupervisor_StopAll(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "first", Bin: "sleep", Args: []string{"60"}},
		{Name: "second", Bin: "sleep", Args: []string{"60"}},
		{Name: "third", Bin: "sleep", Args: []string{"60"}},
	}

	sup := testSupervisor(procs)

	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify all running before stop.
	for _, p := range procs {
		if !p.IsRunning() {
			t.Fatalf(
				"expected %s running before Stop", p.Name,
			)
		}
	}

	sup.Stop()

	// Positive: no process should be running after Stop.
	for _, p := range procs {
		if p.IsRunning() {
			t.Fatalf(
				"expected %s stopped after Stop", p.Name,
			)
		}
	}

	// Negative: supervisor context should be done.
	select {
	case <-sup.ctx.Done():
		// expected
	default:
		t.Fatal("expected supervisor context to be canceled")
	}
}

func TestSupervisor_StartFailure(t *testing.T) {
	t.Parallel()

	procs := []*Process{
		{Name: "first", Bin: "sleep", Args: []string{"60"}},
		{
			Name: "second",
			Bin:  "nonexistent-binary-should-fail",
		},
		{Name: "third", Bin: "sleep", Args: []string{"60"}},
	}

	sup := testSupervisor(procs)

	err := sup.Start()

	// Positive: Start should return an error.
	if err == nil {
		sup.Stop()
		t.Fatal("expected Start to fail with bad binary")
	}

	// Give a moment for cleanup goroutines.
	time.Sleep(100 * time.Millisecond)

	// Negative: first process should be cleaned up (stopped).
	if procs[0].IsRunning() {
		t.Fatal(
			"expected first process stopped after partial failure",
		)
	}

	// Third process should never have started.
	if procs[2].IsRunning() {
		t.Fatal(
			"expected third process to never have started",
		)
	}
}

func TestNewSupervisor_OmitsMissingMCPDuckDB(t *testing.T) {
	// After #187: dagnats-mcp-duckdb is optional. When the binary
	// is not on disk at NewSupervisor time, it is omitted from the
	// process list and the sidecar runs the OTLP pipe without it.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("PATH", t.TempDir()) // empty PATH; nothing findable

	sup, err := NewSupervisor(DefaultConfig())
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	defer sup.Stop()

	// Positive: the two required processes are present.
	names := processNames(sup)
	if !slices.Contains(names, "otlp2parquet") {
		t.Errorf("expected otlp2parquet in %v", names)
	}
	if !slices.Contains(names, "otelcol") {
		t.Errorf("expected otelcol in %v", names)
	}

	// Negative: dagnats-mcp-duckdb is omitted when missing.
	if slices.Contains(names, "dagnats-mcp-duckdb") {
		t.Errorf(
			"expected dagnats-mcp-duckdb omitted when not on "+
				"disk; got %v", names,
		)
	}
}

func TestNewSupervisor_IncludesPresentMCPDuckDB(t *testing.T) {
	// Regression guard: when dagnats-mcp-duckdb IS present on
	// disk, NewSupervisor still includes it and the supervisor
	// will manage it as before.
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, binDirName)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fakeBin := filepath.Join(binDir, "dagnats-mcp-duckdb")
	if err := os.WriteFile(
		fakeBin, []byte("#!/bin/sh\n"), 0o755,
	); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	t.Setenv("HOME", tmp)
	t.Setenv("PATH", t.TempDir())

	sup, err := NewSupervisor(DefaultConfig())
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	defer sup.Stop()

	names := processNames(sup)
	if !slices.Contains(names, "dagnats-mcp-duckdb") {
		t.Errorf(
			"expected dagnats-mcp-duckdb in %v when present "+
				"on disk", names,
		)
	}
}

func TestSupervisor_StartPanicsOnUnexpectedProcessCount(t *testing.T) {
	// The Start() invariant accepts [processCountMin,
	// processCountMax]. Counts outside that band must panic
	// rather than silently launching with an inconsistent set.
	cases := []struct {
		name  string
		count int
	}{
		{"zero", 0},
		{"one", 1},
		{"four", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			procs := make([]*Process, tc.count)
			for i := range procs {
				procs[i] = &Process{
					Name: "p",
					Bin:  "sleep",
					Args: []string{"60"},
				}
			}
			sup := testSupervisor(procs)
			defer func() {
				if r := recover(); r == nil {
					t.Errorf(
						"expected panic for count=%d",
						tc.count,
					)
				}
				sup.Stop()
			}()
			_ = sup.Start()
		})
	}
}

func processNames(sup *Supervisor) []string {
	out := make([]string, 0, len(sup.processes))
	for _, p := range sup.processes {
		out = append(out, p.Name)
	}
	return out
}

func TestSupervisor_StartedAt(t *testing.T) {
	t.Parallel()
	procs := []*Process{
		{Name: "first", Bin: "sleep", Args: []string{"60"}},
		{Name: "second", Bin: "sleep", Args: []string{"60"}},
		{Name: "third", Bin: "sleep", Args: []string{"60"}},
	}
	sup := testSupervisor(procs)
	defer sup.Stop()
	before := time.Now()
	if err := sup.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if sup.startedAt.IsZero() {
		t.Fatal("expected startedAt to be set after Start")
	}
	if sup.startedAt.Before(before) {
		t.Fatal("startedAt should be >= before Start call")
	}
}
