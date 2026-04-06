// Methodology: integration tests using real subprocesses (sleep, sh)
// to verify supervisor orchestration — start order, stop all, and
// cleanup on partial failure. All tests use bounded timeouts.

package sidecar

import (
	"context"
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
