// cli/workers_test.go
package cli

import (
	"testing"

	"github.com/danmestas/dagnats/worker"
)

func TestPrintWorkersTable(t *testing.T) {
	// Positive: table prints without panic for non-empty list
	workers := []worker.WorkerRegistration{
		{
			WorkerID:  "worker-1",
			TaskTypes: []string{"task-a", "task-b"},
			Language:  "go",
			Transport: "nats",
			MaxTasks:  10,
		},
		{
			WorkerID:  "worker-2",
			TaskTypes: []string{"task-c"},
			Language:  "python",
			Transport: "nats",
			MaxTasks:  5,
		},
	}
	// Just verify no panic - output goes to stdout
	printWorkersTable(workers)

	// Negative: empty list panics
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty workers list")
		}
	}()
	printWorkersTable([]worker.WorkerRegistration{})
}

func TestPrintWorkersTableBound(t *testing.T) {
	// Positive: list within bounds works
	workers := make([]worker.WorkerRegistration, 100)
	for i := 0; i < 100; i++ {
		workers[i] = worker.WorkerRegistration{
			WorkerID:  "worker-test",
			TaskTypes: []string{"task"},
			Language:  "go",
			MaxTasks:  1,
		}
	}
	printWorkersTable(workers)

	// Negative: oversized list panics
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for oversized workers list")
		}
	}()
	oversized := make([]worker.WorkerRegistration, 100001)
	printWorkersTable(oversized)
}
