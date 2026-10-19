// worker/nil_option_test.go
// Methodology: real embedded NATS. A nil functional option must be a
// no-op for every variadic option constructor, never a nil dereference
// (#784).
package worker

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestNilOptionsAreSkipped(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	w := NewWorker(nc, nil, WithPartitions(2), nil)
	if w == nil {
		t.Fatal("NewWorker returned nil")
	}
	w.Handle("nil-opt-task", func(TaskContext) error { return nil }, nil)
	if _, ok := w.handlers["nil-opt-task"]; !ok {
		t.Fatal("handler not registered when a nil HandlerOption is passed")
	}
	typed := Typed[struct{}, struct{}](
		func(TaskContext, struct{}) (struct{}, error) {
			return struct{}{}, nil
		}, nil)
	if typed == nil {
		t.Fatal("Typed returned nil with a nil option")
	}
}
