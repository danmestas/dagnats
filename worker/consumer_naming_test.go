// worker/consumer_naming_test.go
// Pins that the worker-side bindings delegate to the shared scheme in
// internal/consumername. The exhaustive tables for the scheme itself live
// with the implementation (internal/consumername/consumername_test.go);
// duplicating them here would only re-test that package. What matters at
// this seam is that each binding forwards to the RIGHT function — a
// name/filter swap would compile and silently break the bridge's
// byte-identical-durable contract (issue #532).
package worker

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/consumername"
)

func TestConsumerNamingBindingsDelegate(t *testing.T) {
	if got := consumerNameFor("render.gpu", ""); got !=
		consumername.NameFor("render.gpu", "") {
		t.Fatalf("consumerNameFor diverged from consumername.NameFor: %q",
			got)
	}
	if got := consumerNameFor("render.gpu", ""); got !=
		"workers-render-gpu" {
		t.Fatalf("consumerNameFor = %q, want %q", got,
			"workers-render-gpu")
	}
	if got := consumerFilterFor("render", "gpu"); got !=
		consumername.FilterFor("render", "gpu") {
		t.Fatalf(
			"consumerFilterFor diverged from consumername.FilterFor: %q",
			got)
	}
	if got := consumerFilterFor("render", "gpu"); got !=
		"task.render.gpu.>" {
		t.Fatalf("consumerFilterFor = %q, want %q", got,
			"task.render.gpu.>")
	}
	if got := sanitizeConsumerName("a b"); got != "a_b" {
		t.Fatalf("sanitizeConsumerName = %q, want %q", got, "a_b")
	}
}

func TestDefaultAckWait_IsFiveMinutes(t *testing.T) {
	if defaultAckWait != 5*time.Minute {
		t.Fatalf("defaultAckWait = %v, want %v",
			defaultAckWait, 5*time.Minute)
	}
	if defaultAckWait <= 0 {
		t.Fatalf("defaultAckWait must be positive, got %v", defaultAckWait)
	}
}
