// worker/metadata_test.go
// Methodology: pure unit tests — no NATS required. newTaskContext is the
// single construction path for taskContext; we verify the metadata field
// threads through from TaskPayload to ctx.Metadata() without mutation.
// Positive: a payload with Metadata populated produces the same map from
// ctx.Metadata(). Negative: a nil Metadata in the payload leaves
// ctx.Metadata() nil (no allocation, no map swapped in).
package worker

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/protocol"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestTaskContext_Metadata(t *testing.T) {
	tr := tracenoop.NewTracerProvider().Tracer("test")
	bgCtx := context.Background()
	_, span := tr.Start(bgCtx, "test-meta")

	// Positive: payload with metadata → ctx.Metadata() returns the same map.
	payload := protocol.TaskPayload{
		RunID:  "run-meta",
		StepID: "step-meta",
		Metadata: map[string]string{
			"image": "golang:1.24",
			"call":  "test",
		},
	}
	tc := newTaskContext(nil, tr, nil, payload, bgCtx, span,
		&testJetstreamMsg{}, nil, nil)
	got := tc.Metadata()
	if got == nil {
		t.Fatal("Metadata() returned nil for non-nil payload.Metadata")
	}
	if got["call"] != "test" {
		t.Errorf(`Metadata()["call"] = %q, want "test"`, got["call"])
	}
	if got["image"] != "golang:1.24" {
		t.Errorf(`Metadata()["image"] = %q, want "golang:1.24"`,
			got["image"])
	}

	// Negative: payload with nil Metadata → ctx.Metadata() is nil
	// (no spurious empty map allocated).
	nilPayload := protocol.TaskPayload{
		RunID:  "run-nil",
		StepID: "step-nil",
	}
	_, span2 := tr.Start(bgCtx, "test-meta-nil")
	tcNil := newTaskContext(nil, tr, nil, nilPayload, bgCtx, span2,
		&testJetstreamMsg{}, nil, nil)
	if tcNil.Metadata() != nil {
		t.Errorf("Metadata() = %v, want nil for nil payload.Metadata",
			tcNil.Metadata())
	}
}
