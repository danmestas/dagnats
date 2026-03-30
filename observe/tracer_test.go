// observe/tracer_test.go
// Tests for Tracer interface, Span interface, noop implementations, and
// Attribute constructors. Methodology: verify compile-time interface
// satisfaction, runtime safety of noop, and typed attribute construction.
package observe

import (
	"context"
	"testing"
)

func TestNoopTracerSatisfiesInterface(t *testing.T) {
	var tracer Tracer = NewNoopTracer()
	if tracer == nil {
		t.Fatal("NewNoopTracer returned nil")
	}
	ctx, span := tracer.Start(context.Background(), "test-span")
	if ctx == nil {
		t.Fatal("Start returned nil context")
	}
	if span == nil {
		t.Fatal("Start returned nil span")
	}
	// Span methods must not panic on noop
	span.SetStatus(StatusOK, "")
	span.SetAttributes(StringAttr("key", "val"))
	span.RecordError(nil)
	span.AddEvent("evt")
	span.End()
}

func TestNoopSpanImplementsSpanContext(t *testing.T) {
	tracer := NewNoopTracer()
	_, span := tracer.Start(context.Background(), "test")
	sc, ok := span.(SpanContext)
	if !ok {
		t.Fatal("noopSpan should implement SpanContext")
	}
	// Noop returns empty strings — not nil, not panic.
	if sc.TraceID() != "" {
		t.Fatalf("TraceID = %q, want empty", sc.TraceID())
	}
	if sc.SpanID() != "" {
		t.Fatalf("SpanID = %q, want empty", sc.SpanID())
	}
}

func TestContextWithParentInfo(t *testing.T) {
	ctx := ContextWithParentInfo(
		context.Background(), "trace123", "span456",
	)
	info, ok := ParentInfoFromContext(ctx)
	if !ok {
		t.Fatal("ParentInfoFromContext returned false")
	}
	if info.TraceID != "trace123" {
		t.Fatalf("TraceID = %q, want trace123", info.TraceID)
	}
	if info.SpanID != "span456" {
		t.Fatalf("SpanID = %q, want span456", info.SpanID)
	}
}

func TestParentInfoFromContextEmpty(t *testing.T) {
	_, ok := ParentInfoFromContext(context.Background())
	if ok {
		t.Fatal("should return false for empty context")
	}
	// Negative: ensure no default info is injected.
	ctx := context.WithValue(context.Background(), "other", "val")
	_, ok = ParentInfoFromContext(ctx)
	if ok {
		t.Fatal("should return false for unrelated context")
	}
}

func TestAttributeConstructors(t *testing.T) {
	s := StringAttr("k", "v")
	if s.Key != "k" || s.Value != "v" {
		t.Fatalf("StringAttr = %+v, want key=k val=v", s)
	}
	i := Int64Attr("n", 42)
	if i.Key != "n" || i.Value != int64(42) {
		t.Fatalf("Int64Attr = %+v, want key=n val=42", i)
	}
	f := Float64Attr("f", 3.14)
	if f.Key != "f" || f.Value != 3.14 {
		t.Fatalf("Float64Attr = %+v, want key=f val=3.14", f)
	}
	b := BoolAttr("ok", true)
	if b.Key != "ok" || b.Value != true {
		t.Fatalf("BoolAttr = %+v, want key=ok val=true", b)
	}
}
