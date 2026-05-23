// engine/benchmark_test.go
// Methodology: measure TracingPublisher overhead vs raw js.PublishMsg
// on a single-subject publish loop, to confirm #334's <5% target.
// Each benchmark publishes N messages and reports ns/op. Run with
// `go test ./internal/engine/ -bench=BenchmarkPublish -benchmem`.
package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// BenchmarkPublishRaw establishes the baseline: raw jetstream
// PublishMsg with no trace-context injection. Numbers from this
// run are what the wrapper overhead is measured against.
func BenchmarkPublishRaw(b *testing.B) {
	_, nc := natsutil.StartTestServer(b)
	if err := natsutil.SetupAll(nc); err != nil {
		b.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()
	body := []byte(`{"benchmark":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &nats.Msg{
			Subject: fmt.Sprintf("history.bench.raw.%d", i),
			Data:    body,
		}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			b.Fatalf("PublishMsg: %v", err)
		}
	}
}

// BenchmarkPublishWrapped measures the same workload through
// TracingPublisher.JSPublishMsg. Includes the InjectTraceContext
// call on every iteration. The wrapper's overhead target is <5%
// vs the raw baseline.
func BenchmarkPublishWrapped(b *testing.B) {
	_, nc := natsutil.StartTestServer(b)
	if err := natsutil.SetupAll(nc); err != nil {
		b.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)
	ctx := context.Background()
	body := []byte(`{"benchmark":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &nats.Msg{
			Subject: fmt.Sprintf("history.bench.wrapped.%d", i),
			Data:    body,
		}
		if _, err := tp.JSPublishMsg(ctx, msg); err != nil {
			b.Fatalf("JSPublishMsg: %v", err)
		}
	}
}
