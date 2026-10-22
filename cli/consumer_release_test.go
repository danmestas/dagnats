// cli/consumer_release_test.go
// The one-shot telemetry readers (log tail, log search, run-span
// collection) each open an ordered consumer on TELEMETRY. They must
// release it on return: a server-side consumer left per invocation
// counts toward the stream's consumer cap until its inactivity
// threshold reaps it.
// Methodology: one embedded server with a few telemetry messages; each
// reader runs repeatedly and the TELEMETRY consumer count (stream Info)
// must be back at baseline afterwards. Every wait is bounded.
package cli

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/observe/spanread"
	"github.com/nats-io/nats.go/jetstream"
)

func telemetryConsumers(t *testing.T, js jetstream.JetStream) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "TELEMETRY")
	if err != nil {
		t.Fatalf("Stream TELEMETRY: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info TELEMETRY: %v", err)
	}
	return info.State.Consumers
}

func TestTelemetryReaders_ReleaseTheirConsumer(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, subject := range []string{"telemetry.logs.svc.INFO", "telemetry.spans.svc.run-1"} {
		if _, err := js.Publish(ctx, subject, []byte(`{"body":"hi","severity":"INFO"}`)); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}
	baseline := telemetryConsumers(t, js)
	for i := 0; i < 5; i++ {
		runLogsTail(js, "telemetry.logs.>", 10)
		fetchSearchResults(js, "telemetry.logs.>", time.Now().Add(-time.Hour), 10, "")
		if _, err := spanread.CollectRunSpans(ctx, js, "run-1", 10); err != nil {
			t.Fatalf("CollectRunSpans: %v", err)
		}
	}
	if got := telemetryConsumers(t, js); got != baseline {
		t.Fatalf("TELEMETRY consumers = %d after 15 reads, want baseline %d", got, baseline)
	}
	if baseline != 0 {
		t.Errorf("baseline = %d, want 0", baseline)
	}
}
