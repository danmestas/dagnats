// observe/simple/setup_test.go
// Tests for SetupTelemetry. Methodology: verify zero-config defaults
// produce working collectors with real NATS.
package simple

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestSetupTelemetryWithNATS(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	tel, shutdown := SetupTelemetry(nc)
	defer shutdown()
	if tel == nil {
		t.Fatal("SetupTelemetry returned nil")
	}
	if tel.Tracer == nil || tel.Logger == nil ||
		tel.Metrics == nil || tel.Errors == nil {
		t.Fatal("Telemetry has nil fields")
	}
	// Verify tracer works
	_, span := tel.Tracer.Start(context.Background(), "setup.test")
	span.End()
}

func TestSetupTelemetryNilPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("SetupTelemetry with nil nc should panic")
		}
	}()
	SetupTelemetry(nil)
}
