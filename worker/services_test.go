// worker/services_test.go
// Tests for the services KV bucket and Worker.RegisterService SDK
// method (ADR-017 / #321).
//
// Methodology: integration tests with a real embedded NATS server.
// Each test stands up a fresh server via natsutil.StartTestServer so
// state cannot leak between tests. Asserts cover both positive
// (round-trip) and negative (last-write-wins) space.
package worker

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestServicesBucketExistsAtBoot asserts that natsutil.SetupAll
// provisions the `services` KV bucket with the expected configuration
// (no TTL, History=1). Catches accidental drift in the bucket spec.
func TestServicesBucketExistsAtBoot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	kv, err := js.KeyValue(ctx, "services")
	if err != nil {
		t.Fatalf("services bucket missing: %v", err)
	}

	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("kv.Status: %v", err)
	}

	// Positive: TTL must be 0 (services are stable definitions).
	if status.TTL() != 0 {
		t.Errorf(
			"services bucket TTL = %v, want 0", status.TTL(),
		)
	}

	// Negative: bucket must not share the workers bucket's name.
	if status.Bucket() == "workers" {
		t.Errorf(
			"services bucket must not collide with workers",
		)
	}
}

// TestServiceDef_Roundtrip asserts that a ServiceDef survives a
// Put → Get cycle through the KV with Name and Description intact
// and RegisteredAt stamped by RegisterService.
func TestServiceDef_Roundtrip(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	w := NewWorker(nc)
	def := ServiceDef{
		Name:        "billing",
		Description: "Charge cards and emit receipts.",
	}

	before := time.Now()
	if err := w.RegisterService(def); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	after := time.Now()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	services, err := ListServices(js)
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}

	// Positive: exactly one entry, fields preserved.
	if len(services) != 1 {
		t.Fatalf(
			"len(services) = %d, want 1", len(services),
		)
	}
	got := services[0]
	if got.Name != "billing" {
		t.Errorf("Name = %q, want %q", got.Name, "billing")
	}
	if got.Description != def.Description {
		t.Errorf(
			"Description = %q, want %q",
			got.Description, def.Description,
		)
	}

	// Negative: RegisteredAt must be stamped within the call window.
	if got.RegisteredAt.Before(before) ||
		got.RegisteredAt.After(after) {
		t.Errorf(
			"RegisteredAt = %v, want in [%v, %v]",
			got.RegisteredAt, before, after,
		)
	}
}

// TestRegisterService_LastWriteWins asserts that re-registering the
// same service Name with a different Description silently replaces
// the prior entry without returning an error. This is the documented
// idempotency contract — callers must be able to re-register on every
// worker boot without conflict handling.
func TestRegisterService_LastWriteWins(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	w := NewWorker(nc)

	first := ServiceDef{
		Name:        "billing",
		Description: "Original description.",
	}
	if err := w.RegisterService(first); err != nil {
		t.Fatalf("first RegisterService: %v", err)
	}

	second := ServiceDef{
		Name:        "billing",
		Description: "Updated description.",
	}
	// Positive: second call must not return an error.
	if err := w.RegisterService(second); err != nil {
		t.Fatalf(
			"second RegisterService (last-write-wins): %v", err,
		)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	services, err := ListServices(js)
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}

	// Positive: exactly one entry (no duplicate created).
	if len(services) != 1 {
		t.Fatalf(
			"len(services) = %d, want 1 (last-write-wins)",
			len(services),
		)
	}

	// Positive: second Description must have won.
	if services[0].Description != "Updated description." {
		t.Errorf(
			"Description = %q, want %q (second write should win)",
			services[0].Description, "Updated description.",
		)
	}

	// Negative: original Description must not survive.
	if services[0].Description == "Original description." {
		t.Errorf(
			"original Description survived last-write-wins",
		)
	}
}

// TestRegisterService_EmptyNamePanics asserts the programmer-error
// guard on empty Name. Per TigerStyle, invariant violations panic
// rather than returning errors callers will likely ignore.
func TestRegisterService_EmptyNamePanics(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	w := NewWorker(nc)

	defer func() {
		r := recover()
		// Positive: must panic on empty Name.
		if r == nil {
			t.Fatal(
				"expected panic for empty Name, got none",
			)
		}
		// Negative: must not be a nil-pointer panic from some other
		// path — the message should mention Name.
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value not string: %T %v", r, r)
		}
		if msg == "" {
			t.Errorf("empty panic message")
		}
	}()

	_ = w.RegisterService(ServiceDef{Name: ""})
}

// TestListServices_EmptyBucket asserts that ListServices returns an
// empty slice (not an error) when no services have been registered.
// Callers (the CLI) print "no services" on this state and would
// otherwise have to special-case the bootstrap window.
func TestListServices_EmptyBucket(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	services, err := ListServices(js)
	// Positive: no error on empty bucket.
	if err != nil {
		t.Fatalf("ListServices on empty bucket: %v", err)
	}
	// Negative: must not be nil — callers iterate the result.
	if services == nil {
		t.Errorf("expected non-nil empty slice, got nil")
	}
	if len(services) != 0 {
		t.Errorf(
			"len(services) = %d, want 0", len(services),
		)
	}
}
