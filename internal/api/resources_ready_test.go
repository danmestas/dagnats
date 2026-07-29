// api/resources_ready_test.go
// Tests for ResourcesReady, the error-returning resource probe (#599 item 2).
// Methodology: real embedded NATS. "Server not provisioned yet" is an
// operationally-expected condition for a CLI that may run before
// `dagnats serve` finishes bootstrapping, so it must surface as an error,
// not a panic. Genuine invariant violations (nil nc) still panic.
package api

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestResourcesReadyMissingBucketsReturnsError(t *testing.T) {
	// Connect to a real NATS but skip SetupAll: the workflow_defs bucket
	// does not exist. This is the operationally-expected "not provisioned
	// yet" case and must return an error instead of panicking.
	_, nc := natsutil.StartTestServer(t)
	defer nc.Close()

	err := ResourcesReady(nc)

	// Positive: an error surfaces for the not-provisioned case.
	if err == nil {
		t.Fatal("expected error when workflow_defs bucket is missing")
	}
	// Negative: the probe did not return nil-as-ready when unprovisioned.
	if err.Error() == "" {
		t.Fatal("expected a descriptive error message, got empty")
	}
}

func TestResourcesReadyProvisionedReturnsNil(t *testing.T) {
	// After SetupAll the workflow_defs bucket exists, so the probe reports
	// ready with a nil error and NewService would succeed.
	_, nc := natsutil.StartTestServer(t)
	defer nc.Close()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	err := ResourcesReady(nc)

	// Positive: provisioned resources report ready.
	if err != nil {
		t.Fatalf("expected nil error when provisioned, got %v", err)
	}
	// Negative: NewService must not panic once the probe reports ready.
	svc := NewService(nc)
	if svc == nil {
		t.Fatal("expected non-nil service after ready probe")
	}
}

func TestResourcesReadyPanicsNilNC(t *testing.T) {
	// A nil connection is a genuine programmer error (invariant violation),
	// which stays a panic per TigerStyle — not an operational error return.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil nc")
		}
	}()
	_ = ResourcesReady(nil)
}
