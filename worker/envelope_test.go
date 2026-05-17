// Methodology: compile-time proof that the public re-exports on the
// worker package are Go type aliases of the internal envelope types.
// If aliases drift to distinct named types, the assignments below
// stop compiling — that is the regression signal for #235.
package worker

import (
	"testing"

	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/internal/trigger"
)

// TestEnvelopeAliases proves the public re-exports are identical to
// the internal types. Regression for #235.
func TestEnvelopeAliases(t *testing.T) {
	// Positive: an internal value is directly assignable to the
	// public alias (no conversion). This is what makes it an alias
	// rather than a wrapper.
	var pubHTTP HTTPEnvelope = httpenvelope.Envelope{Method: "GET"}
	if pubHTTP.Method != "GET" {
		t.Fatalf("HTTPEnvelope alias did not preserve field value")
	}

	var pubTrig TriggerEnvelope = trigger.TriggerEnvelope{Trigger: "http"}
	if pubTrig.Trigger != "http" {
		t.Fatalf("TriggerEnvelope alias did not preserve field value")
	}

	// Negative: the reverse direction must also work without a
	// conversion. If either alias degraded to a named type, one
	// of these lines would fail to compile.
	var rtHTTP httpenvelope.Envelope = pubHTTP
	if rtHTTP.Method != "GET" {
		t.Fatalf("alias did not round-trip")
	}
	var rtTrig trigger.TriggerEnvelope = pubTrig
	if rtTrig.Trigger != "http" {
		t.Fatalf("alias did not round-trip")
	}
}
