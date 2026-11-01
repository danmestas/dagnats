// relay_test.go
// Unit tests for the relay routing in TracingPublisher: which subjects
// go through the engine relay, and the guards on WithRelay. The relay's
// round trip is tested against the engine in internal/api.
// Methodology: pure functions plus a core connection to an embedded
// server for the publisher; no relay responder.
package natsutil

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRelayOp(t *testing.T) {
	cases := map[string]string{
		"history.run1":         relayEventsOp,
		"journal.run1.step":    relayJournalOp,
		"logs.run1.step.out.0": "",
		"stream.run1.step":     "",
		"historyx.run1":        "",
		"task.notify.work.x":   "",
	}
	for subject, want := range cases {
		if got := RelayOp(subject); got != want {
			t.Errorf("RelayOp(%q) = %q, want %q", subject, got, want)
		}
	}
}

func TestWithRelayIsACopy(t *testing.T) {
	_, nc := natstest.StartServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	direct := NewTracingPublisher(nc, js)
	relayed := direct.WithRelay("api.p.notify.")
	if direct.Relayed() || !relayed.Relayed() {
		t.Fatalf("Relayed: direct %v, relayed %v", direct.Relayed(), relayed.Relayed())
	}
	for _, bad := range []string{"", "api.p.notify", "history."} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithRelay(%q) did not panic", bad)
				}
			}()
			direct.WithRelay(bad)
		}()
	}
}
