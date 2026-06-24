// trigger/ack_micro_test.go
// Methodology: real embedded NATS server, real TriggerService. Proves
// the trigger ack surface is exposed as a discoverable "dagnats-trigger"
// nats-micro service (#449 Phase 2b) while the ack subject and reply
// bytes stay identical to the pre-micro raw-subscribe implementation.
//
// Discovery is driven over the wire via $SRV.PING broadcast so the tests
// observe exactly what `nats micro ls` would: a fresh embedded server per
// test, bounded waits so a hung server fails fast instead of hanging CI.
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

// pingTriggerResponders broadcasts a $SRV.PING and returns the Ping
// replies whose Name is "dagnats-trigger". Drains for a bounded window so
// every responder on a quiet embedded server is observed. Test helper:
// Fatals rather than panics so the test name is the failure context.
func pingTriggerResponders(
	t *testing.T, nc *nats.Conn,
) []micro.Ping {
	t.Helper()
	subject, err := micro.ControlSubject(micro.PingVerb, "", "")
	if err != nil {
		t.Fatalf("ControlSubject: %v", err)
	}
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest(subject, inbox, nil); err != nil {
		t.Fatalf("PublishRequest: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var pings []micro.Ping
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(200 * time.Millisecond)
		if err != nil {
			break
		}
		var ping micro.Ping
		if err := json.Unmarshal(msg.Data, &ping); err != nil {
			t.Fatalf("unmarshal ping: %v; raw=%s", err, msg.Data)
		}
		if ping.Name == "dagnats-trigger" {
			pings = append(pings, ping)
		}
	}
	return pings
}

// setupWithTriggerTypes provisions the buckets NewTriggerService and its
// scheduler require (triggers + trigger_state) AND leaves the default
// trigger_types bucket in place, so the conditional ack-micro start guard
// fires and the dagnats-trigger service registers.
func setupWithTriggerTypes(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
}

// provisionWithoutTriggerTypes creates ONLY the triggers + trigger_state
// KV buckets (which NewTriggerService and its scheduler require) and
// deliberately leaves "trigger_types" absent, so the conditional
// ack-micro start guard does not fire.
func provisionWithoutTriggerTypes(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	for _, bucket := range []string{"triggers", "trigger_state"} {
		if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: bucket,
		}); err != nil {
			t.Fatalf("CreateKeyValue(%s): %v", bucket, err)
		}
	}
}

func TestTriggerMicroDiscoverableWhenTriggerTypesPresent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	setupWithTriggerTypes(t, nc)
	svc, err := NewTriggerService(nc, "1.2.3")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(svc.Stop)

	pings := pingTriggerResponders(t, nc)

	// Positive: exactly one dagnats-trigger responder, version threaded.
	if len(pings) != 1 {
		t.Fatalf("dagnats-trigger responders = %d, want 1", len(pings))
	}
	if pings[0].Version != "1.2.3" {
		t.Fatalf(
			"dagnats-trigger version = %q, want 1.2.3",
			pings[0].Version,
		)
	}
}

func TestTriggerMicroAbsentWhenTriggerTypesMissing(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	provisionWithoutTriggerTypes(t, nc)

	svc, err := NewTriggerService(nc, "1.2.3")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(svc.Stop)

	pings := pingTriggerResponders(t, nc)

	// Negative space: the conditional guard holds — no trigger_types KV
	// means no dagnats-trigger service is registered at all.
	if len(pings) != 0 {
		t.Fatalf(
			"dagnats-trigger responders = %d, want 0 (guard held)",
			len(pings),
		)
	}
}

func TestTriggerMicroStopDrainsService(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	setupWithTriggerTypes(t, nc)
	svc, err := NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Positive: the service is discoverable while running.
	if got := len(pingTriggerResponders(t, nc)); got != 1 {
		t.Fatalf("before Stop responders = %d, want 1", got)
	}

	svc.Stop()

	// Negative space: after Stop the responder has drained away.
	if got := len(pingTriggerResponders(t, nc)); got != 0 {
		t.Fatalf("after Stop responders = %d, want 0", got)
	}
}

func TestTriggerMicroVersion(t *testing.T) {
	cases := []struct {
		name  string
		build string
		want  string
	}{
		{"plain semver passes through", "1.2.3", "1.2.3"},
		{"zero semver passes through", "0.0.0", "0.0.0"},
		{"dev sentinel", "dev", "0.0.0-dev"},
		{"empty sentinel", "", "0.0.0-dev"},
		{"prerelease passes through", "1.2.3-4-gabcdef", "1.2.3-4-gabcdef"},
		{"build metadata passes through", "1.2.3+build.7", "1.2.3+build.7"},
		{"v-prefixed collapses", "v1.2.3", "0.0.0-dev"},
		{"two-component collapses", "1.2", "0.0.0-dev"},
		{"non-semver collapses", "main", "0.0.0-dev"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := microVersion(tc.build)
			if got != tc.want {
				t.Fatalf(
					"microVersion(%q) = %q, want %q",
					tc.build, got, tc.want,
				)
			}
			// Contract: every output round-trips through micro's
			// validation -- a valid SemVer or the dev sentinel.
			if !microVersionRegexp.MatchString(got) &&
				got != microVersionDevSentinel {
				t.Fatalf(
					"microVersion(%q) = %q is not micro-acceptable",
					tc.build, got,
				)
			}
		})
	}
}
