// api/natsapi_leases_test.go
// Methodology: real embedded NATS + SetupAll + a started NATSAPI; the
// tests speak only the wire contract a downstream service would (JSON
// over api.leases.*), never the Go lease package. They cover the full
// acquire/renew/release/owner lifecycle, the {error, kind} envelope for
// bad input and lost fences, and the queue-group invariant: with two
// dagnats instances running, a lease request gets exactly one reply.
package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startLeaseAPI(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	natsAPI := NewNATSAPI(NewService(nc), nc, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	return nc
}

func leaseCall(t *testing.T, nc *nats.Conn, subject string, body any) leaseReply {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request(subject, data, 5*time.Second)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	var reply leaseReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("%s: unmarshal %q: %v", subject, msg.Data, err)
	}
	return reply
}

func TestLeaseWireLifecycle(t *testing.T) {
	nc := startLeaseAPI(t)
	acquire := map[string]any{"key": "leader", "holder": "notify-1", "ttl_ms": 10000}

	got := leaseCall(t, nc, "api.leases.acquire", acquire)
	if !got.Granted || got.Revision == 0 || got.ExpiresAt == nil {
		t.Fatalf("acquire reply = %+v, want granted with revision + expiry", got)
	}
	other := map[string]any{"key": "leader", "holder": "notify-2", "ttl_ms": 10000}
	denied := leaseCall(t, nc, "api.leases.acquire", other)
	if denied.Granted || !denied.Held || denied.Holder != "notify-1" || denied.Error != "" {
		t.Fatalf("contended acquire = %+v, want denied naming notify-1", denied)
	}

	renew := map[string]any{
		"key": "leader", "holder": "notify-1", "revision": got.Revision, "ttl_ms": 10000,
	}
	renewed := leaseCall(t, nc, "api.leases.renew", renew)
	if !renewed.Granted || renewed.Revision <= got.Revision {
		t.Fatalf("renew = %+v, want a newer revision", renewed)
	}
	stale := leaseCall(t, nc, "api.leases.renew", renew)
	if stale.Kind != leaseKindLost || stale.Granted {
		t.Fatalf("renew with stale revision = %+v, want kind lost", stale)
	}

	owner := leaseCall(t, nc, "api.leases.owner", map[string]any{"key": "leader"})
	if !owner.Held || owner.Holder != "notify-1" || owner.Revision != renewed.Revision {
		t.Fatalf("owner = %+v, want notify-1 at %d", owner, renewed.Revision)
	}
	release := map[string]any{
		"key": "leader", "holder": "notify-1", "revision": renewed.Revision,
	}
	if released := leaseCall(t, nc, "api.leases.release", release); released.Error != "" {
		t.Fatalf("release = %+v, want success", released)
	}
	after := leaseCall(t, nc, "api.leases.owner", map[string]any{"key": "leader"})
	if after.Held || after.Holder != "" {
		t.Fatalf("owner after release = %+v, want not held", after)
	}
}

func TestLeaseWireRejectsBadInput(t *testing.T) {
	nc := startLeaseAPI(t)

	msg, err := nc.Request("api.leases.acquire", []byte("{not json"), 5*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var garbled leaseReply
	if err := json.Unmarshal(msg.Data, &garbled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if garbled.Kind != leaseKindTransport || garbled.Granted {
		t.Fatalf("malformed body = %+v, want kind transport", garbled)
	}
	bad := []map[string]any{
		{"key": "a.b", "holder": "notify-1", "ttl_ms": 10000},
		{"key": "leader", "holder": "", "ttl_ms": 10000},
		{"key": "leader", "holder": "notify-1", "ttl_ms": 0},
		{"key": "leader", "holder": "notify-1", "ttl_ms": 3_600_000},
	}
	for _, body := range bad {
		got := leaseCall(t, nc, "api.leases.acquire", body)
		if got.Kind != leaseKindInvalid || got.Granted || got.Error == "" {
			t.Errorf("acquire %v = %+v, want kind invalid", body, got)
		}
	}
}

// On a NATS server older than 2.11 SetupAll skips the leases bucket;
// deleting it reproduces that state. Every lease call must then say
// why, rather than time out or report a generic internal error.
func TestLeaseWithoutBucketReportsUnsupported(t *testing.T) {
	nc := startLeaseAPI(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteStream(ctx, "KV_"+natsutil.LeasesBucket); err != nil {
		t.Fatalf("delete leases stream: %v", err)
	}
	acquire := map[string]any{"key": "leader", "holder": "notify-1", "ttl_ms": 10000}
	got := leaseCall(t, nc, "api.leases.acquire", acquire)
	if got.Kind != leaseKindUnsupported || got.Granted {
		t.Fatalf("acquire without bucket = %+v, want kind unsupported", got)
	}
	if !strings.Contains(got.Error, "nats-server >= 2.11") {
		t.Fatalf("error %q must name the server requirement", got.Error)
	}
}

func TestLeaseRequestIsServedByOneInstance(t *testing.T) {
	nc := startLeaseAPI(t)
	second := NewNATSAPI(NewService(nc), nc, "1.0.0")
	second.Start()
	t.Cleanup(second.Stop)

	inbox := nc.NewRespInbox()
	replies := make(chan *nats.Msg, 4)
	sub, err := nc.ChanSubscribe(inbox, replies)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	})
	body := []byte(`{"key":"leader","holder":"notify-1","ttl_ms":10000}`)
	if err := nc.PublishRequest("api.leases.acquire", inbox, body); err != nil {
		t.Fatalf("publish: %v", err)
	}
	count := 0
	deadline := time.After(time.Second)
	for waiting := true; waiting; {
		select {
		case <-replies:
			count++
		case <-deadline:
			waiting = false
		}
	}
	if count != 1 {
		t.Fatalf("lease request got %d replies from 2 instances, want 1", count)
	}
}
