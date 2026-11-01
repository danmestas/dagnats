// natsapi_fuzzempty_test.go
// Every NATS control-plane endpoint, global and program-scoped, answers
// empty, "{}", malformed and boundary bodies with a reply instead of
// crashing the server: request bodies are
// caller input, and an assertion reached from one would take down every
// workflow on the node (a remote denial of service).
// Methodology: one embedded server and NATSAPI; each subject is sent
// each body, then the API must still answer a known-good request.
package api

import (
	"encoding/json"
	"github.com/nats-io/nats.go/micro"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestNATSAPIEndpointsSurviveEmptyBodies(t *testing.T) {
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	natsAPI := NewNATSAPI(NewService(nc), nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()
	subjects := []string{
		"api.workflows.register", "api.runs.start", "api.runs.get",
		"api.runtimes.register", "api.runs.spawn", "api.runtimes.budget",
		"api.inputs.submit", "api.inputs.get", "api.inputs.withdraw",
		"api.leases.acquire", "api.leases.renew", "api.leases.release",
		"api.leases.owner",
		// The program-scoped control plane and relay (#885).
		"api.p.notify.workflows.register", "api.p.notify.runs.start",
		"api.p.notify.runs.get", "api.p.notify.runs.cancel",
		"api.p.notify.events", "api.p.notify.journal.append",
		"api.p.notify.journal.view",
		"api.p.Bad.runs.get", "api.p.x.runs.get",
	}
	bodies := []string{
		"", "{}", "null", `{"name":""}`, "not json", `"str"`, "[]",
		`{"name":"notify."}`, `{"name":"notify.x","steps":[{"id":"","task":""}]}`,
		`{"workflow":""}`, `{"workflow":"notify."}`, "*", ">", "a.b",
	}
	for _, subject := range subjects {
		for _, body := range bodies {
			if _, err := nc.Request(subject, []byte(body), 2*time.Second); err != nil {
				t.Errorf("%s %q: %v", subject, body, err)
			}
		}
	}
	if _, err := nc.Request("api.workflows.register", []byte("{}"), 2*time.Second); err != nil {
		t.Fatalf("the API stopped answering: %v", err)
	}
}

// TestGuardedHandlerAnswersAPanic: a handler that panics is answered with
// kind "internal" and the service keeps serving, so a bug no input check
// caught still cannot take the node down.
func TestGuardedHandlerAnswersAPanic(t *testing.T) {
	_, nc := natstest.StartServer(t)
	n := &NATSAPI{nc: nc}
	srv, err := micro.AddService(nc, micro.Config{Name: "guard-test", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	defer srv.Stop() //nolint:errcheck // test teardown
	calls := 0
	boom := n.guarded(func(micro.Request) {
		calls++
		panic("bug")
	})
	if err := srv.AddEndpoint("boom", boom, micro.WithEndpointSubject("guard.boom")); err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}
	for i := 0; i < 2; i++ {
		reply, err := nc.Request("guard.boom", nil, 2*time.Second)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		var body map[string]string
		if err := json.Unmarshal(reply.Data, &body); err != nil || body["kind"] != "internal" {
			t.Fatalf("reply %d = %s, %v; want kind internal", i, reply.Data, err)
		}
	}
	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2: the service stopped serving", calls)
	}
}
