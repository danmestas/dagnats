// natsapi_fuzzempty_test.go
// Every NATS control-plane endpoint answers empty, "{}" and malformed
// bodies with a reply instead of crashing the server: request bodies are
// caller input, and an assertion reached from one would take down every
// workflow on the node (a remote denial of service).
// Methodology: one embedded server and NATSAPI; each subject is sent
// each body, then the API must still answer a known-good request.
package api

import (
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
	}
	bodies := []string{"", "{}", "null", `{"name":""}`, "not json"}
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
