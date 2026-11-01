// scoped_api_test.go
// A program's NATS user reaches its own scoped control plane
// (api.p.<name>.*) and gets replies on its own inbox, while the global
// api.workflows.register and api.runs.* stay denied to it, so a program
// can manage its workflows once programs log in as NKey users (#885).
// Methodology: the shared permission fixture (real NKey users with
// PermissionsFor) plus the real NATSAPI served over the admin
// connection.
package program_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/nats-io/nats.go"
)

func TestProgramUserReachesOnlyItsScopedAPI(t *testing.T) {
	alpha, _ := attackManifests()
	f := startFixture(t, alpha)
	natsAPI := api.NewNATSAPI(api.NewService(f.admin), f.admin, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	nc := f.dialProgram(t, "alpha")

	wb := dag.NewWorkflow("alpha.flow")
	wb.Task("a", "alpha.work")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	body, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	reply, err := nc.Request("api.p.alpha.workflows.register", body, 5*time.Second)
	if err != nil {
		t.Fatalf("scoped register: %v", err)
	}
	var out map[string]string
	if err := json.Unmarshal(reply.Data, &out); err != nil || out["status"] != "registered" {
		t.Fatalf("scoped register reply = %s, %v", reply.Data, err)
	}
	denied := []string{"api.workflows.register", "api.runs.start", "api.p.beta.runs.get"}
	for _, subject := range denied {
		_, err := nc.Request(subject, body, time.Second)
		refused := errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
			errors.Is(err, nats.ErrPermissionViolation)
		if !refused {
			t.Errorf("%s: err = %v, want a denied request", subject, err)
		}
		if !f.violations.contains(subject) {
			t.Errorf("%s: no permission violation recorded", subject)
		}
	}
}
