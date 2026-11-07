// scoped_triggers_test.go
// A program registers cron triggers for its own workflows over its
// api.p.<name>.triggers.* subjects (#957). Methodology: the permission
// fixture (real NKey users) with the real NATSAPI, orchestrator and
// TriggerService on the admin connection. The fire test forces a
// scheduler tick with "* * * * *" (matches every minute) rather than
// waiting out the 30s tick. Refusals are asserted on the reply text and
// on the stored trigger, so a refused request is shown to change nothing.
package program_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
)

type triggerFixture struct {
	f     *fixture
	svc   *api.Service
	alpha *nats.Conn
}

func startTriggerFixture(t *testing.T) *triggerFixture {
	t.Helper()
	alpha, beta := attackManifests()
	f := startFixture(t, alpha, beta)
	svc := api.NewService(f.admin)
	natsAPI := api.NewNATSAPI(svc, f.admin, "1.0.0")
	natsAPI.Start()
	t.Cleanup(natsAPI.Stop)
	return &triggerFixture{f: f, svc: svc, alpha: f.dialProgram(t, "alpha")}
}

func cronDef(id, workflow string) trigger.TriggerDef {
	return trigger.TriggerDef{ID: id, WorkflowID: workflow, Enabled: true,
		Cron: &trigger.CronConfig{Expression: "* * * * *", Timezone: "UTC"}}
}

func requestJSON(t *testing.T, nc *nats.Conn, subject string, body []byte) map[string]string {
	t.Helper()
	reply, err := nc.Request(subject, body, 5*time.Second)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	var out map[string]string
	if err := json.Unmarshal(reply.Data, &out); err != nil {
		t.Fatalf("%s reply %s: %v", subject, reply.Data, err)
	}
	return out
}

func createTrigger(
	t *testing.T, nc *nats.Conn, name string, def trigger.TriggerDef,
) map[string]string {
	t.Helper()
	body, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return requestJSON(t, nc, "api.p."+name+".triggers.create", body)
}

func storedTrigger(t *testing.T, svc *api.Service, id string) (trigger.TriggerDef, bool) {
	t.Helper()
	defs, err := svc.ListTriggers(context.Background())
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	for _, d := range defs {
		if d.ID == id {
			return d, true
		}
	}
	return trigger.TriggerDef{}, false
}

func wantRefused(t *testing.T, got map[string]string, substr string) {
	t.Helper()
	if !strings.Contains(got["error"], substr) {
		t.Fatalf("reply = %v, want error containing %q", got, substr)
	}
}

func TestProgramCronTriggerFires(t *testing.T) {
	tf := startTriggerFixture(t)
	orch := engine.NewOrchestrator(tf.f.admin)
	if err := orch.Start(); err != nil {
		t.Fatalf("orch.Start: %v", err)
	}
	t.Cleanup(orch.Stop)
	wb := dag.NewWorkflow("alpha.daily")
	wb.Task("a", "alpha.work")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := tf.svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ts, err := trigger.NewTriggerService(tf.f.admin, "test")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := ts.Start(); err != nil {
		t.Fatalf("ts.Start: %v", err)
	}
	t.Cleanup(ts.Stop)

	got := createTrigger(t, tf.alpha, "alpha", cronDef("alpha.daily-check", "alpha.daily"))
	if got["status"] != "created" || got["id"] != "alpha.daily-check" {
		t.Fatalf("create reply = %v", got)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ts.TickNow()
		filter := api.RunsFilter{Workflow: "alpha.daily"}
		env, err := tf.svc.ListRuns(context.Background(), filter, 10)
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		if env.Total > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no run of alpha.daily started within 20s of the trigger being created")
}

func TestProgramCreateTriggerIsIdempotentAndStampsSource(t *testing.T) {
	tf := startTriggerFixture(t)
	def := cronDef("alpha.nightly", "alpha.flow")
	def.Source = "file:evil.yaml"
	for i := 0; i < 2; i++ {
		if got := createTrigger(t, tf.alpha, "alpha", def); got["status"] != "created" {
			t.Fatalf("create #%d reply = %v", i, got)
		}
	}
	defs, err := tf.svc.ListTriggers(context.Background())
	if err != nil || len(defs) != 1 {
		t.Fatalf("triggers = %+v, %v; want exactly one", defs, err)
	}
	if defs[0].Source != "program:alpha" {
		t.Fatalf("Source = %q, want program:alpha", defs[0].Source)
	}
}

func TestProgramCreateTriggerRefusals(t *testing.T) {
	tf := startTriggerFixture(t)
	operator := cronDef("alpha.ops", "alpha.flow")
	operator.Source = "file:ops.yaml"
	if err := tf.svc.CreateTrigger(context.Background(), operator); err != nil {
		t.Fatalf("operator CreateTrigger: %v", err)
	}
	subject := cronDef("alpha.sub", "alpha.flow")
	subject.Cron = nil
	subject.Subject = &trigger.SubjectConfig{Subject: "x.y"}
	both := cronDef("alpha.both", "alpha.flow")
	both.Subject = &trigger.SubjectConfig{Subject: "x.y"}
	cases := []struct {
		name   string
		def    trigger.TriggerDef
		substr string
	}{
		{"id outside prefix", cronDef("beta.t", "alpha.flow"), "not under"},
		{"workflow outside prefix", cronDef("alpha.t", "beta.flow"), "not under"},
		{"subject kind", subject, "cron"},
		{"cron plus subject", both, "cron"},
		{"operator trigger under prefix", cronDef("alpha.ops", "alpha.flow"), "not created by"},
	}
	for _, c := range cases {
		wantRefused(t, createTrigger(t, tf.alpha, "alpha", c.def), c.substr)
		if c.def.ID == "alpha.ops" {
			continue
		}
		if _, found := storedTrigger(t, tf.svc, c.def.ID); found {
			t.Errorf("%s: refused trigger %q was stored", c.name, c.def.ID)
		}
	}
	kept, found := storedTrigger(t, tf.svc, "alpha.ops")
	if !found || kept.Source != "file:ops.yaml" {
		t.Fatalf("operator trigger = %+v, found=%v; want untouched", kept, found)
	}
}

func TestProgramDeleteTrigger(t *testing.T) {
	tf := startTriggerFixture(t)
	ctx := context.Background()
	mine := createTrigger(t, tf.alpha, "alpha", cronDef("alpha.mine", "alpha.flow"))
	if mine["status"] != "created" {
		t.Fatalf("create reply = %v", mine)
	}
	operator := cronDef("alpha.ops", "alpha.flow")
	operator.Source = "file:ops.yaml"
	foreign := cronDef("beta.theirs", "beta.flow")
	foreign.Source = "program:beta"
	for _, d := range []trigger.TriggerDef{operator, foreign} {
		if err := tf.svc.CreateTrigger(ctx, d); err != nil {
			t.Fatalf("CreateTrigger %s: %v", d.ID, err)
		}
	}
	for _, id := range []string{"alpha.ops", "beta.theirs", "alpha.missing"} {
		got := requestJSON(t, tf.alpha, "api.p.alpha.triggers.delete", []byte(id))
		wantRefused(t, got, "trigger not found")
	}
	wantRefused(t, requestJSON(t, tf.alpha, "api.p.alpha.triggers.delete", nil), "required")
	for _, id := range []string{"alpha.ops", "beta.theirs"} {
		if _, found := storedTrigger(t, tf.svc, id); !found {
			t.Errorf("trigger %q was deleted by a program that does not own it", id)
		}
	}
	got := requestJSON(t, tf.alpha, "api.p.alpha.triggers.delete", []byte("alpha.mine"))
	if got["status"] != "deleted" || got["id"] != "alpha.mine" {
		t.Fatalf("delete reply = %v", got)
	}
	if _, found := storedTrigger(t, tf.svc, "alpha.mine"); found {
		t.Fatal("own trigger still stored after delete")
	}
}
