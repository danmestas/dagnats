package configfile

// Methodology: pure unit tests for the Load / Validate / convert path.
// No filesystem (callers pass an io.Reader; we use strings.Reader).
// No NATS. Golden-shape assertions exercise both positive and
// negative space per the dagnats coding rules.

import (
	"strings"
	"testing"
)

func TestLoadPolicyRoundTrips(t *testing.T) {
	src := `
policy:
  control_plane:
    grant: [planner, supervisor]
    promote: [supervisor]
`
	cfg, err := Load(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Policy == nil || cfg.Policy.ControlPlane == nil {
		t.Fatal("policy.control_plane must round-trip")
	}
	cp := cfg.Policy.ControlPlane
	if len(cp.Grant) != 2 || cp.Grant[0] != "planner" || cp.Grant[1] != "supervisor" {
		t.Fatalf("grant = %v, want [planner supervisor]", cp.Grant)
	}
	if len(cp.Promote) != 1 || cp.Promote[0] != "supervisor" {
		t.Fatalf("promote = %v, want [supervisor]", cp.Promote)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateRejectsPromoteNotInGrant(t *testing.T) {
	cfg := ConfigFile{Policy: &PolicyYAML{ControlPlane: &ControlPlaneGrantYAML{
		Grant:   []string{"planner"},
		Promote: []string{"supervisor"}, // not in grant
	}}}
	if err := Validate(cfg); err == nil {
		t.Fatal("Validate must reject promote entry not in grant")
	}
	// Negative space: promote ⊆ grant validates.
	cfg.Policy.ControlPlane.Promote = []string{"planner"}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate must accept promote ⊆ grant: %v", err)
	}
}

func TestValidateRejectsPolicyDuplicates(t *testing.T) {
	cfg := ConfigFile{Policy: &PolicyYAML{ControlPlane: &ControlPlaneGrantYAML{
		Grant: []string{"planner", "planner"},
	}}}
	if err := Validate(cfg); err == nil {
		t.Fatal("Validate must reject duplicate grant entries")
	}
}

func TestValidateRejectsPolicyEmptyString(t *testing.T) {
	cfg := ConfigFile{Policy: &PolicyYAML{ControlPlane: &ControlPlaneGrantYAML{
		Grant: []string{"planner", ""},
	}}}
	if err := Validate(cfg); err == nil {
		t.Fatal("Validate must reject empty grant entry")
	}
}

func TestLoadValidYAML(t *testing.T) {
	src := `
workflows:
  - name: hello
    version: "1"
    steps:
      - id: greet
        task: echo
triggers:
  - id: hello-cron
    workflow_id: hello
    enabled: true
    cron:
      expression: "*/5 * * * *"
      timezone: UTC
`
	cfg, err := Load(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Workflows) != 1 {
		t.Fatalf("workflows len = %d, want 1", len(cfg.Workflows))
	}
	if cfg.Workflows[0].Name != "hello" {
		t.Fatalf("workflow name = %q, want hello",
			cfg.Workflows[0].Name)
	}
	if len(cfg.Triggers) != 1 {
		t.Fatalf("triggers len = %d, want 1", len(cfg.Triggers))
	}
	tr := cfg.Triggers[0]
	if tr.ID != "hello-cron" || tr.WorkflowID != "hello" {
		t.Fatalf("trigger id/wf = %q/%q, want hello-cron/hello",
			tr.ID, tr.WorkflowID)
	}
	if tr.Cron == nil || tr.Cron.Expression != "*/5 * * * *" {
		t.Fatalf("cron expr = %v, want */5 * * * *", tr.Cron)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestLoadIgnoresLegacyServerKeys(t *testing.T) {
	// The same dagnats.yaml carries server.Config keys at the top
	// level. yaml.v3 with KnownFields(false) tolerates them.
	src := `
data_dir: /tmp/foo
http_addr: 127.0.0.1:9999
workflows:
  - name: hello
    steps:
      - id: a
        task: echo
`
	cfg, err := Load(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Load with legacy keys: %v", err)
	}
	if len(cfg.Workflows) != 1 {
		t.Fatalf("workflows len = %d, want 1", len(cfg.Workflows))
	}
}

func TestLoadEmptyFile(t *testing.T) {
	cfg, err := Load(strings.NewReader(""))
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if len(cfg.Workflows) != 0 || len(cfg.Triggers) != 0 {
		t.Fatalf("empty file should yield zero entries, got %+v",
			cfg)
	}
}

func TestLoadInvalidYAMLReportsLine(t *testing.T) {
	// Unterminated string at line 3 produces a yaml.v3 error
	// whose message includes the line number.
	src := "workflows:\n  - name: hello\n    steps: [\n"
	_, err := Load(strings.NewReader(src))
	if err == nil {
		t.Fatalf("Load invalid yaml: want error, got nil")
	}
	if !strings.Contains(err.Error(), "line") {
		t.Fatalf("error %q missing line number", err)
	}
}

func TestValidateRejectsEmptyWorkflowName(t *testing.T) {
	cfg := ConfigFile{
		Workflows: []WorkflowYAML{
			{Steps: []StepYAML{{ID: "a", Task: "echo"}}},
		},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatalf("Validate: want error for empty name")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Fatalf("error %q does not mention name", err)
	}
}

func TestValidateRejectsDuplicateWorkflowName(t *testing.T) {
	cfg := ConfigFile{
		Workflows: []WorkflowYAML{
			{Name: "x", Steps: []StepYAML{{ID: "a", Task: "echo"}}},
			{Name: "x", Steps: []StepYAML{{ID: "b", Task: "echo"}}},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("Validate: want duplicate-name error")
	}
}

func TestValidateRejectsTriggerWithMultipleKinds(t *testing.T) {
	cfg := ConfigFile{
		Workflows: []WorkflowYAML{
			{Name: "wf", Steps: []StepYAML{{ID: "a", Task: "echo"}}},
		},
		Triggers: []TriggerYAML{
			{
				ID: "bad", WorkflowID: "wf",
				Cron:    &CronYAML{Expression: "* * * * *"},
				Subject: &SubjectYAML{Subject: "events.>"},
			},
		},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatalf("Validate: want exactly-one-kind error")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error %q missing 'exactly one'", err)
	}
}

func TestValidateRejectsTriggerWithUnknownWorkflow(t *testing.T) {
	cfg := ConfigFile{
		Triggers: []TriggerYAML{
			{
				ID: "t1", WorkflowID: "missing",
				Cron: &CronYAML{Expression: "* * * * *"},
			},
		},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatalf("Validate: want cross-reference error")
	}
	if !strings.Contains(err.Error(), "not declared") {
		t.Fatalf("error %q missing 'not declared'", err)
	}
}

func TestToTriggerDefSetsSource(t *testing.T) {
	tr := TriggerYAML{
		ID: "x", WorkflowID: "wf", Enabled: true,
		Cron: &CronYAML{Expression: "* * * * *"},
	}
	def := ToTriggerDef(tr, SourceLabel("dagnats.yaml"))
	want := SourceFilePrefix + "dagnats.yaml"
	if def.Source != want {
		t.Fatalf("Source = %q, want %q", def.Source, want)
	}
	if def.Cron == nil || def.Cron.Expression != "* * * * *" {
		t.Fatalf("Cron = %+v, want expression set", def.Cron)
	}
}

func TestToWorkflowDefMapsSteps(t *testing.T) {
	wf := WorkflowYAML{
		Name: "x",
		Steps: []StepYAML{
			{ID: "a", Task: "echo"},
			{ID: "b", Task: "noop", Type: "sleep"},
		},
	}
	def := ToWorkflowDef(wf)
	if def.Name != "x" || len(def.Steps) != 2 {
		t.Fatalf("WorkflowDef = %+v", def)
	}
	if def.Steps[1].Type.String() != "sleep" {
		t.Fatalf("step[1].Type = %s, want sleep",
			def.Steps[1].Type)
	}
}
