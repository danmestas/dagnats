package trigger

// Methodology: test validation rules for TriggerDef. Each test covers
// one rule with positive and negative cases.

import (
	"strings"
	"testing"
)

func TestValidateRejectsNoTriggerType(t *testing.T) {
	def := TriggerDef{ID: "t1", WorkflowID: "wf", Enabled: true}
	err := Validate(def)

	// Positive: error returned
	if err == nil {
		t.Fatalf("expected error for no trigger type")
	}
	// Positive: mentions "exactly one"
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %q, should mention 'exactly one'", err)
	}
}

func TestValidateRejectsMultipleTriggerTypes(t *testing.T) {
	def := TriggerDef{
		ID: "t2", WorkflowID: "wf", Enabled: true,
		Cron:    &CronConfig{Expression: "* * * * *"},
		Subject: &SubjectConfig{Subject: "foo"},
	}
	err := Validate(def)

	if err == nil {
		t.Fatalf("expected error for multiple trigger types")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateRejectsEmptyID(t *testing.T) {
	def := TriggerDef{
		WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	if err := Validate(def); err == nil {
		t.Fatalf("expected error for empty ID")
	}
}

func TestValidateRejectsEmptyWorkflowID(t *testing.T) {
	def := TriggerDef{
		ID: "t1", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	if err := Validate(def); err == nil {
		t.Fatalf("expected error for empty WorkflowID")
	}
}

func TestValidateRejectsEmptySubject(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Subject: &SubjectConfig{Subject: ""},
	}
	if err := Validate(def); err == nil {
		t.Fatalf("expected error for empty subject")
	}
}

func TestValidateRejectsWebhookPathWithoutSlash(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Webhook: &WebhookConfig{Path: "no-slash"},
	}
	if err := Validate(def); err == nil {
		t.Fatalf("expected error for path without /")
	}
}

func TestValidateAcceptsValidCron(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "0 9 * * 1-5"},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid cron rejected: %v", err)
	}
}

func TestValidateAcceptsValidSubject(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Subject: &SubjectConfig{Subject: "events.deploy.>"},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid subject rejected: %v", err)
	}
}

func TestValidateAcceptsValidWebhook(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Webhook: &WebhookConfig{Path: "/hooks/deploy"},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("valid webhook rejected: %v", err)
	}
}
