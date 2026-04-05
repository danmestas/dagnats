package trigger

// Methodology: test validation rules for TriggerDef. Each test covers
// one rule with positive and negative cases.

import (
	"strings"
	"testing"
	"time"
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

func TestValidateRejectsEmptyCronExpression(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: ""},
	}
	err := Validate(def)

	// Positive: error returned
	if err == nil {
		t.Fatalf("expected error for empty cron expression")
	}
	// Positive: mentions "expression"
	if !strings.Contains(err.Error(), "expression") {
		t.Fatalf("error = %q, should mention expression", err)
	}
}

func TestValidateRejectsInvalidCronExpression(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "bad cron"},
	}
	err := Validate(def)

	// Positive: error returned
	if err == nil {
		t.Fatalf("expected error for invalid cron expression")
	}
	// Negative: valid expression does not error
	def.Cron.Expression = "*/5 * * * *"
	if err := Validate(def); err != nil {
		t.Fatalf("valid expression rejected: %v", err)
	}
}

func TestValidateRejectsEmptyWebhookPath(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Webhook: &WebhookConfig{Path: ""},
	}
	err := Validate(def)

	// Positive: error returned
	if err == nil {
		t.Fatalf("expected error for empty webhook path")
	}
	// Positive: mentions "path"
	if !strings.Contains(err.Error(), "path") {
		t.Fatalf("error = %q, should mention path", err)
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

func TestValidateAllThreeTypes(t *testing.T) {
	def := TriggerDef{
		ID: "t1", WorkflowID: "wf", Enabled: true,
		Cron:    &CronConfig{Expression: "* * * * *"},
		Subject: &SubjectConfig{Subject: "foo"},
		Webhook: &WebhookConfig{Path: "/bar"},
	}
	err := Validate(def)

	// Positive: error returned for 3 types
	if err == nil {
		t.Fatalf("expected error for all three types set")
	}
	// Positive: mentions count mismatch
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateDebounceValid(t *testing.T) {
	def := TriggerDef{
		ID: "d1", WorkflowID: "wf",
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: 5 * time.Second},
	}
	// Positive: valid debounce config
	if err := Validate(def); err != nil {
		t.Fatalf("valid debounce rejected: %v", err)
	}
}

func TestValidateDebounceWithTimeout(t *testing.T) {
	def := TriggerDef{
		ID: "d2", WorkflowID: "wf",
		Subject: &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{
			Period:  5 * time.Second,
			Timeout: 30 * time.Second,
		},
	}
	// Positive: timeout >= period is valid
	if err := Validate(def); err != nil {
		t.Fatalf("valid debounce+timeout rejected: %v", err)
	}
}

func TestValidateDebounceRejectsCron(t *testing.T) {
	def := TriggerDef{
		ID: "d3", WorkflowID: "wf",
		Cron:     &CronConfig{Expression: "* * * * *"},
		Debounce: &DebounceConfig{Period: 5 * time.Second},
	}
	err := Validate(def)
	// Positive: cron+debounce rejected
	if err == nil {
		t.Fatal("expected error for cron+debounce")
	}
	if !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("error = %q", err)
	}
}

func TestValidateDebounceZeroPeriod(t *testing.T) {
	def := TriggerDef{
		ID: "d4", WorkflowID: "wf",
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: 0},
	}
	// Positive: zero period rejected
	if err := Validate(def); err == nil {
		t.Fatal("expected error for zero period")
	}
}

func TestValidateDebounceTimeoutLessThanPeriod(t *testing.T) {
	def := TriggerDef{
		ID: "d5", WorkflowID: "wf",
		Subject: &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{
			Period:  10 * time.Second,
			Timeout: 5 * time.Second,
		},
	}
	// Positive: timeout < period rejected
	if err := Validate(def); err == nil {
		t.Fatal("expected error for timeout < period")
	}
}
