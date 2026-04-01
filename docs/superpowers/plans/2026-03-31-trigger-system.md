# Trigger System — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add automatic workflow triggering via cron schedules, NATS subject subscriptions, and HTTP webhooks — all producing standard `workflow.started` events.

**Architecture:** New `trigger/` package with types, validation, cron scheduler, NATS subject subscriber, webhook HTTP handler, and a TriggerService that coordinates them all. Trigger definitions stored in `triggers` KV bucket, cron state in `trigger_state` KV. All triggers produce a `TriggerEnvelope` as workflow input. Cron parsing is in-house (~80 LOC, no external dependency).

**Tech Stack:** Go, NATS JetStream KV, stdlib `net/http`, stdlib `crypto/hmac`

**Spec:** `docs/superpowers/specs/2026-03-31-trigger-system-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `trigger/types.go` | TriggerDef, CronConfig, SubjectConfig, WebhookConfig, TriggerEnvelope |
| `trigger/types_test.go` | JSON round-trip, envelope construction |
| `trigger/validate.go` | TriggerDef validation rules |
| `trigger/validate_test.go` | Positive + negative validation tests |
| `trigger/cron.go` | Cron expression parser and next-fire-time calculator |
| `trigger/cron_test.go` | Parsing, matching, next-time tests |
| `trigger/scheduler.go` | CronScheduler: tick loop, dedup, backfill |
| `trigger/scheduler_test.go` | Integration tests with embedded NATS |
| `trigger/subject.go` | SubjectTrigger: NATS subscriber → workflow start |
| `trigger/subject_test.go` | Integration tests with embedded NATS |
| `trigger/webhook.go` | WebhookHandler: HTTP→NATS with HMAC validation |
| `trigger/webhook_test.go` | HTTP handler tests (httptest) |
| `trigger/service.go` | TriggerService: lifecycle, KV watcher, coordination |
| `trigger/service_test.go` | Full integration tests |

---

## Chunk 1: Types, Validation, and Cron Parser

### Task 1: Trigger types and envelope

**Files:**
- Create: `trigger/types.go`
- Test: `trigger/types_test.go`

- [ ] **Step 1: Write failing test for TriggerDef JSON round-trip**

Create `trigger/types_test.go`:

```go
package trigger

// Methodology: unit tests for trigger types. No NATS dependency.
// Each test verifies JSON round-trip and envelope construction.

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTriggerDefCronJSON(t *testing.T) {
	def := TriggerDef{
		ID:         "t1",
		WorkflowID: "deploy-wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "0 9 * * 1-5",
			Timezone:   "America/Denver",
			Backfill:   true,
		},
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got TriggerDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: fields round-trip
	if got.Cron.Expression != "0 9 * * 1-5" {
		t.Fatalf("Expression = %q, want %q",
			got.Cron.Expression, "0 9 * * 1-5")
	}
	if !got.Cron.Backfill {
		t.Fatalf("Backfill should be true")
	}

	// Positive: Subject and Webhook are nil
	if got.Subject != nil {
		t.Fatalf("Subject should be nil for cron trigger")
	}
	if got.Webhook != nil {
		t.Fatalf("Webhook should be nil for cron trigger")
	}
}

func TestTriggerEnvelopeJSON(t *testing.T) {
	env := TriggerEnvelope{
		Trigger:   "cron",
		Source:    "0 9 * * 1-5",
		Timestamp: time.Date(2026, 3, 31, 9, 0, 0, 0, time.UTC),
		Data:      nil,
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got TriggerEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: trigger type preserved
	if got.Trigger != "cron" {
		t.Fatalf("Trigger = %q, want cron", got.Trigger)
	}

	// Positive: nil data omitted
	if got.Data != nil {
		t.Fatalf("Data should be nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestTrigger -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement types**

Create `trigger/types.go`:

```go
// trigger/types.go
// The trigger package provides automatic workflow triggering via cron
// schedules, NATS subject subscriptions, and HTTP webhooks. All trigger
// types produce standard workflow.started events on the history stream.
package trigger

import (
	"encoding/json"
	"time"
)

// TriggerDef defines a single trigger. Exactly one of Cron, Subject,
// or Webhook must be non-nil.
type TriggerDef struct {
	ID         string         `json:"id"`
	WorkflowID string        `json:"workflow_id"`
	Enabled    bool           `json:"enabled"`
	Cron       *CronConfig    `json:"cron,omitempty"`
	Subject    *SubjectConfig `json:"subject,omitempty"`
	Webhook    *WebhookConfig `json:"webhook,omitempty"`
}

// CronConfig defines a cron-scheduled trigger.
type CronConfig struct {
	Expression string `json:"expression"`
	Timezone   string `json:"timezone"`
	Backfill   bool   `json:"backfill"`
}

// SubjectConfig defines a NATS subject trigger.
type SubjectConfig struct {
	Subject string `json:"subject"`
}

// WebhookConfig defines an HTTP webhook trigger.
type WebhookConfig struct {
	Path   string `json:"path"`
	Secret string `json:"secret,omitempty"`
}

// TriggerEnvelope is the standard workflow input produced by all
// trigger types. Workflows always know how they were triggered.
type TriggerEnvelope struct {
	Trigger   string          `json:"trigger"`
	Source    string          `json:"source"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/types.go trigger/types_test.go
git commit -m "feat(trigger): add TriggerDef, CronConfig, SubjectConfig, WebhookConfig, Envelope types"
```

---

### Task 2: TriggerDef validation

**Files:**
- Create: `trigger/validate.go`
- Test: `trigger/validate_test.go`

- [ ] **Step 1: Write failing validation tests**

Create `trigger/validate_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestValidate -v`
Expected: FAIL — `Validate` undefined

- [ ] **Step 3: Implement validation**

Create `trigger/validate.go`:

```go
package trigger

import "fmt"

// Validate checks a TriggerDef for structural correctness.
// Returns nil if valid, descriptive error otherwise.
func Validate(def TriggerDef) error {
	if def.ID == "" {
		return fmt.Errorf("trigger ID must not be empty")
	}
	if def.WorkflowID == "" {
		return fmt.Errorf("trigger %q: workflow_id must not be empty",
			def.ID)
	}

	count := countTriggerTypes(def)
	if count != 1 {
		return fmt.Errorf(
			"trigger %q: exactly one of cron/subject/webhook "+
				"must be set (got %d)", def.ID, count)
	}

	if def.Cron != nil {
		if err := validateCronConfig(def.ID, def.Cron); err != nil {
			return err
		}
	}
	if def.Subject != nil {
		if def.Subject.Subject == "" {
			return fmt.Errorf(
				"trigger %q: subject must not be empty", def.ID)
		}
	}
	if def.Webhook != nil {
		if err := validateWebhookConfig(def.ID, def.Webhook); err != nil {
			return err
		}
	}
	return nil
}

func countTriggerTypes(def TriggerDef) int {
	count := 0
	if def.Cron != nil {
		count++
	}
	if def.Subject != nil {
		count++
	}
	if def.Webhook != nil {
		count++
	}
	return count
}

func validateCronConfig(id string, c *CronConfig) error {
	if c.Expression == "" {
		return fmt.Errorf(
			"trigger %q: cron expression must not be empty", id)
	}
	_, err := ParseCron(c.Expression)
	if err != nil {
		return fmt.Errorf(
			"trigger %q: invalid cron expression: %w", id, err)
	}
	return nil
}

func validateWebhookConfig(id string, w *WebhookConfig) error {
	if w.Path == "" {
		return fmt.Errorf(
			"trigger %q: webhook path must not be empty", id)
	}
	if w.Path[0] != '/' {
		return fmt.Errorf(
			"trigger %q: webhook path must start with /", id)
	}
	return nil
}
```

Note: `ParseCron` will be implemented in Task 3. For now add a stub at the bottom of `validate.go`:

```go
// ParseCron is defined in cron.go — stub here for compilation.
// Remove this stub once cron.go is created.
```

Actually, implement Tasks 2 and 3 together since validate depends on ParseCron. Write the cron.go first (step 3a below), then validate.go.

- [ ] **Step 3a: Create cron.go stub so validate.go compiles**

Create `trigger/cron.go` with just the `ParseCron` function signature and `CronExpr` type (full implementation in Task 3):

```go
package trigger

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronExpr is a parsed 5-field cron expression.
type CronExpr struct {
	Minutes    []int // 0-59
	Hours      []int // 0-23
	DaysOfMonth []int // 1-31
	Months     []int // 1-12
	DaysOfWeek []int // 0-6 (0=Sunday)
}

// ParseCron parses a 5-field cron expression into a CronExpr.
// Supports *, */N, N-M, and comma-separated values.
func ParseCron(expr string) (*CronExpr, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf(
			"expected 5 fields, got %d", len(fields))
	}

	minutes, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute field: %w", err)
	}
	hours, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour field: %w", err)
	}
	dom, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day-of-month field: %w", err)
	}
	months, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month field: %w", err)
	}
	dow, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("day-of-week field: %w", err)
	}

	return &CronExpr{
		Minutes:     minutes,
		Hours:       hours,
		DaysOfMonth: dom,
		Months:      months,
		DaysOfWeek:  dow,
	}, nil
}

// Matches returns true if the given time matches this cron expression.
func (c *CronExpr) Matches(t time.Time) bool {
	return contains(c.Minutes, t.Minute()) &&
		contains(c.Hours, t.Hour()) &&
		contains(c.DaysOfMonth, t.Day()) &&
		contains(c.Months, int(t.Month())) &&
		contains(c.DaysOfWeek, int(t.Weekday()))
}

func contains(vals []int, target int) bool {
	for _, v := range vals {
		if v == target {
			return true
		}
	}
	return false
}

// parseField parses one cron field (*, */N, N-M, N, comma-separated).
func parseField(field string, min, max int) ([]int, error) {
	if field == "*" {
		return rangeInts(min, max), nil
	}

	// Handle comma-separated values
	if strings.Contains(field, ",") {
		var result []int
		for _, part := range strings.Split(field, ",") {
			vals, err := parseField(part, min, max)
			if err != nil {
				return nil, err
			}
			result = append(result, vals...)
		}
		return result, nil
	}

	// Handle */N (step)
	if strings.HasPrefix(field, "*/") {
		stepStr := field[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step: %q", field)
		}
		var result []int
		for i := min; i <= max; i += step {
			result = append(result, i)
		}
		return result, nil
	}

	// Handle N-M (range)
	if strings.Contains(field, "-") {
		parts := strings.SplitN(field, "-", 2)
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid range start: %q", field)
		}
		hi, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid range end: %q", field)
		}
		if lo < min || hi > max || lo > hi {
			return nil, fmt.Errorf("range out of bounds: %q", field)
		}
		return rangeInts(lo, hi), nil
	}

	// Single value
	val, err := strconv.Atoi(field)
	if err != nil {
		return nil, fmt.Errorf("invalid value: %q", field)
	}
	if val < min || val > max {
		return nil, fmt.Errorf(
			"value %d out of range [%d, %d]", val, min, max)
	}
	return []int{val}, nil
}

func rangeInts(min, max int) []int {
	result := make([]int, 0, max-min+1)
	for i := min; i <= max; i++ {
		result = append(result, i)
	}
	return result
}
```

- [ ] **Step 4: Run all trigger tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/types.go trigger/types_test.go trigger/validate.go trigger/validate_test.go trigger/cron.go
git commit -m "feat(trigger): add types, validation, and cron expression parser"
```

---

### Task 3: Cron parser tests

**Files:**
- Create: `trigger/cron_test.go`

- [ ] **Step 1: Write cron parser tests**

Create `trigger/cron_test.go`:

```go
package trigger

// Methodology: unit tests for cron expression parsing and matching.
// No NATS dependency — pure time logic.

import (
	"testing"
	"time"
)

func TestParseCronEveryMinute(t *testing.T) {
	expr, err := ParseCron("* * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: matches any time
	now := time.Date(2026, 3, 31, 14, 30, 0, 0, time.UTC)
	if !expr.Matches(now) {
		t.Fatalf("* * * * * should match any time")
	}

	// Positive: 60 minute values
	if len(expr.Minutes) != 60 {
		t.Fatalf("minutes = %d, want 60", len(expr.Minutes))
	}
}

func TestParseCronWeekdayMorning(t *testing.T) {
	expr, err := ParseCron("0 9 * * 1-5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: Monday 9am matches
	mon9 := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC) // Monday
	if !expr.Matches(mon9) {
		t.Fatalf("should match Monday 9am")
	}

	// Negative: Sunday 9am does not match
	sun9 := time.Date(2026, 3, 29, 9, 0, 0, 0, time.UTC) // Sunday
	if expr.Matches(sun9) {
		t.Fatalf("should not match Sunday")
	}

	// Negative: Monday 10am does not match
	mon10 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if expr.Matches(mon10) {
		t.Fatalf("should not match 10am")
	}
}

func TestParseCronEvery5Minutes(t *testing.T) {
	expr, err := ParseCron("*/5 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: minute 0 matches
	if !expr.Matches(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("minute 0 should match */5")
	}

	// Positive: minute 15 matches
	if !expr.Matches(time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)) {
		t.Fatalf("minute 15 should match */5")
	}

	// Negative: minute 3 does not match
	if expr.Matches(time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)) {
		t.Fatalf("minute 3 should not match */5")
	}
}

func TestParseCronCommaList(t *testing.T) {
	expr, err := ParseCron("0,30 * * * *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Positive: minute 0 and 30 match
	if len(expr.Minutes) != 2 {
		t.Fatalf("minutes = %d, want 2", len(expr.Minutes))
	}

	// Negative: minute 15 does not
	if expr.Matches(time.Date(2026, 1, 1, 0, 15, 0, 0, time.UTC)) {
		t.Fatalf("minute 15 should not match 0,30")
	}
}

func TestParseCronRejectsInvalid(t *testing.T) {
	bad := []string{
		"",
		"* * *",
		"* * * * * *",
		"60 * * * *",
		"* 25 * * *",
		"abc * * * *",
	}
	for _, expr := range bad {
		_, err := ParseCron(expr)
		if err == nil {
			t.Errorf("expected error for %q", expr)
		}
	}
}
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestParseCron -v`
Expected: PASS (cron.go already implements ParseCron)

- [ ] **Step 3: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/cron_test.go
git commit -m "test(trigger): cron expression parsing and matching tests"
```

---

## Chunk 2: Scheduler, Subject Trigger, Webhook

### Task 4: Cron scheduler

**Files:**
- Create: `trigger/scheduler.go`
- Test: `trigger/scheduler_test.go`

- [ ] **Step 1: Write failing test for scheduler firing a cron trigger**

Create `trigger/scheduler_test.go`:

```go
package trigger

// Methodology: integration tests for the cron scheduler. Uses
// real embedded NATS to verify workflow.started events appear
// on the history stream when cron triggers fire.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestSchedulerFiresCronTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()

	// Subscribe to history stream to catch workflow.started
	sub, err := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Create scheduler with a trigger that matches now
	sched := NewScheduler(nc)
	def := TriggerDef{
		ID: "t1", WorkflowID: "test-wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	sched.AddTrigger(def)

	// Tick manually (don't wait for real timer)
	sched.Tick(time.Now())

	// Wait for event
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event: %v", err)
	}
	msg.Ack()

	evt, _ := protocol.UnmarshalEvent(msg.Data)
	// Positive: event is workflow.started
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}

	// Positive: payload is TriggerEnvelope
	var env TriggerEnvelope
	if err := json.Unmarshal(evt.Payload, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Trigger != "cron" {
		t.Fatalf("trigger = %q, want cron", env.Trigger)
	}
}

func TestSchedulerDeduplicatesSameMinute(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	sched := NewScheduler(nc)
	def := TriggerDef{
		ID: "t1", WorkflowID: "test-wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	sched.AddTrigger(def)

	now := time.Now().Truncate(time.Minute)
	sched.Tick(now)
	sched.Tick(now) // Same minute — should be deduped

	// First message should arrive
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected first event: %v", err)
	}
	msg.Ack()

	// Second should NOT arrive (dedup)
	msg2, err := sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		msg2.Ack()
		t.Fatalf("expected no second event (dedup), but got one")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestScheduler -v -timeout 30s`
Expected: FAIL — `NewScheduler` undefined

- [ ] **Step 3: Implement scheduler**

Create `trigger/scheduler.go`:

```go
package trigger

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// Scheduler manages cron triggers. Call Tick() periodically (every 30s)
// or use Start() for an auto-ticking goroutine.
type Scheduler struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	triggers map[string]cronEntry
	stateKV  nats.KeyValue
}

type cronEntry struct {
	def  TriggerDef
	expr *CronExpr
}

// NewScheduler creates a cron scheduler bound to the given NATS
// connection. The trigger_state KV bucket must exist.
func NewScheduler(nc *nats.Conn) *Scheduler {
	if nc == nil {
		panic("NewScheduler: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewScheduler: JetStream: " + err.Error())
	}
	stateKV, err := js.KeyValue("trigger_state")
	if err != nil {
		panic("NewScheduler: trigger_state bucket: " + err.Error())
	}
	return &Scheduler{
		nc:       nc,
		js:       js,
		triggers: make(map[string]cronEntry),
		stateKV:  stateKV,
	}
}

// AddTrigger registers a cron trigger. Panics if def.Cron is nil.
func (s *Scheduler) AddTrigger(def TriggerDef) {
	if def.Cron == nil {
		panic("Scheduler.AddTrigger: Cron must not be nil")
	}
	expr, err := ParseCron(def.Cron.Expression)
	if err != nil {
		panic("Scheduler.AddTrigger: bad cron: " + err.Error())
	}
	s.triggers[def.ID] = cronEntry{def: def, expr: expr}
}

// RemoveTrigger unregisters a trigger by ID.
func (s *Scheduler) RemoveTrigger(id string) {
	delete(s.triggers, id)
}

// Tick checks all triggers against the given time and fires matches.
// Call this every 30 seconds or use Start() for auto-ticking.
func (s *Scheduler) Tick(now time.Time) {
	for _, entry := range s.triggers {
		if !entry.def.Enabled {
			continue
		}
		t := s.resolveTime(now, entry.def.Cron)
		if !entry.expr.Matches(t) {
			continue
		}
		s.fire(entry.def, t)
	}
}

func (s *Scheduler) resolveTime(
	now time.Time, cfg *CronConfig,
) time.Time {
	if cfg.Timezone == "" || cfg.Timezone == "UTC" {
		return now.UTC()
	}
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return now.UTC()
	}
	return now.In(loc)
}

// fire publishes a workflow.started event for the trigger.
func (s *Scheduler) fire(def TriggerDef, t time.Time) {
	env := TriggerEnvelope{
		Trigger:   "cron",
		Source:    def.Cron.Expression,
		Timestamp: t,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return
	}

	runID := nuid.Next()
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return
	}

	// Dedup key: trigger ID + minute timestamp
	minuteKey := t.Truncate(time.Minute).Format(time.RFC3339)
	msgID := fmt.Sprintf("trigger.%s.%s", def.ID, minuteKey)

	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	s.js.PublishMsg(msg)

	// Update last-run state
	s.stateKV.Put(def.ID, []byte(minuteKey))
}

// Start begins auto-ticking every 30 seconds. Returns a stop func.
func (s *Scheduler) Start() func() {
	ticker := time.NewTicker(30 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case t := <-ticker.C:
				s.Tick(t)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestScheduler -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/scheduler.go trigger/scheduler_test.go
git commit -m "feat(trigger): add cron Scheduler with dedup and timezone support"
```

---

### Task 5: NATS subject trigger

**Files:**
- Create: `trigger/subject.go`
- Test: `trigger/subject_test.go`

- [ ] **Step 1: Write failing test for subject trigger**

Create `trigger/subject_test.go`:

```go
package trigger

// Methodology: integration test with embedded NATS. Publish a message
// to a trigger subject, verify workflow.started event appears.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestSubjectTriggerStartsWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithStreams(
			natsutil.StreamConfig{
				Name:     "TRIGGER_EVENTS",
				Subjects: []string{"events.>"},
			},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	st := NewSubjectTrigger(nc)
	def := TriggerDef{
		ID: "st1", WorkflowID: "deploy-wf", Enabled: true,
		Subject: &SubjectConfig{Subject: "events.deploy.done"},
	}
	if err := st.AddTrigger(def); err != nil {
		t.Fatalf("add trigger: %v", err)
	}
	defer st.RemoveTrigger("st1")

	// Publish event on the trigger subject
	js.Publish("events.deploy.done", []byte(`{"sha":"abc123"}`))

	// Wait for workflow.started
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started: %v", err)
	}
	msg.Ack()

	evt, _ := protocol.UnmarshalEvent(msg.Data)
	// Positive: correct event type
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}

	// Positive: envelope has nats trigger type and original data
	var env TriggerEnvelope
	json.Unmarshal(evt.Payload, &env)
	if env.Trigger != "nats" {
		t.Fatalf("trigger = %q, want nats", env.Trigger)
	}
	if string(env.Data) != `{"sha":"abc123"}` {
		t.Fatalf("data = %q", string(env.Data))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestSubjectTrigger -v -timeout 30s`
Expected: FAIL — `NewSubjectTrigger` undefined

- [ ] **Step 3: Implement subject trigger**

Create `trigger/subject.go`:

```go
package trigger

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// SubjectTrigger subscribes to NATS subjects and starts workflows
// when messages arrive. One subscription per trigger definition.
type SubjectTrigger struct {
	nc   *nats.Conn
	js   nats.JetStreamContext
	subs map[string]*nats.Subscription
}

// NewSubjectTrigger creates a subject trigger manager.
func NewSubjectTrigger(nc *nats.Conn) *SubjectTrigger {
	if nc == nil {
		panic("NewSubjectTrigger: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewSubjectTrigger: JetStream: " + err.Error())
	}
	return &SubjectTrigger{
		nc:   nc,
		js:   js,
		subs: make(map[string]*nats.Subscription),
	}
}

// AddTrigger subscribes to the configured NATS subject.
func (st *SubjectTrigger) AddTrigger(
	def TriggerDef,
) error {
	if def.Subject == nil {
		panic("SubjectTrigger.AddTrigger: Subject must not be nil")
	}

	sub, err := st.js.Subscribe(
		def.Subject.Subject,
		st.makeHandler(def),
		nats.AckExplicit(),
		nats.DeliverAll(),
	)
	if err != nil {
		return fmt.Errorf("subscribe %q: %w",
			def.Subject.Subject, err)
	}
	st.subs[def.ID] = sub
	return nil
}

// RemoveTrigger unsubscribes the trigger.
func (st *SubjectTrigger) RemoveTrigger(id string) {
	if sub, ok := st.subs[id]; ok {
		sub.Unsubscribe()
		delete(st.subs, id)
	}
}

// StopAll unsubscribes all triggers.
func (st *SubjectTrigger) StopAll() {
	for id := range st.subs {
		st.RemoveTrigger(id)
	}
}

func (st *SubjectTrigger) makeHandler(
	def TriggerDef,
) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		env := TriggerEnvelope{
			Trigger:   "nats",
			Source:    def.Subject.Subject,
			Timestamp: time.Now().UTC(),
			Data:      msg.Data,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			msg.NakWithDelay(5 * time.Second)
			return
		}

		runID := nuid.Next()
		evt := protocol.NewWorkflowEvent(
			protocol.EventWorkflowStarted, runID, payload)
		data, err := evt.Marshal()
		if err != nil {
			msg.NakWithDelay(5 * time.Second)
			return
		}

		pubMsg := &nats.Msg{
			Subject: evt.NATSSubject(),
			Data:    data,
			Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
		}
		if _, err := st.js.PublishMsg(pubMsg); err != nil {
			msg.NakWithDelay(5 * time.Second)
			return
		}
		msg.Ack()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestSubjectTrigger -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/subject.go trigger/subject_test.go
git commit -m "feat(trigger): add NATS SubjectTrigger for event-driven workflow starts"
```

---

### Task 6: Webhook handler

**Files:**
- Create: `trigger/webhook.go`
- Test: `trigger/webhook_test.go`

- [ ] **Step 1: Write failing test for webhook trigger**

Create `trigger/webhook_test.go`:

```go
package trigger

// Methodology: HTTP handler tests using httptest. Verify HMAC
// validation, body forwarding, and error responses.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestWebhookHandlerAcceptsValidPost(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	wh := NewWebhookHandler(nc)
	def := TriggerDef{
		ID: "wh1", WorkflowID: "deploy-wf", Enabled: true,
		Webhook: &WebhookConfig{Path: "/hooks/deploy"},
	}
	wh.AddTrigger(def)

	req := httptest.NewRequest("POST", "/hooks/deploy",
		strings.NewReader(`{"ref":"main"}`))
	w := httptest.NewRecorder()
	wh.ServeHTTP(w, req)

	// Positive: 202 Accepted
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}

	// Positive: workflow.started appears on history
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected event: %v", err)
	}
	msg.Ack()
}

func TestWebhookHandlerRejects404(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)

	wh := NewWebhookHandler(nc)

	req := httptest.NewRequest("POST", "/unknown", nil)
	w := httptest.NewRecorder()
	wh.ServeHTTP(w, req)

	// Positive: 404
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestWebhookHandlerValidatesHMAC(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	natsutil.SetupAll(nc)

	wh := NewWebhookHandler(nc)
	def := TriggerDef{
		ID: "wh2", WorkflowID: "wf", Enabled: true,
		Webhook: &WebhookConfig{Path: "/hooks/secure", Secret: "s3cret"},
	}
	wh.AddTrigger(def)

	body := `{"event":"push"}`

	// Negative: no signature → 401
	req := httptest.NewRequest("POST", "/hooks/secure",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	wh.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no sig: status = %d, want 401", w.Code)
	}

	// Positive: valid signature → 202
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write([]byte(body))
	sig := hex.EncodeToString(mac.Sum(nil))

	req2 := httptest.NewRequest("POST", "/hooks/secure",
		strings.NewReader(body))
	req2.Header.Set("X-Signature-256", "sha256="+sig)
	w2 := httptest.NewRecorder()
	wh.ServeHTTP(w2, req2)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("valid sig: status = %d, want 202", w2.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestWebhook -v -timeout 30s`
Expected: FAIL — `NewWebhookHandler` undefined

- [ ] **Step 3: Implement webhook handler**

Create `trigger/webhook.go`:

```go
package trigger

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

const maxWebhookBody = 1 << 20 // 1MB

// WebhookHandler serves HTTP webhook endpoints and publishes
// workflow.started events to the history stream.
type WebhookHandler struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	mu       sync.RWMutex
	triggers map[string]TriggerDef // path → def
}

// NewWebhookHandler creates a webhook handler.
func NewWebhookHandler(nc *nats.Conn) *WebhookHandler {
	if nc == nil {
		panic("NewWebhookHandler: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewWebhookHandler: JetStream: " + err.Error())
	}
	return &WebhookHandler{
		nc:       nc,
		js:       js,
		triggers: make(map[string]TriggerDef),
	}
}

// AddTrigger registers a webhook path.
func (wh *WebhookHandler) AddTrigger(def TriggerDef) {
	if def.Webhook == nil {
		panic("WebhookHandler.AddTrigger: Webhook must not be nil")
	}
	wh.mu.Lock()
	wh.triggers[def.Webhook.Path] = def
	wh.mu.Unlock()
}

// RemoveTrigger removes a webhook path by trigger ID.
func (wh *WebhookHandler) RemoveTrigger(id string) {
	wh.mu.Lock()
	for path, def := range wh.triggers {
		if def.ID == id {
			delete(wh.triggers, path)
			break
		}
	}
	wh.mu.Unlock()
}

// ServeHTTP implements http.Handler.
func (wh *WebhookHandler) ServeHTTP(
	w http.ResponseWriter, r *http.Request,
) {
	wh.mu.RLock()
	def, ok := wh.triggers[r.URL.Path]
	wh.mu.RUnlock()

	if !ok {
		http.NotFound(w, r)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	if def.Webhook.Secret != "" {
		if !wh.validateHMAC(body, r.Header.Get("X-Signature-256"),
			def.Webhook.Secret) {
			http.Error(w, "invalid signature",
				http.StatusUnauthorized)
			return
		}
	}

	wh.publishWorkflowStarted(def, body)
	w.WriteHeader(http.StatusAccepted)
}

func (wh *WebhookHandler) validateHMAC(
	body []byte, header, secret string,
) bool {
	if header == "" {
		return false
	}
	sig := strings.TrimPrefix(header, "sha256=")
	expected := hmacSHA256(body, secret)
	return hmac.Equal([]byte(sig), []byte(expected))
}

func hmacSHA256(data []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func (wh *WebhookHandler) publishWorkflowStarted(
	def TriggerDef, body []byte,
) {
	env := TriggerEnvelope{
		Trigger:   "webhook",
		Source:    def.Webhook.Path,
		Timestamp: time.Now().UTC(),
		Data:      body,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return
	}

	runID := nuid.Next()
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payload)
	data, err := evt.Marshal()
	if err != nil {
		return
	}

	msg := &nats.Msg{
		Subject: evt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {evt.NATSMsgID()}},
	}
	wh.js.PublishMsg(msg)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestWebhook -v -timeout 30s`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/webhook.go trigger/webhook_test.go
git commit -m "feat(trigger): add WebhookHandler with HMAC-SHA256 validation"
```

---

## Chunk 3: TriggerService and Full Suite Verification

### Task 7: TriggerService — unified lifecycle and KV watcher

**Files:**
- Create: `trigger/service.go`
- Test: `trigger/service_test.go`

- [ ] **Step 1: Write failing test for TriggerService**

Create `trigger/service_test.go`:

```go
package trigger

// Methodology: integration test with embedded NATS. Verify that
// TriggerService loads triggers from KV and routes to the right handler.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestServiceLoadsCronFromKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
		natsutil.WithStreams(
			natsutil.StreamConfig{
				Name:     "TRIGGER_EVENTS",
				Subjects: []string{"events.>", "webhook.>"},
			},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store a cron trigger in KV
	def := TriggerDef{
		ID: "svc-t1", WorkflowID: "test-wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("svc-t1", defData)

	// Subscribe to catch events
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	// Start service
	svc := NewTriggerService(nc)
	svc.Start()
	defer svc.Stop()

	// Manually tick the scheduler (don't wait 30s)
	svc.TickNow()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected event: %v", err)
	}
	msg.Ack()

	evt, _ := protocol.UnmarshalEvent(msg.Data)
	// Positive: workflow started
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}

	// Positive: from cron trigger
	var env TriggerEnvelope
	json.Unmarshal(evt.Payload, &env)
	if env.Trigger != "cron" {
		t.Fatalf("trigger = %q, want cron", env.Trigger)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -run TestService -v -timeout 30s`
Expected: FAIL — `NewTriggerService` undefined

- [ ] **Step 3: Implement TriggerService**

Create `trigger/service.go`:

```go
package trigger

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

const maxActiveTriggers = 500

// TriggerService coordinates all trigger types. It loads definitions
// from the triggers KV bucket on startup and watches for live changes.
type TriggerService struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	triggerKV nats.KeyValue
	scheduler *Scheduler
	subjects  *SubjectTrigger
	webhooks  *WebhookHandler
	stopSched func()
	watcher   nats.KeyWatcher
}

// NewTriggerService creates the service. KV buckets must exist.
func NewTriggerService(nc *nats.Conn) *TriggerService {
	if nc == nil {
		panic("NewTriggerService: nc must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewTriggerService: JetStream: " + err.Error())
	}
	triggerKV, err := js.KeyValue("triggers")
	if err != nil {
		panic("NewTriggerService: triggers bucket: " + err.Error())
	}
	return &TriggerService{
		nc:        nc,
		js:        js,
		triggerKV: triggerKV,
		scheduler: NewScheduler(nc),
		subjects:  NewSubjectTrigger(nc),
		webhooks:  NewWebhookHandler(nc),
	}
}

// Start loads triggers from KV, starts all handlers, and begins
// watching for changes.
func (ts *TriggerService) Start() {
	ts.loadAllTriggers()
	ts.stopSched = ts.scheduler.Start()
	ts.startKVWatcher()
}

// Stop terminates all triggers and the KV watcher.
func (ts *TriggerService) Stop() {
	if ts.stopSched != nil {
		ts.stopSched()
	}
	ts.subjects.StopAll()
	if ts.watcher != nil {
		ts.watcher.Stop()
	}
}

// TickNow forces an immediate scheduler tick (for testing).
func (ts *TriggerService) TickNow() {
	ts.scheduler.Tick(now())
}

// WebhookHandler returns the HTTP handler for mounting on a server.
func (ts *TriggerService) WebhookHandler() *WebhookHandler {
	return ts.webhooks
}

func (ts *TriggerService) loadAllTriggers() {
	keys, err := ts.triggerKV.Keys()
	if err != nil {
		return // Empty bucket is fine
	}
	for i, key := range keys {
		if i >= maxActiveTriggers {
			break
		}
		entry, err := ts.triggerKV.Get(key)
		if err != nil {
			continue
		}
		var def TriggerDef
		if err := json.Unmarshal(entry.Value(), &def); err != nil {
			continue
		}
		ts.addTrigger(def)
	}
}

func (ts *TriggerService) addTrigger(def TriggerDef) {
	if err := Validate(def); err != nil {
		return
	}
	if !def.Enabled {
		return
	}
	switch {
	case def.Cron != nil:
		ts.scheduler.AddTrigger(def)
	case def.Subject != nil:
		ts.subjects.AddTrigger(def)
	case def.Webhook != nil:
		ts.webhooks.AddTrigger(def)
	}
}

func (ts *TriggerService) removeTrigger(id string) {
	ts.scheduler.RemoveTrigger(id)
	ts.subjects.RemoveTrigger(id)
	ts.webhooks.RemoveTrigger(id)
}

func (ts *TriggerService) startKVWatcher() {
	watcher, err := ts.triggerKV.WatchAll()
	if err != nil {
		return
	}
	ts.watcher = watcher
	go func() {
		for entry := range watcher.Updates() {
			if entry == nil {
				continue
			}
			if entry.Operation() == nats.KeyValueDelete {
				ts.removeTrigger(entry.Key())
				continue
			}
			var def TriggerDef
			if err := json.Unmarshal(entry.Value(), &def); err != nil {
				continue
			}
			ts.removeTrigger(def.ID)
			ts.addTrigger(def)
		}
	}()
}

// now is a package-level function for testing seam.
var now = defaultNow

func defaultNow() interface{ Truncate(d interface{}) interface{} } {
	return nil
}
```

Actually, simplify `now()`:

```go
import "time"

// timeNow is a testing seam for the current time.
var timeNow = time.Now

func (ts *TriggerService) TickNow() {
	ts.scheduler.Tick(timeNow())
}
```

Replace the `now` variable with `timeNow = time.Now` at package level, and use `timeNow()` in `TickNow()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -v -count=1 -timeout 30s`
Expected: ALL PASS

- [ ] **Step 5: Run full project suite**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support
git add trigger/service.go trigger/service_test.go
git commit -m "feat(trigger): add TriggerService with KV loading and live reload"
```

---

### Task 8: Full test suite verification

- [ ] **Step 1: Run all trigger tests**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./trigger/ -v -count=1 -timeout 30s`

- [ ] **Step 2: Run full project suite**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go test ./... -count=1 -timeout 120s`

- [ ] **Step 3: Verify go vet**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && go vet ./trigger/`

- [ ] **Step 4: Check line counts**

Run: `cd /Users/dmestas/projects/dagnats/.worktrees/feat-core-agent-support && wc -l trigger/*.go`
Expected: ~700 LOC implementation, no file over 200 lines
