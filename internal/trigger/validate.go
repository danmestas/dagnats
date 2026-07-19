package trigger

import (
	"context"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/cronexpr"
	"github.com/nats-io/nats.go/jetstream"
)

// Validate checks a TriggerDef for structural correctness.
// Returns nil if valid, descriptive error otherwise.
// Panics if called with a completely uninitialized def (programmer error).
//
// External triggers cannot be fully validated here because their
// schema lookup requires a KV handle: callers holding an External
// must use ValidateWithKV instead. Validate refuses such defs with
// a clear redirect error rather than silently succeeding.
func Validate(def TriggerDef) error {
	return validateCommon(def, nil)
}

// ValidateWithKV is the KV-aware overload. Behaviour matches Validate
// for non-External defs; for External defs it looks up the kind in
// the trigger_types bucket and validates Config against the registered
// JSON schema. Per-call fetch — no caching layer.
func ValidateWithKV(
	ctx context.Context, kv jetstream.KeyValue, def TriggerDef,
) error {
	if ctx == nil {
		panic("ValidateWithKV: ctx must not be nil")
	}
	return validateCommon(def, &kvHandle{ctx: ctx, kv: kv})
}

// kvHandle bundles the ctx+kv pair so validateCommon stays a single
// param past the def. nil means "no KV available" (Validate path).
type kvHandle struct {
	ctx context.Context
	kv  jetstream.KeyValue
}

func validateCommon(def TriggerDef, kvh *kvHandle) error {
	if def.ID == "" && def.WorkflowID == "" &&
		def.Cron == nil && def.Subject == nil &&
		def.Webhook == nil && def.HTTP == nil &&
		def.External == nil {
		panic("Validate: completely empty TriggerDef is a programmer error")
	}
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
			"trigger %q: exactly one of cron/subject/webhook/"+
				"http/external must be set (got %d)",
			def.ID, count)
	}
	if err := validateTypeBranch(def, kvh); err != nil {
		return err
	}
	if def.Debounce != nil {
		if err := validateDebounceConfig(def); err != nil {
			return err
		}
	}
	return nil
}

func validateTypeBranch(def TriggerDef, kvh *kvHandle) error {
	if def.Cron != nil {
		return validateCronConfig(def.ID, def.Cron)
	}
	if def.Subject != nil {
		if def.Subject.Subject == "" {
			return fmt.Errorf(
				"trigger %q: subject must not be empty", def.ID)
		}
		return nil
	}
	if def.Webhook != nil {
		return validateWebhookConfig(def.ID, def.Webhook)
	}
	if def.HTTP != nil {
		if err := def.HTTP.Validate(); err != nil {
			return fmt.Errorf(
				"trigger %q: http config: %w", def.ID, err,
			)
		}
		return nil
	}
	if def.External != nil {
		return validateExternal(def, kvh)
	}
	return nil
}

const maxDebouncePeriod = 7 * 24 * time.Hour // 7 days

func validateDebounceConfig(def TriggerDef) error {
	if def.ID == "" {
		panic("validateDebounceConfig: def.ID must not be empty")
	}
	if def.Debounce == nil {
		panic("validateDebounceConfig: Debounce must not be nil")
	}
	d := def.Debounce

	if def.Cron != nil {
		return fmt.Errorf(
			"trigger %q: debounce is incompatible with cron",
			def.ID,
		)
	}
	if d.Period <= 0 {
		return fmt.Errorf(
			"trigger %q: debounce period must be > 0", def.ID,
		)
	}
	if d.Period > maxDebouncePeriod {
		return fmt.Errorf(
			"trigger %q: debounce period exceeds 7 days", def.ID,
		)
	}
	if d.Timeout != 0 && d.Timeout < d.Period {
		return fmt.Errorf(
			"trigger %q: debounce timeout must be >= period",
			def.ID,
		)
	}
	if d.Timeout > maxDebouncePeriod {
		return fmt.Errorf(
			"trigger %q: debounce timeout exceeds 7 days", def.ID,
		)
	}
	return nil
}

func countTriggerTypes(def TriggerDef) int {
	if def.ID == "" {
		panic("countTriggerTypes: def.ID must not be empty")
	}
	if def.WorkflowID == "" {
		panic("countTriggerTypes: def.WorkflowID must not be empty")
	}

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
	if def.HTTP != nil {
		count++
	}
	if def.External != nil {
		count++
	}
	return count
}

func validateCronConfig(id string, c *CronConfig) error {
	if c == nil {
		panic("validateCronConfig: CronConfig must not be nil")
	}
	if id == "" {
		panic("validateCronConfig: id must not be empty")
	}

	if c.Expression == "" {
		return fmt.Errorf(
			"trigger %q: cron expression must not be empty", id)
	}
	_, err := cronexpr.ParseCron(c.Expression)
	if err != nil {
		return fmt.Errorf(
			"trigger %q: invalid cron expression: %w", id, err)
	}
	return nil
}

func validateWebhookConfig(id string, w *WebhookConfig) error {
	if w == nil {
		panic("validateWebhookConfig: WebhookConfig must not be nil")
	}
	if id == "" {
		panic("validateWebhookConfig: id must not be empty")
	}

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
