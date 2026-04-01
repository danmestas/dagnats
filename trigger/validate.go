package trigger

import "fmt"

// Validate checks a TriggerDef for structural correctness.
// Returns nil if valid, descriptive error otherwise.
// Panics if called with a completely uninitialized def (programmer error).
func Validate(def TriggerDef) error {
	if def.ID == "" && def.WorkflowID == "" &&
		def.Cron == nil && def.Subject == nil && def.Webhook == nil {
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
	_, err := ParseCron(c.Expression)
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
