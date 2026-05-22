package trigger

import (
	"context"
	"fmt"
)

// cronRegistrar adapts the existing Scheduler to the TriggerRegistrar
// interface. The Scheduler owns the trigger table — this struct is a
// thin wrapper that satisfies the kind-dispatch contract. ADR-016.
type cronRegistrar struct {
	scheduler *Scheduler
}

func newCronRegistrar(s *Scheduler) *cronRegistrar {
	if s == nil {
		panic("newCronRegistrar: scheduler must not be nil")
	}
	return &cronRegistrar{scheduler: s}
}

// Activate adds the cron def to the scheduler. Scheduler.AddTrigger
// is map[id]=def so a repeat call with the same def is a no-op.
func (r *cronRegistrar) Activate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("cronRegistrar.Activate: def.ID must not be empty")
	}
	if def.Cron == nil {
		return fmt.Errorf("trigger %q: cron config missing", def.ID)
	}
	return r.scheduler.AddTrigger(def)
}

// Deactivate removes the cron entry from the scheduler. RemoveTrigger
// already tolerates unknown IDs (delete on absent key is a no-op),
// so this is idempotent.
func (r *cronRegistrar) Deactivate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("cronRegistrar.Deactivate: def.ID must not be empty")
	}
	return r.scheduler.RemoveTrigger(def.ID)
}

// ValidateConfig delegates to the shared cron validator.
func (r *cronRegistrar) ValidateConfig(def TriggerDef) error {
	if def.Cron == nil {
		return fmt.Errorf("trigger %q: cron config missing", def.ID)
	}
	return validateCronConfig(def.ID, def.Cron)
}
