package trigger

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

// subjectRegistrar owns the SubjectTrigger table. ADR-016.
//
// The map is constructed here and shared by reference with
// TriggerService — keeping the trigger-package-internal field
// `ts.subjects` working for the watcher-replay regression guard in
// service_test.go without changing those tests. The registrar is the
// canonical owner; TriggerService holds the same reference for
// in-package observation only.
type subjectRegistrar struct {
	nc       *nats.Conn
	subjects map[string]*SubjectTrigger
	mu       *sync.RWMutex // shared with TriggerService
}

func newSubjectRegistrar(
	nc *nats.Conn,
	subjects map[string]*SubjectTrigger,
	mu *sync.RWMutex,
) *subjectRegistrar {
	if nc == nil {
		panic("newSubjectRegistrar: nc must not be nil")
	}
	if subjects == nil {
		panic("newSubjectRegistrar: subjects map must not be nil")
	}
	if mu == nil {
		panic("newSubjectRegistrar: mu must not be nil")
	}
	return &subjectRegistrar{nc: nc, subjects: subjects, mu: mu}
}

// Activate subscribes to def.Subject.Subject and stores the trigger.
// Idempotent: a second call with the same def.ID is a no-op.
func (r *subjectRegistrar) Activate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("subjectRegistrar.Activate: def.ID must not be empty")
	}
	if _, exists := r.subjects[def.ID]; exists {
		return nil
	}
	trigger, err := NewSubjectTrigger(r.nc, def)
	if err != nil {
		return fmt.Errorf("NewSubjectTrigger: %w", err)
	}
	r.subjects[def.ID] = trigger
	return nil
}

// Deactivate unsubscribes and removes the entry. Idempotent.
func (r *subjectRegistrar) Deactivate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("subjectRegistrar.Deactivate: def.ID must not be empty")
	}
	st, ok := r.subjects[def.ID]
	if !ok {
		return nil
	}
	_ = st.Close()
	delete(r.subjects, def.ID)
	return nil
}

// ValidateConfig checks the subject field is non-empty.
func (r *subjectRegistrar) ValidateConfig(def TriggerDef) error {
	if def.Subject == nil {
		return fmt.Errorf("trigger %q: subject config missing", def.ID)
	}
	if def.Subject.Subject == "" {
		return fmt.Errorf("trigger %q: subject must not be empty", def.ID)
	}
	return nil
}
