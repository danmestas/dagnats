// api/service_triggers.go
// Split out of service.go (#566): trigger CRUD/fire/enable + fire history domain of the control
// plane Service. Shares the private Service NATS/KV bundle; no new
// connection layer. Behavior identical to the pre-split file.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
)

// CreateTrigger validates and stores a trigger definition.
func (s *Service) CreateTrigger(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if ctx == nil {
		panic("CreateTrigger: ctx must not be nil")
	}
	if def.ID == "" {
		panic("CreateTrigger: def.ID must not be empty")
	}
	return s.observed(ctx, "createTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", def.ID),
			attribute.String("workflow_id", def.WorkflowID),
		},
		func(ctx context.Context) error {
			return s.createTriggerInner(ctx, def)
		},
	)
}

// createTriggerInner validates and writes the trigger to KV. For HTTP
// triggers it additionally checks for an existing trigger that already
// claims the same (method, path) and refuses with a typed
// RouteConflictError. Self-replace (same trigger ID) is allowed so
// operators can update a route's config without temporary unregister.
func (s *Service) createTriggerInner(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		panic("createTriggerInner: def.ID must not be empty")
	}
	if def.WorkflowID == "" {
		panic(
			"createTriggerInner: def.WorkflowID must not be empty",
		)
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf("invalid trigger: %w", err)
	}
	if err := s.checkHTTPRouteConflict(ctx, def); err != nil {
		return err
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.triggerKV.Put(
		ctx, def.ID, data,
	)
	return err
}

// checkHTTPRouteConflict returns a *trigger.RouteConflictError when
// def is an HTTP trigger whose (method, path) is already claimed by
// a different trigger ID. Non-HTTP triggers are pass-through. Same-ID
// re-registration (idempotent update) is allowed.
func (s *Service) checkHTTPRouteConflict(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if def.HTTP == nil {
		return nil
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	existing, err := s.listTriggersInner(ctx)
	if err != nil {
		// No keys yet is a benign "first trigger" case.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return fmt.Errorf("list triggers for conflict check: %w", err)
	}
	for _, other := range existing {
		if other.ID == def.ID {
			continue
		}
		if other.HTTP == nil {
			continue
		}
		if other.HTTP.Method != def.HTTP.Method {
			continue
		}
		if other.HTTP.Path != def.HTTP.Path {
			continue
		}
		return &trigger.RouteConflictError{
			Method:          def.HTTP.Method,
			Path:            def.HTTP.Path,
			HolderTriggerID: other.ID,
		}
	}
	return nil
}

// ListTriggers retrieves all trigger definitions from KV.
func (s *Service) ListTriggers(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if ctx == nil {
		panic("ListTriggers: ctx must not be nil")
	}
	if s.js == nil {
		panic("ListTriggers: js must not be nil")
	}
	var defs []trigger.TriggerDef
	err := s.observed(ctx, "listTriggers", nil,
		func(ctx context.Context) error {
			var innerErr error
			defs, innerErr = s.listTriggersInner(ctx)
			return innerErr
		},
	)
	return defs, err
}

// listTriggersInner holds the KV iteration logic.
func (s *Service) listTriggersInner(
	ctx context.Context,
) ([]trigger.TriggerDef, error) {
	if s.js == nil {
		panic("listTriggersInner: js must not be nil")
	}
	if s.triggerKV == nil {
		return []trigger.TriggerDef{}, nil
	}
	keys, err := s.triggerKV.Keys(ctx)
	if err != nil {
		return nil, err
	}

	entries, err := natsutil.ParallelGetJS(
		s.triggerKV, keys, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}

	defs := make([]trigger.TriggerDef, 0, len(entries))
	for _, entry := range entries {
		var def trigger.TriggerDef
		if err := json.Unmarshal(
			entry.Value(), &def,
		); err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// DeleteTrigger removes a trigger definition from KV.
func (s *Service) DeleteTrigger(
	ctx context.Context, triggerID string,
) error {
	if ctx == nil {
		panic("DeleteTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("DeleteTrigger: triggerID must not be empty")
	}
	return s.observed(ctx, "deleteTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.deleteTriggerInner(ctx, triggerID)
		},
	)
}

// deleteTriggerInner deletes the trigger from KV.
func (s *Service) deleteTriggerInner(
	ctx context.Context, triggerID string,
) error {
	if triggerID == "" {
		panic(
			"deleteTriggerInner: triggerID must not be empty",
		)
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	return s.triggerKV.Delete(
		ctx, triggerID,
	)
}

// SetTriggerEnabled updates the enabled state of a trigger.
func (s *Service) SetTriggerEnabled(
	ctx context.Context, triggerID string, enabled bool,
) error {
	if ctx == nil {
		panic("SetTriggerEnabled: ctx must not be nil")
	}
	if triggerID == "" {
		panic("SetTriggerEnabled: triggerID must not be empty")
	}
	return s.observed(ctx, "setTriggerEnabled",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.setTriggerEnabledInner(
				ctx, triggerID, enabled,
			)
		},
	)
}

// setTriggerEnabledInner reads, updates, and writes the trigger.
func (s *Service) setTriggerEnabledInner(
	ctx context.Context, triggerID string, enabled bool,
) error {
	if triggerID == "" {
		panic(
			"setTriggerEnabledInner: triggerID must not be empty",
		)
	}
	if s.js == nil {
		panic("setTriggerEnabledInner: js must not be nil")
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return fmt.Errorf("unmarshal trigger: %w", err)
	}
	def.Enabled = enabled
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	_, err = s.triggerKV.Put(ctx, triggerID, data)
	return err
}

// ErrTriggerKindNotFireable is returned by FireTrigger when the
// targeted trigger isn't a kind the manual fire-now path supports.
// #352 scopes manual fires to cron + webhook triggers — subject and
// HTTP triggers carry caller-bound input the console has no way to
// synthesize, so a manual fire of them would produce a malformed run.
var ErrTriggerKindNotFireable = errors.New(
	"trigger kind not fireable from manual fire-now path",
)

// ErrTriggerDisabled is returned by FireTrigger when the operator
// targets a trigger whose Enabled bit is false. The operator must
// re-enable it first; firing a disabled trigger would write a fire
// row history that contradicts the trigger's configured state.
var ErrTriggerDisabled = errors.New(
	"trigger is disabled; enable it before firing",
)

// FireTrigger publishes one workflow.started + TriggerFire history
// record for the given trigger. Returns the run ID the workflow
// orchestrator will observe so the operator can deep-link to the
// run in the console (or the CLI can echo it to stdout). #352.
//
// Allowed kinds: cron + webhook. Other kinds return
// ErrTriggerKindNotFireable so the handler can short-circuit to 400
// rather than fire a partial run. Disabled triggers return
// ErrTriggerDisabled.
//
// All transport / dedup logic lives in trigger.Fire — this method
// just resolves the def from KV, validates kind / enabled, and
// delegates. The dedup-msg-id strategy for SourceManual is the
// nanosecond-unique form so consecutive operator clicks each produce
// a distinct run.
func (s *Service) FireTrigger(
	ctx context.Context, triggerID string,
) (string, error) {
	if ctx == nil {
		panic("FireTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("FireTrigger: triggerID must not be empty")
	}
	var runID string
	err := s.observed(ctx, "fireTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			var innerErr error
			runID, innerErr = s.fireTriggerInner(ctx, triggerID)
			return innerErr
		},
	)
	return runID, err
}

// fireTriggerInner is the un-observed core. Split out so the
// observed() wrapper above stays at ≤70 lines under the project rule.
func (s *Service) fireTriggerInner(
	ctx context.Context, triggerID string,
) (string, error) {
	if s.triggerKV == nil {
		return "", fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return "", fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return "", fmt.Errorf("unmarshal trigger: %w", err)
	}
	if def.Cron == nil && def.Webhook == nil {
		return "", ErrTriggerKindNotFireable
	}
	if !def.Enabled {
		return "", ErrTriggerDisabled
	}
	return trigger.Fire(
		ctx, s.tp, def, trigger.SourceManual, time.Now(),
	)
}

// TriggerUpdates holds optional field overrides for UpdateTrigger.
// Pointer fields distinguish "not provided" from "set to zero value".
type TriggerUpdates struct {
	CronExpr *string
	Timezone *string
	Backfill *bool
	Subject  *string
	Webhook  *string
	Secret   *string
}

// UpdateTrigger reads an existing trigger, applies overrides, validates,
// and writes back. Only non-nil fields in updates are applied.
func (s *Service) UpdateTrigger(
	ctx context.Context, triggerID string, updates TriggerUpdates,
) error {
	if ctx == nil {
		panic("UpdateTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("UpdateTrigger: triggerID must not be empty")
	}
	return s.observed(ctx, "updateTrigger",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(ctx context.Context) error {
			return s.updateTriggerInner(
				ctx, triggerID, updates,
			)
		},
	)
}

// updateTriggerInner reads, patches, validates, and writes the trigger.
func (s *Service) updateTriggerInner(
	ctx context.Context, triggerID string, updates TriggerUpdates,
) error {
	if triggerID == "" {
		panic("updateTriggerInner: triggerID must not be empty")
	}
	if s.triggerKV == nil {
		return fmt.Errorf("triggers KV bucket not available")
	}
	entry, err := s.triggerKV.Get(ctx, triggerID)
	if err != nil {
		return fmt.Errorf(
			"trigger %q not found: %w", triggerID, err,
		)
	}
	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return fmt.Errorf("unmarshal trigger: %w", err)
	}
	applyTriggerUpdates(&def, updates)
	if err := trigger.Validate(def); err != nil {
		return fmt.Errorf(
			"invalid trigger after update: %w", err,
		)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	_, err = s.triggerKV.Put(ctx, triggerID, data)
	return err
}

// applyTriggerUpdates patches non-nil fields from updates onto def.
func applyTriggerUpdates(
	def *trigger.TriggerDef, updates TriggerUpdates,
) {
	if def == nil {
		panic("applyTriggerUpdates: def must not be nil")
	}
	if def.ID == "" {
		panic("applyTriggerUpdates: def.ID must not be empty")
	}
	if updates.CronExpr != nil && def.Cron != nil {
		def.Cron.Expression = *updates.CronExpr
	}
	if updates.Timezone != nil && def.Cron != nil {
		def.Cron.Timezone = *updates.Timezone
	}
	if updates.Backfill != nil && def.Cron != nil {
		def.Cron.Backfill = *updates.Backfill
	}
	if updates.Subject != nil && def.Subject != nil {
		def.Subject.Subject = *updates.Subject
	}
	if updates.Webhook != nil && def.Webhook != nil {
		def.Webhook.Path = *updates.Webhook
	}
	if updates.Secret != nil && def.Webhook != nil {
		def.Webhook.Secret = *updates.Secret
	}
}

// TriggerFireEntry is a trigger fire record enriched with
// run status information for CLI display.
type TriggerFireEntry struct {
	trigger.TriggerFire
	Status   string        `json:"status,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
}

// ListTriggerFires retrieves fire history for the given
// trigger. Creates an ephemeral consumer on TRIGGER_HISTORY
// and fetches up to limit messages.
func (s *Service) ListTriggerFires(
	ctx context.Context, triggerID string, limit int,
) ([]TriggerFireEntry, error) {
	if ctx == nil {
		panic("ListTriggerFires: ctx must not be nil")
	}
	if triggerID == "" {
		panic(
			"ListTriggerFires: triggerID must not be empty",
		)
	}
	var fires []TriggerFireEntry
	err := s.observed(ctx, "listTriggerFires",
		[]attribute.KeyValue{
			attribute.String("trigger_id", triggerID),
		},
		func(_ context.Context) error {
			var innerErr error
			fires, innerErr = s.listTriggerFiresInner(
				triggerID, limit,
			)
			return innerErr
		},
	)
	return fires, err
}

// listTriggerFiresInner fetches trigger fire records from the
// TRIGGER_HISTORY stream via an ephemeral consumer.
func (s *Service) listTriggerFiresInner(
	triggerID string, limit int,
) ([]TriggerFireEntry, error) {
	if triggerID == "" {
		panic(
			"listTriggerFiresInner: triggerID must not be empty",
		)
	}
	if s.js == nil {
		panic("listTriggerFiresInner: js must not be nil")
	}
	ctx := context.Background()
	subject := "trigger.fire." + triggerID
	cons, err := s.js.CreateOrUpdateConsumer(
		ctx, "TRIGGER_HISTORY",
		jetstream.ConsumerConfig{
			FilterSubject:     subject,
			DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
			AckPolicy:         jetstream.AckNonePolicy,
			InactiveThreshold: 10 * time.Second,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create consumer: %w", err,
		)
	}
	return s.fetchFireEntries(cons, limit)
}

// fetchFireEntries reads messages from the consumer and
// unmarshals them into TriggerFireEntry records. Enriches
// each record with run status when a RunID is present.
func (s *Service) fetchFireEntries(
	cons jetstream.Consumer, limit int,
) ([]TriggerFireEntry, error) {
	if cons == nil {
		panic("fetchFireEntries: cons must not be nil")
	}
	if limit <= 0 {
		panic("fetchFireEntries: limit must be positive")
	}
	ctx := context.Background()
	batch, err := cons.Fetch(limit,
		jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	entries := make([]TriggerFireEntry, 0, limit)
	for msg := range batch.Messages() {
		var fire trigger.TriggerFire
		if json.Unmarshal(msg.Data(), &fire) != nil {
			continue
		}
		entry := TriggerFireEntry{TriggerFire: fire}
		if fire.RunID != "" {
			entry.Status, entry.Duration =
				s.enrichFireStatus(ctx, fire.RunID)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// enrichFireStatus loads run status and duration for a fire
// record. Returns empty values on error (best-effort).
func (s *Service) enrichFireStatus(
	ctx context.Context, runID string,
) (string, time.Duration) {
	if runID == "" {
		panic("enrichFireStatus: runID must not be empty")
	}
	if ctx == nil {
		panic("enrichFireStatus: ctx must not be nil")
	}
	run, err := s.store.Load(ctx, runID)
	if err != nil {
		return "", 0
	}
	var dur time.Duration
	if run.CompletedAt != nil {
		dur = run.CompletedAt.Sub(run.CreatedAt)
	} else if run.Status != dag.RunStatusPending &&
		run.Status != dag.RunStatusRunning {
		// Terminal but pre-#443 snapshot w/ no CompletedAt:
		// best-effort fallback to wall-clock age.
		dur = time.Since(run.CreatedAt)
	}
	return run.Status.String(), dur
}
