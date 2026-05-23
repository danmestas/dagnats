// Package main is the filewatcher External trigger type example
// (parent #273 Phase 2.6, issue #350).
//
// End-to-end demonstration of the Phase 2.4 worker SDK
// (RegisterTriggerType + WatchTriggers). A single in-process worker:
//
//  1. Publishes a TriggerTypeDef into the trigger_types KV bucket and
//     asks the engine to allocate a registrar for kind "filewatcher".
//  2. Watches `_TRIGGER.filewatcher.{activate,deactivate}` via the
//     worker SDK; on activate, spawns an fsnotify watcher for the
//     configured path; on deactivate, closes it.
//  3. On filesystem event, publishes workflow.started so the engine
//     dispatches the configured workflow.
//
// Stable Version + stable OwnerWorkerID are load-bearing: Phase 2.7
// (#351) makes version-mismatch a hard error when live triggers
// already exist for the kind. A random-per-boot ID or auto-bumped
// version would trip that conflict on every rebuild. The constants
// below are deliberately fixed so re-running the example is a no-op
// at the registrar layer.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/fsnotify/fsnotify"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// filewatcherKind is the External trigger kind. Matches the Name field
// of the TriggerTypeDef published into the trigger_types KV bucket and
// the subject suffix on `_TRIGGER.<kind>.{activate,deactivate}`.
const filewatcherKind = "filewatcher"

// filewatcherWorkerID is the stable OwnerWorkerID for this example.
// Hardcoded (rather than crypto/rand per boot) so repeated runs hit
// the engine's idempotent-re-register path instead of an owner-mismatch
// error. See ack_micro.go installExternalRegistrar.
const filewatcherWorkerID = "filewatcher-example"

// filewatcherVersion is the trigger-type version recorded in the
// TriggerTypeDef. Hardcoded "1" — the example never bumps. Phase 2.7
// (#351) makes a bumped version a hard error against live triggers;
// pinning to "1" keeps the example restart-safe.
const filewatcherVersion = "1"

// configSchemaJSON is the JSON Schema for filewatcher trigger
// configuration. Validated by the engine's
// santhosh-tekuri/jsonschema/v5 path on trigger creation (Phase 2.2,
// internal/trigger/validate_external.go). Required field `path`; an
// optional `events` array selects which fsnotify operations fire the
// workflow (create / write / remove / rename / chmod).
const configSchemaJSON = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path"],
  "properties": {
    "path":   {"type": "string", "minLength": 1},
    "events": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": ["create", "write", "remove", "rename", "chmod"]
      }
    }
  },
  "additionalProperties": false
}`

// payloadSchemaJSON describes the TriggerEnvelope.Data shape that fired
// workflows can rely on. Two fields: the absolute path of the file
// whose modification fired the trigger, and the fsnotify event name
// ("create" / "write" / ...). The engine does not enforce this shape
// — payload schemas are an informational contract between trigger
// authors and workflow authors (parent #273 Phase 2.1).
const payloadSchemaJSON = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["path", "event"],
  "properties": {
    "path":  {"type": "string"},
    "event": {"type": "string"}
  },
  "additionalProperties": false
}`

// filewatcherConfig is the decoded trigger config. Mirrors
// configSchemaJSON.
type filewatcherConfig struct {
	Path   string   `json:"path"`
	Events []string `json:"events,omitempty"`
}

// firePayload is the TriggerEnvelope.Data shape. Mirrors
// payloadSchemaJSON.
type firePayload struct {
	Path  string `json:"path"`
	Event string `json:"event"`
}

// activeWatch holds the live fsnotify watcher and metadata for one
// trigger. Indexed by TriggerDef.ID inside Service.
type activeWatch struct {
	watcher    *fsnotify.Watcher
	workflowID string
	triggerID  string
	cfg        filewatcherConfig
	cancel     context.CancelFunc
}

// Service owns the lifetime of the filewatcher worker:
// RegisterTriggerType + WatchTriggers + per-trigger fsnotify watchers
// + the workflow.started publish path. Kept as a struct rather than
// package-level state so the integration tests can spin up an isolated
// instance per t.Run.
type Service struct {
	nc *nats.Conn
	js jetstream.JetStream
	w  *worker.Worker

	mu      sync.Mutex
	watches map[string]*activeWatch
}

// newService constructs a Service bound to nc. Panics if nc is nil —
// programmer error. The Worker uses the default crypto/rand worker ID
// for its own directory registration (observability only); the stable
// filewatcherWorkerID is what we put in the TriggerTypeDef.
func newService(nc *nats.Conn) *Service {
	if nc == nil {
		panic("newService: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("newService: jetstream.New: " + err.Error())
	}
	return &Service{
		nc:      nc,
		js:      js,
		w:       worker.NewWorker(nc),
		watches: make(map[string]*activeWatch),
	}
}

// start registers the filewatcher trigger type and binds the
// activate / deactivate callbacks. Returns the first error from either
// path; callers should bail on non-nil.
//
// Idempotent on re-run: same Name + same OwnerWorkerID + same
// ConfigSchema returns nil at the ack-micro layer. The example never
// changes the schema bytes between rebuilds, so successive boots are
// no-ops.
func (s *Service) start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("start: ctx must not be nil")
	}
	if s.w == nil {
		panic("start: worker must not be nil")
	}

	def := trigger.TriggerTypeDef{
		Name:          filewatcherKind,
		OwnerWorkerID: filewatcherWorkerID,
		Description:   "Fires when a filesystem path changes (fsnotify)",
		ConfigSchema:  json.RawMessage(configSchemaJSON),
		PayloadSchema: json.RawMessage(payloadSchemaJSON),
		Version:       filewatcherVersion,
	}
	if err := s.w.RegisterTriggerType(ctx, def); err != nil {
		return fmt.Errorf("RegisterTriggerType: %w", err)
	}

	if err := s.w.WatchTriggers(ctx, filewatcherKind,
		s.onActivate, s.onDeactivate,
	); err != nil {
		return fmt.Errorf("WatchTriggers: %w", err)
	}
	return nil
}

// stop tears down all live watchers and the worker subscriptions.
// Safe to call multiple times — fsnotify.Watcher.Close is idempotent
// at the OS level (subsequent calls return an error we drop).
func (s *Service) stop() {
	s.mu.Lock()
	watches := s.watches
	s.watches = make(map[string]*activeWatch)
	s.mu.Unlock()
	for id, aw := range watches {
		if err := aw.watcher.Close(); err != nil {
			slog.Warn("filewatcher: watcher close on stop",
				"trigger_id", id, "error", err)
		}
		aw.cancel()
	}
	s.w.Stop()
}

// onActivate spins up a fsnotify watcher for the trigger's configured
// path. Idempotent: re-activating an already-watched trigger ID is a
// no-op (engine fires Activate again on catch-up scans).
func (s *Service) onActivate(
	ctx context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		return fmt.Errorf("onActivate: def.ID must not be empty")
	}
	if def.External == nil {
		return fmt.Errorf("onActivate: def.External must not be nil")
	}

	var cfg filewatcherConfig
	if err := json.Unmarshal(def.External.Config, &cfg); err != nil {
		return fmt.Errorf("decode filewatcher config: %w", err)
	}
	if cfg.Path == "" {
		return fmt.Errorf("filewatcher config: path must not be empty")
	}

	s.mu.Lock()
	if _, exists := s.watches[def.ID]; exists {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify.NewWatcher: %w", err)
	}
	if err := watcher.Add(cfg.Path); err != nil {
		_ = watcher.Close()
		return fmt.Errorf("watcher.Add %q: %w", cfg.Path, err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	aw := &activeWatch{
		watcher:    watcher,
		workflowID: def.WorkflowID,
		triggerID:  def.ID,
		cfg:        cfg,
		cancel:     cancel,
	}

	s.mu.Lock()
	s.watches[def.ID] = aw
	s.mu.Unlock()

	go s.watchLoop(loopCtx, aw)
	slog.Info("filewatcher: activated",
		"trigger_id", def.ID, "workflow_id", def.WorkflowID,
		"path", cfg.Path, "events", cfg.Events,
	)
	return nil
}

// onDeactivate closes the watcher for def.ID. Idempotent: removing an
// unknown trigger is nil so the worker doesn't surface stale-state
// errors back to the engine.
func (s *Service) onDeactivate(
	_ context.Context, def trigger.TriggerDef,
) error {
	if def.ID == "" {
		return fmt.Errorf("onDeactivate: def.ID must not be empty")
	}
	s.mu.Lock()
	aw, ok := s.watches[def.ID]
	delete(s.watches, def.ID)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	if err := aw.watcher.Close(); err != nil {
		slog.Warn("filewatcher: watcher close on deactivate",
			"trigger_id", def.ID, "error", err)
	}
	aw.cancel()
	slog.Info("filewatcher: deactivated", "trigger_id", def.ID)
	return nil
}

// watchLoop pumps fsnotify events into the workflow.started publisher.
// Bounded by ctx cancellation (deactivate or service stop) and by the
// watcher's own Events channel close. No internal timer — fsnotify
// drives the cadence.
//
// Each event is filtered by cfg.Events (when set) — an empty slice
// means "fire on every operation" so the simplest configs work without
// a list. Errors from the watcher itself are logged at warn level
// rather than ending the loop, mirroring fsnotify upstream guidance.
func (s *Service) watchLoop(ctx context.Context, aw *activeWatch) {
	if aw == nil {
		panic("watchLoop: aw must not be nil")
	}
	if aw.watcher == nil {
		panic("watchLoop: aw.watcher must not be nil")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-aw.watcher.Events:
			if !ok {
				return
			}
			name := opName(ev.Op)
			if name == "" {
				continue
			}
			if !eventEnabled(aw.cfg.Events, name) {
				continue
			}
			absPath := ev.Name
			if abs, err := filepath.Abs(ev.Name); err == nil {
				absPath = abs
			}
			if err := s.fireWorkflow(ctx, aw, absPath, name); err != nil {
				slog.Warn("filewatcher: fire workflow",
					"trigger_id", aw.triggerID,
					"workflow_id", aw.workflowID,
					"error", err,
				)
			}
		case err, ok := <-aw.watcher.Errors:
			if !ok {
				return
			}
			slog.Warn("filewatcher: watcher error",
				"trigger_id", aw.triggerID, "error", err)
		}
	}
}

// opName maps an fsnotify.Op bitmask to a single-event lowercase name.
// fsnotify can OR multiple ops on one event (e.g. Create|Write on
// some platforms) — we pick the most specific one in declaration
// order. Returns "" for an empty op bitmask so the loop can skip.
func opName(op fsnotify.Op) string {
	switch {
	case op&fsnotify.Create != 0:
		return "create"
	case op&fsnotify.Write != 0:
		return "write"
	case op&fsnotify.Remove != 0:
		return "remove"
	case op&fsnotify.Rename != 0:
		return "rename"
	case op&fsnotify.Chmod != 0:
		return "chmod"
	default:
		return ""
	}
}

// eventEnabled returns true when name should fire the workflow. An
// empty allow list means "all events"; otherwise the name must appear
// in the list.
func eventEnabled(allow []string, name string) bool {
	if len(allow) == 0 {
		return true
	}
	for _, a := range allow {
		if a == name {
			return true
		}
	}
	return false
}

// fireWorkflow publishes a workflow.started event for the given
// trigger so the engine dispatches the configured workflow. Mirrors
// the cron / webhook publishers — TriggerEnvelope on history.<runID>
// with a runid.New() RunID. No dedup ID because filesystem events do
// not have a natural minute-windowed identity the way cron does;
// dedup-by-msg-id would suppress legitimate back-to-back fires.
func (s *Service) fireWorkflow(
	ctx context.Context, aw *activeWatch, path, event string,
) error {
	if aw == nil {
		panic("fireWorkflow: aw must not be nil")
	}
	if aw.workflowID == "" {
		return fmt.Errorf("fireWorkflow: aw.workflowID is empty")
	}
	payload := firePayload{Path: path, Event: event}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	envelope := trigger.TriggerEnvelope{
		Trigger:    filewatcherKind,
		Source:     aw.triggerID,
		WorkflowID: aw.workflowID,
		Timestamp:  time.Now().UTC(),
		Data:       payloadBytes,
	}
	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	runID := runid.New()
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, envBytes,
	)
	evtBytes, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = s.js.Publish(pubCtx, evt.NATSSubject(), evtBytes)
	if err != nil {
		return fmt.Errorf("publish %s: %w", evt.NATSSubject(), err)
	}
	slog.Info("filewatcher: fired workflow",
		"trigger_id", aw.triggerID,
		"workflow_id", aw.workflowID,
		"run_id", runID,
		"path", path, "event", event,
	)
	return nil
}

// main wires the Service to a NATS connection and blocks on SIGINT /
// SIGTERM. NATS_URL overrides the default URL for local docker-compose
// or remote setups.
func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", url, err)
		os.Exit(1)
	}
	defer nc.Close()

	svc := newService(nc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("filewatcher worker ready. Ctrl+C to stop.")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\nshutting down on %s...\n", sig)
	svc.stop()
}
