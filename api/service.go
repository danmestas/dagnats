// api/service.go
// Control plane service: register workflow definitions, start runs, query state.
// This layer is shared by REST and NATS request/reply handlers — it owns no
// transport concerns, only business logic backed by NATS KV and JetStream.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

// Service is the control plane for DagNats. It writes workflow definitions to
// KV, publishes WorkflowStarted events to the history stream, and snapshots
// initial run state so the engine can pick up immediately after StartRun returns.
type Service struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	defKV  nats.KeyValue
	store  *engine.SnapshotStore
	logger observe.Logger
}

// NewService binds the control plane to an active NATS connection.
// Panics if JetStream init fails or the workflow_defs bucket does not exist —
// callers must call natsutil.SetupAll before constructing a Service.
func NewService(nc *nats.Conn, logger observe.Logger) *Service {
	if nc == nil {
		panic("NewService: nc must not be nil")
	}
	if logger == nil {
		panic("NewService: logger must not be nil")
	}
	js, err := nc.JetStream()
	if err != nil {
		panic("NewService: JetStream init failed: " + err.Error())
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		panic("NewService: workflow_defs bucket not found: " + err.Error())
	}
	return &Service{
		nc:     nc,
		js:     js,
		defKV:  defKV,
		store:  engine.NewSnapshotStore(js),
		logger: logger,
	}
}

// RegisterWorkflow validates and persists a workflow definition under its name.
// Subsequent calls with the same name overwrite the previous version — the engine
// reads the definition at run-start time, so in-flight runs are unaffected.
func (s *Service) RegisterWorkflow(def dag.WorkflowDef) error {
	if err := dag.Validate(def); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	_, err = s.defKV.Put(def.Name, data)
	return err
}

// GetWorkflow retrieves the registered definition for the named workflow.
// Returns a NATS key-not-found error when the workflow has not been registered.
func (s *Service) GetWorkflow(name string) (dag.WorkflowDef, error) {
	entry, err := s.defKV.Get(name)
	if err != nil {
		return dag.WorkflowDef{}, err
	}
	var def dag.WorkflowDef
	err = json.Unmarshal(entry.Value(), &def)
	return def, err
}

// StartRun creates a new run for the named workflow, publishes a WorkflowStarted
// event, and snapshots the initial run state. The run ID is a 32-char hex string
// derived from crypto/rand — collision probability is negligible in practice.
func (s *Service) StartRun(workflowName string, input []byte) (string, error) {
	entry, err := s.defKV.Get(workflowName)
	if err != nil {
		return "", fmt.Errorf("workflow %q not found: %w", workflowName, err)
	}
	runID := generateRunID()
	evt := engine.NewWorkflowEvent(engine.EventWorkflowStarted, runID, entry.Value())
	data, err := evt.Marshal()
	if err != nil {
		return "", err
	}
	_, err = s.js.Publish(evt.NATSSubject(), data, nats.MsgId(evt.NATSMsgID()))
	if err != nil {
		return "", err
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		return "", fmt.Errorf("unmarshal workflow def: %w", err)
	}
	run := dag.NewWorkflowRun(def, runID)
	if err := s.store.Save(run); err != nil {
		return "", fmt.Errorf("save run snapshot: %w", err)
	}
	s.logger.Info("started run",
		observe.String("run_id", runID),
		observe.String("workflow", workflowName),
	)
	return runID, nil
}

// GetRun retrieves the current snapshot for the given run ID.
// Returns engine.ErrRunNotFound when no snapshot exists.
func (s *Service) GetRun(runID string) (dag.WorkflowRun, error) {
	return s.store.Load(runID)
}

// generateRunID returns a 32-character lowercase hex string from 16 crypto-random bytes.
// Panics only if the OS entropy source is unavailable — a fatal system condition.
func generateRunID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("generateRunID: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}
