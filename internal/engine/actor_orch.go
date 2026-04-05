package engine

import (
	"sync"
	"time"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ActorOrchestrator is an actor-based workflow orchestrator. It
// subscribes to the WORKFLOW_HISTORY stream and routes events to
// per-run WorkflowActors managed by the actor runtime.
//
// Unlike Orchestrator, run state lives in-memory within each
// WorkflowActor. Snapshots still save to KV for durability.
type ActorOrchestrator struct {
	nc       *nats.Conn
	jsLegacy nats.JetStreamContext
	js       jetstream.JetStream
	tel      *observe.Telemetry
	rt       *actor.Runtime
	store    *SnapshotStore
	sub      *nats.Subscription
	actors   sync.Map // runID → *WorkflowActor
}

// NewActorOrchestrator creates an actor-based orchestrator.
func NewActorOrchestrator(
	nc *nats.Conn, tel *observe.Telemetry,
) *ActorOrchestrator {
	if nc == nil {
		panic("NewActorOrchestrator: nc must not be nil")
	}
	if tel == nil {
		panic("NewActorOrchestrator: tel must not be nil")
	}
	jsLegacy, err := nc.JetStream()
	if err != nil {
		panic("NewActorOrchestrator: JetStream: " + err.Error())
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(
			"NewActorOrchestrator: jetstream.New: " + err.Error(),
		)
	}
	return &ActorOrchestrator{
		nc:       nc,
		jsLegacy: jsLegacy,
		js:       js,
		tel:      tel,
		rt:       actor.NewRuntime(),
		store:    NewSnapshotStore(jsLegacy),
	}
}

// Start subscribes to the WORKFLOW_HISTORY stream.
func (ao *ActorOrchestrator) Start() {
	if ao.sub != nil {
		panic("ActorOrchestrator.Start: already started")
	}
	sub, err := ao.jsLegacy.Subscribe("history.>", ao.handleEvent,
		nats.DeliverAll(),
		nats.AckExplicit(),
	)
	if err != nil {
		panic("ActorOrchestrator.Start: subscribe: " + err.Error())
	}
	ao.sub = sub
}

// Stop drains and terminates all actors.
func (ao *ActorOrchestrator) Stop() {
	if ao.sub != nil {
		ao.sub.Unsubscribe()
		ao.sub = nil
	}
	ao.rt.StopAll()
}

// handleEvent routes a history event to the per-run actor.
func (ao *ActorOrchestrator) handleEvent(msg *nats.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data)
	if err != nil {
		ao.tel.Logger.Error("unmarshal event", err)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	if !isHandledEventType(evt.Type) {
		msg.Ack()
		return
	}

	// Ensure actor exists for this run
	ao.ensureActor(evt.RunID)

	// Route event to actor
	addr := actor.Address{Type: "workflow", ID: evt.RunID}
	sendErr := ao.rt.Send(addr, actor.Message{Payload: evt})
	if sendErr != nil {
		ao.tel.Logger.Error("route event to actor", sendErr,
			observe.String("run_id", evt.RunID),
		)
		msg.NakWithDelay(5 * time.Second)
		return
	}
	msg.Ack()
}

// ensureActor spawns a WorkflowActor for the run if one doesn't
// exist. Idempotent — safe to call multiple times.
func (ao *ActorOrchestrator) ensureActor(runID string) {
	if _, loaded := ao.actors.Load(runID); loaded {
		return
	}

	wa := NewWorkflowActor(runID, ao.store, ao.jsLegacy, ao.js)
	addr := actor.Address{Type: "workflow", ID: runID}

	err := ao.rt.Spawn(addr, wa,
		actor.WithSupervision(&actor.OneForOne{}),
	)
	if err != nil {
		// Already exists (race between concurrent events) — fine
		return
	}
	ao.actors.Store(runID, wa)
}

// GetWorkflowActor returns the actor for a run, or nil if not found.
// Used for testing and inspection.
func (ao *ActorOrchestrator) GetWorkflowActor(
	runID string,
) *WorkflowActor {
	val, ok := ao.actors.Load(runID)
	if !ok {
		return nil
	}
	return val.(*WorkflowActor)
}
