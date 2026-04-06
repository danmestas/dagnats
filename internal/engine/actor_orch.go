package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/actor"
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
	nc     *nats.Conn
	js     jetstream.JetStream
	rt     *actor.Runtime
	store  *SnapshotStore
	cc     jetstream.ConsumeContext
	actors sync.Map // runID → *WorkflowActor
}

// NewActorOrchestrator creates an actor-based orchestrator.
func NewActorOrchestrator(
	nc *nats.Conn,
) *ActorOrchestrator {
	if nc == nil {
		panic("NewActorOrchestrator: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(
			"NewActorOrchestrator: jetstream.New: " + err.Error(),
		)
	}
	return &ActorOrchestrator{
		nc:    nc,
		js:    js,
		rt:    actor.NewRuntime(),
		store: NewSnapshotStore(js),
	}
}

// Start subscribes to the WORKFLOW_HISTORY stream.
func (ao *ActorOrchestrator) Start() {
	if ao.cc != nil {
		panic("ActorOrchestrator.Start: already started")
	}
	stream, err := ao.js.Stream(
		context.Background(), "WORKFLOW_HISTORY",
	)
	if err != nil {
		panic(
			"ActorOrchestrator.Start: stream: " + err.Error(),
		)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		context.Background(), jetstream.ConsumerConfig{
			FilterSubject: "history.>",
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		panic(
			"ActorOrchestrator.Start: consumer: " + err.Error(),
		)
	}
	cc, err := cons.Consume(ao.handleEventJS)
	if err != nil {
		panic(
			"ActorOrchestrator.Start: consume: " + err.Error(),
		)
	}
	ao.cc = cc
}

// Stop drains and terminates all actors.
func (ao *ActorOrchestrator) Stop() {
	if ao.cc != nil {
		ao.cc.Stop()
		ao.cc = nil
	}
	ao.rt.StopAll()
}

// handleEventJS routes a history event to the per-run actor.
// Accepts jetstream.Msg from the new consumer API.
func (ao *ActorOrchestrator) handleEventJS(msg jetstream.Msg) {
	if msg == nil {
		return
	}
	evt, err := protocol.UnmarshalEvent(msg.Data())
	if err != nil {
		slog.Error("unmarshal event", "error", err)
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
		slog.Error("route event to actor",
			"error", sendErr,
			"run_id", evt.RunID,
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

	wa := NewWorkflowActor(runID, ao.store, ao.js)
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
