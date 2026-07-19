package actor

import (
	"fmt"
	"sync"
	"time"
)

// ErrActorNotFound is returned when sending to an unknown address.
var ErrActorNotFound = fmt.Errorf("actor: not found")

// ErrMailboxFull is returned when an actor's mailbox is at capacity.
var ErrMailboxFull = fmt.Errorf("actor: mailbox full")

// ErrAlreadyExists is returned when spawning at an occupied address.
var ErrAlreadyExists = fmt.Errorf("actor: already exists")

// Runtime manages actor lifecycle, message delivery, and supervision.
// All methods are safe for concurrent use.
type Runtime struct {
	mu     sync.RWMutex
	actors map[string]*actorCell
}

// actorCell is the internal bookkeeping for a running actor.
type actorCell struct {
	addr     Address
	actor    Actor
	mailbox  chan Message
	parent   Address // zero value = root actor
	children []Address
	strategy SupervisionStrategy
	restarts *RestartTracker
	done     chan struct{}
}

// NewRuntime creates an empty actor runtime.
func NewRuntime() *Runtime {
	return &Runtime{
		actors: make(map[string]*actorCell),
	}
}

// Spawn starts a new root-level actor. Use Context.Spawn for
// supervised child actors. Returns ErrAlreadyExists if the address
// is taken.
func (r *Runtime) Spawn(
	addr Address, actor Actor, opts ...SpawnOption,
) error {
	return r.spawn(addr, actor, Address{}, spawnWithOpts(opts))
}

func spawnWithOpts(opts []SpawnOption) spawnOptions {
	so := spawnDefaults()
	for _, o := range opts {
		o(&so)
	}
	return so
}

// spawn is the internal spawn used by both Runtime.Spawn and
// Context.Spawn. parent is zero-value for root actors.
func (r *Runtime) spawn(
	addr Address,
	a Actor,
	parent Address,
	opts spawnOptions,
) error {
	if addr.Type == "" || addr.ID == "" {
		panic("actor: address Type and ID must not be empty")
	}
	if a == nil {
		panic("actor: actor must not be nil")
	}

	r.mu.Lock()
	key := addr.String()
	if _, exists := r.actors[key]; exists {
		r.mu.Unlock()
		return ErrAlreadyExists
	}

	cell := &actorCell{
		addr:     addr,
		actor:    a,
		mailbox:  make(chan Message, opts.mailboxSize),
		parent:   parent,
		strategy: opts.strategy,
		restarts: NewRestartTracker(5, 1*time.Minute),
		done:     make(chan struct{}),
	}
	r.actors[key] = cell

	// Register as child of parent
	if parent != (Address{}) {
		if pc, ok := r.actors[parent.String()]; ok {
			pc.children = append(pc.children, addr)
		}
	}
	r.mu.Unlock()

	// Run actor loop in goroutine
	go r.runActor(cell)
	return nil
}

// runActor is the main actor goroutine. It calls PreStart (if
// Lifecycle), then processes messages until the done channel closes.
func (r *Runtime) runActor(cell *actorCell) {
	if cell == nil {
		panic("actor: runActor cell must not be nil")
	}
	ctx := &Context{self: cell.addr, runtime: r}

	// Call PreStart if actor implements Lifecycle
	if lc, ok := cell.actor.(Lifecycle); ok {
		if err := lc.PreStart(ctx); err != nil {
			r.handleFailure(cell, err)
			return
		}
	}

	// Message loop — sequential delivery, one at a time
	for {
		select {
		case msg := <-cell.mailbox:
			if err := cell.actor.Receive(ctx, msg); err != nil {
				r.handleFailure(cell, err)
				return
			}
		case <-cell.done:
			// Graceful shutdown
			if lc, ok := cell.actor.(Lifecycle); ok {
				lc.PostStop(ctx)
			}
			return
		}
	}
}

// handleFailure applies the parent's supervision strategy.
//
// recursion:allow escalation walks up the supervision tree one parent
// per call, so depth is the tree's height -- a structural bound set when
// actors are spawned, not by anything this function controls.
func (r *Runtime) handleFailure(cell *actorCell, err error) {
	if cell == nil {
		panic("handleFailure: cell must not be nil")
	}

	// Root actors have zero-value parent — skip lookup.
	var parent *actorCell
	var hasParent bool
	if cell.parent != (Address{}) {
		r.mu.RLock()
		parent, hasParent = r.actors[cell.parent.String()]
		r.mu.RUnlock()
	}

	directive := Stop // Default: stop if no supervisor
	if hasParent && parent.strategy != nil {
		directive = parent.strategy.Decide(err)
	}

	switch directive {
	case Restart:
		r.restartActor(cell)
	case Stop:
		r.stopActor(cell)
	case Escalate:
		r.stopActor(cell)
		if hasParent {
			r.handleFailure(parent, err)
		}
	case Resume:
		// Re-enter message loop
		go r.runActor(cell)
	}
}

// restartActor stops and relaunches the actor if within limits.
func (r *Runtime) restartActor(cell *actorCell) {
	if cell == nil {
		panic("actor: restartActor cell must not be nil")
	}
	if !cell.restarts.Allow() {
		// Exhausted restart budget — stop permanently
		r.stopActor(cell)
		return
	}

	// Call PostStop on the old instance
	if lc, ok := cell.actor.(Lifecycle); ok {
		ctx := &Context{self: cell.addr, runtime: r}
		lc.PostStop(ctx)
	}

	// Re-enter message loop with same actor instance
	cell.done = make(chan struct{})
	go r.runActor(cell)
}

// stopActor terminates an actor and removes it from the runtime.
//
// recursion:allow stopping descends the supervision tree one generation
// per call, mirroring handleFailure's ascent. Bounded by tree height.
func (r *Runtime) stopActor(cell *actorCell) {
	if cell == nil {
		panic("actor: stopActor cell must not be nil")
	}
	// Signal done (safe to close multiple times via select)
	select {
	case <-cell.done:
		// Already closed
	default:
		close(cell.done)
	}

	r.mu.Lock()
	delete(r.actors, cell.addr.String())

	// Remove from parent's children list
	if cell.parent != (Address{}) {
		if pc, ok := r.actors[cell.parent.String()]; ok {
			for i, child := range pc.children {
				if child == cell.addr {
					pc.children = append(
						pc.children[:i],
						pc.children[i+1:]...,
					)
					break
				}
			}
		}
	}
	r.mu.Unlock()

	// Stop children recursively (iterative)
	r.mu.RLock()
	children := make([]Address, len(cell.children))
	copy(children, cell.children)
	r.mu.RUnlock()

	for _, child := range children {
		r.Stop(child)
	}
}

// Send delivers a message to an actor's mailbox. Returns
// ErrActorNotFound if the address is unknown, ErrMailboxFull
// if the channel is at capacity.
func (r *Runtime) Send(
	to Address, msg Message,
) error {
	if to.Type == "" || to.ID == "" {
		panic("actor: Send target Type and ID must not be empty")
	}
	r.mu.RLock()
	cell, ok := r.actors[to.String()]
	r.mu.RUnlock()
	if !ok {
		return ErrActorNotFound
	}

	select {
	case cell.mailbox <- msg:
		return nil
	default:
		return ErrMailboxFull
	}
}

// Stop gracefully terminates an actor and its children.
func (r *Runtime) Stop(addr Address) error {
	if addr.Type == "" || addr.ID == "" {
		panic("actor: Stop address must not be empty")
	}
	r.mu.RLock()
	cell, ok := r.actors[addr.String()]
	r.mu.RUnlock()
	if !ok {
		return ErrActorNotFound
	}
	r.stopActor(cell)
	return nil
}

// StopAll terminates all actors. Used in defer for cleanup.
func (r *Runtime) StopAll() {
	if r.actors == nil {
		panic("actor: StopAll called on uninitialized runtime")
	}
	r.mu.RLock()
	addrs := make([]Address, 0, len(r.actors))
	for _, cell := range r.actors {
		addrs = append(addrs, cell.addr)
	}
	r.mu.RUnlock()

	for _, addr := range addrs {
		r.Stop(addr)
	}
}
