// actor/actor.go
// The actor package provides a lightweight actor runtime for DagNats.
// Actors are goroutines with channel mailboxes, supervised by parent actors.
// Pure Go — no NATS imports. NATS integration lives in engine/.
package actor

import "fmt"

// Address uniquely identifies an actor within the runtime.
type Address struct {
	Type string // e.g. "workflow", "worker", "tool"
	ID   string // e.g. run ID, worker ID
}

// String formats the address as "type.id" for logging and map keys.
func (a Address) String() string {
	return a.Type + "." + a.ID
}

// Message is the envelope delivered to an actor's mailbox.
type Message struct {
	From    Address
	Payload interface{}
}

// Directive tells a supervisor how to handle a failed child.
type Directive int

const (
	Restart  Directive = iota // Restart the failed actor
	Stop                      // Stop the actor permanently
	Escalate                  // Escalate failure to parent
	Resume                    // Ignore the error, continue
)

var directiveStrings = [...]string{
	"restart", "stop", "escalate", "resume",
}

// String returns the lowercase name of the directive.
func (d Directive) String() string {
	if int(d) < len(directiveStrings) {
		return directiveStrings[d]
	}
	panic(fmt.Sprintf("unknown Directive %d", d))
}

// Actor is the interface all actors implement. Receive processes one
// message at a time — the runtime guarantees sequential delivery.
type Actor interface {
	// Receive processes a single message. Returning an error
	// triggers the supervision strategy.
	Receive(ctx *Context, msg Message) error
}

// Lifecycle extends Actor with optional startup/shutdown hooks.
// Actors that don't need hooks can implement Actor alone.
type Lifecycle interface {
	// PreStart runs before the actor begins receiving messages.
	// Errors here trigger supervision (the actor never starts).
	PreStart(ctx *Context) error

	// PostStop runs after the actor stops receiving messages.
	// Used for cleanup. Errors are logged but not supervised.
	PostStop(ctx *Context)
}

// Context provides services to a running actor.
type Context struct {
	self    Address
	runtime *Runtime
}

// Self returns this actor's address.
func (c *Context) Self() Address { return c.self }

// Send delivers a message to another actor's mailbox. Returns an
// error if the target actor is not found or its mailbox is full.
func (c *Context) Send(to Address, payload interface{}) error {
	return c.runtime.Send(to, Message{
		From:    c.self,
		Payload: payload,
	})
}

// Spawn creates a child actor supervised by this actor.
func (c *Context) Spawn(
	addr Address, actor Actor, opts ...SpawnOption,
) error {
	so := spawnDefaults()
	for _, opt := range opts {
		opt(&so)
	}
	return c.runtime.spawn(addr, actor, c.self, so)
}

// SpawnOption configures actor spawning.
type SpawnOption func(*spawnOptions)

type spawnOptions struct {
	mailboxSize int
	strategy    SupervisionStrategy
}

func spawnDefaults() spawnOptions {
	return spawnOptions{
		mailboxSize: 64,
		strategy:    nil, // no children = no strategy needed
	}
}

// WithMailboxSize sets the buffered channel capacity.
func WithMailboxSize(size int) SpawnOption {
	return func(o *spawnOptions) {
		if size < 1 {
			panic("actor: mailbox size must be >= 1")
		}
		o.mailboxSize = size
	}
}

// WithSupervision sets the strategy for this actor's children.
func WithSupervision(s SupervisionStrategy) SpawnOption {
	return func(o *spawnOptions) {
		if s == nil {
			panic("actor: supervision strategy must not be nil")
		}
		o.strategy = s
	}
}
