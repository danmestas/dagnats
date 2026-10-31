package program

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// Manifest is everything dagnats needs to admit a program. Name is the
// prefix root: task types and workflow names "name.*", subjects
// "name.>", streams "NAME_*", buckets "name_*".
type Manifest struct {
	// Name is [a-z][a-z0-9]{1,31}. It may not shadow an engine subject
	// root, stream or bucket (Validate lists them).
	Name string
	// Version is the program's own version, informational.
	Version string
	// Dagnats is the semver range the program was built for, e.g.
	// ">=0.0.35 <0.1.0". Registration evaluates it.
	Dagnats string
	// TaskTypes are the task types the program's worker handles, each
	// "name.<rest>". Host.Handle refuses any other.
	TaskTypes []string
	// Workflows are the workflow names the program registers, each
	// "name.<rest>".
	Workflows []string
	Streams   []StreamSpec
	Buckets   []BucketSpec
	// Protocols are shared subject roots the program speaks, e.g.
	// "tool.exec": publish and subscribe on the root and below it.
	Protocols []string
	// Console links the program from the dagnats console nav; nil for
	// none.
	Console *Link
}

// StreamSpec declares one of the program's own JetStream streams. Its
// MaxBytes is resolved by dagnats from the fraction and floor and
// handed back in Grant.StreamMaxBytes.
type StreamSpec struct {
	Name             string // "NAME_<REST>", upper case
	MaxBytesFraction float64
	MaxBytesFloor    int64
	MaxAge           time.Duration
}

// BucketSpec declares one of the program's own KV buckets.
type BucketSpec struct {
	Name     string // "name_<rest>", lower case
	History  uint8
	TTL      time.Duration
	MaxBytes int64
}

// Link is a console navigation entry.
type Link struct {
	Label string
	URL   string
}

// Grant is what attach returns. Sizes are absolute: the program never
// does budget arithmetic.
type Grant struct {
	Replicas int
	// StoreBudgetBytes is the total admitted for this program's streams
	// and buckets.
	StoreBudgetBytes int64
	// StreamMaxBytes sizes every declared stream, by stream name.
	StreamMaxBytes map[string]int64
	ServerVersion  string
}

// Host is what Start receives. It is data: the connection the program
// was attached with, its Grant, a logger tagged with its name, and
// Handle, which registers a task handler on the worker Attach runs.
type Host struct {
	Conn   *nats.Conn
	Grant  Grant
	Logger *slog.Logger
	// Handle registers fn for a declared task type. Attach wraps it in
	// a per-dispatch recover and starts consuming once Start returns.
	// Calling it after Start returns, or for an undeclared type, panics.
	Handle func(taskType string, fn worker.HandlerFunc, opts ...worker.HandlerOption)
}

// Program is a separately released binary's view of dagnats.
type Program interface {
	Manifest() Manifest
	// Start provisions the program's streams and buckets at the sizes in
	// h.Grant, registers handlers and workflows, and returns once
	// serving. Idempotent. Must not retain h.Conn after Stop.
	Start(ctx context.Context, h Host) error
	// Stop drains within ctx. Must not use the control plane.
	Stop(ctx context.Context) error
}

// HTTPHandler is optional. The host decides the listener and prefix.
type HTTPHandler interface{ Handler() http.Handler }

// InboxPrefix is the reply-inbox prefix a program must dial with,
// nats.CustomInboxPrefix(InboxPrefix(name)). Its permissions grant
// subscribe on this prefix only, so no program reads another's replies.
func InboxPrefix(name string) string {
	if name == "" {
		panic("program.InboxPrefix: name must not be empty")
	}
	prefix := "_INBOX." + name
	if len(prefix) <= len("_INBOX.") {
		panic("program.InboxPrefix: prefix must extend _INBOX.")
	}
	return prefix
}
