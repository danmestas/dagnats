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
	// Workflows optionally lists fixed workflow names the program
	// registers, each "name.<rest>". It is documentation, not a limit:
	// api.p.<name>.workflows.register accepts any name under "name.",
	// including ones made up at runtime.
	Workflows []string
	Streams   []StreamSpec
	Buckets   []BucketSpec
	// Reads are other programs' KV buckets this program may read (get
	// and watch, never write), each a full bucket name such as
	// "tools_registry" under ANOTHER program's prefix. Not the
	// program's own buckets (those already get full access) and not an
	// engine bucket. Whether the owner program exists is not checkable
	// by Validate: an entry for a bucket nobody owns grants nothing
	// usable.
	//
	// An entry may name the JetStream domain the bucket lives in,
	// "<domain>:<bucket>" such as "eu:chat_cards", for a bucket in
	// another region (ADR-031). It grants the same reads as
	// "$JS.<domain>.API.*", which jetstream.NewWithDomain sends, instead
	// of "$JS.API.*"; a bare entry means this server's own domain. The
	// bucket follows the same rules, except that it may be under the
	// program's own prefix, since its full access is local. Adding one
	// widens like any new read, and the join key must cover the bucket's
	// owner: a key scopes names, not domains.
	Reads []string
	// Protocols are shared subject roots the program speaks, e.g.
	// "tool.exec": publish and subscribe on the root and below it.
	Protocols []string
	// Discovers are micro service names the program may discover: it may
	// send $SRV.PING, $SRV.INFO and $SRV.STATS requests for each name
	// and each of its instances, and read the replies on its own inbox.
	// Discovery only: calling a service's endpoints still needs a
	// Protocol or one of the program's own subjects. Like every grant
	// here it is a guardrail for a trusted program, not isolation
	// (dagnats#909).
	Discovers []string
	// Archives are JetStream streams this program archives and evicts:
	// on each it may drain through one named ephemeral consumer
	// (ArchiveConsumer), read every consumer's info for ack floors, and
	// purge. Declaring is not enough: the operator must also grant the
	// stream to the program in dagnats.yaml (policy.archives), and only
	// streams both declared and granted get rights (EffectiveArchives).
	// A declared stream the operator has not granted registers fine and
	// grants nothing; the server logs the mismatch.
	Archives []string
	// Console links the program from the dagnats console nav; nil for
	// none.
	Console *Link
	// BridgeToken asks dagnats for an HTTP-bridge worker token scoped to
	// TaskTypes, so a bridge worker (a TypeScript one, say) shares the
	// program's identity. Every attach mints a new token and revokes the
	// previous one, so there is one live token per program: a second
	// instance revokes the first's. It reaches the program as
	// Host.BridgeToken, only from Run, which confirms on its own
	// connection to receive it, and is never stored.
	BridgeToken bool `json:"bridge_token,omitempty"`
}

// StreamSpec declares one of the program's own JetStream streams. The
// host creates it (Provision); the program only reads and consumes it.
// Its MaxBytes is resolved by dagnats from the fraction and floor and
// handed back in Grant.StreamMaxBytes.
type StreamSpec struct {
	Name string // "NAME_<REST>", upper case
	// Subjects the stream captures, each under the program's "name."
	// prefix; wildcards allowed.
	Subjects         []string
	MaxBytesFraction float64
	MaxBytesFloor    int64
	MaxAge           time.Duration
}

// BucketSpec declares one of the program's own KV buckets. The host
// creates it (Provision); the program reads and writes keys.
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
	// BridgeToken is the bearer minted for this attach when the manifest
	// sets BridgeToken; empty otherwise, and from Attach called directly.
	BridgeToken string
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
