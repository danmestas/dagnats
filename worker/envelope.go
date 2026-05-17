// Public re-exports of the internal envelope types. Go's internal/
// rule blocks downstream workers from importing the canonical
// definitions directly, so without these aliases every consumer
// redeclares the struct shape and drifts over time. Aliases (not
// wrapper types) keep the engine and the worker on the same
// underlying type with zero conversion at the call site. See #235.
package worker

import (
	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/internal/trigger"
)

// HTTPEnvelope is the request shape lifted from inbound HTTP and
// webhook triggers. Bind it via worker.HandleTyped[HTTPEnvelope]
// when worker.UnwrapTrigger() is set.
type HTTPEnvelope = httpenvelope.Envelope

// TriggerEnvelope is the standard outer envelope every trigger
// publishes. Bind it when the worker needs the trigger metadata
// (kind, source, workflow_id, timestamp) alongside the inner data.
type TriggerEnvelope = trigger.TriggerEnvelope
