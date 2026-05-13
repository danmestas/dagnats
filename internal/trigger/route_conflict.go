// trigger/route_conflict.go
//
// RouteConflictError is the typed error returned when an HTTP trigger
// registration would collide on (method, path) with an already-
// registered HTTP trigger. Defined here so the API service (and any
// future surface — gRPC, NATS request/reply) can errors.As switch on
// it and map to the appropriate transport status without duplicating
// the conflict-detection logic.
package trigger

import "fmt"

// RouteConflictError signals a (method, path) collision between two
// distinct HTTP trigger IDs. The holder is the trigger that already
// owns the route; the caller (the one that just tried to register)
// must change route or unregister the holder first.
type RouteConflictError struct {
	Method          string
	Path            string
	HolderTriggerID string
}

// Error implements the error interface. Format intended for log lines
// and operator-visible response bodies — surfaces all three fields so
// the operator can act without a second lookup.
func (e *RouteConflictError) Error() string {
	if e == nil {
		return "<nil RouteConflictError>"
	}
	return fmt.Sprintf(
		"route conflict: %s %s already registered by trigger %q",
		e.Method, e.Path, e.HolderTriggerID,
	)
}
