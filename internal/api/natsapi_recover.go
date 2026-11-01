package api

import (
	"log/slog"
	"runtime/debug"

	"github.com/nats-io/nats.go/micro"
)

// internalErrorKind marks a reply to a request the handler failed on
// itself (a recovered panic). Nothing the request asked for was applied
// past the point of failure; it is a server bug, not a refusal.
const internalErrorKind = "internal"

// guarded wraps a control-plane handler so a panic, from a bug or from
// an assertion caller input reached, is logged with its subject and stack
// and answered with {"error", "kind":"internal"} instead of killing the
// process: a micro handler's panic is not recovered by nats.go, and one
// request would take every workflow on the node down with it (#902).
func (n *NATSAPI) guarded(handler micro.HandlerFunc) micro.HandlerFunc {
	if handler == nil {
		panic("NATSAPI.guarded: handler must not be nil")
	}
	return func(req micro.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			slog.Error("control-plane handler panicked; answered internal",
				"subject", req.Subject(), "panic", recovered, "stack", string(debug.Stack()))
			n.reply(req, map[string]string{
				"error": "internal error handling " + req.Subject(), "kind": internalErrorKind,
			})
		}()
		handler(req)
	}
}
