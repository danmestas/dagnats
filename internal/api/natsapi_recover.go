package api

import (
	"log/slog"
	"runtime/debug"
	"strings"

	"github.com/danmestas/dagnats/program"
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
		if !replyAllowed(req) {
			return
		}
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

// replyAllowed checks a program-scoped request's reply subject. Every
// reply is published by dagnats' own (root) connection, and NATS checks
// no permission on a reply subject, so a reply-to the requester chose
// would be written with root's rights: a "$KV.programs..." key that
// crashes every node's user set, a "$JS.API.STREAM.PURGE..." call,
// another program's inbox. A program's requests (api.p.<name>.*) must
// therefore ask for their reply under that program's own inbox,
// "_INBOX.<name>.", the only one its NATS user may subscribe to;
// anything else is dropped unanswered, with a log. Global api.* subjects
// are reachable only by fully credentialed clients.
func replyAllowed(req micro.Request) bool {
	rest, scoped := strings.CutPrefix(req.Subject(), "api.p.")
	if !scoped {
		return true
	}
	name, _, _ := strings.Cut(rest, ".")
	if program.ValidateName(name) != nil {
		slog.Warn("dropped a program request with an invalid program name",
			"subject", req.Subject())
		return false
	}
	if strings.HasPrefix(req.Reply(), program.InboxPrefix(name)+".") {
		return true
	}
	slog.Warn("dropped a program request whose reply is not that program's inbox",
		"subject", req.Subject(), "reply", req.Reply())
	return false
}
