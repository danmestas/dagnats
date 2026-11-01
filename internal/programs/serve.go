package programs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// queueGroup makes one dagnats instance answer each registration, so a
// cluster does not admit a program once per node.
const queueGroup = "dagnats-programs"

// registerTimeout bounds one registration, which provisions streams.
const registerTimeout = 30 * time.Second

// registerBodyBytesMax bounds a request body; a large manifest is a few
// kilobytes.
const registerBodyBytesMax = 256 << 10

// Serve answers program.RegisterSubject and the confirm subjects on nc
// with r. Stop the returned service to stop answering. A request carries
// a join key, so it is never logged.
func Serve(nc *nats.Conn, r *Registrar) (micro.Service, error) {
	if nc == nil || r == nil {
		panic("programs.Serve: nc and r must not be nil")
	}
	service, err := micro.AddService(nc, micro.Config{
		Name:        "dagnats-programs",
		Version:     "0.0.1",
		Description: "program registration",
		QueueGroup:  queueGroup,
	})
	if err != nil {
		return nil, err
	}
	err = service.AddEndpoint("register", guard(r.handleRegister),
		micro.WithEndpointSubject(program.RegisterSubject))
	if err != nil {
		return nil, err
	}
	// A program logged in with its key confirms here; the subject names
	// the key, and only that key's NATS user may publish it.
	err = service.AddEndpoint("confirm", guard(r.handleConfirm),
		micro.WithEndpointSubject(program.ConfirmSubjectWildcard))
	if err != nil {
		return nil, err
	}
	return service, nil
}

// guard runs a registration handler under a recover: a request comes
// from an anonymous client or a program, so no bug it reaches may take
// the node down. A panic is logged with its stack and answered as a
// server error, but only to an inbox-shaped reply subject: the responder
// publishes as root, and NATS does not permission reply subjects.
func guard(handler func(micro.Request)) micro.HandlerFunc {
	if handler == nil {
		panic("programs.guard: handler must not be nil")
	}
	return func(req micro.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			slog.Error("programs: registration handler panicked",
				"subject", req.Subject(), "panic", recovered, "stack", string(debug.Stack()))
			reply := req.Reply()
			if strings.HasPrefix(reply, program.JoinInboxPrefix+".") ||
				strings.HasPrefix(reply, "_INBOX.") {
				respond(req, program.RegisterReply{Error: "internal error"})
			}
		}()
		handler(req)
	}
}

// replyUnder reports whether req's reply subject is under prefix. Every
// reply here is published by dagnats' own (root) connection, and NATS
// checks no permission on a reply subject, so a reply-to the requester
// chose (a "$KV..." key, a "$JS.API..." purge, another program's inbox)
// would be written with root's rights. A request whose reply is not
// where its caller can legitimately receive is dropped unanswered.
func replyUnder(req micro.Request, prefix string) bool {
	if prefix == "" || !strings.HasSuffix(prefix, ".") {
		panic("programs.replyUnder: prefix must end in '.'")
	}
	if strings.HasPrefix(req.Reply(), prefix) {
		return true
	}
	slog.Warn("programs: dropped a request whose reply is not its caller's inbox",
		"subject", req.Subject(), "reply", req.Reply(), "want_under", prefix)
	return false
}

// handleRegister answers a registration, which arrives on the anonymous
// connection program.Run dials with the _JOIN inbox prefix, the only
// inbox the join user may subscribe.
func (r *Registrar) handleRegister(req micro.Request) {
	if req == nil {
		panic("programs.handleRegister: req must not be nil")
	}
	if !replyUnder(req, program.JoinInboxPrefix+".") {
		return
	}
	respond(req, r.answerWith(req.Data(), r.Register))
}

// handleConfirm answers a confirmation, which arrives from the program's
// own connection: its reply must be under that program's inbox. The
// program is the one whose record holds the subject's key, never a name
// the body claims, so a key cannot have its answer sent to another
// program's inbox.
func (r *Registrar) handleConfirm(req micro.Request) {
	if req == nil {
		panic("programs.handleConfirm: req must not be nil")
	}
	publicKey := strings.TrimPrefix(req.Subject(), strings.TrimSuffix(
		program.ConfirmSubjectWildcard, "*"))
	name, ok := r.confirmingProgram(req.Data(), publicKey)
	if !ok {
		slog.Warn("programs: dropped a confirmation for no program of its key",
			"subject", req.Subject())
		return
	}
	if !replyUnder(req, program.InboxPrefix(name)+".") {
		return
	}
	respond(req, r.answerWith(req.Data(), func(
		ctx context.Context, request program.RegisterRequest,
	) (program.Grant, error) {
		return r.Confirm(ctx, publicKey, request)
	}))
}

// confirmingProgram names the program a confirmation is for: the one the
// body names, provided its record holds publicKey.
func (r *Registrar) confirmingProgram(body []byte, publicKey string) (string, bool) {
	if r.kv == nil {
		panic("programs.confirmingProgram: registrar not constructed")
	}
	var named struct {
		Manifest struct {
			Name string `json:"Name"`
		} `json:"manifest"`
	}
	if len(body) > registerBodyBytesMax || json.Unmarshal(body, &named) != nil ||
		program.ValidateName(named.Manifest.Name) != nil || publicKey == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), registerTimeout)
	defer cancel()
	record, found, err := r.lookup(ctx, named.Manifest.Name)
	if err != nil || !found || record.PublicKey != publicKey {
		return "", false
	}
	if record.Manifest.Name != named.Manifest.Name {
		panic("programs.confirmingProgram: lookup returned another program's record")
	}
	return record.Manifest.Name, true
}

func respond(req micro.Request, reply program.RegisterReply) {
	data, err := json.Marshal(reply)
	if err != nil {
		panic("programs.respond: a RegisterReply always encodes")
	}
	if err := req.Respond(data); err != nil {
		slog.Warn("programs: registration reply not sent", "error", err)
	}
}

// answerWith decodes a request body, runs op, and turns its outcome into
// exactly one of Grant, Refused or Error, with Confirm set when the
// program must confirm with its key.
func (r *Registrar) answerWith(
	body []byte, op func(context.Context, program.RegisterRequest) (program.Grant, error),
) program.RegisterReply {
	if len(body) > registerBodyBytesMax {
		return program.RegisterReply{Refused: &program.ErrRefused{
			Code: program.RefusedRequestInvalid, Detail: "request body too large",
		}}
	}
	var request program.RegisterRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return program.RegisterReply{Refused: &program.ErrRefused{
			Code: program.RefusedRequestInvalid, Detail: "request is not JSON: " + err.Error(),
		}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), registerTimeout)
	defer cancel()
	grant, err := op(ctx, request)
	if errors.Is(err, errConfirmRequired) {
		return program.RegisterReply{Confirm: program.ConfirmSubject(request.PublicKey)}
	}
	if err == nil {
		reply := program.RegisterReply{Grant: &grant}
		if record, found, lookupErr := r.lookup(ctx, request.Manifest.Name); lookupErr == nil &&
			found && record.Pending {
			reply.Confirm = program.ConfirmSubject(request.PublicKey)
		}
		return reply
	}
	var refusal *program.ErrRefused
	if errors.As(err, &refusal) {
		slog.Info("programs: registration refused", "program", request.Manifest.Name,
			"code", refusal.Code)
		return program.RegisterReply{Refused: refusal}
	}
	slog.Error("programs: registration failed", "program", request.Manifest.Name,
		"error", err)
	return program.RegisterReply{Error: err.Error()}
}
