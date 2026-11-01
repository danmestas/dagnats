package programs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime/debug"
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

// Serve answers program.RegisterSubject on nc with r. Stop the returned
// service to stop answering. The request carries a join key, so it is
// never logged.
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
	err = service.AddEndpoint("register", micro.HandlerFunc(r.handleRegister),
		micro.WithEndpointSubject(program.RegisterSubject))
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (r *Registrar) handleRegister(req micro.Request) {
	if req == nil {
		panic("programs.handleRegister: req must not be nil")
	}
	// A registration request is caller input from an anonymous client; no
	// bug it reaches may take the node down. A panic is answered as a
	// server error (RegisterReply.Error) and logged with its stack.
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		slog.Error("programs: registration handler panicked",
			"panic", recovered, "stack", string(debug.Stack()))
		data, err := json.Marshal(program.RegisterReply{Error: "internal error"})
		if err == nil {
			if respondErr := req.Respond(data); respondErr != nil {
				slog.Warn("programs: internal-error reply not sent", "error", respondErr)
			}
		}
	}()
	reply := r.answer(req.Data())
	data, err := json.Marshal(reply)
	if err != nil {
		panic("programs.handleRegister: a RegisterReply always encodes")
	}
	if err := req.Respond(data); err != nil {
		slog.Warn("programs: registration reply not sent", "error", err)
	}
}

// answer turns a request body into exactly one of Grant, Refused or
// Error.
func (r *Registrar) answer(body []byte) program.RegisterReply {
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
	grant, err := r.Register(ctx, request)
	if err == nil {
		return program.RegisterReply{Grant: &grant}
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
