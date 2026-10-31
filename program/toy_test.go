// program/toy_test.go
// A toy program for the Attach and permission tests: one task type, one
// micro service with a protocol endpoint, its own stream and bucket.
// Methodology: helpers only. Counters let tests assert how many times
// Attach drove Start and Stop.
package program_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/micro"
)

type toyProgram struct {
	manifest program.Manifest
	// service makes Start serve the micro service.
	service bool
	// handle, when set, replaces the default toy.echo handler.
	handle worker.HandlerFunc

	starts atomic.Int32
	stops  atomic.Int32
	// resumed counts toy.wait deliveries that found the pause marker.
	resumed atomic.Int32
	svc     micro.Service
}

func (p *toyProgram) Manifest() program.Manifest { return p.manifest }

func (p *toyProgram) Start(ctx context.Context, h program.Host) error {
	if h.Conn == nil {
		panic("toyProgram.Start: h.Conn must not be nil")
	}
	p.starts.Add(1)
	handler := p.handle
	if handler == nil {
		handler = echoHandler
	}
	h.Handle("toy.echo", handler)
	if slices.Contains(p.manifest.TaskTypes, "toy.send.email") {
		h.Handle("toy.send.email", handler)
	}
	if slices.Contains(p.manifest.TaskTypes, "toy.wait") {
		h.Handle("toy.wait", p.waitHandler)
	}
	if p.service {
		svc, err := startToyService(h)
		if err != nil {
			return err
		}
		p.svc = svc
	}
	return nil
}

func (p *toyProgram) Stop(ctx context.Context) error {
	if ctx == nil {
		panic("toyProgram.Stop: ctx must not be nil")
	}
	p.stops.Add(1)
	if p.svc != nil {
		return p.svc.Stop()
	}
	return nil
}

// echoHandler touches every engine crossing a task can reach: the
// stream and log subjects, the heartbeat run-state read, the
// checkpoints bucket and the agent journal.
func echoHandler(tc worker.TaskContext) error {
	j, err := tc.Journal()
	if err != nil {
		return err
	}
	if _, err := j.Append(tc.Context(), "toy.note", []byte(`{}`)); err != nil {
		return err
	}
	if err := tc.PutStream([]byte("chunk")); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(tc.LogOut(), "echo"); err != nil {
		return err
	}
	if err := tc.Heartbeat(); err != nil {
		return err
	}
	if err := tc.Checkpoint([]byte("state")); err != nil {
		return err
	}
	if _, err := tc.LoadCheckpoint(); err != nil {
		return err
	}
	return tc.Complete(tc.Input())
}

// waitHandler is a long wait as notify writes it: Pause records a
// marker in the checkpoints bucket and NAKs with a delay; the
// redelivery finds the marker through LoadCheckpoint and completes.
// No journal, so Pause takes the checkpoints-KV path.
func (p *toyProgram) waitHandler(tc worker.TaskContext) error {
	state, err := tc.LoadCheckpoint()
	if err != nil {
		return err
	}
	var marker struct {
		PauseResume string `json:"pause_resume"`
	}
	if len(state) > 0 {
		if err := json.Unmarshal(state, &marker); err != nil {
			return err
		}
	}
	if marker.PauseResume != "toy-wait" {
		return tc.Pause("toy-wait", 100*time.Millisecond)
	}
	p.resumed.Add(1)
	return tc.Complete(tc.Input())
}

func startToyService(h program.Host) (micro.Service, error) {
	svc, err := micro.AddService(h.Conn, micro.Config{
		Name: "toy", Version: "1.0.0",
	})
	if err != nil {
		return nil, err
	}
	reply := func(r micro.Request) {
		if err := r.Respond(append([]byte("pong:"), r.Data()...)); err != nil {
			h.Logger.Warn("respond", "error", err)
		}
	}
	errs := errors.Join(
		svc.AddEndpoint("echo", micro.HandlerFunc(reply),
			micro.WithEndpointSubject("toy.svc.echo")),
		svc.AddEndpoint("ping", micro.HandlerFunc(reply),
			micro.WithEndpointSubject("toyproto.ping")),
	)
	if errs != nil {
		return nil, errors.Join(errs, svc.Stop())
	}
	return svc, nil
}

// toyGrant is the literal Grant D1's tests attach with; D2's register
// handler computes the real one.
func toyGrant() program.Grant {
	return program.Grant{
		Replicas:         1,
		StoreBudgetBytes: 8 << 20,
		StreamMaxBytes:   map[string]int64{"TOY_EVENTS": 4 << 20},
		ServerVersion:    "dev",
	}
}
