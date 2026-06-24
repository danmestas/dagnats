// api/natsapi.go
// NATS request/reply transport for the DagNats control plane.
// The three control-plane subjects are exposed as a single nats-micro
// service ("dagnats-api") so that $SRV.PING/INFO/STATS discovery and
// `nats micro ls` work, while the subjects and reply bytes stay
// identical to the pre-micro raw-subscribe implementation. All transport
// concerns (subject routing, JSON framing) are isolated here so Service
// remains transport-agnostic.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// microServiceName is the registered nats-micro service name and the
// suffix used on $SRV.{PING,INFO,STATS}.<name> control subjects.
const microServiceName = "dagnats-api"

// microVersionRegexp mirrors the SemVer pattern micro enforces in
// Config.valid (nats.go@v1.50.0/micro/service.go). We validate the build
// string ourselves so we can substitute a safe sentinel instead of
// letting AddService fail on an un-stamped build.
var microVersionRegexp = regexp.MustCompile(
	`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)` +
		`(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)` +
		`(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?` +
		`(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`,
)

// microVersionDevSentinel is used whenever the build string is not a
// valid SemVer core (incl. "dev", "", git-describe output, v-prefixed
// tags). It is a valid SemVer pre-release form -- proven accepted by
// micro.AddService in TestStartWithDevBuildDoesNotPanic -- so it keeps
// "this is a dev build" visible in $SRV.INFO without breaking discovery.
const microVersionDevSentinel = "0.0.0-dev"

// NATSAPI wires Service methods to NATS request/reply subjects via a
// nats-micro service. It owns no business logic -- it only translates
// between wire bytes and typed calls.
type NATSAPI struct {
	svc     *Service
	nc      *nats.Conn
	version string
	micro   micro.Service
}

// NewNATSAPI constructs a NATSAPI bound to svc and nc. version is the
// binary's build string; it is sanitized to a valid SemVer for micro.
// Panics if svc or nc is nil.
func NewNATSAPI(
	svc *Service, nc *nats.Conn, version string,
) *NATSAPI {
	if svc == nil {
		panic("NewNATSAPI: svc must not be nil")
	}
	if nc == nil {
		panic("NewNATSAPI: nc must not be nil")
	}
	return &NATSAPI{svc: svc, nc: nc, version: version}
}

// Start registers the micro service and its endpoints. Panics on
// any AddService/AddEndpoint error -- the endpoint set is static, so a
// failure is an unrecoverable programmer error. Panics on double-start.
func (n *NATSAPI) Start() {
	if n.nc == nil {
		panic("NATSAPI.Start: nc must not be nil")
	}
	if n.svc == nil {
		panic("NATSAPI.Start: svc must not be nil")
	}
	if n.micro != nil {
		panic("NATSAPI.Start: already started")
	}
	srv, err := micro.AddService(n.nc, micro.Config{
		Name:        microServiceName,
		Version:     microVersion(n.version),
		Description: "DagNats control plane (workflows + runs)",
	})
	if err != nil {
		panic("NATSAPI.Start: AddService failed: " + err.Error())
	}
	// micro wraps the connection's ClosedHandler/ErrorHandler to
	// auto-Stop the service on connection close. Safe here because our
	// explicit Stop() is idempotent; but a caller that sets conn handlers
	// AFTER Start() will find them chained, not replaced.
	endpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{"register", "api.workflows.register", n.handleRegister},
		{"start", "api.runs.start", n.handleStartRun},
		{"get", "api.runs.get", n.handleGetRun},
		// Additive control-plane endpoints (#376). Existing subjects
		// above are unchanged.
		{"runtimes-register", "api.runtimes.register",
			n.handleRuntimeRegister},
		{"runs-spawn", "api.runs.spawn", n.handleRunSpawn},
	}
	for _, e := range endpoints {
		// Without a queue group, every running INSTANCE receives each
		// message -- preserving pre-micro behavior (raw nc.Subscribe).
		// micro's default queue group "q" would instead load-balance
		// each message across instances.
		if err := srv.AddEndpoint(
			e.name, e.handler,
			micro.WithEndpointSubject(e.subject),
			micro.WithEndpointQueueGroupDisabled(),
		); err != nil {
			panic("NATSAPI.Start: AddEndpoint failed for " +
				e.subject + ": " + err.Error())
		}
	}
	n.micro = srv
}

// Stop drains the micro service. The signature stays func() so server
// and main call sites (and their defers) are unchanged; a drain error is
// logged rather than dropped (F3). Safe on never-started/double-stop.
func (n *NATSAPI) Stop() {
	if n.micro == nil {
		return
	}
	if err := n.micro.Stop(); err != nil {
		slog.Warn("natsapi stop: micro drain failed", "error", err)
	}
	n.micro = nil
}

// handleRegister unmarshals a WorkflowDef and calls RegisterWorkflow.
func (n *NATSAPI) handleRegister(req micro.Request) {
	if req == nil {
		panic("handleRegister: req must not be nil")
	}
	if n.svc == nil {
		panic("handleRegister: svc must not be nil")
	}
	var def dag.WorkflowDef
	if err := json.Unmarshal(req.Data(), &def); err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	if err := n.svc.RegisterWorkflow(
		context.Background(), def,
	); err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	n.reply(req, map[string]string{
		"status": "registered", "name": def.Name,
	})
}

// handleStartRun unmarshals a startRunRequest and calls StartRun.
func (n *NATSAPI) handleStartRun(req micro.Request) {
	if req == nil {
		panic("handleStartRun: req must not be nil")
	}
	if n.svc == nil {
		panic("handleStartRun: svc must not be nil")
	}
	var r startRunRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	runID, err := n.svc.StartRun(
		context.Background(), r.Workflow, r.Input,
	)
	if err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	n.reply(req, map[string]string{"run_id": runID})
}

// handleGetRun reads the run ID from the raw request body and returns
// the current snapshot. The body is plain text (not JSON).
func (n *NATSAPI) handleGetRun(req micro.Request) {
	if req == nil {
		panic("handleGetRun: req must not be nil")
	}
	if n.svc == nil {
		panic("handleGetRun: svc must not be nil")
	}
	runID := string(req.Data())
	resp, err := n.svc.GetRunResponse(
		context.Background(), runID,
	)
	if err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		n.reply(req, map[string]string{"error": err.Error()})
		return
	}
	req.Respond(data) //nolint:errcheck -- reply failure is non-fatal
}

// handleRuntimeRegister unmarshals a runtime-register request, scopes +
// validates + persists the def server-side, and replies with the scoped
// name or a {error, kind} envelope the worker maps to a typed error.
func (n *NATSAPI) handleRuntimeRegister(req micro.Request) {
	if req == nil {
		panic("handleRuntimeRegister: req must not be nil")
	}
	if n.svc == nil {
		panic("handleRuntimeRegister: svc must not be nil")
	}
	var r struct {
		Def        dag.WorkflowDef `json:"def"`
		OwnerRunID string          `json:"owner_run_id"`
		Promote    bool            `json:"promote"`
	}
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.reply(req, map[string]string{
			"error": err.Error(), "kind": "transport",
		})
		return
	}
	scoped, kind, err := n.svc.RegisterRuntimeWorkflow(
		context.Background(), r.Def, r.OwnerRunID, r.Promote,
	)
	if err != nil {
		n.reply(req, map[string]string{
			"error": err.Error(), "kind": kind,
		})
		return
	}
	n.reply(req, map[string]string{"scoped_name": scoped})
}

// handleRunSpawn unmarshals a spawn request and launches a child run via
// the EventWorkflowSpawn path. Replies with the child run ID or a
// {error, kind} envelope (e.g. depth_exceeded).
func (n *NATSAPI) handleRunSpawn(req micro.Request) {
	if req == nil {
		panic("handleRunSpawn: req must not be nil")
	}
	if n.svc == nil {
		panic("handleRunSpawn: svc must not be nil")
	}
	var r struct {
		ChildWorkflow string          `json:"child_workflow"`
		ParentRunID   string          `json:"parent_run_id"`
		ParentStepID  string          `json:"parent_step_id"`
		Input         json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		n.reply(req, map[string]string{
			"error": err.Error(), "kind": "transport",
		})
		return
	}
	runID, kind, err := n.svc.SpawnChildRun(
		context.Background(), r.ChildWorkflow,
		r.ParentRunID, r.ParentStepID, r.Input,
	)
	if err != nil {
		n.reply(req, map[string]string{
			"error": err.Error(), "kind": kind,
		})
		return
	}
	n.reply(req, map[string]string{"run_id": runID})
}

// reply marshals payload to JSON and sends it as a reply. We use
// req.Respond (not req.Error) so the {"error":...} JSON envelope and the
// raw reply bytes match the pre-micro implementation exactly -- req.Error
// would set Nats-Service-Error headers and change what callers parse. A
// marshal error is logged rather than panicking the handler goroutine.
func (n *NATSAPI) reply(req micro.Request, payload any) {
	if req == nil {
		panic("reply: req must not be nil")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("reply: marshal failed", "error", err)
		return
	}
	req.Respond(data) //nolint:errcheck -- best-effort reply
}

// microVersion returns a SemVer string micro.AddService will accept. A
// valid SemVer build passes through unchanged; anything else (incl.
// "dev", "", git-describe, v-prefixed tags) collapses to the dev
// sentinel so an un-stamped build never fails service registration.
func microVersion(build string) string {
	if microVersionRegexp.MatchString(build) {
		return build
	}
	return microVersionDevSentinel
}
