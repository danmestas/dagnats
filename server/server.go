package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/bridge"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/console"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/openapi"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/danmestas/dagnats/internal/web"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const shutdownDeadline = 15 * time.Second

// Server is the all-in-one DagNats server lifecycle manager.
type Server struct {
	cfg         Config
	ns          *natsserver.Server
	nc          *nats.Conn
	orch        *engine.Orchestrator
	svc         *api.Service
	natsAPI     *api.NATSAPI
	trig        *trigger.TriggerService
	bridge      *bridge.Bridge
	httpSrv     *http.Server
	telShutdown func(context.Context)
	ready       atomic.Bool
	stopCh      chan struct{}
	workerShims []*WorkerShim
	workers     []*worker.Worker
	running     atomic.Bool
	tempCreds   string // inline creds temp file; cleaned on shutdown
}

// New creates a Server with the given config. Panics if DataDir is empty.
func New(cfg Config) *Server {
	if cfg.DataDir == "" {
		panic("New: cfg.DataDir is empty")
	}
	return &Server{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Run starts all server components, serves HTTP, and blocks until shutdown.
// Returns nil on clean shutdown, error otherwise.
func (s *Server) Run() error {
	if s == nil {
		panic("Run: s is nil")
	}
	if s.cfg.DataDir == "" {
		panic("Run: DataDir is empty")
	}

	s.running.Store(true)

	if err := os.MkdirAll(s.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	if err := s.startComponents(); err != nil {
		return err
	}

	httpErrCh, err := s.startHTTP()
	if err != nil {
		s.shutdown()
		return err
	}

	s.ready.Store(true)
	printBanner(os.Stderr, s.cfg.HTTPAddr, s.ns.ClientURL())

	return s.waitAndShutdown(httpErrCh)
}

// startComponents initializes NATS, telemetry, and all services.
// Cleans up on failure to prevent resource leaks.
func (s *Server) startComponents() error {
	if s == nil {
		panic("startComponents: s is nil")
	}
	if s.cfg.DataDir == "" {
		panic("startComponents: DataDir is empty")
	}

	var err error

	// Resolve inline credentials to temp file if needed
	if s.cfg.LeafCredentials != "" {
		resolved, resolveErr := resolveCredentials(
			s.cfg.LeafCredentials,
		)
		if resolveErr != nil {
			return fmt.Errorf("resolve credentials: %w", resolveErr)
		}
		if resolved != s.cfg.LeafCredentials {
			s.tempCreds = resolved
		}
		s.cfg.LeafCredentials = resolved
	}

	s.ns, err = startNATS(s.cfg)
	if err != nil {
		if s.tempCreds != "" {
			os.Remove(s.tempCreds)
		}
		return fmt.Errorf("start NATS: %w", err)
	}
	printStep(os.Stderr, "nats server started")

	s.nc, err = nats.Connect(s.ns.ClientURL())
	if err != nil {
		s.ns.Shutdown()
		return fmt.Errorf("connect to NATS: %w", err)
	}
	printStep(os.Stderr, "nats client connected")

	err = natsutil.SetupAll(s.nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
			natsutil.KVConfig{Bucket: "signals"},
			natsutil.KVConfig{Bucket: "checkpoints"},
			natsutil.KVConfig{Bucket: "concurrency_runs"},
		),
		natsutil.WithCluster(natsutil.ClusterOptions{
			Routes:           s.cfg.NATSClusterRoutes,
			ReplicasOverride: s.cfg.NATSJetStreamReplicas,
		}),
	)
	if err != nil {
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("setup NATS resources: %w", err)
	}
	printStep(os.Stderr, "streams and kv buckets ready")

	telShutdown, telErr := observe.InitTelemetry(
		context.Background(), observe.Config{
			ServiceName:  "dagnats",
			NATSConn:     s.nc,
			OTLPEndpoint: s.cfg.OTLPEndpoint,
		},
	)
	if telErr != nil {
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("init telemetry: %w", telErr)
	}
	s.telShutdown = telShutdown
	printStep(os.Stderr, "telemetry initialized")

	s.svc = api.NewService(s.nc)
	s.natsAPI = api.NewNATSAPI(s.svc, s.nc)
	s.natsAPI.Start()
	printStep(os.Stderr, "nats api started")

	s.orch = engine.NewOrchestrator(s.nc)
	s.orch.Start()
	printStep(os.Stderr, "orchestrator started")

	s.bridge = bridge.NewBridge(s.nc)
	printStep(os.Stderr, "http bridge ready")

	s.trig, err = trigger.NewTriggerService(s.nc)
	if err != nil {
		s.orch.Stop()
		s.natsAPI.Stop()
		s.telShutdown(context.Background())
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("create trigger service: %w", err)
	}

	if err := s.trig.Start(); err != nil {
		s.trig.Stop()
		s.orch.Stop()
		s.natsAPI.Stop()
		s.telShutdown(context.Background())
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("start trigger service: %w", err)
	}
	printStep(os.Stderr, "trigger service started")

	// Materialize embedded workers (after streams & KV exist)
	for _, shim := range s.workerShims {
		var opts []worker.WorkerOption
		if len(shim.groups) > 0 {
			opts = append(
				opts, worker.WithGroups(shim.groups...),
			)
		}
		w := worker.NewWorker(s.nc, opts...)
		for _, reg := range shim.registrations {
			switch reg.role {
			case roleSingleton:
				w.HandleSingleton(reg.taskType, reg.handler)
			default:
				w.Handle(reg.taskType, reg.handler)
			}
		}
		if len(shim.registrations) > 0 {
			w.Start()
			s.workers = append(s.workers, w)
		}
		shim.started = true
	}
	if len(s.workers) > 0 {
		printStep(os.Stderr, "embedded workers started")
	}
	s.workerShims = nil // no ambiguous stale state

	return nil
}

// startHTTP creates and launches the HTTP server in a goroutine.
// If the default port is taken, picks a free one automatically.
// Returns a channel that receives any Serve error.
func (s *Server) startHTTP() (<-chan error, error) {
	if s.svc == nil {
		panic("startHTTP: svc is nil")
	}

	mux := http.NewServeMux()
	mux.Handle("/", api.NewRESTHandler(s.svc))
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle(
		"/health/cluster",
		api.NewClusterHealthHandler(s.nc, s.cfg.NATSClusterRoutes),
	)
	mux.HandleFunc("/ready", s.handleReady)
	if s.trig != nil {
		mux.Handle("/hooks/", s.trig.WebhookHandler())
		// ADR-013 HTTP trigger routes mount under /api/. The router
		// receives the full request path so HTTPConfig.Path entries
		// must start with /api/ (validation enforces a leading "/"
		// only; the convention here is documented in the example
		// workflow). Future iterations may surface a configurable
		// prefix; v1 is fixed.
		mux.Handle("/api/", s.trig.HTTPRouter())
	}
	if s.bridge != nil {
		mux.Handle("/v1/", s.bridge.Handler())
	}
	mux.Handle("/ui/", web.New(s.svc, s.nc).Handler())

	// OpenAPI spec + Scalar-rendered explorer. Routes mount as
	// fixed entries so the catch-all REST mux ("/") never sees them.
	docsHandler := openapi.Handler(
		"dagnats HTTP API", openapiSpecVersion,
		newOpenAPIProvider(s.svc),
	)
	mux.Handle("/openapi.json", docsHandler)
	mux.Handle("/docs", docsHandler)
	mux.Handle("/docs/", docsHandler)

	ln, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil && s.cfg.HTTPAddr == defaultHTTPAddr {
		printStep(os.Stderr,
			fmt.Sprintf("%s in use, picking a free port", s.cfg.HTTPAddr))
		ln, err = net.Listen("tcp", ":0")
	}
	if err != nil {
		return nil, fmt.Errorf("listen HTTP: %w", err)
	}
	s.cfg.HTTPAddr = ln.Addr().String()

	mountConsole(mux, s.cfg.HTTPAddr, s.svc)

	s.httpSrv = &http.Server{Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		err := s.httpSrv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	return errCh, nil
}

// waitAndShutdown blocks until signal, stopCh, or HTTP error, then shuts down.
func (s *Server) waitAndShutdown(httpErrCh <-chan error) error {
	if s == nil {
		panic("waitAndShutdown: s is nil")
	}
	if httpErrCh == nil {
		panic("waitAndShutdown: httpErrCh is nil")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var shutdownErr error
	select {
	case <-sigCh:
		// Clean shutdown requested
	case <-s.stopCh:
		// Programmatic shutdown
	case err := <-httpErrCh:
		if err != nil {
			shutdownErr = fmt.Errorf("HTTP server error: %w", err)
		}
	}

	if err := s.shutdown(); err != nil && shutdownErr == nil {
		shutdownErr = err
	}

	return shutdownErr
}

// Stop closes the stopCh to trigger shutdown. Safe to call multiple times.
func (s *Server) Stop() {
	if s == nil {
		panic("Stop: s is nil")
	}
	if s.stopCh == nil {
		panic("Stop: stopCh is nil")
	}

	select {
	case <-s.stopCh:
		// Already closed
	default:
		close(s.stopCh)
	}
}

// shutdown orchestrates graceful shutdown of all components.
// Panics on programmer errors, returns operational errors.
func (s *Server) shutdown() error {
	if s.ns == nil {
		panic("shutdown: server never started")
	}
	if shutdownDeadline <= 0 {
		panic("shutdown: shutdownDeadline must be positive")
	}

	s.ready.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownDeadline)
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		printStep(os.Stderr, "shutting down http...")
		httpCtx, httpCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer httpCancel()
		if s.httpSrv != nil {
			if err := s.httpSrv.Shutdown(httpCtx); err != nil {
				printStep(os.Stderr, "http shutdown error: "+err.Error())
			}
		}

		printStep(os.Stderr, "stopping triggers...")
		if s.trig != nil {
			s.trig.Stop()
		}
		if s.natsAPI != nil {
			s.natsAPI.Stop()
		}
		// Stop embedded workers before orchestrator so
		// in-flight tasks can publish completion events.
		for _, w := range s.workers {
			w.Stop()
		}
		if len(s.workers) > 0 {
			printStep(
				os.Stderr, "embedded workers stopped",
			)
		}
		printStep(os.Stderr, "stopping orchestrator...")
		if s.orch != nil {
			s.orch.Stop()
		}
		if s.telShutdown != nil {
			s.telShutdown(context.Background())
			printStep(os.Stderr, "telemetry shut down")
		}

		printStep(os.Stderr, "draining nats...")
		if s.nc != nil {
			if err := s.nc.Drain(); err != nil {
				printStep(os.Stderr, "nats drain error: "+err.Error())
			}
		}

		s.ns.Shutdown()
		s.ns.WaitForShutdown()
		if s.tempCreds != "" {
			os.Remove(s.tempCreds)
		}
		printStep(os.Stderr, "shutdown complete")
	}()

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown timeout after %v", shutdownDeadline)
	}
}

// handleHealth checks NATS connectivity and JetStream status.
// Returns 200 on success, 503 on failure.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if w == nil {
		panic("handleHealth: w is nil")
	}
	if r == nil {
		panic("handleHealth: r is nil")
	}

	if s.nc == nil || !s.nc.IsConnected() {
		http.Error(w, "NATS not connected", http.StatusServiceUnavailable)
		return
	}

	js, err := jetstream.New(s.nc)
	if err != nil {
		http.Error(
			w, "JetStream unavailable",
			http.StatusServiceUnavailable,
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	_, err = js.AccountInfo(ctx)
	if err != nil {
		http.Error(
			w, "JetStream account error",
			http.StatusServiceUnavailable,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
	// Client may have disconnected; no recovery action.
	_, _ = w.Write([]byte("ok"))
}

// handleReady checks if the server is ready to accept requests.
// Returns 200 when ready, 503 when not ready.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if w == nil {
		panic("handleReady: w is nil")
	}
	if r == nil {
		panic("handleReady: r is nil")
	}

	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		// Client may have disconnected; no recovery action.
		_, _ = w.Write([]byte("ready"))
	} else {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}

// mountConsole wires the embedded operator UI at /console/. The mount
// is unconditional: when the listener is bound to a non-loopback
// interface without auth configured, the console.Mount handler still
// installs but every request returns 503 with the documented JSON body
// and the loud startup log line is emitted here. Doing the gate at
// mount time (rather than skipping the mount) keeps the route table
// honest — every binary serves the same surface.
func mountConsole(mux *http.ServeMux, httpAddr string, svc *api.Service) {
	if mux == nil {
		panic("mountConsole: mux is nil")
	}
	if httpAddr == "" {
		panic("mountConsole: httpAddr is empty")
	}
	if svc == nil {
		panic("mountConsole: svc is nil")
	}
	cfg := console.AuthConfig{
		HTTPAddr:    httpAddr,
		ForwardAuth: os.Getenv("DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH") == "true",
		Password:    os.Getenv("DAGNATS_CONSOLE_PASSWORD"),
	}
	mode, err := console.ResolveAuthMode(cfg)
	if err != nil {
		// Operator configured both forward-auth and basic-auth — the
		// resolver returns AuthDisabled in that case alongside an
		// error explaining the conflict.
		printStep(os.Stderr, "console: "+err.Error())
	}
	if mode == console.AuthDisabled {
		printStep(os.Stderr, console.DisabledLogMessage)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler := console.Mount(console.Config{
		HTTPAddr: httpAddr,
		AuthMode: mode,
		Password: cfg.Password,
		Build:    "dev",
		Logger:   logger,
		Data:     console.NewAPIDataSource(svc),
	})
	mux.Handle("/console/", handler)
}

// openapiSpecVersion is the value reported in the OpenAPI `info.version`
// field. Bumped manually when the spec shape changes — distinct from
// the dagnats binary version so the spec can iterate independently.
const openapiSpecVersion = "1.0.0"

// newOpenAPIProvider returns an openapi.ProviderFunc closure over the
// running api.Service. Each /openapi.json request rereads the
// triggers / workflow defs KVs so the spec always reflects the live
// state, which matches the brief's "on-demand, no caching" rule.
func newOpenAPIProvider(svc *api.Service) openapi.ProviderFunc {
	if svc == nil {
		panic("newOpenAPIProvider: svc must not be nil")
	}
	return func(ctx context.Context) (
		[]trigger.TriggerDef, map[string]dag.WorkflowDef, error,
	) {
		triggers, err := svc.ListTriggers(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("list triggers: %w", err)
		}
		defs, err := svc.ListWorkflows(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("list workflows: %w", err)
		}
		idx := make(map[string]dag.WorkflowDef, len(defs))
		for _, d := range defs {
			idx[d.Name] = d
		}
		return triggers, idx, nil
	}
}
