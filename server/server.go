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
	"github.com/danmestas/dagnats/internal/configfile"
	"github.com/danmestas/dagnats/internal/console"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/observe/logring"
	"github.com/danmestas/dagnats/internal/observe/metrics"
	"github.com/danmestas/dagnats/internal/observe/prom"
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
	cfg                Config
	ns                 *natsserver.Server
	nc                 *nats.Conn
	orch               *engine.Orchestrator
	svc                *api.Service
	natsAPI            *api.NATSAPI
	trig               *trigger.TriggerService
	bridge             *bridge.Bridge
	httpSrv            *http.Server
	telShutdown        func(context.Context)
	metricsAgg         *metrics.Aggregator
	metricsStop        func()
	metricsErrorReason string
	ready              atomic.Bool
	stopCh             chan struct{}
	workerShims        []*WorkerShim
	workers            []*worker.Worker
	running            atomic.Bool
	tempCreds          string              // inline creds temp file; cleaned on shutdown
	cfgWatcher         *configfile.Watcher // hot-reload watcher (#358)
	cfgWatcherCancel   context.CancelFunc  // cleanup paired with cfgWatcher
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
		natsutil.WithStoreBudget(s.cfg.MaxStoreBytes),
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
	s.natsAPI = api.NewNATSAPI(s.svc, s.nc, s.cfg.Build)
	s.natsAPI.Start()
	printStep(os.Stderr, "nats api started")

	s.orch = engine.NewOrchestrator(
		s.nc, engine.WithRunsMaxAge(s.cfg.RunsMaxAge),
	)
	s.orch.Start()
	printStep(os.Stderr, "orchestrator started")

	bridgeJS, err := jetstream.New(s.nc)
	if err != nil {
		s.orch.Stop()
		s.natsAPI.Stop()
		s.telShutdown(context.Background())
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("bridge jetstream init: %w", err)
	}
	bridgePub := natsutil.NewTracingPublisher(s.nc, bridgeJS)
	s.bridge = bridge.NewBridge(bridgePub)
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

	// Phase 4 / ADR-018: start the dagnats.yaml hot-reload watcher
	// after all components are up. A failure here is logged and
	// swallowed — the rest of the server is healthy without the
	// declarative config layer, and refusing to start over a
	// watcher problem would be worse than running without reload.
	if err := s.startConfigWatcher(); err != nil {
		slog.Warn("configfile watcher disabled",
			"path", s.cfg.ConfigFilePath, "err", err)
	}

	return nil
}

// startConfigWatcher boots the configfile.Watcher and drives a
// Diff+Apply pipeline on every reload. Returns nil when no config
// file path was supplied (a missing dagnats.yaml is a normal mode,
// not a failure). Per ADR-018 — file edits update workflows and
// triggers without restart.
func (s *Server) startConfigWatcher() error {
	if s.cfg.ConfigFilePath == "" {
		return nil
	}
	if s.nc == nil {
		panic("startConfigWatcher: nats conn must not be nil")
	}

	js, err := jetstream.New(s.nc)
	if err != nil {
		return fmt.Errorf("jetstream.New: %w", err)
	}
	kvCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second)
	defer cancel()
	wfKV, err := js.KeyValue(kvCtx, "workflow_defs")
	if err != nil {
		return fmt.Errorf("workflow_defs KV: %w", err)
	}
	trKV, err := js.KeyValue(kvCtx, "triggers")
	if err != nil {
		return fmt.Errorf("triggers KV: %w", err)
	}
	kv := configfile.KVHandles{WorkflowDefs: wfKV, Triggers: trKV}

	runCtx, runCancel := context.WithCancel(context.Background())
	src := configfile.SourceLabel(
		filepathBase(s.cfg.ConfigFilePath))

	reload := func(cfg configfile.ConfigFile) error {
		return applyConfigFile(runCtx, kv, cfg, src)
	}

	w, err := configfile.NewWatcher(
		s.cfg.ConfigFilePath, reload, slog.Default())
	if err != nil {
		runCancel()
		return fmt.Errorf("new watcher: %w", err)
	}
	if err := w.Start(runCtx); err != nil {
		runCancel()
		return fmt.Errorf("start watcher: %w", err)
	}

	// Best-effort initial apply so a freshly-started server picks
	// up the file's declarations without waiting for the first edit.
	if cfg, err := loadConfigFileSafe(s.cfg.ConfigFilePath); err == nil {
		_ = reload(cfg)
	}

	s.cfgWatcher = w
	s.cfgWatcherCancel = runCancel
	printStep(os.Stderr, "configfile watcher started")
	return nil
}

// applyConfigFile drives the convert → diff → apply pipeline. Pulled
// out so the reload closure stays under the 70-line limit. Errors
// from individual apply ops are logged via Apply's joined error and
// returned up so the watcher logs the full failure.
func applyConfigFile(
	ctx context.Context, kv configfile.KVHandles,
	cfg configfile.ConfigFile, sourceLabel string,
) error {
	desired := configfile.DesiredState{
		Workflows: map[string]dag.WorkflowDef{},
		Triggers:  map[string]trigger.TriggerDef{},
	}
	for _, wf := range cfg.Workflows {
		desired.Workflows[wf.Name] = configfile.ToWorkflowDef(wf)
	}
	for _, tr := range cfg.Triggers {
		desired.Triggers[tr.ID] =
			configfile.ToTriggerDef(tr, sourceLabel)
	}
	current, err := configfile.ReadCurrent(ctx, kv, sourceLabel)
	if err != nil {
		return fmt.Errorf("read current: %w", err)
	}
	plan := configfile.Diff(current, desired)
	if plan.Empty() {
		return nil
	}
	return configfile.Apply(ctx, kv, plan)
}

// loadConfigFileSafe reads + parses the file once for the initial
// apply. Returns a zero ConfigFile on any error — the caller treats
// "couldn't load" as "no initial state" and waits for the next edit.
func loadConfigFileSafe(path string) (configfile.ConfigFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return configfile.ConfigFile{}, err
	}
	defer f.Close()
	cfg, err := configfile.Load(f)
	if err != nil {
		return configfile.ConfigFile{}, err
	}
	if err := configfile.Validate(cfg); err != nil {
		return configfile.ConfigFile{}, err
	}
	return cfg, nil
}

// filepathBase returns the last path component. Pulled into a tiny
// helper so the import surface stays small at the call site.
func filepathBase(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}

// startHTTP creates and launches the HTTP server in a goroutine.
// If the default port is taken, picks a free one automatically.
// Returns a channel that receives any Serve error.
// rootRedirectOr 302s exact-root requests to /console/ so operators
// landing on the bare host get the UI instead of a "404 page not
// found" from the REST catch-all. Any other path falls through to
// the wrapped REST handler unchanged.
func rootRedirectOr(rest http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/console/", http.StatusFound)
			return
		}
		rest.ServeHTTP(w, r)
	})
}

// listenHTTPWithFallback binds addr, and on a default-address conflict
// either fails fast (failFast) or retries on an ephemeral port that
// PRESERVES the configured bind host (loopback stays loopback), so the
// fallback never widens the bind scope and trips the console's
// non-loopback auth gate. Extracted to keep startHTTP under the 70-line
// limit (#370).
func listenHTTPWithFallback(
	addr string, failFast bool,
) (net.Listener, error) {
	if addr == "" {
		panic("listenHTTPWithFallback: addr is empty")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil && addr == defaultHTTPAddr {
		if failFast {
			return nil, fmt.Errorf(
				"HTTP address %s in use (another process likely "+
					"holds it); --fail-on-port-conflict is set, "+
					"refusing to fall back", addr)
		}
		fallbackAddr := loopbackEphemeralAddr(addr)
		printWarning(os.Stderr, fmt.Sprintf(
			"%s in use, picking a free port on the same host", addr))
		ln, err = net.Listen("tcp", fallbackAddr)
	}
	if err != nil {
		return nil, fmt.Errorf("listen HTTP: %w", err)
	}
	return ln, nil
}

func (s *Server) startHTTP() (<-chan error, error) {
	if s.svc == nil {
		panic("startHTTP: svc is nil")
	}

	mux := http.NewServeMux()
	// Bare-root GETs redirect to /console/ so operators landing on the
	// host get the UI instead of a "404 page not found" from the REST
	// catch-all. Other paths fall through to the REST handler unchanged.
	mux.Handle("/", rootRedirectOr(api.NewRESTHandler(s.svc)))
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

	ln, err := listenHTTPWithFallback(s.cfg.HTTPAddr, s.cfg.FailOnPortConflict)
	if err != nil {
		return nil, err
	}
	s.cfg.HTTPAddr = ln.Addr().String()

	s.startMetricsAggregator()
	mountMetricsExporter(
		mux, s.metricsAgg, slog.Default(), s.cfg.HTTPAddr,
		s.metricsErrorReason,
	)
	mountConsole(mux, s.cfg.HTTPAddr, s.svc, s.nc,
		s.metricsAgg, s.metricsErrorReason, s.ns, s.cfg.Build)

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

		// Stop the configfile watcher first so it can't fire one
		// last reload while the rest of the components are
		// tearing down (#358).
		if s.cfgWatcher != nil {
			s.cfgWatcher.Stop()
			s.cfgWatcher = nil
		}
		if s.cfgWatcherCancel != nil {
			s.cfgWatcherCancel()
			s.cfgWatcherCancel = nil
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
		if s.metricsStop != nil {
			s.metricsStop()
		}
		if s.metricsAgg != nil {
			s.metricsAgg.Close()
			printStep(os.Stderr, "metrics aggregator shut down")
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
func mountConsole(
	mux *http.ServeMux, httpAddr string,
	svc *api.Service, nc *nats.Conn,
	agg *metrics.Aggregator, metricsErrorReason string,
	ns *natsserver.Server, build string,
) {
	if mux == nil {
		panic("mountConsole: mux is nil")
	}
	consoleCfg := buildConsoleConfig(
		httpAddr, svc, nc, agg, metricsErrorReason, ns, build)
	handler := console.Mount(consoleCfg)
	mux.Handle("/console/", handler)
}

// buildConsoleConfig assembles the console.Config the production
// server passes to console.Mount. Extracted from mountConsole so the
// wiring is testable without a live HTTP mux — issue #290's regression
// guard pins MetricsSource to the engine's aggregator (when present)
// so the dashboard's on-demand p99 + success-rate tiles have
// histograms to read from.
//
// The function is the single source of truth for "what does the
// console see in production?" — keep all environment lookups and
// dependency adapter calls here so a test asserting cfg.Metrics != nil
// also covers every other field the production wiring sets.
func buildConsoleConfig(
	httpAddr string, svc *api.Service, nc *nats.Conn,
	agg *metrics.Aggregator, metricsErrorReason string,
	ns *natsserver.Server, build string,
) console.Config {
	if httpAddr == "" {
		panic("buildConsoleConfig: httpAddr is empty")
	}
	if svc == nil {
		panic("buildConsoleConfig: svc is nil")
	}
	authCfg := console.AuthConfig{
		HTTPAddr:    httpAddr,
		ForwardAuth: os.Getenv("DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH") == "true",
		Password:    os.Getenv("DAGNATS_CONSOLE_PASSWORD"),
	}
	mode, err := console.ResolveAuthMode(authCfg)
	if err != nil {
		// Operator configured both forward-auth and basic-auth — the
		// resolver returns AuthDisabled in that case alongside an
		// error explaining the conflict.
		printStep(os.Stderr, "console: "+err.Error())
	}
	if mode == console.AuthDisabled {
		printStep(os.Stderr, console.DisabledLogMessage)
	}
	// Install the bounded in-memory log ring as the engine's slog
	// default so /console/logs (#342) sees every record. The ring is
	// a pass-through — the stderr text handler keeps emitting log
	// lines for stdout-aware deploys; the ring just retains the
	// last 10k entries OR 30 minutes for the console live tail.
	innerSlogHandler := slog.NewTextHandler(os.Stderr, nil)
	ring := logring.New(innerSlogHandler)
	logger := slog.New(ring)
	slog.SetDefault(logger)
	auditKV := openConsoleAuditKV(nc, logger)
	readOnly := console.ReadOnlyFromEnv(os.Getenv("CONSOLE_READ_ONLY"))
	if readOnly {
		printStep(os.Stderr, "console: read-only mode active "+
			"(CONSOLE_READ_ONLY=true); mutations refused")
	}
	if _, generated, err := console.LoadCSRFSecretFromEnv(); err != nil {
		printStep(os.Stderr, "console: CSRF secret load failed: "+
			err.Error())
	} else if generated && mode != console.AuthLoopback {
		printStep(os.Stderr, "console: CONSOLE_CSRF_SECRET not set; "+
			"using random secret (restarts will rotate it). "+
			"Set the env var for stable tokens across restarts.")
	}
	metricsSrc := console.AdaptAggregator(agg)
	consoleCfg := console.Config{
		HTTPAddr: httpAddr,
		AuthMode: mode,
		Password: authCfg.Password,
		Build:    build,
		Logger:   logger,
		Data: console.WithServerStats(
			console.WithMetrics(
				console.NewAPIDataSource(svc, nc, auditKV, logger),
				metricsSrc,
			),
			ns,
		),
		ReadOnly:           readOnly,
		Metrics:            metricsSrc,
		MetricsErrorReason: metricsErrorReason,
		LogRing:            ring,
	}
	if !readOnly {
		enableConsoleSoftDiscard(&consoleCfg, svc, logger)
	}
	return consoleCfg
}

// enableConsoleSoftDiscard wires the DLQ soft-discard tombstone path
// + sweeper goroutine. window is 5s per the brief. The expiry path
// invokes svc.DiscardDeadLetter with a short timeout so a stuck
// JetStream call doesn't wedge the sweeper.
func enableConsoleSoftDiscard(
	cfg *console.Config, svc *api.Service, logger *slog.Logger,
) {
	if cfg == nil {
		panic("enableConsoleSoftDiscard: cfg is nil")
	}
	if svc == nil {
		panic("enableConsoleSoftDiscard: svc is nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	const window = 5 * time.Second
	expire := func(seq uint64) {
		ctx, cancel := context.WithTimeout(
			context.Background(), 4*time.Second)
		defer cancel()
		if err := svc.DiscardDeadLetter(ctx, seq); err != nil {
			logger.Warn("console: soft-discard sweep removal failed",
				"seq", seq, "err", err)
		}
	}
	console.EnableSoftDiscard(cfg, window, expire)
	stop := make(chan struct{})
	go console.RunTombstoneSweeper(cfg, 250*time.Millisecond, stop)
}

// startMetricsAggregator builds the in-memory metric aggregator and
// boots the NATS pump that drains the TELEMETRY stream into it.
// Nil-tolerant: when the JetStream connection isn't available the
// aggregator stays nil and the dashboard renders empty-state tiles
// (the /metrics exporter still mounts but returns the no-data banner).
func (s *Server) startMetricsAggregator() {
	if s == nil {
		panic("startMetricsAggregator: s is nil")
	}
	if s.nc == nil {
		return
	}
	js, err := jetstream.New(s.nc)
	if err != nil {
		s.metricsErrorReason = "jetstream init failed: " + err.Error()
		slog.Default().Warn(
			"metrics: jetstream init failed; aggregator disabled",
			"err", err)
		return
	}
	agg := metrics.NewAggregator(slog.Default())
	ctx := context.Background()
	stop, err := agg.StartPump(ctx, js)
	if err != nil {
		s.metricsErrorReason = "pump start failed: " + err.Error()
		slog.Default().Warn(
			"metrics: pump start failed; aggregator disabled",
			"err", err)
		agg.Close()
		return
	}
	s.metricsAgg = agg
	s.metricsStop = stop
}

// mountMetricsExporter installs /metrics. Auth is loopback-default:
// non-loopback listeners refuse the request unless the operator has
// set METRICS_AUTH=none. METRICS_AUTH=basic reuses the console basic-
// auth password; METRICS_AUTH=forward trusts X-Forwarded-User. Same
// vocabulary as the console gate, kept independent so an operator
// can lock the console down without locking the scraper out.
func mountMetricsExporter(
	mux *http.ServeMux, agg *metrics.Aggregator,
	logger *slog.Logger, httpAddr string, errorReason string,
) {
	if mux == nil {
		panic("mountMetricsExporter: mux is nil")
	}
	if httpAddr == "" {
		panic("mountMetricsExporter: httpAddr is empty")
	}
	if logger == nil {
		logger = slog.Default()
	}
	authCfg := LoadMetricsAuthConfigFromEnv(logger)
	LogMetricsAuthStartup(logger, authCfg, httpAddr)
	if agg == nil {
		// Aggregator init failed earlier — install a stub handler
		// that reports the gap to scrapers rather than 404. The
		// gate still applies so an unauthorised caller can't probe
		// for the aggregator's presence. When the aggregator failed
		// to start (rather than was never wired) include the failure
		// reason so scrapers and operators reading curl output learn
		// the layer is broken, not deferred.
		body := "# metrics aggregator not configured\n"
		if errorReason != "" {
			body = "# metrics aggregator down: " + errorReason + "\n"
		}
		stub := http.HandlerFunc(func(
			w http.ResponseWriter, r *http.Request,
		) {
			if r == nil {
				panic("metrics stub: r is nil")
			}
			w.Header().Set("Content-Type", prom.ContentType)
			_, _ = w.Write([]byte(body))
		})
		mux.Handle("/metrics",
			metricsAuthMiddleware(authCfg, stub))
		return
	}
	mux.Handle("/metrics",
		metricsAuthMiddleware(
			authCfg, prom.Handler(agg, logger),
		))
}

// openConsoleAuditKV opens (or creates) the console_audit KV bucket on
// the live JetStream connection. nil-tolerant: returns nil with a
// slog.Warn when JetStream init or bucket creation fails so the
// console mount path can still serve read-only pages without audit
// support. The audit emitter logs and drops on a nil bucket.
func openConsoleAuditKV(
	nc *nats.Conn, logger *slog.Logger,
) jetstream.KeyValue {
	if nc == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	js, err := jetstream.New(nc)
	if err != nil {
		logger.Warn("console: jetstream for audit init failed",
			"err", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second)
	defer cancel()
	kv, err := console.NewAuditKV(ctx, js)
	if err != nil {
		logger.Warn("console: open audit bucket failed", "err", err)
		return nil
	}
	return kv
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
