package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/observe/simple"
	"github.com/danmestas/dagnats/trigger"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const shutdownDeadline = 15 * time.Second

// Server is the all-in-one DagNats server lifecycle manager.
type Server struct {
	cfg     Config
	ns      *natsserver.Server
	nc      *nats.Conn
	orch    *engine.ActorOrchestrator
	svc     *api.Service
	trig    *trigger.TriggerService
	httpSrv *http.Server
	tel     *observe.Telemetry
	telStop func()
	ready   atomic.Bool
	stopCh  chan struct{}
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

	if err := os.MkdirAll(s.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	if err := s.startComponents(); err != nil {
		return err
	}

	httpErrCh := s.startHTTP()

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

	s.ns, err = startNATS(s.cfg)
	if err != nil {
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
	)
	if err != nil {
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("setup NATS resources: %w", err)
	}
	printStep(os.Stderr, "streams and kv buckets ready")

	s.tel, s.telStop = simple.SetupTelemetry(s.nc)
	s.svc = api.NewService(s.nc, s.tel)
	s.orch = engine.NewActorOrchestrator(s.nc, s.tel)
	s.orch.Start()
	printStep(os.Stderr, "orchestrator started")

	s.trig, err = trigger.NewTriggerService(s.nc)
	if err != nil {
		s.orch.Stop()
		s.telStop()
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("create trigger service: %w", err)
	}

	if err := s.trig.Start(); err != nil {
		s.trig.Stop()
		s.orch.Stop()
		s.telStop()
		s.nc.Close()
		s.ns.Shutdown()
		return fmt.Errorf("start trigger service: %w", err)
	}
	printStep(os.Stderr, "trigger service started")

	return nil
}

// startHTTP creates and launches the HTTP server in a goroutine.
// Returns a channel that receives any ListenAndServe error.
func (s *Server) startHTTP() <-chan error {
	if s.svc == nil {
		panic("startHTTP: svc is nil")
	}

	mux := http.NewServeMux()
	mux.Handle("/", api.NewRESTHandler(s.svc))
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	if s.trig != nil {
		mux.Handle("/hooks/", s.trig.WebhookHandler())
	}

	s.httpSrv = &http.Server{
		Addr:    s.cfg.HTTPAddr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		err := s.httpSrv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	return errCh
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
			_ = s.httpSrv.Shutdown(httpCtx)
		}

		printStep(os.Stderr, "stopping triggers...")
		if s.trig != nil {
			s.trig.Stop()
		}
		printStep(os.Stderr, "stopping orchestrator...")
		if s.orch != nil {
			s.orch.Stop()
		}
		if s.telStop != nil {
			s.telStop()
		}

		printStep(os.Stderr, "draining nats...")
		if s.nc != nil {
			_ = s.nc.Drain()
		}

		s.ns.Shutdown()
		s.ns.WaitForShutdown()
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

	js, err := s.nc.JetStream()
	if err != nil {
		http.Error(w, "JetStream unavailable", http.StatusServiceUnavailable)
		return
	}

	_, err = js.AccountInfo()
	if err != nil {
		http.Error(w, "JetStream account error", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
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
		_, _ = w.Write([]byte("ready"))
	} else {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}
