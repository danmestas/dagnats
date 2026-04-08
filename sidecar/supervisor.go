package sidecar

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	startHealthTimeout  = 5 * time.Second
	stopGrace           = 10 * time.Second
	healthCheckInterval = 5 * time.Second

	healthReadTimeout  = 5 * time.Second
	healthWriteTimeout = 5 * time.Second
	healthShutTimeout  = 5 * time.Second
	healthMaxHeader    = 1 << 16

	processCountExpected = 3
)

// Supervisor orchestrates the three sidecar child processes
// in dependency order: otlp2parquet, otelcol, mcp.
type Supervisor struct {
	cfg        *SidecarConfig
	processes  []*Process // [otlp2parquet, otelcol, mcp]
	ctx        context.Context
	cancel     context.CancelFunc
	startedAt  time.Time
	healthSrv  *http.Server
	healthAddr string // actual listen address after start
}

// NewSupervisor builds a Supervisor with three Process structs
// configured from the sidecar config.
func NewSupervisor(cfg *SidecarConfig) (*Supervisor, error) {
	if cfg == nil {
		panic("NewSupervisor: cfg is nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	procs := []*Process{
		{
			Name: "otlp2parquet",
			Bin:  "otlp2parquet",
			Args: []string{
				"--port", "4319",
				"--output", cfg.Storage.LocalPath,
			},
		},
		{
			Name: "otelcol",
			Bin:  "otelcol",
			Args: []string{
				"--config", collectorConfigPath(cfg),
			},
		},
		{
			Name: "dagnats-mcp-duckdb",
			Bin:  "dagnats-mcp-duckdb",
			Args: []string{
				"--data-dir", cfg.Storage.LocalPath,
			},
		},
	}

	return &Supervisor{
		cfg:       cfg,
		processes: procs,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// collectorConfigPath returns the path where the generated
// OTel Collector config will be written.
func collectorConfigPath(cfg *SidecarConfig) string {
	if cfg == nil {
		panic("collectorConfigPath: cfg is nil")
	}
	return cfg.Storage.LocalPath + "/otelcol-config.yaml"
}

// Start launches processes in dependency order. Each process
// must become healthy within 5s before the next one starts.
// On partial failure, already-started processes are stopped.
func (s *Supervisor) Start() error {
	if len(s.processes) != processCountExpected {
		panic("Supervisor.Start: unexpected process count")
	}

	s.startedAt = time.Now()

	for i, proc := range s.processes {
		if err := proc.Start(s.ctx); err != nil {
			s.stopUpTo(i)
			return fmt.Errorf("start %s: %w", proc.Name, err)
		}
		if err := s.waitHealthy(proc); err != nil {
			s.stopUpTo(i + 1)
			return fmt.Errorf(
				"health %s: %w", proc.Name, err,
			)
		}
	}

	return nil
}

// waitHealthy polls the process health check until it passes
// or the timeout expires. Bounded at 5s with 100ms polling.
func (s *Supervisor) waitHealthy(p *Process) error {
	if p == nil {
		panic("waitHealthy: process is nil")
	}

	deadline := time.After(startHealthTimeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := p.Healthy(); err == nil {
			return nil
		}
		select {
		case <-deadline:
			return fmt.Errorf(
				"process %q not healthy within %v",
				p.Name, startHealthTimeout,
			)
		case <-ticker.C:
			// retry
		}
	}
}

// stopUpTo stops the first n processes in reverse order.
func (s *Supervisor) stopUpTo(n int) {
	if n < 0 || n > len(s.processes) {
		panic(fmt.Sprintf(
			"stopUpTo: n=%d out of range [0,%d]",
			n, len(s.processes),
		))
	}
	for i := n - 1; i >= 0; i-- {
		_ = s.processes[i].Stop(stopGrace)
	}
}

// Stop shuts down the health server, then sends stop to all
// processes in reverse dependency order, then cancels context.
func (s *Supervisor) Stop() {
	s.stopHealthServer()
	s.stopUpTo(len(s.processes))
	s.cancel()
}

// Run starts all processes, runs a health-check loop, and
// blocks until SIGINT or SIGTERM. On signal it stops all
// processes gracefully.
func (s *Supervisor) Run() error {
	if err := s.Start(); err != nil {
		return fmt.Errorf("supervisor start: %w", err)
	}

	if err := s.startHealthServer(); err != nil {
		s.Stop()
		return fmt.Errorf("health server: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			s.Stop()
			return nil
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			s.checkHealth()
		}
	}
}

// checkHealth iterates all processes and restarts any that
// are unhealthy. Bounded by the process list length.
func (s *Supervisor) checkHealth() {
	for _, proc := range s.processes {
		if err := proc.Healthy(); err != nil {
			_ = proc.RestartWithBackoff()
		}
	}
}

// startHealthServer binds the health HTTP server to the
// configured address and starts serving in a goroutine.
func (s *Supervisor) startHealthServer() error {
	if s.cfg == nil {
		panic("startHealthServer: config is nil")
	}
	if s.cfg.Supervisor.Listen == "" {
		panic("startHealthServer: listen address is empty")
	}

	ln, err := net.Listen("tcp", s.cfg.Supervisor.Listen)
	if err != nil {
		return fmt.Errorf(
			"listen %s: %w", s.cfg.Supervisor.Listen, err,
		)
	}

	s.healthAddr = ln.Addr().String()
	s.healthSrv = &http.Server{
		Handler:        newHealthHandler(s),
		ReadTimeout:    healthReadTimeout,
		WriteTimeout:   healthWriteTimeout,
		MaxHeaderBytes: healthMaxHeader,
	}

	go func() { _ = s.healthSrv.Serve(ln) }()
	return nil
}

// stopHealthServer gracefully shuts down the health HTTP
// server with a bounded timeout. No-op if not started.
func (s *Supervisor) stopHealthServer() {
	if s.healthSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), healthShutTimeout,
	)
	defer cancel()
	_ = s.healthSrv.Shutdown(ctx)
}
