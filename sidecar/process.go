package sidecar

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

const (
	backoffInitial         = 1 * time.Second
	backoffMax             = 60 * time.Second
	backoffReset           = 5 * time.Minute
	maxConsecutiveFailures = 5

	stopFallback = 5 * time.Second
)

// Process manages a single child process with start, stop,
// health checking, and restart with exponential backoff.
type Process struct {
	Name   string
	Bin    string
	Args   []string
	Env    []string
	Dir    string
	Health func() error // nil = just check process alive

	cmd      *exec.Cmd
	done     chan struct{} // closed when cmd.Wait returns
	mu       sync.Mutex
	backoff  time.Duration
	failures int
	failedAt time.Time
}

// Start launches the child process. Stdout and stderr are
// piped to a logger with the process name as prefix.
// The context is used only to cancel start; signals are
// managed explicitly via Stop.
func (p *Process) Start(ctx context.Context) error {
	if p.Bin == "" {
		panic("Process.Start: Bin is empty")
	}
	if ctx == nil {
		panic("Process.Start: ctx is nil")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isRunningLocked() {
		return fmt.Errorf(
			"process %q is already running", p.Name,
		)
	}

	cmd := exec.Command(p.Bin, p.Args...)
	cmd.Env = p.Env
	cmd.Dir = p.Dir
	// Create a new process group so we can signal the
	// entire tree (parent + children) on stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	prefix := fmt.Sprintf("[%s] ", p.Name)
	logger := log.New(os.Stderr, prefix, 0)
	cmd.Stdout = newLogWriter(logger)
	cmd.Stderr = newLogWriter(logger)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %q: %w", p.Name, err)
	}

	p.cmd = cmd
	p.done = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()

	return nil
}

// Stop sends SIGTERM to the process group, waits up to
// timeout, then SIGKILL. No-op if not running.
func (p *Process) Stop(timeout time.Duration) error {
	if timeout <= 0 {
		panic("Process.Stop: timeout must be positive")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isRunningLocked() {
		return nil
	}

	return p.stopLocked(timeout)
}

// stopLocked sends SIGTERM then SIGKILL to the process
// group. Caller holds mu.
func (p *Process) stopLocked(timeout time.Duration) error {
	pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
	if err != nil {
		// Process already gone.
		return nil
	}

	// SIGTERM the entire process group.
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
		// Escalate to SIGKILL.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-p.done
		return nil
	}
}

// IsRunning checks whether the child process is alive.
func (p *Process) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isRunningLocked()
}

// isRunningLocked checks process liveness. Caller holds mu.
func (p *Process) isRunningLocked() bool {
	if p.cmd == nil || p.cmd.Process == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Healthy returns nil when the process is healthy.
// Uses the Health func if set, otherwise checks alive.
func (p *Process) Healthy() error {
	if p.Health != nil {
		return p.Health()
	}
	if !p.IsRunning() {
		return fmt.Errorf(
			"process %q is not running", p.Name,
		)
	}
	return nil
}

// RestartWithBackoff stops the process (if running), waits
// the current backoff duration, then starts again. Backoff
// doubles each failure from 1s to 60s. Resets after 5 min
// of stability. Returns error after 5 consecutive failures.
func (p *Process) RestartWithBackoff() error {
	p.mu.Lock()

	now := time.Now()
	if !p.failedAt.IsZero() &&
		now.Sub(p.failedAt) >= backoffReset {
		p.failures = 0
		p.backoff = 0
	}

	p.failures++
	p.failedAt = now

	if p.failures > maxConsecutiveFailures {
		p.mu.Unlock()
		return fmt.Errorf(
			"process %q: %d consecutive failures, giving up",
			p.Name, p.failures-1,
		)
	}

	p.backoff = p.nextBackoff()
	wait := p.backoff

	if p.isRunningLocked() {
		_ = p.stopLocked(stopFallback)
	}
	p.mu.Unlock()

	time.Sleep(wait)

	return p.Start(context.Background())
}

// nextBackoff calculates the next backoff duration.
// Caller holds mu.
func (p *Process) nextBackoff() time.Duration {
	if p.backoff == 0 {
		return backoffInitial
	}
	next := p.backoff * 2
	if next > backoffMax {
		return backoffMax
	}
	return next
}

// logWriter adapts a *log.Logger to an io.Writer so child
// process output can be prefixed with the process name.
type logWriter struct {
	logger *log.Logger
}

func newLogWriter(l *log.Logger) io.Writer {
	if l == nil {
		panic("newLogWriter: logger is nil")
	}
	return &logWriter{logger: l}
}

func (w *logWriter) Write(data []byte) (int, error) {
	w.logger.Print(string(data))
	return len(data), nil
}
