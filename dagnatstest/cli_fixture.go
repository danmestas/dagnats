// dagnatstest/cli_fixture.go
// CLIFixture drives the dagnats CLI in-process against the fixture's
// embedded NATS server. Stdout/stderr are captured by redirecting the
// process-global file descriptors during the call. The CLI calls
// os.Exit on errors; the fixture intercepts that via the
// ExitInterceptor wired by the caller (typically the cli package
// itself, which owns the os.Exit indirection).
//
// The fixture takes the CLI entry point as a function value rather
// than importing the cli package directly. This avoids a package
// import cycle (cli tests already import dagnatstest, so dagnatstest
// must not import cli). The cli package wires the bridge by passing
// cli.Run and cli.SwapExitFunc into NewCLIFixture.
package dagnatstest

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

// CLIRunner is the in-process entry point for the dagnats CLI. The
// concrete implementation is cli.Run, but the fixture takes it as a
// value to break the cli ↔ dagnatstest import cycle.
type CLIRunner func(args []string)

// ExitSwapper installs an exit hook and returns the previous one. It
// mirrors cli.SwapExitFunc and is passed in for the same cycle
// reason.
type ExitSwapper func(next func(int)) func(int)

// CLIFixture drives the dagnats CLI in-process. Constructed off a
// Harness; the harness owns NATS lifecycle, the fixture owns the
// shape of "execute the CLI and observe its output".
type CLIFixture struct {
	h         *Harness
	natsURL   string
	runCLI    CLIRunner
	swapExit  ExitSwapper
}

// NewCLIFixture constructs a CLIFixture. runCLI is typically cli.Run
// and swapExit is typically cli.SwapExitFunc — the caller injects
// them so this package does not import cli (which already imports
// this one via tests).
func NewCLIFixture(
	h *Harness, runCLI CLIRunner, swapExit ExitSwapper,
) *CLIFixture {
	if h == nil {
		panic("NewCLIFixture: harness must not be nil")
	}
	if h.NC == nil {
		panic("NewCLIFixture: harness.NC must not be nil")
	}
	if runCLI == nil {
		panic("NewCLIFixture: runCLI must not be nil")
	}
	if swapExit == nil {
		panic("NewCLIFixture: swapExit must not be nil")
	}
	url := h.NC.ConnectedUrl()
	if url == "" {
		panic("NewCLIFixture: connected URL is empty")
	}
	return &CLIFixture{
		h:        h,
		natsURL:  url,
		runCLI:   runCLI,
		swapExit: swapExit,
	}
}

// Run executes the dagnats CLI with the given args. On non-zero exit
// it fatals the test. Returns stdout (stderr discarded — use
// RunSplit when stderr matters).
func (f *CLIFixture) Run(
	t *testing.T, args ...string,
) string {
	t.Helper()
	out, _, err := f.runInternal(t, args)
	if err != nil {
		t.Fatalf("CLI %v: %v", args, err)
	}
	return out
}

// RunErr executes the CLI and returns stdout plus any non-nil error
// produced by a non-zero exit. Stdout may be partially populated
// even when err is non-nil.
func (f *CLIFixture) RunErr(
	t *testing.T, args ...string,
) (string, error) {
	t.Helper()
	out, errOut, err := f.runInternal(t, args)
	if err != nil {
		// Surface stderr text on the error so test assertions
		// can match on the error message.
		return out, fmt.Errorf("%w: stderr=%s", err, errOut)
	}
	return out, nil
}

// RunSplit executes the CLI and returns stdout + stderr separately.
// Fatals on non-zero exit; for negative-path tests use RunErr.
func (f *CLIFixture) RunSplit(
	t *testing.T, args ...string,
) (string, string) {
	t.Helper()
	out, errOut, err := f.runInternal(t, args)
	if err != nil {
		t.Fatalf("CLI %v: %v (stderr=%s)", args, err, errOut)
	}
	return out, errOut
}

// runInternal is the single concrete implementation behind Run,
// RunErr, and RunSplit. It points the CLI at the fixture's NATS URL
// via env, captures stdout/stderr, intercepts exitFunc, and runs.
func (f *CLIFixture) runInternal(
	t *testing.T, args []string,
) (string, string, error) {
	t.Helper()
	if f == nil {
		panic("runInternal: fixture must not be nil")
	}
	if len(args) == 0 {
		panic("runInternal: args must not be empty")
	}
	if len(args) > 100 {
		panic("runInternal: args exceeds bound")
	}

	t.Setenv("DAGNATS_NATS_URL", f.natsURL)
	t.Setenv("NATS_URL", "")
	t.Setenv("NO_COLOR", "1")

	stdout, stderr, restore := captureFds(t)
	exitCode, restoreExit := f.installExitInterceptor()
	defer restoreExit()

	full := append([]string{"dagnats"}, args...)
	f.runCLIRecover(full)

	restore()
	out := stdout()
	errOut := stderr()
	if *exitCode != 0 {
		return out, errOut, fmt.Errorf(
			"cli exited with code %d: %s",
			*exitCode, errOut,
		)
	}
	return out, errOut, nil
}

// runCLIRecover invokes the injected CLIRunner and recovers the
// exitPanic raised by the interceptor. Any other panic re-panics so
// genuine programmer errors still fail the test loudly.
func (f *CLIFixture) runCLIRecover(args []string) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(exitPanic); ok {
				return
			}
			panic(r)
		}
	}()
	f.runCLI(args)
}

// installExitInterceptor swaps the CLI's exit hook so a non-zero
// exit becomes a captured int rather than a process-killing call.
// The returned restore callback reinstates the prior hook.
func (f *CLIFixture) installExitInterceptor() (
	*int, func(),
) {
	captured := 0
	prev := f.swapExit(func(code int) {
		captured = code
		// Replicate os.Exit's "do not return" effect via panic
		// so downstream code does not dereference nil returns.
		// runCLIRecover catches the sentinel exitPanic.
		panic(exitPanic{code: code})
	})
	return &captured, func() {
		f.swapExit(prev)
	}
}

// captureFds redirects os.Stdout and os.Stderr to pipes that drain
// concurrently. Returns stdout-getter, stderr-getter, and a restore
// callback. Concurrent draining prevents deadlock on writes larger
// than the OS pipe buffer (~64 KiB).
func captureFds(t *testing.T) (
	func() string, func() string, func(),
) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("captureFds: stdout pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("captureFds: stderr pipe: %v", err)
	}
	os.Stdout = outW
	os.Stderr = errW

	outCh := drainPipe(outR)
	errCh := drainPipe(errR)

	var outStr, errStr string
	restore := func() {
		if err := outW.Close(); err != nil {
			t.Fatalf("captureFds: close stdout: %v", err)
		}
		if err := errW.Close(); err != nil {
			t.Fatalf("captureFds: close stderr: %v", err)
		}
		os.Stdout = oldOut
		os.Stderr = oldErr
		outStr = <-outCh
		errStr = <-errCh
	}
	return func() string { return outStr },
		func() string { return errStr },
		restore
}

// drainPipe reads r to EOF on a goroutine and returns a channel that
// yields the collected bytes as a string once the reader closes.
func drainPipe(r *os.File) <-chan string {
	ch := make(chan string, 1)
	go func() {
		b, err := io.ReadAll(r)
		if err != nil && !errors.Is(err, os.ErrClosed) {
			ch <- "drain error: " + err.Error()
			return
		}
		ch <- string(b)
	}()
	return ch
}

// exitPanic is the sentinel panic raised by the interceptor. It is
// recovered inside runCLIRecover so the CLI's exit path unwinds.
type exitPanic struct{ code int }
