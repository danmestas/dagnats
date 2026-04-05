// cli/dev_runner.go
// Build and restart logic for the dev watch mode. Compiles the
// current directory to a temporary binary and manages its lifecycle.
package cli

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// devBinaryName is the compiled output used by dev mode.
const devBinaryName = ".dagnats-dev"

// devRunner manages building and running a Go binary for
// live-reload development.
type devRunner struct {
	dir string
	cmd *exec.Cmd
}

// newDevRunner creates a runner for the given project directory.
// Panics if dir is empty.
func newDevRunner(dir string) *devRunner {
	if dir == "" {
		panic("newDevRunner: dir must not be empty")
	}
	return &devRunner{dir: dir}
}

// build compiles the Go project in dir to the dev binary.
// Returns the build error if compilation fails.
func (r *devRunner) build() error {
	if r.dir == "" {
		panic("build: dir must not be empty")
	}
	outputPath := filepath.Join(r.dir, devBinaryName)
	cmd := exec.Command(
		"go", "build", "-o", outputPath, ".",
	)
	cmd.Dir = r.dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// start launches the compiled dev binary as a child process
// with DAGNATS_DEV_MODE=true set in the environment.
func (r *devRunner) start() error {
	if r.dir == "" {
		panic("start: dir must not be empty")
	}
	if r.cmd != nil {
		panic("start: previous process still tracked")
	}
	binPath := filepath.Join(r.dir, devBinaryName)
	r.cmd = exec.Command(binPath)
	r.cmd.Dir = r.dir
	r.cmd.Stdout = os.Stdout
	r.cmd.Stderr = os.Stderr
	r.cmd.Env = append(os.Environ(), "DAGNATS_DEV_MODE=true")
	return r.cmd.Start()
}

// stop sends SIGTERM and waits up to 5 seconds, then SIGKILL.
// No-op if no process is running.
func (r *devRunner) stop() {
	if r.cmd == nil || r.cmd.Process == nil {
		return
	}
	_ = r.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = r.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = r.cmd.Process.Kill()
		<-done
	}
	r.cmd = nil
}

// cleanup stops the running process and removes the dev binary.
func (r *devRunner) cleanup() {
	r.stop()
	binPath := filepath.Join(r.dir, devBinaryName)
	_ = os.Remove(binPath)
}

// ensureGitignore adds the dev binary name to .gitignore if
// it is not already present. Creates .gitignore if needed.
func (r *devRunner) ensureGitignore() {
	if r.dir == "" {
		panic("ensureGitignore: dir must not be empty")
	}
	gitignorePath := filepath.Join(r.dir, ".gitignore")
	if containsLine(gitignorePath, devBinaryName) {
		return
	}
	f, err := os.OpenFile(
		gitignorePath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644,
	)
	if err != nil {
		return // best-effort
	}
	defer f.Close()
	_, _ = f.WriteString(devBinaryName + "\n")
}

// containsLine checks if a file contains the exact line.
// Returns false if the file cannot be read.
func containsLine(path, line string) bool {
	if path == "" {
		panic("containsLine: path must not be empty")
	}
	if line == "" {
		panic("containsLine: line must not be empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	const maxLines = 10_000
	for i := 0; scanner.Scan() && i < maxLines; i++ {
		if strings.TrimSpace(scanner.Text()) == line {
			return true
		}
	}
	return false
}
