// Methodology: e2e acceptance for `dagnats serve --die-with-parent`
// (#476). Builds the real dagnats binary, launches it under an
// intermediate shell parent on isolated ports + a temp data dir, then
// SIGKILLs the parent (whose cleanup therefore never runs, mimicking
// `go test -timeout`/SIGKILL). The spawned dagnats must notice its
// parent is gone and self-terminate via the existing graceful
// shutdown, releasing its HTTP port within a few poll intervals. A
// companion case asserts the default (flag off) leaves the server
// running after the parent dies. Bounded timeouts on every wait; never
// touches :4222.
package server

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// buildDagnatsBinary compiles cmd/dagnats into the test's temp dir and
// returns the path. Skips the test if `go` is unavailable.
func buildDagnatsBinary(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go toolchain not found: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "dagnats")
	cmd := exec.Command(goBin, "build", "-o", bin, "../cmd/dagnats")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build dagnats: %v\n%s", err, out)
	}
	return bin
}

// freeHTTPAddr returns a currently-free 127.0.0.1:PORT for the spawned
// server's HTTP listener. NATS gets a random port (DAGNATS_NATS_PORT=-1)
// rather than reusing this one — sharing a port would collide and crash
// startup, masking the behavior under test. Never returns :4222.
func freeHTTPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// portListening reports whether something accepts TCP on addr.
func portListening(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// waitListening polls until addr accepts connections or the deadline
// passes.
func waitListening(addr string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if portListening(addr) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitNotListening polls until addr stops accepting connections or the
// deadline passes.
func waitNotListening(addr string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if !portListening(addr) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// spawnUnderParent launches an intermediate `sh` parent that starts the
// dagnats binary as its own child (via `&`), records the child PID to
// pidFile, then blocks on `sleep`. Returns the parent *exec.Cmd. When
// the parent is SIGKILLed its cleanup never runs, so without
// --die-with-parent the dagnats child would orphan.
func spawnUnderParent(
	t *testing.T, bin, httpAddr string,
	dataDir, pidFile string, dieWithParent bool,
) (*exec.Cmd, string) {
	t.Helper()
	flag := ""
	if dieWithParent {
		flag = "--die-with-parent"
	}
	// The shell starts dagnats in the background with its stdio
	// redirected to a log file — NOT inherited from the test's pipes —
	// so when the shell is killed and its pipes close, the detached
	// child can't be taken down by a SIGPIPE on a closed stdout. That
	// isolates the variable under test: the child ends only if its own
	// die-with-parent watcher fires (reparent detection), not as OS
	// teardown collateral. The shell records the child PID then blocks
	// on sleep so killing it doesn't cascade to the child.
	logPath := filepath.Join(dataDir, "child.log")
	script := fmt.Sprintf(
		"%q serve %s >%q 2>&1 < /dev/null & echo $! > %q; exec sleep 120",
		bin, flag, logPath, pidFile,
	)
	parent := exec.Command("sh", "-c", script)
	parent.Env = append(os.Environ(),
		"DAGNATS_HTTP_ADDR="+httpAddr,
		"DAGNATS_NATS_PORT=-1", // random NATS port; never collides with HTTP
		"DAGNATS_DATA_DIR="+dataDir,
	)
	// New process group so the shell isn't in the test's group; we kill
	// the shell explicitly and the detached dagnats survives the kill
	// unless its own watcher fires.
	parent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := parent.Start(); err != nil {
		t.Fatalf("start parent shell: %v", err)
	}
	return parent, logPath
}

// dumpChildLog logs the spawned server's captured stdout/stderr so a
// failing CI run can tell a watcher bug from a startup failure or slow
// drain. Read errors (e.g. the child never wrote anything) are reported
// inline rather than failing the test — this is diagnostic, not a
// gate. Bounded by os.ReadFile; the child log is small.
func dumpChildLog(t *testing.T, logPath string) {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Logf("child log unavailable (%s): %v", logPath, err)
		return
	}
	if len(data) == 0 {
		t.Logf("child log %s is empty", logPath)
		return
	}
	t.Logf("--- spawned dagnats serve log (%s) ---\n%s", logPath, data)
}

// readChildPid waits for the pidFile written by the shell and parses it.
func readChildPid(t *testing.T, pidFile string, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidFile)
		if err == nil {
			s := string(data)
			for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == ' ') {
				s = s[:len(s)-1]
			}
			if pid, perr := strconv.Atoi(s); perr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child pid not written to %s within %v", pidFile, within)
	return 0
}

// processAlive reports whether pid is a live process (signal 0 probe).
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// TestServe_DieWithParent_SelfTerminatesOnParentKill is the acceptance
// criterion: a `dagnats serve --die-with-parent` whose parent is
// SIGKILLed self-terminates and releases its HTTP port.
func TestServe_DieWithParent_SelfTerminatesOnParentKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process-tree semantics required")
	}
	if testing.Short() {
		t.Skip("e2e build+spawn skipped in -short")
	}
	bin := buildDagnatsBinary(t)
	httpAddr := freeHTTPAddr(t)
	dataDir := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "child.pid")

	parent, logPath := spawnUnderParent(
		t, bin, httpAddr, dataDir, pidFile, true)
	// Dump the child's captured output on any failure path so a flake
	// in CI is diagnosable (watcher bug vs. startup failure vs. drain).
	t.Cleanup(func() {
		if t.Failed() {
			dumpChildLog(t, logPath)
		}
	})

	if !waitListening(httpAddr, 30*time.Second) {
		_ = parent.Process.Kill()
		t.Fatalf("dagnats serve never came up on %s", httpAddr)
	}
	childPid := readChildPid(t, pidFile, 5*time.Second)

	// Kill the parent shell hard — its cleanup cannot run.
	if err := parent.Process.Kill(); err != nil {
		t.Fatalf("kill parent: %v", err)
	}
	_, _ = parent.Process.Wait()

	// The orphaned child must notice and shut down. Watcher interval is
	// ~500ms + graceful drain; allow a generous bound.
	if !waitNotListening(httpAddr, 20*time.Second) {
		_ = syscall.Kill(childPid, syscall.SIGKILL)
		t.Fatalf("dagnats serve still listening on %s after parent kill", httpAddr)
	}
	// Belt-and-suspenders: the child process itself should be gone.
	gone := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !processAlive(childPid) {
			gone = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !gone {
		_ = syscall.Kill(childPid, syscall.SIGKILL)
		t.Fatalf("dagnats child pid %d still alive after parent kill", childPid)
	}
}

// TestServe_NoDieWithParent_SurvivesParentKill pins the default-off
// behavior: without the flag, killing the parent leaves the server
// running (it is the orphan the feature exists to prevent — here we
// assert the OLD behavior is unchanged when the flag is absent, then
// clean it up).
func TestServe_NoDieWithParent_SurvivesParentKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process-tree semantics required")
	}
	if testing.Short() {
		t.Skip("e2e build+spawn skipped in -short")
	}
	bin := buildDagnatsBinary(t)
	httpAddr := freeHTTPAddr(t)
	dataDir := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "child.pid")

	parent, logPath := spawnUnderParent(
		t, bin, httpAddr, dataDir, pidFile, false)
	// Dump the child's captured output on any failure path so a flake
	// in CI is diagnosable (watcher bug vs. startup failure vs. drain).
	t.Cleanup(func() {
		if t.Failed() {
			dumpChildLog(t, logPath)
		}
	})

	if !waitListening(httpAddr, 30*time.Second) {
		_ = parent.Process.Kill()
		t.Fatalf("dagnats serve never came up on %s", httpAddr)
	}
	childPid := readChildPid(t, pidFile, 5*time.Second)

	// Always reap the orphan so the test never leaks a process.
	t.Cleanup(func() {
		_ = syscall.Kill(childPid, syscall.SIGKILL)
	})

	if err := parent.Process.Kill(); err != nil {
		t.Fatalf("kill parent: %v", err)
	}
	_, _ = parent.Process.Wait()

	// Without the flag the server keeps listening after the parent dies.
	// Give the (non-existent) watcher more than an interval to (not) act.
	time.Sleep(3 * time.Second)
	if !portListening(httpAddr) {
		t.Fatalf(
			"default-off serve stopped listening on %s after parent kill; "+
				"the watcher must not run when --die-with-parent is absent",
			httpAddr)
	}
}
