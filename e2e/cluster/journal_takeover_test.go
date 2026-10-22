// Methodology: multi-process journal takeover (ADR-026, #804). A
// three-process dagnats cluster runs one journal step. Worker w1 is a
// separate OS process (this test binary re-executed in worker mode); it
// claims the step's journal, appends, and parks at a gate file. The
// test SIGSTOPs w1 past AckWait, starts w2, which receives the
// redelivery and claims over w1. The test then SIGCONTs w1 and opens
// its gate: its next append must be rejected as fenced and it must
// never commit. Only then does w2 finish the step. The journal checker
// must report no violations, the journal holds exactly one commit (w2's,
// with a higher delivery count than w1's claim) and history holds no
// step.failed.
//
// Heavy (three servers plus two workers), so it is opt-in:
// DAGNATS_E2E_CLUSTER=1, like the failover test. Every wait is bounded
// and journalTakeoverBudget, checked against the go test deadline up
// front, covers their sum. Children run in their own process group that
// t.Cleanup SIGKILLs, and a worker also exits on its own deadline.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
)

const (
	journalWorkerEnv  = "DAGNATS_E2E_JOURNAL_WORKER"
	journalURLsEnv    = "DAGNATS_E2E_JOURNAL_URLS"
	journalDirEnv     = "DAGNATS_E2E_JOURNAL_DIR"
	journalTaskType   = "journal-takeover"
	takeoverAckWait   = 5 * time.Second
	claimTimeout      = 90 * time.Second
	takeoverTimeout   = 120 * time.Second
	fenceTimeout      = 30 * time.Second
	gateTimeout       = 150 * time.Second
	workerLifetimeMax = 8 * time.Minute
	filePollPeriod    = 100 * time.Millisecond
	heartbeatPeriod   = time.Second
	// journalTakeoverBudget is formation, the waits above, and slack. It
	// leaves the failover test the 9 minutes it needs inside the 900s
	// go test timeout.
	journalTakeoverBudget = 6 * time.Minute
)

// TestMain turns this binary into a journal worker when the test
// re-executes it; otherwise it runs the tests.
func TestMain(m *testing.M) {
	if name := os.Getenv(journalWorkerEnv); name != "" {
		os.Exit(journalWorkerMain(name))
	}
	os.Exit(m.Run())
}

// journalWorkerMain runs one worker process until killed.
func journalWorkerMain(name string) int {
	urls, dir := os.Getenv(journalURLsEnv), os.Getenv(journalDirEnv)
	if urls == "" || dir == "" {
		fmt.Fprintln(os.Stderr, "journal worker: urls and dir are required")
		return 2
	}
	time.AfterFunc(workerLifetimeMax, func() { os.Exit(3) })
	nc, err := nats.Connect(urls, nats.MaxReconnects(-1))
	if err != nil {
		fmt.Fprintln(os.Stderr, "journal worker: connect:", err)
		return 2
	}
	var executions atomic.Int32
	w := worker.NewWorker(nc)
	w.Handle(journalTaskType, func(tc worker.TaskContext) error {
		if executions.Add(1) > 1 {
			// A stalled consumer can hold older deliveries in its
			// prefetch buffer; processed after SIGCONT one would claim
			// over the live successor. The test is about the first
			// delivery's fence, so this process takes the task once.
			writeMarker(dir, name+".extra", "")
			return nil
		}
		return journalTakeoverHandler(name, dir, tc)
	}, worker.WithAckWait(takeoverAckWait))
	w.Start()
	select {}
}

// journalTakeoverHandler claims, appends, parks at this worker's gate,
// then appends again. A fenced append ends the handler with the fence
// error and no commit; an accepted one completes the step.
func journalTakeoverHandler(name, dir string, tc worker.TaskContext) error {
	j, err := tc.Journal()
	if err != nil {
		writeMarker(dir, name+".journal-error", err.Error())
		return err
	}
	ctx := context.Background()
	if _, err := j.Append(ctx, journal.TypeEntry, []byte(`{"phase":"before"}`)); err != nil {
		writeMarker(dir, name+".append-error", err.Error())
		return err
	}
	writeMarker(dir, name+".claimed", fmt.Sprint(j.Task().Delivered))
	if !waitGate(filepath.Join(dir, name+".proceed"), tc) {
		return errors.New("gate never opened")
	}
	if _, err := j.Append(ctx, journal.TypeEntry, []byte(`{"phase":"after"}`)); err != nil {
		if errors.Is(err, journal.ErrFenced) {
			writeMarker(dir, name+".fenced", err.Error())
		} else {
			writeMarker(dir, name+".append-error", err.Error())
		}
		return err
	}
	if err := tc.Complete([]byte(`"done"`)); err != nil {
		return err
	}
	writeMarker(dir, name+".completed", "")
	return nil
}

// writeMarker publishes a marker file atomically.
func writeMarker(dir, name, content string) {
	tmp := filepath.Join(dir, name+".tmp")
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "marker:", err)
		return
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		fmt.Fprintln(os.Stderr, "marker:", err)
	}
}

// waitFile polls for path until timeout and reports whether it exists.
func waitFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(filePollPeriod)
	}
	return false
}

// waitGate parks until path exists, extending the task's AckWait every
// heartbeatPeriod so a parked live worker is not redelivered. Bounded
// by gateTimeout.
func waitGate(path string, tc worker.TaskContext) bool {
	deadline := time.Now().Add(gateTimeout)
	for time.Now().Before(deadline) {
		if waitFile(path, heartbeatPeriod) {
			return true
		}
		if err := tc.Heartbeat(); err != nil {
			fmt.Fprintln(os.Stderr, "heartbeat:", err)
		}
	}
	return false
}

// journalWorker is one spawned worker process.
type journalWorker struct {
	name    string
	cmd     *exec.Cmd
	logPath string
}

// spawnJournalWorker re-executes the test binary as a worker in its own
// process group and registers cleanup that kills the group.
func spawnJournalWorker(t *testing.T, name, urls, dir string) *journalWorker {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	logPath := filepath.Join(dir, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create worker log: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), journalWorkerEnv+"="+name,
		journalURLsEnv+"="+urls, journalDirEnv+"="+dir)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker %s: %v", name, err)
	}
	w := &journalWorker{name: name, cmd: cmd, logPath: logPath}
	reaped := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-reaped:
		case <-time.After(10 * time.Second):
		}
		if t.Failed() {
			if data, err := os.ReadFile(logPath); err == nil {
				t.Logf("--- worker %s log ---\n%s", name, string(data))
			}
		}
	})
	return w
}

// signal sends sig to the worker process.
func (w *journalWorker) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := syscall.Kill(w.cmd.Process.Pid, sig); err != nil {
		t.Fatalf("signal %s to %s: %v", sig, w.name, err)
	}
}

// requireMarker waits for a marker file and returns its content.
func requireMarker(t *testing.T, dir, name string, timeout time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if !waitFile(path, timeout) {
		t.Fatalf("marker %s did not appear within %s", name, timeout)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker %s: %v", name, err)
	}
	return string(data)
}

// openGate lets the named worker pass its gate.
func openGate(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+".proceed"), nil, 0o600); err != nil {
		t.Fatalf("open gate %s: %v", name, err)
	}
}

func TestMultiProcessCluster_JournalTakeoverFencesStalledWorker(t *testing.T) {
	if os.Getenv("DAGNATS_E2E_CLUSTER") != "1" {
		t.Skip("set DAGNATS_E2E_CLUSTER=1 to run the 3-process cluster test")
	}
	if testing.Short() {
		t.Skip("multi-process cluster test skipped in -short mode")
	}
	if dl, ok := t.Deadline(); ok && time.Until(dl) < journalTakeoverBudget {
		t.Fatalf("go test -timeout leaves %s; this test needs %s",
			time.Until(dl).Round(time.Second), journalTakeoverBudget)
	}
	nodes := formCluster(t, buildBinary(t))
	urls := ""
	for _, n := range nodes {
		urls += fmt.Sprintf("nats://127.0.0.1:%d,", n.natsPort)
	}
	urls = strings.TrimSuffix(urls, ",")
	nc, err := nats.Connect(urls, nats.MaxReconnects(-1),
		nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	runTakeover(t, nc, urls)
}

// runTakeover drives the stall, takeover and fence, then audits.
func runTakeover(t *testing.T, nc *nats.Conn, urls string) {
	t.Helper()
	dir := t.TempDir()
	svc := harness.NewTestService(t, nc)
	wb := dag.NewWorkflow(harness.UniqueName(t, "journal-takeover"))
	wb.Task("a", journalTaskType)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build workflow: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	w1 := spawnJournalWorker(t, "w1", urls, dir)
	runID, err := svc.StartRun(ctx, def.Name, nil)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	delivered1 := requireMarker(t, dir, "w1.claimed", claimTimeout)
	w1.signal(t, syscall.SIGSTOP)
	spawnJournalWorker(t, "w2", urls, dir)
	delivered2 := requireMarker(t, dir, "w2.claimed", takeoverTimeout)
	if d1, d2 := atoi(t, delivered1), atoi(t, delivered2); d2 <= d1 {
		t.Fatalf("takeover claim delivery %d must exceed the stalled claim's %d", d2, d1)
	}

	openGate(t, dir, "w1")
	w1.signal(t, syscall.SIGCONT)
	requireMarker(t, dir, "w1.fenced", fenceTimeout)
	if _, err := os.Stat(filepath.Join(dir, "w1.completed")); err == nil {
		t.Fatal("the fenced worker completed the step")
	}

	openGate(t, dir, "w2")
	harness.WaitForRunStatus(t, svc, runID, dag.RunStatusCompleted, runTimeout)
	auditTakeover(t, nc, svc, runID, delivered2)
}

// auditTakeover checks the journal and history after the takeover.
func auditTakeover(
	t *testing.T, nc *nats.Conn, svc *api.Service, runID, winnerDelivered string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := journal.Check(ctx, nc, runID)
	if err != nil {
		t.Fatalf("journal.Check: %v", err)
	}
	if !report.OK() || len(report.Steps) != 1 {
		t.Fatalf("journal check: want one clean step, got %+v", report)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	records, err := journal.Read(ctx, js, runID, "a")
	if err != nil {
		t.Fatalf("journal.Read: %v", err)
	}
	var commits []journal.Record
	for _, rec := range records {
		if rec.Type == journal.TypeCommit {
			commits = append(commits, rec)
		}
	}
	if len(commits) != 1 || fmt.Sprint(commits[0].Delivered) != winnerDelivered {
		t.Fatalf("want one commit by the takeover claim (delivery %s), got %+v",
			winnerDelivered, commits)
	}
	events, err := svc.ListRunEvents(ctx, runID, false)
	if err != nil {
		t.Fatalf("ListRunEvents: %v", err)
	}
	for _, evt := range events {
		if evt.Type == string(protocol.EventStepFailed) {
			t.Fatalf("history holds a step.failed the fenced worker should never publish: %+v", evt)
		}
	}
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return n
}
