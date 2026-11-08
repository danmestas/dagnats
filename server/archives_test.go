// archives_test.go
// The operator's archive grant (policy.archives in dagnats.yaml) on a
// real Server: a program that declares Manifest.Archives gets archive
// rights on a stream only while the file also grants it, and editing the
// file adds and removes them on the connected program without a restart.
// Methodology: a real Server in required mode watching a real file, a
// real program.Run, and raw requests on the program's own connection;
// every wait is bounded.
package server

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

type archiverProgram struct {
	conn atomic.Pointer[nats.Conn]
}

func (p *archiverProgram) Manifest() program.Manifest {
	return program.Manifest{Name: "lode", Archives: []string{"WORKFLOW_HISTORY"}}
}

func (p *archiverProgram) Start(_ context.Context, h program.Host) error {
	if h.Conn == nil {
		panic("archiverProgram.Start: h.Conn must not be nil")
	}
	p.conn.Store(h.Conn)
	return nil
}

func (p *archiverProgram) Stop(context.Context) error { return nil }

const archiveGrantYAML = `
policy:
  archives:
    lode: [WORKFLOW_HISTORY]
`

// writeConfig replaces the watched file in one rename, as an editor does.
func writeConfig(t *testing.T, path, body string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename config: %v", err)
	}
}

// eventually polls cond for up to 15s: the watcher debounces 500ms.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func TestServer_ArchiveGrantFollowsConfigReload(t *testing.T) {
	cfg := loginConfig(t)
	cfg.ConfigFilePath = filepath.Join(t.TempDir(), "dagnats.yaml")
	writeConfig(t, cfg.ConfigFilePath, "workflows: []\n")
	srv, _ := startProgramsServer(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prog := &archiverProgram{}
	running, err := program.Run(ctx, prog, program.Options{
		URL: srv.ns.ClientURL(), SeedPath: filepath.Join(t.TempDir(), "lode.seed"),
		JoinKey: createJoinKey(t, srv),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		if err := running.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	conn := prog.conn.Load()
	names := "$JS.API.CONSUMER.NAMES.WORKFLOW_HISTORY"
	// Declared but not granted: no rights.
	if !denied(conn, names) {
		t.Fatal("declared-only archive stream is reachable before the grant")
	}
	writeConfig(t, cfg.ConfigFilePath, archiveGrantYAML)
	if !eventually(func() bool { return permitted(conn, names) }) {
		t.Fatal("archive grant not applied after the file granted it")
	}
	// Positive and negative in one: the grant is the declared stream only.
	if !denied(conn, "$JS.API.CONSUMER.NAMES.RUN_SNAPSHOTS") {
		t.Fatal("an undeclared, ungranted stream became reachable")
	}
	writeConfig(t, cfg.ConfigFilePath, "workflows: []\n")
	if !eventually(func() bool { return denied(conn, names) }) {
		t.Fatal("archive grant not removed after the file dropped it")
	}
}
