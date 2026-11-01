// programs_test.go
// Program NKey users on a real Server (#885), in both NATS auth modes.
// Required mode (a nats_user login): anonymous is the join user, which
// may only request registration; a program registers with a join key,
// logs in as its own NKey user with PermissionsFor its manifest, and its
// user is rebuilt from the programs bucket after a restart. Open mode:
// anonymous stays root, and a client connected before a program
// registered keeps working across the reload. A record written to the
// bucket by another node's registrar reaches this node through its watch.
// Methodology: a real Server per test, real program.Run, raw NATS clients
// for the boundaries; bounded waits throughout.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/programs"
	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startProgramsServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	t.Cleanup(func() {
		srv.Stop()
		select {
		case <-errCh:
		case <-time.After(20 * time.Second):
			t.Error("Run() did not return within 20s")
		}
	})
	return srv, waitHTTPAddr(t, srv)
}

func loginConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.NATSUser = "dagnats"
	cfg.NATSPasswordFile = writeSecret(t, "server-pw")
	return cfg
}

type tinyProgram struct {
	conn atomic.Pointer[nats.Conn]
}

func (p *tinyProgram) Manifest() program.Manifest {
	return program.Manifest{Name: "notify", TaskTypes: []string{"notify.work"}}
}

func (p *tinyProgram) Start(_ context.Context, h program.Host) error {
	if h.Conn == nil {
		panic("tinyProgram.Start: h.Conn must not be nil")
	}
	p.conn.Store(h.Conn)
	return nil
}

func (p *tinyProgram) Stop(context.Context) error { return nil }

func createJoinKey(t *testing.T, srv *Server) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, plaintext, err := srv.joinKeyStore.Create(ctx, joinkey.Spec{Label: "test"}, "tester")
	if err != nil {
		t.Fatalf("create join key: %v", err)
	}
	return plaintext
}

// denied reports whether a request on subject never got through: a
// permission violation, or a timeout because the publish or the reply
// subscription was refused. A permitted subject with no listener answers
// ErrNoResponders at once instead, and one with a listener replies.
func denied(nc *nats.Conn, subject string) bool {
	_, err := nc.Request(subject, []byte("{}"), time.Second)
	return errors.Is(err, nats.ErrPermissionViolation) || errors.Is(err, nats.ErrTimeout)
}

// permitted reports whether a publish on subject got through to the
// server's routing: a reply, or ErrNoResponders when nothing listens.
func permitted(nc *nats.Conn, subject string) bool {
	_, err := nc.Request(subject, []byte("{}"), 5*time.Second)
	return err == nil || errors.Is(err, nats.ErrNoResponders)
}

func TestServer_RequiredModeProgramRegistersAndLogsIn(t *testing.T) {
	cfg := loginConfig(t)
	srv, _ := startProgramsServer(t, cfg)
	url := srv.ns.ClientURL()
	seed := filepath.Join(t.TempDir(), "notify.seed")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prog := &tinyProgram{}
	_, err := program.Run(ctx, prog, program.Options{URL: url, SeedPath: seed})
	var refusal *program.ErrRefused
	if !errors.As(err, &refusal) || refusal.Code != program.RefusedJoinKeyInvalid {
		t.Fatalf("keyless Run err = %v, want join_key_invalid", err)
	}
	running, err := program.Run(ctx, prog, program.Options{
		URL: url, SeedPath: seed, JoinKey: createJoinKey(t, srv),
	})
	if err != nil {
		t.Fatalf("Run with a join key: %v", err)
	}
	t.Cleanup(func() {
		if err := running.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	conn := prog.conn.Load()
	// Positive: the program reaches its own scope. Negative: as its NKey
	// user it cannot reach the global control plane.
	if !permitted(conn, "api.p.notify.runs.get") {
		t.Fatal("program user cannot publish under its scoped API")
	}
	if !denied(conn, "api.workflows.register") {
		t.Fatal("program user reached api.workflows.register")
	}
}

func TestServer_RequiredModeAnonymousIsTheJoinUser(t *testing.T) {
	srv, _ := startProgramsServer(t, loginConfig(t))
	anonymous, err := nats.Connect(srv.ns.ClientURL(),
		nats.CustomInboxPrefix(program.JoinInboxPrefix))
	if err != nil {
		t.Fatalf("anonymous connect: %v", err)
	}
	defer anonymous.Close()
	reply, err := anonymous.Request(program.RegisterSubject, []byte("not json"), 5*time.Second)
	if err != nil {
		t.Fatalf("join user cannot request registration: %v", err)
	}
	var answer program.RegisterReply
	if err := json.Unmarshal(reply.Data, &answer); err != nil || answer.Refused == nil {
		t.Fatalf("registration reply = %s, %v", reply.Data, err)
	}
	for _, subject := range []string{"api.workflows.register", "api.p.notify.runs.get"} {
		if !denied(anonymous, subject) {
			t.Errorf("join user reached %s", subject)
		}
	}
	defaultInbox, err := nats.Connect(srv.ns.ClientURL())
	if err != nil {
		t.Fatalf("anonymous connect: %v", err)
	}
	defer defaultInbox.Close()
	if !denied(defaultInbox, program.RegisterSubject) {
		t.Fatal("join user got a reply outside _JOIN.>")
	}
	login, err := nats.Connect(srv.ns.ClientURL(), nats.UserInfo("dagnats", "server-pw"))
	if err != nil {
		t.Fatalf("nats_user login refused: %v", err)
	}
	defer login.Close()
	if _, err := login.Request("api.workflows.register", []byte("{}"), 5*time.Second); err != nil {
		t.Fatalf("nats_user is not root: %v", err)
	}
}

func TestServer_ProgramUsersRebuiltAfterRestart(t *testing.T) {
	cfg := loginConfig(t)
	seed := filepath.Join(t.TempDir(), "notify.seed")
	func() {
		srv, _ := startProgramsServer(t, cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		running, err := program.Run(ctx, &tinyProgram{}, program.Options{
			URL: srv.ns.ClientURL(), SeedPath: seed, JoinKey: createJoinKey(t, srv),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if err := running.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		srv.Stop()
	}()
	srv, _ := startProgramsServer(t, cfg)
	key, err := program.LoadOrCreateKey(seed)
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	nc, err := nats.Connect(srv.ns.ClientURL(), key.ConnectOption(),
		nats.CustomInboxPrefix(program.InboxPrefix("notify")))
	if err != nil {
		t.Fatalf("program key refused after restart: %v", err)
	}
	nc.Close()
	stranger, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "stranger.seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	if nc, err := nats.Connect(srv.ns.ClientURL(), stranger.ConnectOption()); err == nil {
		nc.Close()
		t.Fatal("an unregistered NKey logged in")
	}
}

// TestServer_OpenModeAnonymousSurvivesProgramReload: a client connected
// anonymously before any program registered keeps its subscription and
// root reach after the reload that adds the program's NKey user, and the
// HTTP API (served over the server's own connection) is unaffected.
func TestServer_OpenModeAnonymousSurvivesProgramReload(t *testing.T) {
	srv, httpAddr := startProgramsServer(t, testConfig(t))
	anonymous, err := nats.Connect(srv.ns.ClientURL())
	if err != nil {
		t.Fatalf("anonymous connect: %v", err)
	}
	defer anonymous.Close()
	received := make(chan *nats.Msg, 4)
	sub, err := anonymous.ChanSubscribe("anything.>", received)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test teardown

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	running, err := program.Run(ctx, &tinyProgram{}, program.Options{
		URL: srv.ns.ClientURL(), SeedPath: filepath.Join(t.TempDir(), "seed"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer running.Stop(ctx) //nolint:errcheck // test teardown

	if err := anonymous.Publish("anything.after", []byte("x")); err != nil {
		t.Fatalf("publish after reload: %v", err)
	}
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("the anonymous subscription stopped delivering after the reload")
	}
	if anonymous.Status() != nats.CONNECTED {
		t.Fatalf("anonymous client status = %v after the reload", anonymous.Status())
	}
	resp, err := http.Get("http://" + httpAddr + "/workflows")
	if err != nil {
		t.Fatalf("GET /workflows: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /workflows = %d after the reload", resp.StatusCode)
	}
}

// TestServer_RecordFromAnotherNodeReachesThisNode writes a program
// record into the bucket directly, as another cluster node's registrar
// would, and checks this node's watch makes the user live here.
func TestServer_RecordFromAnotherNodeReachesThisNode(t *testing.T) {
	srv, _ := startProgramsServer(t, loginConfig(t))
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	manifest := (&tinyProgram{}).Manifest()
	data, err := json.Marshal(programs.Record{
		Manifest: manifest, PublicKey: key.PublicKey(),
		Permissions: program.PermissionsFor(manifest), RegisteredAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js, err := jetstream.New(srv.nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, programs.BucketName)
	if err != nil {
		t.Fatalf("programs bucket: %v", err)
	}
	if _, err := kv.Create(ctx, manifest.Name, data); err != nil {
		t.Fatalf("write record: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(srv.ns.ClientURL(), key.ConnectOption(),
			nats.CustomInboxPrefix(program.InboxPrefix(manifest.Name)))
		if err == nil {
			nc.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the watched record never became a NATS user: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := kv.Delete(ctx, manifest.Name); err != nil {
		t.Fatalf("delete record: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(srv.ns.ClientURL(), key.ConnectOption())
		if err != nil {
			break
		}
		nc.Close()
		if time.Now().After(deadline) {
			t.Fatal("a deleted record's user can still log in")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
