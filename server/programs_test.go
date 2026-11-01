// programs_test.go
// The server answers program registration (#885 D3) in both NATS auth
// modes: with no client login (open) a program registers with no join
// key and gets a Grant; with a client login (required) registration is
// refused auth_mode_unsupported, because programs have no NKey users
// yet and an admitted program could not log in.
// Methodology: a real Server per mode, a raw RegisterSubject request
// from a client that can reach the port.
package server

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

func startProgramsServer(t *testing.T, login bool) (*Server, []nats.Option) {
	t.Helper()
	cfg := testConfig(t)
	var options []nats.Option
	if login {
		cfg.NATSUser = "dagnats"
		cfg.NATSPasswordFile = writeSecret(t, "server-pw")
		options = append(options, nats.UserInfo("dagnats", "server-pw"))
	}
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
	waitHTTPAddr(t, srv)
	return srv, options
}

func registerRaw(t *testing.T, url string, options []nats.Option) program.RegisterReply {
	t.Helper()
	nc, err := nats.Connect(url, options...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	key, err := program.LoadOrCreateKey(filepath.Join(t.TempDir(), "seed"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	body, err := json.Marshal(program.RegisterRequest{
		Manifest:  program.Manifest{Name: "notify", TaskTypes: []string{"notify.work"}},
		PublicKey: key.PublicKey(),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := nc.Request(program.RegisterSubject, body, 10*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var reply program.RegisterReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("decode %s: %v", msg.Data, err)
	}
	return reply
}

func TestServer_RegistersProgramsInOpenMode(t *testing.T) {
	srv, options := startProgramsServer(t, false)
	reply := registerRaw(t, srv.ns.ClientURL(), options)
	if reply.Grant == nil || reply.Refused != nil || reply.Error != "" {
		t.Fatalf("open-mode reply = %+v, want a grant", reply)
	}
	if reply.Grant.Replicas != 1 || reply.Grant.ServerVersion == "" {
		t.Fatalf("grant = %+v", *reply.Grant)
	}
}

func TestServer_RefusesProgramsInRequiredMode(t *testing.T) {
	srv, options := startProgramsServer(t, true)
	reply := registerRaw(t, srv.ns.ClientURL(), options)
	if reply.Grant != nil || reply.Refused == nil ||
		reply.Refused.Code != program.RefusedAuthModeUnsupported {
		t.Fatalf("required-mode reply = %+v, want auth_mode_unsupported", reply)
	}
}
