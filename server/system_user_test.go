// server/system_user_test.go
// Tests that the built-in system user reaches $SYS only in process.
// Methodology: one real NATS server built from natsOptions, as serve
// builds it. The system user's password is read from the options, the
// way only dagnats itself can; a login with it over the TCP client
// listener must be refused, while the in-process login reaches $SYS.
// Every wait is bounded.
package server

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestSystemUser_OnlyInProcess(t *testing.T) {
	opts, err := natsOptions(testConfig(t))
	if err != nil {
		t.Fatalf("natsOptions: %v", err)
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	password := systemPassword(opts)

	tcp, err := nats.Connect(ns.ClientURL(), nats.UserInfo(systemUser, password),
		nats.MaxReconnects(0), nats.Timeout(5*time.Second))
	if err == nil {
		tcp.Close()
		t.Fatal("system user logged in over TCP; want it refused")
	}

	local, err := nats.Connect("", nats.InProcessServer(ns),
		nats.UserInfo(systemUser, password))
	if err != nil {
		t.Fatalf("in-process system login: %v", err)
	}
	t.Cleanup(local.Close)
	if _, err := local.Request("$SYS.REQ.SERVER.PING", nil, 5*time.Second); err != nil {
		t.Fatalf("in-process system user cannot reach $SYS: %v", err)
	}
}
