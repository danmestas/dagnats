// program_conn_test.go
// Test helper: a connection that asks for replies the way a program does,
// under its own "_INBOX.<name>." prefix. The program-scoped endpoints
// answer only such replies (replyAllowed), because a reply-to a caller
// chose is published with dagnats' root rights.
// Methodology: helpers only.
package api

import (
	"strings"
	"sync"
	"testing"

	"github.com/danmestas/dagnats/program"
	"github.com/nats-io/nats.go"
)

var (
	programConnsMu sync.Mutex
	programConns   = map[*nats.Conn]map[string]*nats.Conn{}
)

// programConn returns a connection to nc's server with name's inbox
// prefix, dialed once per test server and program.
func programConn(t *testing.T, nc *nats.Conn, name string) *nats.Conn {
	t.Helper()
	programConnsMu.Lock()
	defer programConnsMu.Unlock()
	byName := programConns[nc]
	if byName == nil {
		byName = map[string]*nats.Conn{}
		programConns[nc] = byName
		t.Cleanup(func() {
			programConnsMu.Lock()
			defer programConnsMu.Unlock()
			for _, conn := range programConns[nc] {
				conn.Close()
			}
			delete(programConns, nc)
		})
	}
	if conn := byName[name]; conn != nil {
		return conn
	}
	conn, err := nats.Connect(nc.ConnectedUrl(), nats.CustomInboxPrefix(program.InboxPrefix(name)))
	if err != nil {
		t.Fatalf("dial as program %s: %v", name, err)
	}
	byName[name] = conn
	return conn
}

// connFor returns the connection a request on subject should come from:
// the program's own for api.p.<name>.*, nc for anything else.
func connFor(t *testing.T, nc *nats.Conn, subject string) *nats.Conn {
	t.Helper()
	rest, scoped := strings.CutPrefix(subject, "api.p.")
	if !scoped {
		return nc
	}
	name, _, _ := strings.Cut(rest, ".")
	if program.ValidateName(name) != nil {
		return nc
	}
	return programConn(t, nc, name)
}
