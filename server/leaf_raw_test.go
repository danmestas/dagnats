// leaf_raw_test.go: the hub's own enforcement of the leaf login, proved
// with a raw leafnode protocol client instead of a nats-server spoke. A
// nats-server spoke applies the permissions the hub sends it, so a test
// through one would pass even if the hub enforced nothing. Integration
// tests against a real embedded hub per test; each case pairs a refused or
// dropped path with an allowed one on the same connection.
package server

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/danmestas/dagnats/internal/programs"
)

// rawLeafWait bounds every read from the hub on a raw leaf connection.
const rawLeafWait = 5 * time.Second

// rawLeaf is a TCP connection speaking the leafnode protocol to a hub.
type rawLeaf struct {
	conn   net.Conn
	reader *bufio.Reader
	nonce  string
}

// dialRawLeaf connects to a hub's leaf port and reads its INFO.
func dialRawLeaf(t *testing.T, port int) *rawLeaf {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), rawLeafWait)
	if err != nil {
		t.Fatalf("dial leaf port: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	leaf := &rawLeaf{conn: conn, reader: bufio.NewReader(conn)}
	line := leaf.readLine(t)
	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("hub greeting = %q, want INFO", line)
	}
	var info struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "INFO ")), &info); err != nil {
		t.Fatalf("decode INFO: %v", err)
	}
	leaf.nonce = info.Nonce
	return leaf
}

// login sends CONNECT with fields and a PING, and reports whether the hub
// answered PONG (linked) rather than -ERR or closing (refused).
func (l *rawLeaf) login(t *testing.T, fields map[string]any) bool {
	t.Helper()
	if fields["name"] == nil {
		panic("rawLeaf.login: a leaf CONNECT names its server")
	}
	connect, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}
	l.write(t, "CONNECT "+string(connect)+"\r\nPING\r\n")
	for range 64 {
		line, err := l.tryReadLine()
		if err != nil || strings.HasPrefix(line, "-ERR") {
			return false
		}
		if line == "PONG" {
			return true
		}
	}
	t.Fatal("hub sent 64 lines without PONG or -ERR")
	return false
}

// publishDeniedThenAllowed sends a disallowed leaf message and an allowed
// one in a single write. The hub answers a disallowed publish by closing
// the link, so a second write can land after the close and be dropped; in
// one write both frames reach the hub's read loop together, which parses
// the allowed frame after the disallowed one.
func (l *rawLeaf) publishDeniedThenAllowed(t *testing.T, denied, allowed string) {
	t.Helper()
	if denied == allowed {
		panic("rawLeaf.publishDeniedThenAllowed: subjects must differ")
	}
	frame := func(subject, payload string) string {
		return fmt.Sprintf("LMSG %s %d\r\n%s\r\n", subject, len(payload), payload)
	}
	l.write(t, frame(denied, "leak")+frame(allowed, "ok"))
}

func (l *rawLeaf) write(t *testing.T, data string) {
	t.Helper()
	if err := l.conn.SetWriteDeadline(time.Now().Add(rawLeafWait)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if _, err := l.conn.Write([]byte(data)); err != nil {
		t.Fatalf("write to hub: %v", err)
	}
}

func (l *rawLeaf) readLine(t *testing.T) string {
	t.Helper()
	line, err := l.tryReadLine()
	if err != nil {
		t.Fatalf("read from hub: %v", err)
	}
	return line
}

func (l *rawLeaf) tryReadLine() (string, error) {
	if err := l.conn.SetReadDeadline(time.Now().Add(rawLeafWait)); err != nil {
		return "", err
	}
	line, err := l.reader.ReadString('\n')
	return strings.TrimRight(line, "\r\n"), err
}

// The hub drops a disallowed subject from a linked leaf: task.x never
// reaches a hub subscriber, while an allowed subject sent after it on the
// same connection does. In-order delivery makes the allowed message the
// bound on the wait for the disallowed one.
func TestLeafListenAllow_HubDropsDisallowedSubject(t *testing.T) {
	hub, port := startLeafHub(t, Config{LeafListenAllow: regionLinkAllow})
	nc := connectOrFail(t, hub.ClientURL())
	denied, err := nc.SubscribeSync("task.x")
	if err != nil {
		t.Fatalf("subscribe task.x: %v", err)
	}
	allowed, err := nc.SubscribeSync("_INBOX.raw")
	if err != nil {
		t.Fatalf("subscribe _INBOX.raw: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	leaf := dialRawLeaf(t, port)
	if !leaf.login(t, map[string]any{
		"name": "raw-spoke", "user": testLeafUser, "pass": testLeafPassword,
	}) {
		t.Fatal("hub refused the leaf login")
	}
	leaf.publishDeniedThenAllowed(t, "task.x", "_INBOX.raw")
	if _, err := allowed.NextMsg(rawLeafWait); err != nil {
		t.Fatalf("allowed subject did not reach the hub: %v", err)
	}
	if msg, err := denied.NextMsg(10 * time.Millisecond); err == nil {
		t.Fatalf("hub delivered disallowed %s from the leaf", msg.Subject)
	}
}

// A registered program's NKey may connect as a client but not link as a
// leaf; the leaf login on the same listener does.
func TestLeafListener_RefusesProgramNkey(t *testing.T) {
	hub, port := startLeafHub(t, Config{})
	pair, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	public, err := pair.PublicKey()
	if err != nil {
		t.Fatalf("PublicKey: %v", err)
	}
	record := programs.Record{
		Manifest: (&tinyProgram{}).Manifest(), PublicKey: public, RegisteredAt: time.Now(),
	}
	if err := newUserSet(hub).put(record.Manifest.Name, record, false); err != nil {
		t.Fatalf("register program key: %v", err)
	}
	client, err := nats.Connect(hub.ClientURL(), nats.Nkey(public, pair.Sign))
	if err != nil {
		t.Fatalf("program key refused as a client: %v", err)
	}
	client.Close()
	program := dialRawLeaf(t, port)
	if program.nonce == "" {
		t.Fatal("hub sent no nonce to sign")
	}
	sig, err := pair.Sign([]byte(program.nonce))
	if err != nil {
		t.Fatalf("sign nonce: %v", err)
	}
	if program.login(t, map[string]any{
		"name": "program-spoke", "nkey": public,
		"sig": base64.RawURLEncoding.EncodeToString(sig),
	}) {
		t.Fatal("hub linked a leaf that logged in with a program NKey")
	}
	leaf := dialRawLeaf(t, port)
	if !leaf.login(t, map[string]any{
		"name": "leaf-spoke", "user": testLeafUser, "pass": testLeafPassword,
	}) {
		t.Fatal("hub refused the leaf login")
	}
}

// A replica-only node (engine: false) still runs NATS from natsOptions, so
// its leaf listener keeps the leaf-only login and its allow-list: a leaf
// with no login is refused, and the hub drops a disallowed subject.
func TestLeafListener_ReplicaOnlyNodeKeepsLeafLogin(t *testing.T) {
	servers := startReplicaCluster(t, func(i int, cfg *Config) {
		if i != 2 {
			return
		}
		cfg.LeafPort, cfg.LeafListenUser = -1, testLeafUser
		cfg.LeafListenPasswordFile = writeSecret(t, testLeafPassword)
		cfg.LeafListenAllow = regionLinkAllow
	})
	replica := servers[2]
	if !replica.cfg.ReplicaOnly {
		t.Fatal("node 2 is not replica-only")
	}
	varz, err := replica.ns.Varz(nil)
	if err != nil || varz.LeafNode.Port <= 0 {
		t.Fatalf("replica-only leaf listener not bound: %v", err)
	}
	if dialRawLeaf(t, varz.LeafNode.Port).login(t, map[string]any{"name": "anon-spoke"}) {
		t.Fatal("replica-only node linked a leaf with no login")
	}
	nc := connectOrFail(t, replica.NATSURL())
	denied, err := nc.SubscribeSync("task.x")
	if err != nil {
		t.Fatalf("subscribe task.x: %v", err)
	}
	allowed, err := nc.SubscribeSync("_INBOX.raw")
	if err != nil {
		t.Fatalf("subscribe _INBOX.raw: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	leaf := dialRawLeaf(t, varz.LeafNode.Port)
	if !leaf.login(t, map[string]any{
		"name": "raw-spoke", "user": testLeafUser, "pass": testLeafPassword,
	}) {
		t.Fatal("replica-only node refused the leaf login")
	}
	leaf.publishDeniedThenAllowed(t, "task.x", "_INBOX.raw")
	if _, err := allowed.NextMsg(rawLeafWait); err != nil {
		t.Fatalf("allowed subject did not reach the replica-only node: %v", err)
	}
	if msg, err := denied.NextMsg(10 * time.Millisecond); err == nil {
		t.Fatalf("replica-only node delivered disallowed %s from the leaf", msg.Subject)
	}
}
