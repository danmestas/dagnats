// server/leaf_listener_test.go
// The leafnode listener and user/password leaf remotes: config parsing and
// validation as pure unit tests, then integration tests with real embedded
// NATS servers per test. Every login test checks the refused path as well
// as the accepted one; refusals are observed over a bounded window.
package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testLeafUser = "leaf"
const testLeafPassword = "leaf-pw-for-test"

func TestLeafListenerConfig_FileAndEnv(t *testing.T) {
	pwFile := writeSecret(t, testLeafPassword)
	path := writeConfigFile(t, fmt.Sprintf("leaf_port: 7422\nleaf_host: 100.64.0.9\n"+
		"leaf_listen_user: hub\nleaf_listen_password_file: %s\n"+
		"leaf_remotes: nats-leaf://hub:7422\nleaf_user: e\nleaf_password_file: %s\n",
		pwFile, pwFile))
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.LeafPort != 7422 || cfg.LeafHost != "100.64.0.9" || cfg.LeafListenUser != "hub" ||
		cfg.LeafListenPasswordFile != pwFile || cfg.LeafUser != "e" ||
		cfg.LeafPasswordFile != pwFile {
		t.Fatalf("file keys not applied: %+v", cfg)
	}
	t.Setenv("DAGNATS_LEAF_PORT", "7500")
	t.Setenv("DAGNATS_LEAF_HOST", "127.0.0.1")
	t.Setenv("DAGNATS_LEAF_LISTEN_USER", "hub2")
	t.Setenv("DAGNATS_LEAF_LISTEN_PASSWORD_FILE", pwFile)
	t.Setenv("DAGNATS_LEAF_USER", "e2")
	t.Setenv("DAGNATS_LEAF_PASSWORD_FILE", pwFile)
	cfg, _, err = ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath with env: %v", err)
	}
	if cfg.LeafPort != 7500 || cfg.LeafHost != "127.0.0.1" ||
		cfg.LeafListenUser != "hub2" || cfg.LeafUser != "e2" {
		t.Fatalf("env overrides not applied: %+v", cfg)
	}
	t.Setenv("DAGNATS_LEAF_PORT", "seventy")
	if _, _, err := ConfigWithPath(path); err == nil {
		t.Fatal("a non-numeric DAGNATS_LEAF_PORT was accepted")
	}
}

func TestLeafListenerConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.LeafPort != 0 || cfg.LeafHost != "" || cfg.LeafListenUser != "" || cfg.LeafUser != "" {
		t.Fatalf("leaf listener and remote login must default off: %+v", cfg)
	}
	if err := validateLeafConfig(cfg); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

func TestValidateLeafConfig(t *testing.T) {
	listener := func(c *Config) {
		c.LeafPort, c.LeafListenUser, c.LeafListenPasswordFile = 7422, "hub", "/pw"
	}
	remote := func(c *Config) {
		c.LeafRemotes, c.LeafUser, c.LeafPasswordFile = []string{"nats-leaf://hub:7422"}, "e", "/pw"
	}
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"listener with auth", listener, ""},
		{"listener on a private IP with auth", func(c *Config) {
			listener(c)
			c.LeafHost = "100.64.0.9"
		}, ""},
		{"listener without auth on loopback", func(c *Config) {
			c.LeafPort = 7422
		}, "requires leaf_listen_user and leaf_listen_password_file"},
		{"listener without auth on all interfaces", func(c *Config) {
			c.LeafPort, c.LeafHost = 7422, "0.0.0.0"
		}, "requires leaf_listen_user and leaf_listen_password_file"},
		{"listen user without its password file", func(c *Config) {
			listener(c)
			c.LeafListenPasswordFile = ""
		}, "requires leaf_listen_user and leaf_listen_password_file"},
		{"leaf host must be an IP", func(c *Config) {
			listener(c)
			c.LeafHost = "localhost"
		}, "leaf_host must be an IP address"},
		{"leaf host without a port", func(c *Config) {
			c.LeafHost = "127.0.0.1"
		}, "leaf_host is set but leaf_port is not"},
		{"listen login without a port", func(c *Config) {
			c.LeafListenUser, c.LeafListenPasswordFile = "hub", "/pw"
		}, "leaf_listen_user is set but leaf_port is not"},
		{"leaf port out of range", func(c *Config) {
			listener(c)
			c.LeafPort = 70000
		}, "leaf_port"},
		{"leaf port collides with nats port", func(c *Config) {
			listener(c)
			c.LeafPort = c.NATSPort
		}, "leaf_port 4222 collides with nats_port"},
		{"remote login", remote, ""},
		{"remote user without its password file", func(c *Config) {
			remote(c)
			c.LeafPasswordFile = ""
		}, "leaf_user and leaf_password_file must be set together"},
		{"remote login and credentials", func(c *Config) {
			remote(c)
			c.LeafCredentials = "/e.creds"
		}, "leaf_user and leaf_credentials are mutually exclusive"},
		{"remote login without remotes", func(c *Config) {
			remote(c)
			c.LeafRemotes = nil
		}, "leaf_user is set but leaf_remotes is not"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(&cfg)
			err := validateLeafConfig(cfg)
			if tc.want == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// The listener is allowed in cluster mode; leaf remotes stay excluded.
func TestValidateClusterConfig_AllowsLeafListener(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NATSClusterName = "mesh"
	cfg.NATSClusterRoutes = []string{"nats://a:6222", "nats://b:6222"}
	cfg.LeafPort, cfg.LeafListenUser, cfg.LeafListenPasswordFile = 7422, "hub", "/pw"
	validateClusterConfig(&cfg)
	if err := validateLeafConfig(cfg); err != nil {
		t.Fatalf("leaf listener refused in cluster mode: %v", err)
	}
	cfg.LeafPort = defaultClusterPort
	if err := validateLeafConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "collides with nats_cluster_port") {
		t.Fatalf("leaf_port on the cluster port: err = %v, want a collision", err)
	}
}

func TestResolveLeafAuth_ReadsSecretsKeepsThemOutOfErrors(t *testing.T) {
	cfg := Config{
		LeafPort: 7422, LeafListenUser: "hub",
		LeafListenPasswordFile: writeSecret(t, " "+testLeafPassword+"\n"),
		LeafRemotes:            []string{"nats-leaf://hub:7422"}, LeafUser: "e",
		LeafPasswordFile: writeSecret(t, "remote-pw"),
	}
	if err := resolveLeafAuth(&cfg); err != nil {
		t.Fatalf("resolveLeafAuth: %v", err)
	}
	if cfg.leafListenPassword != testLeafPassword || cfg.leafPassword != "remote-pw" {
		t.Fatalf("secrets not resolved: listen %q remote %q",
			cfg.leafListenPassword, cfg.leafPassword)
	}
	cfg.LeafListenPasswordFile = writeSecret(t, "")
	err := resolveLeafAuth(&cfg)
	if err == nil || !strings.Contains(err.Error(), "leaf_listen_password_file") {
		t.Fatalf("empty password file: err = %v, want a leaf_listen_password_file error", err)
	}
	if strings.Contains(err.Error(), testLeafPassword) {
		t.Fatalf("error carries the old secret: %v", err)
	}
}

// With every leaf key unset, the server options carry no leaf state at all.
func TestNATSOptions_NoLeafStateWhenUnset(t *testing.T) {
	opts, err := natsOptions(Config{DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: 1 << 30})
	if err != nil {
		t.Fatalf("natsOptions: %v", err)
	}
	if opts.LeafNode.Port != 0 || opts.LeafNode.Host != "" || opts.LeafNode.Username != "" ||
		opts.LeafNode.Password != "" || len(opts.LeafNode.Remotes) != 0 {
		t.Fatalf("leaf options set with nothing configured: %+v", opts.LeafNode)
	}
	opts, err = natsOptions(Config{
		DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: 1 << 30,
		LeafRemotes: []string{"nats-leaf://hub:7422"},
	})
	if err != nil {
		t.Fatalf("natsOptions with a remote: %v", err)
	}
	if opts.LeafNode.Port != 0 || len(opts.LeafNode.Remotes) != 1 ||
		opts.LeafNode.Remotes[0].URLs[0].User != nil {
		t.Fatalf("a bare remote gained a listener or a login: %+v", opts.LeafNode)
	}
}

func TestLeafListener_AcceptsOnlyTheRightLogin(t *testing.T) {
	hub, port := startLeafHub(t, Config{})
	varz, err := hub.Varz(nil)
	if err != nil || varz.LeafNode.Host != "127.0.0.1" {
		t.Fatalf("leaf listener bound %v (err %v), want the 127.0.0.1 default", varz, err)
	}
	for name, userinfo := range map[string]*url.Userinfo{
		"missing": nil,
		"wrong":   url.UserPassword(testLeafUser, "nope"),
	} {
		leaf := startRawLeaf(t, port, userinfo)
		assertLeafRefused(t, leaf, name)
	}
	leaf := startRawLeaf(t, port, url.UserPassword(testLeafUser, testLeafPassword))
	waitLeafConnected(t, leaf)
	waitLeafConnected(t, hub)
}

// A dagnats leaf logs in to a dagnats hub with leaf_user and its password
// file; the same leaf with a wrong password file stays out.
func TestLeafRemote_UserPasswordLogsIn(t *testing.T) {
	hub, port := startLeafHub(t, Config{})
	waitLeafConnected(t, startDagnatsLeaf(t, port, testLeafPassword))
	waitLeafConnected(t, hub)

	refused := startDagnatsLeaf(t, port, "not-the-password")
	assertLeafRefused(t, refused, "dagnats leaf with a wrong password")
}

// Every node of a 3-node cluster serves the leaf listener; a leaf linked to
// one node is reachable by domain from a client on another.
func TestLeafListener_WorksInThreeNodeCluster(t *testing.T) {
	ports := freeTCPPorts(t, 9)
	clientPorts, routePorts, leafPorts := ports[0:3], ports[3:6], ports[6:9]
	nodes := make([]*natsserver.Server, 3)
	for i := range nodes {
		routes := make([]string, 0, 2)
		for j, routePort := range routePorts {
			if j != i {
				routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", routePort))
			}
		}
		nodes[i], _ = startLeafHub(t, Config{
			NATSPort: clientPorts[i], LeafPort: leafPorts[i],
			NATSClusterName: "mesh", NATSClusterHost: "127.0.0.1",
			NATSClusterPort: routePorts[i], NATSClusterRoutes: routes,
			NATSServerName: fmt.Sprintf("mesh-%d", i),
		})
	}
	waitClusterRoutes(t, nodes)
	leaf := startDagnatsLeaf(t, leafPorts[0], testLeafPassword)
	assertHubReachesLeafByDomain(t, nodes[2].ClientURL(), leaf)
}

// startLeafHub starts a dagnats server with the leaf listener and its login
// on top of base, and returns it with the listener's bound port.
func startLeafHub(t *testing.T, base Config) (*natsserver.Server, int) {
	t.Helper()
	cfg := base
	cfg.DataDir, cfg.MaxStoreBytes, cfg.MaxMemoryBytes = t.TempDir(), 1<<30, 1<<28
	if cfg.NATSPort == 0 {
		cfg.NATSPort = -1
	}
	if cfg.LeafPort == 0 {
		cfg.LeafPort = -1
	}
	cfg.LeafListenUser = testLeafUser
	cfg.LeafListenPasswordFile = writeSecret(t, testLeafPassword)
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("resolve hub auth: %v", err)
	}
	hub, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("start hub: %v", err)
	}
	t.Cleanup(hub.Shutdown)
	varz, err := hub.Varz(nil)
	if err != nil || varz.LeafNode.Port <= 0 {
		t.Fatalf("hub leaf listener not bound: port %v, err %v", varz, err)
	}
	return hub, varz.LeafNode.Port
}

// startDagnatsLeaf starts a dagnats leaf with domain e-mac that logs in to
// the hub's leaf listener on port with leaf_user and password.
func startDagnatsLeaf(t *testing.T, port int, password string) *natsserver.Server {
	t.Helper()
	cfg := Config{
		DataDir: t.TempDir(), NATSPort: -1, NATSHost: "127.0.0.1",
		MaxStoreBytes: 1 << 30, JetStreamDomain: "e-mac",
		LeafRemotes:      []string{fmt.Sprintf("nats-leaf://127.0.0.1:%d", port)},
		LeafUser:         testLeafUser,
		LeafPasswordFile: writeSecret(t, password),
	}
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("resolve leaf auth: %v", err)
	}
	leaf, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("start leaf: %v", err)
	}
	t.Cleanup(leaf.Shutdown)
	return leaf
}

// startRawLeaf starts a plain nats-server leaf that dials port with
// userinfo (nil sends no login).
func startRawLeaf(t *testing.T, port int, userinfo *url.Userinfo) *natsserver.Server {
	t.Helper()
	remote := &url.URL{Scheme: "nats-leaf", Host: fmt.Sprintf("127.0.0.1:%d", port), User: userinfo}
	leaf, err := tryStartNATS(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		LeafNode: natsserver.LeafNodeOpts{
			ReconnectInterval: 50 * time.Millisecond,
			Remotes:           []*natsserver.RemoteLeafOpts{{URLs: []*url.URL{remote}}},
		},
	})
	if err != nil {
		t.Fatalf("start raw leaf: %v", err)
	}
	t.Cleanup(leaf.Shutdown)
	return leaf
}

// assertLeafRefused checks the leaf stays unlinked for a bounded window that
// covers several of its reconnect attempts.
func assertLeafRefused(t *testing.T, leaf *natsserver.Server, name string) {
	t.Helper()
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if leaf.NumLeafNodes() != 0 {
			t.Fatalf("%s login: the leaf connected", name)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitClusterRoutes waits (bounded 20s) for a full route mesh and a
// JetStream meta leader.
func waitClusterRoutes(t *testing.T, nodes []*natsserver.Server) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		meshed := true
		for _, node := range nodes {
			meshed = meshed && node.NumRoutes() >= len(nodes)-1
		}
		if meshed && jetStreamMetaReady(nodes[0]) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("cluster did not mesh and elect a JetStream leader within 20s")
}

func jetStreamMetaReady(node *natsserver.Server) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	nc, err := nats.Connect(node.ClientURL())
	if err != nil {
		return false
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return false
	}
	_, err = js.AccountInfo(ctx)
	return err == nil
}
