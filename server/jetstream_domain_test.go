// server/jetstream_domain_test.go
// jetstream_domain: config parsing and validation as pure unit tests, then
// integration tests with real embedded NATS servers per test. The two-server
// test links a leaf with a domain to a hub without one and checks both that
// the domain reaches the leaf's JetStream and that plain $JS.API does not.
package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamDomainConfig_DefaultFileAndEnv(t *testing.T) {
	if got := DefaultConfig().JetStreamDomain; got != "" {
		t.Fatalf("default jetstream_domain = %q, want empty", got)
	}
	path := writeConfigFile(t, "jetstream_domain: e-mac\n")
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if cfg.JetStreamDomain != "e-mac" {
		t.Fatalf("file key: jetstream_domain = %q, want e-mac", cfg.JetStreamDomain)
	}
	t.Setenv("DAGNATS_JETSTREAM_DOMAIN", "laptop_2")
	cfg, _, err = ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath with env: %v", err)
	}
	if cfg.JetStreamDomain != "laptop_2" {
		t.Fatalf("env override: jetstream_domain = %q, want laptop_2", cfg.JetStreamDomain)
	}
}

func TestJetStreamDomainConfig_RejectsNonTokens(t *testing.T) {
	for _, bad := range []string{"e.mac", "e mac", "e*", ">", "e\tmac", "e>mac"} {
		_, _, err := ConfigWithPath(writeConfigFile(t, "jetstream_domain: "+bad+"\n"))
		if err == nil || !strings.Contains(err.Error(), "jetstream_domain") {
			t.Errorf("jetstream_domain %q: err = %v, want a jetstream_domain error", bad, err)
		}
	}
	for _, good := range []string{"", "e-mac", "hub", "E_MAC_2"} {
		if err := validateJetStreamDomain(good); err != nil {
			t.Errorf("jetstream_domain %q rejected: %v", good, err)
		}
	}
	t.Setenv("DAGNATS_JETSTREAM_DOMAIN", "a.b")
	if _, _, err := ConfigWithPath(writeConfigFile(t, "")); err == nil {
		t.Fatal("DAGNATS_JETSTREAM_DOMAIN with a dot accepted")
	}
}

// A domain-bearing server still answers plain $JS.API locally, so every
// jetstream.New(nc) call site keeps working unchanged.
func TestStartNATS_JetStreamDomainKeepsPlainAPI(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: 1 << 30,
		MaxMemoryBytes: 1 << 28, JetStreamDomain: "e-mac",
	}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer ns.Shutdown()
	nc := connectOrFail(t, ns.ClientURL())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for name, js := range map[string]jetstream.JetStream{
		"plain": newJetStream(t, nc, ""), "domain": newJetStream(t, nc, "e-mac"),
	} {
		info, err := js.AccountInfo(ctx)
		if err != nil {
			t.Fatalf("%s AccountInfo: %v", name, err)
		}
		if info.Domain != "e-mac" {
			t.Fatalf("%s AccountInfo domain = %q, want e-mac", name, info.Domain)
		}
	}

	cfg.JetStreamDomain = "bad.domain"
	if ns, err := startNATS(cfg); err == nil {
		ns.Shutdown()
		t.Fatal("startNATS accepted a domain with a dot")
	}
}

// Server.Run provisions every stream and bucket through SetupAll over plain
// $JS.API; with a domain set it must still do so and complete a run.
func TestServer_JetStreamDomainCompletesRun(t *testing.T) {
	cfg := testConfig(t)
	cfg.JetStreamDomain = "e-mac"
	runEmbeddedWorkflow(t, cfg)
}

// A hub without a domain reaches a leaf's JetStream only through the leaf's
// domain; its own $JS.API stays its own.
func TestJetStreamDomain_HubReachesLeafByDomain(t *testing.T) {
	hubURL, leafRemote := startDomainHub(t)
	leaf, err := startNATS(Config{
		DataDir: t.TempDir(), NATSPort: -1, NATSHost: "127.0.0.1",
		MaxStoreBytes: 1 << 30, JetStreamDomain: "e-mac",
		LeafRemotes: []string{leafRemote},
	})
	if err != nil {
		t.Fatalf("start leaf: %v", err)
	}
	defer leaf.Shutdown()
	assertHubReachesLeafByDomain(t, hubURL, leaf)
}

// startDomainHub starts a plain JetStream server with a leafnode listener and
// returns its client URL and the leaf remote URL that dials it.
func startDomainHub(t *testing.T) (string, string) {
	t.Helper()
	opts := &natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		LeafNode: natsserver.LeafNodeOpts{Host: "127.0.0.1", Port: -1},
		NoLog:    true, NoSigs: true,
	}
	hub, err := tryStartNATS(opts)
	if err != nil {
		t.Fatalf("start hub: %v", err)
	}
	t.Cleanup(hub.Shutdown)
	if opts.LeafNode.Port <= 0 {
		t.Fatalf("hub leaf port not resolved: %d", opts.LeafNode.Port)
	}
	return hub.ClientURL(), fmt.Sprintf("nats-leaf://127.0.0.1:%d", opts.LeafNode.Port)
}

// assertHubReachesLeafByDomain creates a stream on the leaf, then reads it
// from a hub client by domain and fails to find it through plain $JS.API.
func assertHubReachesLeafByDomain(t *testing.T, hubURL string, leaf *natsserver.Server) {
	t.Helper()
	waitLeafConnected(t, leaf)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	leafJS := newJetStream(t, connectOrFail(t, leaf.ClientURL()), "")
	if _, err := leafJS.CreateStream(ctx, jetstream.StreamConfig{
		Name: "LEAF_ONLY", Subjects: []string{"leafonly.>"},
	}); err != nil {
		t.Fatalf("create stream on leaf: %v", err)
	}

	hubNC := connectOrFail(t, hubURL)
	stream, err := newJetStream(t, hubNC, "e-mac").Stream(ctx, "LEAF_ONLY")
	if err != nil {
		t.Fatalf("hub could not read the leaf stream by domain: %v", err)
	}
	if got := stream.CachedInfo().Config.Name; got != "LEAF_ONLY" {
		t.Fatalf("stream by domain = %q, want LEAF_ONLY", got)
	}
	_, err = newJetStream(t, hubNC, "").Stream(ctx, "LEAF_ONLY")
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("plain $JS.API on the hub: err = %v, want stream not found", err)
	}
}

// waitLeafConnected waits (bounded 10s) for the leaf to link to its hub.
func waitLeafConnected(t *testing.T, leaf *natsserver.Server) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leaf.NumLeafNodes() == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("leaf did not connect to the hub within 10s; leafnodes = %d", leaf.NumLeafNodes())
}

func connectOrFail(t *testing.T, url string, options ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, options...)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// newJetStream opens a JetStream context, by domain when domain is set.
func newJetStream(t *testing.T, nc *nats.Conn, domain string) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if domain != "" {
		js, err = jetstream.NewWithDomain(nc, domain)
	}
	if err != nil {
		t.Fatalf("jetstream (domain %q): %v", domain, err)
	}
	return js
}
