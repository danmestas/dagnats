// leaf_allow_test.go: leaf_listen_allow and the leaf login's place in the
// users table (ADR-031). Pure unit tests for config parsing, validation
// and the options natsOptions builds, then integration tests with real
// embedded servers for which logins a leaf may link with. Each case checks
// the refused path as well as the allowed one.
package server

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// regionLinkAllow is the allow-list a region link sets on eu's listener.
var regionLinkAllow = []string{
	"$JS.eu.API.>", "$JS.us.API.>", "$JS.ACK.>", "$JS.FC.>", "_INBOX.>",
}

func TestLeafListenAllowConfig_FileAndEnv(t *testing.T) {
	if got := DefaultConfig().LeafListenAllow; got != nil {
		t.Fatalf("default leaf_listen_allow = %v, want unset", got)
	}
	path := writeConfigFile(t, fmt.Sprintf("leaf_port: 7422\nleaf_listen_user: hub\n"+
		"leaf_listen_password_file: %s\nleaf_listen_allow: %s\n",
		writeSecret(t, "pw"), strings.Join(regionLinkAllow, ", ")))
	cfg, _, err := ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath: %v", err)
	}
	if !slices.Equal(cfg.LeafListenAllow, regionLinkAllow) {
		t.Fatalf("file key: leaf_listen_allow = %q, want %q", cfg.LeafListenAllow, regionLinkAllow)
	}
	t.Setenv("DAGNATS_LEAF_LISTEN_ALLOW", "_INBOX.>,$JS.eu.API.>")
	cfg, _, err = ConfigWithPath(path)
	if err != nil {
		t.Fatalf("ConfigWithPath with env: %v", err)
	}
	if !slices.Equal(cfg.LeafListenAllow, []string{"_INBOX.>", "$JS.eu.API.>"}) {
		t.Fatalf("env override: leaf_listen_allow = %q", cfg.LeafListenAllow)
	}
}

func TestValidateLeafListenAllow(t *testing.T) {
	listener := func(c *Config) {
		c.LeafPort, c.LeafListenUser, c.LeafListenPasswordFile = 7422, "hub", "/pw"
	}
	tooMany := make([]string, leafListenAllowMax+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("s%d.>", i)
	}
	cases := []struct {
		name  string
		mut   func(*Config)
		allow []string
		want  string
	}{
		{"region link list", listener, regionLinkAllow, ""},
		{"allow without a listener", func(*Config) {}, []string{"_INBOX.>"},
			"leaf_listen_allow is set but leaf_port is not"},
		{"empty token", listener, []string{"a..b"}, "not a valid subject"},
		{"whitespace", listener, []string{"a b"}, "not a valid subject"},
		{"> not last", listener, []string{"a.>.b"}, "not a valid subject"},
		{"too many", listener, tooMany, "at most"},
		{"listen user is the client login", func(c *Config) {
			listener(c)
			c.NATSUser = "hub"
		}, nil, "leaf_listen_user must differ from nats_user"},
		{"listen user is reserved", func(c *Config) {
			listener(c)
			c.LeafListenUser = rootUser
		}, nil, "reserved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(&cfg)
			cfg.LeafListenAllow = tc.allow
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

// The leaf login is a users-table user that may only link as a leaf,
// carrying the allow-list; every other user and NKey may not link at all.
func TestNATSOptions_LeafLoginInUsersTable(t *testing.T) {
	for _, allow := range [][]string{nil, regionLinkAllow} {
		cfg := Config{
			DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: 1 << 30,
			LeafPort: -1, LeafListenUser: testLeafUser, LeafListenAllow: allow,
			NATSUser: "operator", natsPassword: "op-pw", leafListenPassword: testLeafPassword,
		}
		opts, err := natsOptions(cfg)
		if err != nil {
			t.Fatalf("natsOptions: %v", err)
		}
		if opts.LeafNode.Username != "" || opts.LeafNode.Password != "" ||
			len(opts.LeafNode.Users) != 0 {
			t.Fatalf("leaf block carries its own auth: %+v", opts.LeafNode)
		}
		leaf := findUser(opts, testLeafUser)
		if leaf == nil || leaf.Password != testLeafPassword ||
			!onlyLeafnode(leaf.AllowedConnectionTypes) {
			t.Fatalf("leaf user = %+v, want a leafnode-only user", leaf)
		}
		assertLeafPermissions(t, leaf.Permissions, allow)
		for _, user := range opts.Users {
			if user.Username != testLeafUser && allowsLeafnode(user.AllowedConnectionTypes) {
				t.Errorf("user %s may link as a leaf", user.Username)
			}
		}
		for _, nkey := range opts.Nkeys {
			if allowsLeafnode(nkey.AllowedConnectionTypes) {
				t.Errorf("nkey %s may link as a leaf", nkey.Nkey)
			}
		}
	}
}

func assertLeafPermissions(t *testing.T, perms *natsserver.Permissions, allow []string) {
	t.Helper()
	if allow == nil {
		if perms != nil {
			t.Fatalf("leaf user without an allow-list has permissions %+v", perms)
		}
		return
	}
	if perms == nil || perms.Publish == nil || perms.Subscribe == nil ||
		!slices.Equal(perms.Publish.Allow, allow) || !slices.Equal(perms.Subscribe.Allow, allow) {
		t.Fatalf("leaf permissions = %+v, want publish and subscribe allow %q", perms, allow)
	}
}

func findUser(opts *natsserver.Options, name string) *natsserver.User {
	for _, user := range opts.Users {
		if user.Username == name {
			return user
		}
	}
	return nil
}

func onlyLeafnode(types map[string]struct{}) bool {
	_, ok := types[jwt.ConnectionTypeLeafnode]
	return ok && len(types) == 1
}

// allowsLeafnode treats an empty set as nats-server does: every type.
func allowsLeafnode(types map[string]struct{}) bool {
	_, ok := types[jwt.ConnectionTypeLeafnode]
	return ok || len(types) == 0
}

// A leaf that dials with no login (the anonymous user) or with any client
// login is refused; only the leaf login links. Covers both open mode
// (anonymous is root) and login mode (anonymous is the join user).
func TestLeafListener_RefusesClientLogins(t *testing.T) {
	open, openPort := startLeafHub(t, Config{})
	assertLeafRefused(t, startRawLeaf(t, openPort, nil), "anonymous root")
	waitLeafConnected(t, startRawLeaf(t, openPort,
		url.UserPassword(testLeafUser, testLeafPassword)))
	waitLeafConnected(t, open)

	login, loginPort := startLeafHub(t, Config{
		NATSUser: "operator", NATSPasswordFile: writeSecret(t, "op-pw"),
	})
	assertLeafRefused(t, startRawLeaf(t, loginPort, nil), "anonymous join")
	assertLeafRefused(t, startRawLeaf(t, loginPort, url.UserPassword("operator", "op-pw")),
		"nats_user")
	waitLeafConnected(t, startRawLeaf(t, loginPort,
		url.UserPassword(testLeafUser, testLeafPassword)))
	waitLeafConnected(t, login)
}

// Every leaf_remotes URL goes into one remote: one connection that fails
// over between the URLs. A remote per URL would open a parallel link to
// each hub node and deliver every crossing message once per link.
func TestNATSOptions_LeafRemotesAreOneFailoverRemote(t *testing.T) {
	remotes := []string{
		"nats-leaf://10.0.0.1:7422", "nats-leaf://10.0.0.2:7422",
		"nats-leaf://e:own@10.0.0.3:7422",
	}
	opts, err := natsOptions(Config{
		DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: 1 << 30,
		LeafRemotes: remotes, LeafUser: testLeafUser, leafPassword: testLeafPassword,
	})
	if err != nil {
		t.Fatalf("natsOptions: %v", err)
	}
	if len(opts.LeafNode.Remotes) != 1 || len(opts.LeafNode.Remotes[0].URLs) != len(remotes) {
		t.Fatalf("remotes = %+v, want one remote holding %d URLs",
			opts.LeafNode.Remotes, len(remotes))
	}
	urls := opts.LeafNode.Remotes[0].URLs
	for i, want := range []string{testLeafUser, testLeafUser, "e"} {
		if urls[i].User == nil || urls[i].User.Username() != want {
			t.Errorf("URL %d login = %v, want user %s", i, urls[i].User, want)
		}
	}
}
