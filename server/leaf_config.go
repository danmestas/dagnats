// server/leaf_config.go
// Leafnode settings beyond leaf_remotes: a listener other servers dial as
// leaf nodes, guarded by a user and password, and a user/password login
// for this server's own leaf remotes.
package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
)

// applyLeafEnv applies the DAGNATS_LEAF_* overrides for the listener and
// the remote login. A leaf_port that does not parse is an error rather than
// a silently closed listener.
func applyLeafEnv(cfg *Config) error {
	if cfg == nil {
		panic("applyLeafEnv: cfg is nil")
	}
	if val := os.Getenv("DAGNATS_LEAF_PORT"); val != "" {
		port, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid DAGNATS_LEAF_PORT: %w", err)
		}
		cfg.LeafPort = port
	}
	overrides := map[string]*string{
		"DAGNATS_LEAF_HOST":                 &cfg.LeafHost,
		"DAGNATS_LEAF_LISTEN_USER":          &cfg.LeafListenUser,
		"DAGNATS_LEAF_LISTEN_PASSWORD_FILE": &cfg.LeafListenPasswordFile,
		"DAGNATS_LEAF_USER":                 &cfg.LeafUser,
		"DAGNATS_LEAF_PASSWORD_FILE":        &cfg.LeafPasswordFile,
	}
	if len(overrides) != 5 {
		panic("applyLeafEnv: lost an override")
	}
	for name, field := range overrides {
		if val := os.Getenv(name); val != "" {
			*field = val
		}
	}
	return nil
}

// validateLeafConfig checks the leaf listener and the remote login. It
// returns errors, not panics, because every value comes from the operator.
func validateLeafConfig(cfg Config) error {
	if err := validateLeafListener(cfg); err != nil {
		return err
	}
	return validateLeafRemoteLogin(cfg)
}

// validateLeafListener requires a login on any leaf listener: a linked leaf
// shares this server's account, so an open listener would hand every
// subject and stream to whoever can reach the port, loopback included.
func validateLeafListener(cfg Config) error {
	if cfg.LeafPort == 0 {
		if cfg.LeafHost != "" {
			return errors.New("leaf_host is set but leaf_port is not")
		}
		if cfg.LeafListenUser != "" || cfg.LeafListenPasswordFile != "" {
			return errors.New("leaf_listen_user is set but leaf_port is not")
		}
		return nil
	}
	if cfg.LeafPort < -1 || cfg.LeafPort > 65535 {
		return fmt.Errorf("leaf_port must be 0 (off) or 1-65535; got %d", cfg.LeafPort)
	}
	if cfg.LeafHost != "" && net.ParseIP(cfg.LeafHost) == nil {
		return fmt.Errorf("leaf_host must be an IP address; got %q", cfg.LeafHost)
	}
	if cfg.LeafListenUser == "" || cfg.LeafListenPasswordFile == "" {
		return fmt.Errorf("leaf_port on %s requires leaf_listen_user and "+
			"leaf_listen_password_file: a leaf that links shares every "+
			"subject and stream", leafBindHost(cfg))
	}
	for name, other := range listenerPorts(cfg) {
		if name != "leaf_port" && other > 0 && other == cfg.LeafPort {
			return fmt.Errorf("leaf_port %d collides with %s", cfg.LeafPort, name)
		}
	}
	return nil
}

// validateLeafRemoteLogin checks leaf_user: it pairs with its password
// file, needs remotes to log in to, and excludes the creds-file login.
func validateLeafRemoteLogin(cfg Config) error {
	if (cfg.LeafUser == "") != (cfg.LeafPasswordFile == "") {
		return errors.New("leaf_user and leaf_password_file must be set together")
	}
	if cfg.LeafUser == "" {
		return nil
	}
	if len(cfg.LeafRemotes) == 0 {
		return errors.New("leaf_user is set but leaf_remotes is not")
	}
	if cfg.LeafCredentials != "" {
		return errors.New("leaf_user and leaf_credentials are mutually exclusive")
	}
	return nil
}

// leafBindHost is the leaf listener's address: cfg.LeafHost, else loopback.
// Never all interfaces by default; a remote hub names its private IP.
func leafBindHost(cfg Config) string {
	if cfg.LeafHost != "" {
		return cfg.LeafHost
	}
	return "127.0.0.1"
}
