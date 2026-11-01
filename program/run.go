package program

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// registerTimeoutDefault bounds registration when Run's ctx has no
// deadline. Registration provisions streams, so it is not instant, but
// a dagnats that has not answered in this long is not going to.
const registerTimeoutDefault = 30 * time.Second

// Options is where a connected program finds its dagnats.
type Options struct {
	// URL is the nats:// address of the server the program dials.
	URL string
	// SeedPath is where the program's NKey seed lives; created on the
	// first run, mode 0600.
	SeedPath string
	// JoinKey is presented at registration. Empty is accepted only by
	// a dagnats whose NATS clients need no login (open mode).
	JoinKey string
	// NATSOptions are applied to both of Run's connections before Run's
	// own (inbox prefix, login, name, reconnect policy, close handler),
	// so they cannot override those. A ClosedHandler here is chained,
	// not replaced.
	NATSOptions []nats.Option
}

// OptionsFromEnv reads <NAME>_DAGNATS_URL, <NAME>_SEED and
// <NAME>_JOIN_KEY, with NAME the program name in upper case: notify
// reads NOTIFY_DAGNATS_URL. The URL defaults to nats://127.0.0.1:4222
// and the seed to ./<name>.seed.
func OptionsFromEnv(name string) Options {
	if err := ValidateName(name); err != nil {
		panic("program.OptionsFromEnv: " + err.Error())
	}
	prefix := strings.ToUpper(name) + "_"
	opts := Options{
		URL:      os.Getenv(prefix + "DAGNATS_URL"),
		SeedPath: os.Getenv(prefix + "SEED"),
		JoinKey:  os.Getenv(prefix + "JOIN_KEY"),
	}
	if opts.URL == "" {
		opts.URL = nats.DefaultURL
	}
	if opts.SeedPath == "" {
		opts.SeedPath = name + ".seed"
	}
	return opts
}

// Run is the connected placement. It loads or creates the program's
// NKey at opts.SeedPath, registers with dagnats over an anonymous
// connection (a restart with the same key and manifest gets the stored
// Grant back), dials as the program's own user with its inbox prefix,
// and Attaches. A refusal is returned as an *ErrRefused before any
// stream, consumer or handler exists.
//
// The program's connection reconnects forever, so a dagnats that is down
// for a long upgrade does not strand the process. Hosts should still
// watch Running.Done and exit when it fires, so their supervisor
// restarts them: it reports a connection closed for good.
func Run(ctx context.Context, prog Program, opts Options) (*Running, error) {
	if ctx == nil || prog == nil {
		panic("program.Run: ctx and prog must not be nil")
	}
	if opts.URL == "" || opts.SeedPath == "" {
		panic("program.Run: opts.URL and opts.SeedPath must be set")
	}
	m := prog.Manifest()
	if err := Validate(m); err != nil {
		return nil, err
	}
	key, err := LoadOrCreateKey(opts.SeedPath)
	if err != nil {
		return nil, fmt.Errorf("program %s: key: %w", m.Name, err)
	}
	req := RegisterRequest{
		JoinKey: opts.JoinKey, Manifest: m, PublicKey: key.PublicKey(), Instance: thisInstance(),
	}
	grant, confirm, err := register(ctx, opts, req)
	if err != nil {
		return nil, err
	}
	running := newRunning(m.Name)
	nc, err := dialAsProgram(opts, m.Name, key, running)
	if err != nil {
		return nil, err
	}
	if confirm != "" {
		// The registration (or a manifest change) is pending until this
		// connection, logged in with the key, confirms it.
		if grant, err = confirmRegistration(ctx, nc, confirm, req); err != nil {
			running.stopping.Store(true)
			nc.Close()
			return nil, err
		}
		if err := waitPromoted(ctx, nc, m.Name); err != nil {
			running.stopping.Store(true)
			nc.Close()
			return nil, err
		}
	}
	stopAttach, err := Attach(ctx, nc, prog, grant)
	if err != nil {
		running.stopping.Store(true)
		nc.Close()
		return nil, err
	}
	running.nc, running.stopAttach = nc, stopAttach
	return running, nil
}

// firstDialRetryWindow bounds how long Run waits for its new NKey user
// to reach the node it dials; firstDialRetryWait paces the attempts.
const (
	firstDialRetryWindow = 10 * time.Second
	firstDialRetryWait   = 250 * time.Millisecond
)

// Reconnect policy for the program's connection: forever, a bounded
// wait with jitter so a fleet of programs does not reconnect in step.
const (
	reconnectWait         = 2 * time.Second
	reconnectJitter       = 500 * time.Millisecond
	reconnectJitterForTLS = 2 * time.Second
)

// dialAsProgram connects as the program's NKey user with its inbox
// prefix. opts.NATSOptions go first and Run's own last, so a caller
// cannot replace the inbox prefix, the login or the close handler; a
// caller's ClosedHandler is chained instead. A server with no client
// auth at all (open mode) sends no nonce, so an NKey login is
// impossible there; anonymous is root on such a server, so Run dials
// anonymously, still with the prefix.
func dialAsProgram(opts Options, name string, key Key, running *Running) (*nats.Conn, error) {
	if name == "" || running == nil {
		panic("program.dialAsProgram: name and running must be set")
	}
	callerClosed, err := closedHandlerOf(opts.NATSOptions)
	if err != nil {
		return nil, fmt.Errorf("program %s: NATSOptions: %w", name, err)
	}
	options := append(append([]nats.Option(nil), opts.NATSOptions...),
		nats.MaxReconnects(-1), nats.ReconnectWait(reconnectWait),
		nats.ReconnectJitter(reconnectJitter, reconnectJitterForTLS),
		nats.ClosedHandler(running.closedHandler(callerClosed)),
		nats.CustomInboxPrefix(InboxPrefix(name)), nats.Name("program "+name),
	)
	nc, err := connectAsKey(opts.URL, options, key)
	if errors.Is(err, nats.ErrNkeysNotSupported) {
		nc, err = nats.Connect(opts.URL, options...)
	}
	if err != nil {
		return nil, fmt.Errorf("program %s: dial as its own user: %w", name, err)
	}
	return nc, nil
}

// connectAsKey dials with the program's NKey, retrying an authorization
// failure for up to firstDialRetryWindow. The registering node adds the
// user before it replies, but a cluster's other nodes add it when their
// watch of the programs bucket catches up, and the dial may land on one
// of those first.
func connectAsKey(url string, options []nats.Option, key Key) (*nats.Conn, error) {
	if url == "" {
		panic("program.connectAsKey: url must be set")
	}
	withKey := append(slices.Clone(options), key.ConnectOption())
	deadline := time.Now().Add(firstDialRetryWindow)
	for {
		nc, err := nats.Connect(url, withKey...)
		if !errors.Is(err, nats.ErrAuthorization) || time.Now().After(deadline) {
			return nc, err
		}
		time.Sleep(firstDialRetryWait)
	}
}

// closedHandlerOf returns the ClosedHandler options sets, if any, by
// applying them to a scratch nats.Options.
func closedHandlerOf(options []nats.Option) (nats.ConnHandler, error) {
	scratch := nats.GetDefaultOptions()
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&scratch); err != nil {
			return nil, err
		}
	}
	return scratch.ClosedCB, nil
}

// register sends req over a short-lived anonymous connection dialed
// with the join inbox prefix, and returns the Grant or the refusal.
func register(ctx context.Context, opts Options, req RegisterRequest) (Grant, string, error) {
	if req.Manifest.Name == "" || req.PublicKey == "" {
		panic("program.register: manifest name and public key must be set")
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, registerTimeoutDefault)
		defer cancel()
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Grant{}, "", fmt.Errorf("program %s: encode registration: %w",
			req.Manifest.Name, err)
	}
	// The caller's options reach the server (TLS, a dialer), but this
	// short-lived connection must not fire the caller's close handler.
	options := append(append([]nats.Option(nil), opts.NATSOptions...),
		nats.ClosedHandler(nil), nats.CustomInboxPrefix(JoinInboxPrefix),
		nats.Name("program "+req.Manifest.Name+" registration"))
	nc, err := nats.Connect(opts.URL, options...)
	if err != nil {
		return Grant{}, "", fmt.Errorf("program %s: dial %s to register: %w",
			req.Manifest.Name, opts.URL, err)
	}
	defer nc.Close()
	msg, err := nc.RequestWithContext(ctx, RegisterSubject, body)
	if errors.Is(err, nats.ErrNoResponders) {
		return Grant{}, "", fmt.Errorf("program %s: no dagnats answers %s at %s",
			req.Manifest.Name, RegisterSubject, opts.URL)
	}
	if err != nil {
		return Grant{}, "", fmt.Errorf("program %s: register: %w", req.Manifest.Name, err)
	}
	return decodeReply(req.Manifest.Name, msg.Data)
}

// waitPromoted waits until nc's own server gives it the confirmed
// program's permissions. The confirming node reloads before it replies,
// but nc may be on another node whose watch has not caught up; a
// subscription Attach made with only pending permissions would fail for
// good. A pending user may not publish under api.p.<name>, so the first
// request there that is answered (or finds no responder, which also
// means the publish was allowed) shows the promotion has arrived.
func waitPromoted(ctx context.Context, nc *nats.Conn, name string) error {
	if nc == nil || name == "" {
		panic("program.waitPromoted: nc and name must be set")
	}
	deadline := time.Now().Add(firstDialRetryWindow)
	for {
		probeCtx, cancel := context.WithTimeout(ctx, promotionProbeWait)
		_, err := nc.RequestWithContext(probeCtx, "api.p."+name+".runs.get", []byte("x"))
		cancel()
		if err == nil || errors.Is(err, nats.ErrNoResponders) {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("program %s: its confirmed permissions did not reach "+
				"the server it is connected to within %s: %w",
				name, firstDialRetryWindow, err)
		}
		time.Sleep(firstDialRetryWait)
	}
}

// promotionProbeWait bounds one waitPromoted probe: a denied publish is
// never answered, so this is how long a pending user's probe takes.
const promotionProbeWait = 500 * time.Millisecond

// confirmRegistration confirms on subject over nc, which is logged in
// with the program's key: that login is what proves the program holds
// the seed (internal/programs/possession.go).
func confirmRegistration(
	ctx context.Context, nc *nats.Conn, subject string, req RegisterRequest,
) (Grant, error) {
	if nc == nil || subject == "" {
		panic("program.confirmRegistration: nc and subject must be set")
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, registerTimeoutDefault)
		defer cancel()
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Grant{}, fmt.Errorf("program %s: encode confirmation: %w", req.Manifest.Name, err)
	}
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return Grant{}, fmt.Errorf("program %s: confirm: %w", req.Manifest.Name, err)
	}
	grant, again, err := decodeReply(req.Manifest.Name, msg.Data)
	if err == nil && again != "" {
		return Grant{}, fmt.Errorf("program %s: confirmation still pending", req.Manifest.Name)
	}
	return grant, err
}

// decodeReply returns the Grant, the subject to confirm on when the
// registration needs it (the Grant may then be empty), or the refusal.
func decodeReply(name string, data []byte) (Grant, string, error) {
	var reply RegisterReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return Grant{}, "", fmt.Errorf("program %s: registration reply: %w", name, err)
	}
	switch {
	case reply.Refused != nil:
		return Grant{}, "", reply.Refused
	case reply.Error != "":
		return Grant{}, "", fmt.Errorf("program %s: registration failed: %s", name, reply.Error)
	case reply.Grant == nil && reply.Confirm == "":
		return Grant{}, "", fmt.Errorf("program %s: registration reply has no grant", name)
	case reply.Grant == nil:
		return Grant{}, reply.Confirm, nil
	}
	return *reply.Grant, reply.Confirm, nil
}

func thisInstance() Instance {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return Instance{Hostname: hostname, Pid: os.Getpid()}
}
