package program

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
// The returned stop drains the worker, calls prog.Stop and closes the
// connection. It makes no control-plane request, so it works while
// dagnats is down. Later calls return the first result.
func Run(
	ctx context.Context, prog Program, opts Options,
) (stop func(context.Context) error, err error) {
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
	grant, err := register(ctx, opts, RegisterRequest{
		JoinKey: opts.JoinKey, Manifest: m, PublicKey: key.PublicKey(), Instance: thisInstance(),
	})
	if err != nil {
		return nil, err
	}
	nc, err := dialAsProgram(opts.URL, m.Name, key)
	if err != nil {
		return nil, err
	}
	stopAttach, err := Attach(ctx, nc, prog, grant)
	if err != nil {
		nc.Close()
		return nil, err
	}
	var once sync.Once
	var stopErr error
	return func(stopCtx context.Context) error {
		once.Do(func() {
			stopErr = stopAttach(stopCtx)
			nc.Close()
		})
		return stopErr
	}, nil
}

// dialAsProgram connects as the program's NKey user with its inbox
// prefix. A server with no client auth at all (open mode) sends no
// nonce, so an NKey login is impossible there; anonymous is root on
// such a server, so Run dials anonymously, still with the prefix.
func dialAsProgram(url, name string, key Key) (*nats.Conn, error) {
	if url == "" || name == "" {
		panic("program.dialAsProgram: url and name must be set")
	}
	options := []nats.Option{
		nats.CustomInboxPrefix(InboxPrefix(name)), nats.Name("program " + name),
	}
	nc, err := nats.Connect(url, append(options, key.ConnectOption())...)
	if errors.Is(err, nats.ErrNkeysNotSupported) {
		nc, err = nats.Connect(url, options...)
	}
	if err != nil {
		return nil, fmt.Errorf("program %s: dial as its own user: %w", name, err)
	}
	return nc, nil
}

// register sends req over a short-lived anonymous connection dialed
// with the join inbox prefix, and returns the Grant or the refusal.
func register(ctx context.Context, opts Options, req RegisterRequest) (Grant, error) {
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
		return Grant{}, fmt.Errorf("program %s: encode registration: %w", req.Manifest.Name, err)
	}
	nc, err := nats.Connect(opts.URL, nats.CustomInboxPrefix(JoinInboxPrefix),
		nats.Name("program "+req.Manifest.Name+" registration"))
	if err != nil {
		return Grant{}, fmt.Errorf("program %s: dial %s to register: %w",
			req.Manifest.Name, opts.URL, err)
	}
	defer nc.Close()
	msg, err := nc.RequestWithContext(ctx, RegisterSubject, body)
	if errors.Is(err, nats.ErrNoResponders) {
		return Grant{}, fmt.Errorf("program %s: no dagnats answers %s at %s",
			req.Manifest.Name, RegisterSubject, opts.URL)
	}
	if err != nil {
		return Grant{}, fmt.Errorf("program %s: register: %w", req.Manifest.Name, err)
	}
	return decodeReply(req.Manifest.Name, msg.Data)
}

func decodeReply(name string, data []byte) (Grant, error) {
	var reply RegisterReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return Grant{}, fmt.Errorf("program %s: registration reply: %w", name, err)
	}
	switch {
	case reply.Refused != nil:
		return Grant{}, reply.Refused
	case reply.Error != "":
		return Grant{}, fmt.Errorf("program %s: registration failed: %s", name, reply.Error)
	case reply.Grant == nil:
		return Grant{}, fmt.Errorf("program %s: registration reply has no grant", name)
	}
	return *reply.Grant, nil
}

func thisInstance() Instance {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return Instance{Hostname: hostname, Pid: os.Getpid()}
}
