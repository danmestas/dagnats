// examples/program-min/main.go
// The smallest dagnats program: one task type, served by the worker
// program.Attach starts, over a NATS user whose permissions come from
// the manifest and whose reply inbox is confined to "_INBOX.min".
//
// Until self-registration lands (program.Run), the operator binds the
// program's key by hand: -print-permissions prints the public key and
// the permission set to configure on the server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/danmestas/dagnats/program"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// minProgram upper-cases its input.
type minProgram struct{}

func (minProgram) Manifest() program.Manifest {
	return program.Manifest{
		Name:      "min",
		Version:   "0.1.0",
		Dagnats:   ">=0.0.35",
		TaskTypes: []string{"min.upper"},
		Workflows: []string{"min.upper"},
	}
}

func (minProgram) Start(_ context.Context, h program.Host) error {
	if h.Handle == nil {
		panic("minProgram.Start: Host.Handle must be set")
	}
	h.Handle("min.upper", worker.Typed(
		func(_ worker.TaskContext, in string) (string, error) {
			return strings.ToUpper(in), nil
		}))
	h.Logger.Info("serving", "task_type", "min.upper")
	return nil
}

func (minProgram) Stop(context.Context) error { return nil }

// grant is what dagnats' attach handler will return; min declares no
// streams or buckets, so it needs no budget.
var grant = program.Grant{Replicas: 1, ServerVersion: "dev"}

// serve dials url as key with the confined inbox and attaches min.
func serve(ctx context.Context, url string, key program.Key) (func(context.Context) error, error) {
	name := minProgram{}.Manifest().Name
	nc, err := nats.Connect(url, key.ConnectOption(),
		nats.CustomInboxPrefix(program.InboxPrefix(name)))
	if err != nil {
		return nil, err
	}
	stop, err := program.Attach(ctx, nc, minProgram{}, grant)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return func(ctx context.Context) error {
		defer nc.Close()
		return stop(ctx)
	}, nil
}

func main() {
	printPermissions := flag.Bool("print-permissions", false,
		"print the public key and NATS permissions to bind, then exit")
	seedPath := flag.String("seed", "min.seed", "NKey seed file; created on first run")
	flag.Parse()
	key, err := program.LoadOrCreateKey(*seedPath)
	if err != nil {
		fail(err)
	}
	if *printPermissions {
		out, err := json.MarshalIndent(map[string]any{
			"public_key":  key.PublicKey(),
			"permissions": program.PermissionsFor(minProgram{}.Manifest()),
		}, "", "  ")
		if err != nil {
			fail(err)
		}
		fmt.Println(string(out))
		return
	}
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	stop, err := serve(ctx, url, key)
	if err != nil {
		fail(err)
	}
	<-ctx.Done()
	if err := stop(context.Background()); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "program-min:", err)
	os.Exit(1)
}
