// sdk/ts/test/devserver/main.go
// An in-process dagnats dev-mode server for the TypeScript client's
// integration tests, started the way sdk/httpclient's tests start one:
// ephemeral ports, a temp data dir, no bridge token. It prints one JSON
// line, {"url": "http://127.0.0.1:PORT"}, once the HTTP listener is up,
// then runs until stdin closes or it is signalled, so a test process that
// dies cannot leave the server behind.
//
// The line also names a second bridge on the same NATS, "second_url",
// which is also where "POST /_test/publish" puts a hand-made task on the
// queue. Together they stage what one bridge cannot: a task redelivered to
// a second poller, which fences the first (bridge/journal_test.go does the
// same with two bridges in one process).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danmestas/dagnats/bridge"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// bindWaitMax bounds the wait for the HTTP listener.
const bindWaitMax = 15 * time.Second

func main() {
	dir, err := os.MkdirTemp("", "dagnats-sdkts-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "tempdir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	cfg := server.DefaultConfig()
	cfg.DataDir = dir
	cfg.NATSPort = -1
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.MaxStoreBytes = 1 << 30
	srv := server.New(cfg)

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run() }()

	addr := waitForAddr(srv)
	if addr == "" {
		fmt.Fprintln(os.Stderr, "server did not bind an HTTP address")
		srv.Stop()
		os.Exit(1)
	}
	secondURL, closeSecond, err := startSecondBridge(srv.NATSURL())
	if err != nil {
		fmt.Fprintln(os.Stderr, "second bridge:", err)
		srv.Stop()
		os.Exit(1)
	}
	defer closeSecond()
	out, err := json.Marshal(map[string]string{
		"url": "http://" + addr, "second_url": secondURL,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))

	stop := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(stop) }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-stop:
	case <-sig:
	case err := <-runErr:
		fmt.Fprintln(os.Stderr, "server exited:", err)
		os.Exit(1)
	}
	srv.Stop()
	<-runErr
}

func waitForAddr(srv *server.Server) string {
	deadline := time.Now().Add(bindWaitMax)
	for time.Now().Before(deadline) {
		if a := srv.HTTPAddr(); a != "" {
			return a
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ""
}

// startSecondBridge serves a second bridge on its own listener, sharing
// the server's NATS, plus the test-only publish endpoint.
func startSecondBridge(natsURL string) (string, func(), error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return "", nil, fmt.Errorf("connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return "", nil, fmt.Errorf("jetstream: %w", err)
	}
	b := bridge.NewBridge(natsutil.NewTracingPublisher(nc, js))
	mux := http.NewServeMux()
	mux.Handle("/", b.Handler())
	mux.HandleFunc("POST /_test/publish", func(w http.ResponseWriter, r *http.Request) {
		publishTask(js, w, r)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		nc.Close()
		return "", nil, fmt.Errorf("listen: %w", err)
	}
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.Serve(ln) }()
	closeFn := func() {
		_ = hs.Close()
		nc.Close()
	}
	return "http://" + ln.Addr().String(), closeFn, nil
}

// publishTask puts the posted TaskPayload on the queue of its task type,
// given as ?type=. A second publish of the same payload is a dispatch
// resend: a fresh message with the same identity.
func publishTask(js jetstream.JetStream, w http.ResponseWriter, r *http.Request) {
	var payload protocol.TaskPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	taskType := r.URL.Query().Get("type")
	if taskType == "" || payload.RunID == "" {
		http.Error(w, "type and run_id are required", http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, "task."+taskType+"."+payload.RunID, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
