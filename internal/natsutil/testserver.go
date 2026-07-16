package natsutil

import (
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// storeDirRemoveAttemptsMax bounds removeDirWithRetry. The embedded
// server's late filestore flush is a single trailing write, so a couple
// of attempts always win in practice; the extra headroom costs nothing
// on the happy path (first attempt returns immediately).
const storeDirRemoveAttemptsMax = 20

// storeDirRemoveRetryDelay spaces removeDirWithRetry's attempts. Short
// enough that a real leak fails teardown quickly, long enough to let an
// in-flight filestore write land before the next RemoveAll walk.
const storeDirRemoveRetryDelay = 25 * time.Millisecond

// StartTestServer starts an embedded NATS server with JetStream enabled
// and returns both the server and a connected client. The server and
// connection are shut down via t.Cleanup when the test ends, and the
// JetStream store directory is removed by removeDirWithRetry AFTER the
// server has fully stopped. Accepts testing.TB so the same helper works
// for *testing.T and *testing.B.
//
// The store directory is created with os.MkdirTemp — deliberately NOT
// t.TempDir() — because testing's TempDir cleanup runs an unconditional
// os.RemoveAll that fails the test on any error. The embedded server's
// filestore and consumer-state writers can flush to disk after
// Server.Shutdown() and even after WaitForShutdown() returns, so that
// RemoveAll intermittently raced a late write and failed unrelated
// tests with "TempDir RemoveAll cleanup: unlinkat ... directory not
// empty". Owning the removal lets a late write be absorbed by a bounded
// retry instead of reddening the suite.
func StartTestServer(t testing.TB) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	storeDir, err := os.MkdirTemp("", "dagnats-nats-store-*")
	if err != nil {
		t.Fatalf("failed to create NATS store dir: %v", err)
	}
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		os.RemoveAll(storeDir)
		t.Fatalf("failed to create test NATS server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5_000_000_000) {
		os.RemoveAll(storeDir)
		t.Fatal("NATS server not ready after 5s")
	}
	// One cleanup owns the full teardown order: stop the server, wait
	// for its goroutines to exit, THEN remove the store dir. Registered
	// before the nc.Close() cleanup below so LIFO ordering runs
	// nc.Close() first (clients gone before the server stops).
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
		if err := removeDirWithRetry(storeDir); err != nil {
			// Best-effort: a leaked temp dir is harmless (the OS reaps
			// its tmp tree) and must not fail an unrelated test. Log so
			// a genuine leak stays visible.
			t.Logf("NATS store dir cleanup left %s: %v", storeDir, err)
		}
	})
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to test NATS server: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return ns, nc
}

// removeDirWithRetry removes dir and everything under it, retrying up to
// storeDirRemoveAttemptsMax times when a removal fails. A failure here is
// almost always ENOTEMPTY from a file the embedded server's filestore
// flushed into dir after the previous RemoveAll had already listed it;
// the next attempt picks that file up. Returns nil once dir is gone, or
// the final attempt's error if it never clears within the retry budget.
func removeDirWithRetry(dir string) error {
	if dir == "" {
		panic("removeDirWithRetry: dir must not be empty")
	}
	var err error
	for attempt := 0; attempt < storeDirRemoveAttemptsMax; attempt++ {
		err = os.RemoveAll(dir)
		if err == nil {
			if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
				return nil
			}
		}
		time.Sleep(storeDirRemoveRetryDelay)
	}
	return err
}
