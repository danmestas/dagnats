// natsutil/grow_storage_test.go
// Tests for a budget raise the server cannot reserve yet. On a cluster,
// raising max_store_bytes on one node before its peers makes every
// existing stream's larger ceiling fail with JetStream 10047; the node
// must keep running on the streams as they are, not exit. A stream that
// does not exist yet still fails. Methodology: one embedded server whose
// JetStream store is the test budget stands in for peers that cannot
// reserve more; SetupAll runs at that budget, then at three times it.
// Bounded 5-second timeouts on every lookup.
package natsutil

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/nats-io/nats.go/jetstream"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

func TestSetupAll_UnreservableGrowthKeepsExistingStreams(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc, WithStoreBudget(natstest.MaxStoreBytes)); err != nil {
		t.Fatalf("SetupAll at the server's budget: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	maxBytes := func(name string) int64 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stream, err := js.Stream(ctx, name)
		if err != nil {
			t.Fatalf("stream %s: %v", name, err)
		}
		return stream.CachedInfo().Config.MaxBytes
	}
	history := maxBytes("WORKFLOW_HISTORY")
	logged := captureLog(t)
	if err := SetupAll(nc, WithStoreBudget(3*natstest.MaxStoreBytes)); err != nil {
		t.Fatalf("SetupAll with a budget the server cannot reserve yet: %v", err)
	}
	// Negative: nothing grew, since the server could not reserve it.
	if got := maxBytes("WORKFLOW_HISTORY"); got != history {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, want unchanged %d", got, history)
	}
	// Positive: the operator is told which stream waits, and why.
	for _, want := range []string{"stream=WORKFLOW_HISTORY", "wanted_max_bytes", "level=WARN"} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("log %q does not mention %q", logged.String(), want)
		}
	}
}

func TestCreateStream_UnreservableNewStreamFails(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = createStreamWithRetry(ctx, js, jetstream.StreamConfig{
		Name: "TOO_BIG", Subjects: []string{"too.big.>"}, MaxBytes: 3 * natstest.MaxStoreBytes,
	})
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jsErrCodeStorageResourcesExceeded {
		t.Fatalf("create = %v, want JetStream %d", err, jsErrCodeStorageResourcesExceeded)
	}
	if _, err := js.Stream(ctx, "TOO_BIG"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("stream TOO_BIG: %v, want not found", err)
	}
}

// TestCreateKeyValue_UnreservableGrowthKeepsBucket: a bucket's ceiling
// (the submissions bucket's) takes the same path as a stream's.
func TestCreateKeyValue_UnreservableGrowthKeepsBucket(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := jetstream.KeyValueConfig{Bucket: "grow", MaxBytes: 1 << 20, Replicas: 1}
	if err := createKeyValueWithRetry(ctx, js, cfg); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	logged := captureLog(t)
	cfg.MaxBytes = 3 * natstest.MaxStoreBytes
	if err := createKeyValueWithRetry(ctx, js, cfg); err != nil {
		t.Fatalf("grow bucket past the store: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_grow")
	if err != nil {
		t.Fatalf("stream KV_grow: %v", err)
	}
	// Negative: the ceiling the server could not reserve was not applied.
	if got := stream.CachedInfo().Config.MaxBytes; got != 1<<20 {
		t.Fatalf("KV_grow MaxBytes = %d, want unchanged %d", got, 1<<20)
	}
	// Positive: and the operator is told.
	if !strings.Contains(logged.String(), "stream=KV_grow") {
		t.Fatalf("log %q does not name KV_grow", logged.String())
	}
}
