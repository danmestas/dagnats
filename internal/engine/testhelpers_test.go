// internal/engine/testhelpers_test.go
// Test helpers for the engine package. Each fails the test on
// any error so swallowed setup-error bugs don't leave the test
// running against a broken fixture.
package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// mustMarshal calls json.Marshal and fails the test on error.
// Use only for fixture setup in tests.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	if v == nil {
		panic("mustMarshal: v must not be nil")
	}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return data
}

// mustPublish publishes via the legacy NATS JetStream client and
// fails the test on error.
func mustPublish(
	t *testing.T, js nats.JetStreamContext,
	subject string, data []byte, opts ...nats.PubOpt,
) {
	t.Helper()
	if subject == "" {
		panic("mustPublish: subject must not be empty")
	}
	if _, err := js.Publish(subject, data, opts...); err != nil {
		t.Fatalf("publish %q: %v", subject, err)
	}
}

// mustPublishMsg publishes via the legacy NATS JetStream client
// using a *nats.Msg (preserves headers).
func mustPublishMsg(
	t *testing.T, js nats.JetStreamContext, msg *nats.Msg,
) {
	t.Helper()
	if msg == nil {
		panic("mustPublishMsg: msg must not be nil")
	}
	if msg.Subject == "" {
		panic("mustPublishMsg: msg.Subject must not be empty")
	}
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publishMsg %q: %v", msg.Subject, err)
	}
}

// mustPut writes to a NATS KV bucket and fails the test on error.
func mustPut(
	t *testing.T, kv nats.KeyValue, key string, value []byte,
) {
	t.Helper()
	if key == "" {
		panic("mustPut: key must not be empty")
	}
	if _, err := kv.Put(key, value); err != nil {
		t.Fatalf("kv.Put %q: %v", key, err)
	}
}

// mustPutJS writes to a jetstream-package KV bucket. The new
// jetstream.KeyValue interface differs from the legacy
// nats.KeyValue.
func mustPutJS(
	t *testing.T, ctx context.Context, kv jetstream.KeyValue,
	key string, value []byte,
) {
	t.Helper()
	if key == "" {
		panic("mustPutJS: key must not be empty")
	}
	if _, err := kv.Put(ctx, key, value); err != nil {
		t.Fatalf("jskv.Put %q: %v", key, err)
	}
}
