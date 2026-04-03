// natsutil/parallel_test.go
// Tests for ParallelGet: concurrent KV fetching with bounded parallelism.
// Methodology: use real embedded NATS KV, populate entries, verify all
// fetched correctly. Deleted keys are silently skipped.
package natsutil

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestParallelGetAllKeys(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	kv, err := js.CreateKeyValue(
		&nats.KeyValueConfig{Bucket: "test_parallel"},
	)
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	// Populate 20 entries
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		_, err := kv.Put(
			key, []byte(fmt.Sprintf("val-%02d", i)),
		)
		if err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
	}

	entries, err := ParallelGet(kv, keys, 8)
	if err != nil {
		t.Fatalf("ParallelGet: %v", err)
	}

	// Positive: all 20 entries returned
	if len(entries) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(entries))
	}

	// Negative: values match keys
	for i, entry := range entries {
		expected := fmt.Sprintf("val-%02d", i)
		if string(entry.Value()) != expected {
			t.Errorf("entry %d: got %q, want %q",
				i, string(entry.Value()), expected)
		}
	}
}

func TestParallelGetSkipsDeletedKeys(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	kv, err := js.CreateKeyValue(
		&nats.KeyValueConfig{Bucket: "test_deleted"},
	)
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	kv.Put("exists", []byte("data"))
	kv.Put("gone", []byte("data"))
	kv.Delete("gone")

	entries, err := ParallelGet(
		kv, []string{"exists", "gone"}, 8,
	)
	if err != nil {
		t.Fatalf("ParallelGet: %v", err)
	}

	// Positive: only the existing key is returned
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Negative: deleted key was silently dropped
	if string(entries[0].Value()) != "data" {
		t.Errorf("got %q, want %q",
			string(entries[0].Value()), "data")
	}
}

func TestParallelGetEmptyKeys(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	kv, err := js.CreateKeyValue(
		&nats.KeyValueConfig{Bucket: "test_empty"},
	)
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	entries, err := ParallelGet(kv, nil, 8)
	if err != nil {
		t.Fatalf("ParallelGet: %v", err)
	}

	// Positive: returns empty slice, not nil
	if entries == nil {
		t.Fatal("expected non-nil empty slice")
	}
	// Negative: no entries
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}
