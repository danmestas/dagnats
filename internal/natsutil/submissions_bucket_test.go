// internal/natsutil/submissions_bucket_test.go
// The submissions bucket (ADR-027): provisioned by SetupAll with the
// cluster's replica count, a TTL, and a byte ceiling that is an
// explicit share of the store budget, taken from headroom.
//
// Methodology: one embedded server per test; the bucket is read back
// through its backing stream so the live config is what is asserted.
// Each test checks a positive and a negative property.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSubmissionsBucket_ProvisionedWithTTLAndCeiling(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc, WithStoreBudget(testServerMaxStoreBytes)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "KV_"+SubmissionsBucket)
	if err != nil {
		t.Fatalf("submissions bucket not provisioned: %v", err)
	}
	cfg := stream.CachedInfo().Config
	if cfg.MaxAge != submissionsTTL {
		t.Errorf("TTL = %s, want %s", cfg.MaxAge, submissionsTTL)
	}
	want := proportionalMaxBytes(testServerMaxStoreBytes, fractionSubmissions)
	if cfg.MaxBytes != want {
		t.Errorf("MaxBytes = %d, want %d (%.2f of the budget)",
			cfg.MaxBytes, want, fractionSubmissions)
	}
	if cfg.MaxBytes <= 0 || cfg.MaxBytes >= testServerMaxStoreBytes {
		t.Errorf("MaxBytes = %d is not a strict share of the budget", cfg.MaxBytes)
	}
}

func TestSubmissionsBucketConfig_RejectsBadReplicas(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("replicas 2 must panic")
		}
	}()
	submissionsBucketConfig(2, testServerMaxStoreBytes)
}
