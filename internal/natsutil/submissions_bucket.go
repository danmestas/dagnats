// internal/natsutil/submissions_bucket.go
// The submissions bucket (ADR-027): one key per input submitted to a
// running agent step, {run}.{step}.{request_id}, created with KV
// Create so a repeated request_id is answered with the existing
// submission for as long as the key lives. Provisioned by SetupAll
// with the other buckets, at the cluster's replica count.
package natsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// SubmissionsBucket is the bucket's name.
const SubmissionsBucket = "submissions"

// submissionsTTL is how long a submission's key lives: the idempotency
// window of a request_id, and the longest a settled status is readable.
// Longer than any stream dedup window (seconds to minutes), which is
// why the bucket exists; shorter than history's 30 days because the
// journal, not the bucket, is the record of what was placed.
const submissionsTTL = 7 * 24 * time.Hour

// submissionValueSizeMax bounds one submission. Inputs are model input,
// so they are text-sized; the same 1 MiB the signal endpoint accepts.
const submissionValueSizeMax = 1 << 20

// fractionSubmissions is the bucket's share of the store budget, taken
// from the headroom under the 85% guard like AGENT_JOURNAL's was
// (conn.go's table), and small enough to leave headroom under it: half
// a percent of a 10 GiB store is 50 MiB, two hundred 256 KiB inputs
// or tens of thousands of ordinary ones within their TTL.
const fractionSubmissions = 0.005

// submissionsBucketConfig is the bucket definition. History 1: a
// submission's status is replaced in place and read by revision, and
// Create on a deleted (withdrawn) key is what lets a request_id be
// resubmitted after a withdrawal.
func submissionsBucketConfig(replicas int, maxStoreBytes int64) jetstream.KeyValueConfig {
	if replicas != 1 && replicas != 3 && replicas != 5 {
		panic(fmt.Sprintf(
			"submissionsBucketConfig: replicas must be 1, 3, or 5; got %d", replicas,
		))
	}
	cfg := jetstream.KeyValueConfig{
		Bucket:       SubmissionsBucket,
		History:      1,
		TTL:          submissionsTTL,
		MaxValueSize: submissionValueSizeMax,
		MaxBytes:     proportionalMaxBytes(maxStoreBytes, fractionSubmissions),
		Replicas:     replicas,
	}
	if cfg.TTL < time.Minute {
		panic("submissionsBucketConfig: TTL must outlast any stream dedup window")
	}
	return cfg
}

// setupSubmissionsBucket provisions the bucket, sized against the same
// budget as the streams.
func setupSubmissionsBucket(
	ctx context.Context, js jetstream.JetStream, replicas int, maxStoreBytes int64,
) error {
	if ctx == nil {
		panic("setupSubmissionsBucket: ctx must not be nil")
	}
	if js == nil {
		panic("setupSubmissionsBucket: js must not be nil")
	}
	err := createKeyValueWithRetry(ctx, js, submissionsBucketConfig(replicas, maxStoreBytes))
	if err != nil {
		return fmt.Errorf("create kv bucket %s: %w", SubmissionsBucket, err)
	}
	return nil
}

// SubmissionKey is the one place the key shape is assembled:
// {run}.{step}.{request_id}, with the step sanitised the way the
// journal subject sanitises it. requestID must already be validated
// (SubjectToken-safe); the key is also a NATS subject token sequence.
func SubmissionKey(runID, stepID, requestID string) string {
	if runID == "" || stepID == "" || requestID == "" {
		panic("SubmissionKey: run, step and request id must not be empty")
	}
	if SubjectToken(requestID) != requestID {
		panic("SubmissionKey: requestID must be a validated subject token")
	}
	return runID + "." + SubjectToken(stepID) + "." + requestID
}

// SubmissionKeyPrefix is the key prefix of one step's submissions,
// without the trailing dot.
func SubmissionKeyPrefix(runID, stepID string) string {
	if runID == "" || stepID == "" {
		panic("SubmissionKeyPrefix: run and step must not be empty")
	}
	if SubjectToken(stepID) == "" {
		panic("SubmissionKeyPrefix: step must sanitise to a non-empty token")
	}
	return runID + "." + SubjectToken(stepID)
}
