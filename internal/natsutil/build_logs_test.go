// internal/natsutil/build_logs_test.go
// Tests for the BUILD_LOGS hot lane (#624): TTL env-var resolution and
// the provisioned stream's shape. Methodology: resolveBuildLogsTTL is a
// pure table-driven unit test (no NATS); SetupBuildLogsStream is an
// integration test against an embedded NATS server. Bounded 5s timeouts.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestResolveBuildLogsTTL(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{"unset uses default", "", buildLogsTTLDefault, false},
		{"valid duration", "48h", 48 * time.Hour, false},
		{"min boundary accepted", "1h", 1 * time.Hour, false},
		{"max boundary accepted", "8760h", 8760 * time.Hour, false},
		{"below min rejected", "30m", 0, true},
		{"above max rejected", "9000h", 0, true},
		{"garbage rejected", "not-a-duration", 0, true},
		{"negative rejected", "-1h", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveBuildLogsTTL(tc.val)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveBuildLogsTTL(%q) = nil error, want error", tc.val)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBuildLogsTTL(%q) unexpected error: %v", tc.val, err)
			}
			if got != tc.want {
				t.Fatalf("resolveBuildLogsTTL(%q) = %v, want %v", tc.val, got, tc.want)
			}
		})
	}
}

func TestSetupBuildLogsStreamShape(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	const maxStoreBytes = 1 << 30 // 1 GiB
	ttl := 24 * time.Hour
	if err := SetupBuildLogsStream(js, maxStoreBytes, ttl, 1); err != nil {
		t.Fatalf("SetupBuildLogsStream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "BUILD_LOGS")
	if err != nil {
		t.Fatalf("Stream(BUILD_LOGS): %v", err)
	}
	cfg := stream.CachedInfo().Config

	// Positive: subjects, retention policy, storage, TTL, dedup window.
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "logs.>" {
		t.Fatalf("Subjects = %v, want [logs.>]", cfg.Subjects)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Fatalf("Retention = %v, want LimitsPolicy", cfg.Retention)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("Storage = %v, want FileStorage", cfg.Storage)
	}
	if cfg.MaxAge != ttl {
		t.Fatalf("MaxAge = %v, want %v", cfg.MaxAge, ttl)
	}
	if cfg.MaxBytes <= 0 {
		t.Fatalf("MaxBytes = %d, want a proportional positive ceiling", cfg.MaxBytes)
	}
	if cfg.Duplicates < 2*time.Minute {
		t.Fatalf("Duplicates = %v, want >= 2m", cfg.Duplicates)
	}

	// Negative: AllowDirect is NOT set for BUILD_LOGS — this stream is
	// read via ordered consumers (internal/api's tail API), not
	// direct-get, so there is no reason to enable it here.
	if cfg.AllowDirect {
		t.Fatalf("AllowDirect = true, want false (BUILD_LOGS is consumer-read only)")
	}
}

// TestLogSubjectAndLogMsgID pins the single builder every BUILD_LOGS
// producer/consumer (worker, bridge, internal/api, examples/log-offload)
// must route through — a test of these two functions is a test of every
// site that formerly grew its own fmt.Sprintf (#624 review round 4).
func TestLogSubjectAndLogMsgID(t *testing.T) {
	// Positive: exact shapes, sanitized stepID.
	subject := LogSubject("run-1", "build step!", 2, 3)
	if want := "logs.run-1.build_step_.2.3"; subject != want {
		t.Fatalf("LogSubject = %q, want %q", subject, want)
	}
	msgID := LogMsgID("run-1", "build step!", 2, 3, 7)
	if want := "log-run-1-build_step_-2-3-7"; msgID != want {
		t.Fatalf("LogMsgID = %q, want %q", msgID, want)
	}

	// Negative: different iterations must not collide on subject or
	// Msg-Id even with everything else held constant — this is exactly
	// the round-4 regression (retry left iteration at the Go zero
	// value instead of routing through this builder).
	if LogSubject("run-1", "build", 2, 0) == LogSubject("run-1", "build", 2, 3) {
		t.Fatalf("LogSubject must vary with iteration")
	}
	if LogMsgID("run-1", "build", 2, 0, 0) == LogMsgID("run-1", "build", 2, 3, 0) {
		t.Fatalf("LogMsgID must vary with iteration")
	}
}
