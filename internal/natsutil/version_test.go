// natsutil/version_test.go
// Tests for server version gating utility.
// Methodology: uses embedded NATS server to verify version comparison logic.
// Positive: current server version exceeds a low minimum.
// Negative: impossibly high version requirement fails.
package natsutil

import "testing"

func TestRequireServerVersion_OK(t *testing.T) {
	_, nc := StartTestServer(t)

	// Positive: embedded server is v2.12.x, so 2.12.0 should pass.
	err := RequireServerVersion(nc, "2.12.0")
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	// Negative: future version should fail.
	err = RequireServerVersion(nc, "99.0.0")
	if err == nil {
		t.Fatal("expected error for impossibly high version")
	}
}

func TestParseVersion(t *testing.T) {
	// Positive: well-formed version parses correctly.
	major, minor, patch, err := parseVersion("2.12.6")
	if err != nil {
		t.Fatalf("parseVersion failed: %v", err)
	}
	if major != 2 || minor != 12 || patch != 6 {
		t.Fatalf("got %d.%d.%d, want 2.12.6", major, minor, patch)
	}

	// Negative: malformed version returns error.
	_, _, _, err = parseVersion("bad")
	if err == nil {
		t.Fatal("expected error for malformed version")
	}
}

func TestVersionAtLeast(t *testing.T) {
	// Positive: equal versions pass.
	if !versionAtLeast(2, 12, 6, 2, 12, 6) {
		t.Fatal("equal versions should pass")
	}

	// Negative: lower major fails.
	if versionAtLeast(1, 99, 99, 2, 0, 0) {
		t.Fatal("lower major should fail")
	}
}
